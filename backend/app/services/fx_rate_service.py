import logging
import uuid
from datetime import date
from decimal import Decimal
from typing import Optional

from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.config import get_settings
from app.models.fx_rate import FxRate
from app.models.user import User
from app.providers.bcb_ptax import BcbPtaxProvider

logger = logging.getLogger(__name__)

_provider = BcbPtaxProvider()


import time as _fx_time

# Cache em processo para as consultas quentes de cambio. `get_rate(USD, BRL,
# hoje)` levava ~1,4 s quando errava: tentava a leitura por data exata no
# banco e, falhando, disparava um sync PTAX pela rede. A cotacao nao muda
# depois de publicada para o dia, entao 10 minutos e folgado e tira o fetch
# sincrono do caminho quente de /api/assets e /api/asset-groups.
#
# Indexado por (origem, destino, data ISO) para dias diferentes nao
# colidirem. So guarda resultado nao-nulo: um None significa "ainda nao
# existe cotacao", e cachear isso seguraria o valor real quando ele
# chegasse. Por isso tambem nao entra `allow_fetch` na chave -- uma taxa ja
# resolvida vale para qualquer chamador.
_FX_RATE_CACHE: dict[tuple[str, str, str], tuple[float, Decimal]] = {}
_FX_RATE_CACHE_TTL_S = 600.0


def invalidate_fx_cache() -> None:
    """Descarta todo o cache. Chamado depois de um sync_rates bem-sucedido
    para a proxima leitura pegar a linha nova do banco."""
    _FX_RATE_CACHE.clear()


async def sync_rates(
    session: AsyncSession, target_date: Optional[date] = None
) -> int:
    """Fetch rates from the provider for the given date and upsert into fx_rates.

    Only saves rates for currencies in `supported_currencies`.
    Idempotent — existing rates for the same date are updated.
    Returns the number of rates synced.
    """
    requested_target = target_date or date.today()
    # Providers cannot return a historical rate for a date that has not
    # happened yet. Treat a future request as a request for today's latest
    # published rate and store it under today's date.
    target = min(requested_target, date.today())
    supported = set(get_settings().supported_currencies.split(","))

    if target == date.today():
        # Nunca gravamos uma linha com a data de hoje: o PTAX de hoje ainda
        # nao fechou. Volta ate 7 dias uteis e grava contra a data real da
        # cotacao. Sem isso, cada chamada sob demanda (painel, asset_service)
        # inseriria uma linha de hoje com o boletim parcial do momento.
        from datetime import timedelta as _td
        today_local = date.today()
        target = None
        rates = {}
        for delta in range(1, 8):
            d = today_local - _td(days=delta)
            r = await _provider.fetch_historical(d)
            if r:
                target = d
                rates = r
                break
        if target is None:
            logger.warning("No PTAX rate found in last 7 business days")
            return 0
    else:
        rates = await _provider.fetch_historical(target)

    count = 0
    for currency_code, rate in rates.items():
        if currency_code not in supported:
            continue
        stmt = pg_insert(FxRate).values(
            base_currency="USD",
            quote_currency=currency_code,
            date=target,
            rate=rate,
            source=_provider.name,
        )
        stmt = stmt.on_conflict_do_update(
            constraint="uq_fx_rate_base_quote_date",
            set_={"rate": rate, "source": _provider.name},
        )
        await session.execute(stmt)
        count += 1

    await session.commit()
    # Sem isto o cache em processo seguraria a cotacao antiga por ate 10
    # minutos, e o botao de atualizar do painel nao surtiria efeito. O
    # comentario do cache sempre prometeu esta chamada; ela nunca existiu.
    invalidate_fx_cache()
    logger.info("Synced %d FX rates for %s", count, target)
    return count


async def _resolve_rate(
    session: AsyncSession,
    from_currency: str,
    to_currency: str,
    target_date: Optional[date] = None,
    *,
    allow_fetch: bool = True,
) -> Optional[Decimal]:
    """Resolve the true FX rate, or None when no rate can be found.

    Uses cross-rate through USD: rate = usd_to_target / usd_to_source.
    Priority: exact date → on-demand fetch → closest available.
    Returns None (not a fake 1:1) when no rate is available, so callers that
    persist a conversion can honestly leave it NULL instead of storing a wrong
    amount (issue #353). Pass ``allow_fetch=False`` to skip the on-demand
    provider call and rely only on already-stored rates.
    """
    if from_currency == to_currency:
        return Decimal("1")

    requested_target = target_date or date.today()
    cache_key = (from_currency, to_currency, requested_target.isoformat())
    entry = _FX_RATE_CACHE.get(cache_key)
    if entry is not None:
        exp, cached = entry
        if _fx_time.monotonic() < exp:
            return cached
        _FX_RATE_CACHE.pop(cache_key, None)

    # A future transaction must use the latest real rate, never query a
    # not-yet-existing historical snapshot. Looking up against today also
    # lets the closest-rate fallback select the last cached business day.
    target = min(requested_target, date.today())

    # Step 1: Try exact date
    usd_to_source = await _get_exact_date_rate(session, from_currency, target)
    usd_to_target = await _get_exact_date_rate(session, to_currency, target)

    # Step 2: If missing, fetch from provider for exact date
    if allow_fetch and (usd_to_source is None or usd_to_target is None):
        try:
            synced = await sync_rates(session, target)
            if synced > 0:
                logger.info("On-demand sync fetched %d rates for %s", synced, target)
                if usd_to_source is None:
                    usd_to_source = await _get_exact_date_rate(session, from_currency, target)
                if usd_to_target is None:
                    usd_to_target = await _get_exact_date_rate(session, to_currency, target)
        except Exception:
            logger.warning("On-demand FX rate sync failed for %s", target, exc_info=True)

    # Step 3: Fall back to closest available date
    if usd_to_source is None:
        usd_to_source = await _get_closest_rate(session, from_currency, target)
    if usd_to_target is None:
        usd_to_target = await _get_closest_rate(session, to_currency, target)

    if from_currency == "USD":
        usd_to_source = Decimal("1")
    if to_currency == "USD":
        usd_to_target = Decimal("1")

    if usd_to_source is None or usd_to_target is None or usd_to_source == 0:
        return None

    resolved = usd_to_target / usd_to_source
    _FX_RATE_CACHE[cache_key] = (_fx_time.monotonic() + _FX_RATE_CACHE_TTL_S, resolved)
    return resolved


async def get_rate(
    session: AsyncSession,
    from_currency: str,
    to_currency: str,
    target_date: Optional[date] = None,
    *,
    allow_fetch: bool = True,
) -> Decimal:
    """Get FX rate from from_currency to to_currency.

    Uses cross-rate through USD. When no rate can be resolved, returns a 1:1
    fallback so live reads (balances, dashboards) still render a number. This
    fallback is NOT meant to be persisted — persisting paths use
    :func:`_resolve_rate` directly and leave the value NULL instead (issue #353).
    """
    rate = await _resolve_rate(
        session, from_currency, to_currency, target_date, allow_fetch=allow_fetch
    )
    if rate is None:
        logger.warning(
            "No FX rate found for %s -> %s on %s, returning 1",
            from_currency, to_currency, target_date or date.today(),
        )
        return Decimal("1")
    return rate


async def _get_exact_date_rate(session: AsyncSession, currency: str, target: date) -> Optional[Decimal]:
    """Get the rate for an exact date."""
    if currency == "USD":
        return Decimal("1")
    result = await session.scalar(
        select(FxRate.rate)
        .where(
            FxRate.base_currency == "USD",
            FxRate.quote_currency == currency,
            FxRate.date == target,
        )
    )
    return result


async def _get_closest_rate(session: AsyncSession, currency: str, target: date) -> Optional[Decimal]:
    """Get the closest available rate to a target date (preferring before, then after)."""
    if currency == "USD":
        return Decimal("1")
    # Try closest before or on target date
    result = await session.scalar(
        select(FxRate.rate)
        .where(
            FxRate.base_currency == "USD",
            FxRate.quote_currency == currency,
            FxRate.date <= target,
        )
        .order_by(desc(FxRate.date))
        .limit(1)
    )
    if result is not None:
        return result
    # Try closest after target date
    from sqlalchemy import asc
    result = await session.scalar(
        select(FxRate.rate)
        .where(
            FxRate.base_currency == "USD",
            FxRate.quote_currency == currency,
            FxRate.date > target,
        )
        .order_by(asc(FxRate.date))
        .limit(1)
    )
    return result


async def convert(
    session: AsyncSession,
    amount: Decimal,
    from_currency: str,
    to_currency: str,
    target_date: Optional[date] = None,
    *,
    allow_fetch: bool = True,
) -> tuple[Decimal, Decimal]:
    """Convert an amount from one currency to another.

    Returns (converted_amount, rate_used). Uses the 1:1 fallback from
    :func:`get_rate` when no rate is available, so this is for live reads, not
    for persisting a stored conversion.
    """
    if from_currency == to_currency:
        return amount, Decimal("1")

    rate = await get_rate(
        session, from_currency, to_currency, target_date, allow_fetch=allow_fetch
    )
    converted = amount * rate
    return converted.quantize(Decimal("0.01")), rate


async def stamp_primary_amount(
    session: AsyncSession,
    user_id: uuid.UUID,
    obj,
    amount_field: str = "amount",
    primary_field: str = "amount_primary",
    rate_field: str = "fx_rate_used",
    date_field: str = "date",
    currency_field: str = "currency",
    *,
    allow_fetch: bool = True,
) -> None:
    """Set obj's primary amount and fx_rate_used based on user's primary currency.

    Works for Transaction, RecurringTransaction, etc.

    When the object is in a foreign currency and no real FX rate is available,
    the fields are left untouched instead of persisting a fake 1:1 conversion
    (issue #353). A brand-new object therefore stays NULL (honest "not converted",
    reads fall back to the native amount via ``COALESCE(amount_primary, amount)``),
    while an already-stamped row keeps its current value — so re-stamping never
    pushes a visible transaction into limbo. The row heals on a later pass once a
    rate for its date lands. Pass ``allow_fetch=False`` to avoid the on-demand
    provider call (used by the healer to stay frugal).
    """
    user = await session.get(User, user_id)
    if not user:
        return

    primary_currency = user.primary_currency
    obj_currency = getattr(obj, currency_field, get_settings().default_currency)
    obj_amount = getattr(obj, amount_field, None)

    if obj_amount is None:
        return

    amount_dec = Decimal(str(obj_amount))

    # Genuine same-currency 1:1 — always safe to persist.
    if obj_currency == primary_currency:
        setattr(obj, primary_field, amount_dec.quantize(Decimal("0.01")))
        if hasattr(obj, rate_field):
            setattr(obj, rate_field, Decimal("1"))
        return

    obj_date = getattr(obj, date_field, None)
    rate = await _resolve_rate(
        session, obj_currency, primary_currency, obj_date, allow_fetch=allow_fetch
    )

    if rate is None:
        # No real rate available yet. Leave the fields untouched: a brand-new
        # object stays NULL (honest "not converted"), an existing row keeps its
        # current value. Either way we never persist a fake 1:1, and never push a
        # visible transaction into limbo. It heals on a later pass once a rate lands.
        return

    setattr(obj, primary_field, (amount_dec * rate).quantize(Decimal("0.01")))
    if hasattr(obj, rate_field):
        setattr(obj, rate_field, rate)
