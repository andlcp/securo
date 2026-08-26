import bisect
import logging
import uuid
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal
from typing import Any, Optional, cast

from fastapi import HTTPException, status
from sqlalchemy import select, func, desc
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.core.workspace_autostamp import resolve_workspace_id
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.asset import Asset
from app.models.asset_transaction import AssetTransaction
from app.models.asset_value import AssetValue
from app.models.user import User
from app.core.config import get_settings
from app.providers.market_price import (
    MarketPriceProvider,
    MarketPriceRateLimitedError,
    get_market_price_provider,
)
from app.schemas.asset import AssetCreate, AssetUpdate, AssetValueCreate, AssetRead, AssetValueRead
from app.services.portfolio_timeseries_service import invalidate_ts_cache
from app.services.fx_rate_service import convert, stamp_primary_amount

logger = logging.getLogger(__name__)

ValueRecord = tuple[date, Decimal, Optional[Decimal]]  # (date, amount, price_per_share)
TxRecord = tuple[date, str, Decimal, Optional[Decimal]]  # (date, kind, quantity, price_per_share)


def _next_due_date(last_date: date, frequency: str) -> date:
    """Calculate the next due date based on frequency."""
    if frequency == "daily":
        return last_date + timedelta(days=1)
    elif frequency == "weekly":
        return last_date + timedelta(weeks=1)
    elif frequency == "monthly":
        month = last_date.month + 1
        year = last_date.year
        if month > 12:
            month = 1
            year += 1
        day = min(last_date.day, 28)
        return date(year, month, day)
    elif frequency == "yearly":
        return date(last_date.year + 1, last_date.month, last_date.day)
    return last_date + timedelta(days=1)


def _compute_current_value(asset: Asset, latest_value: Optional[AssetValue]) -> Optional[float]:
    """Compute the current value of an asset from its latest AssetValue.
    Falls back to cost basis (purchase_price × units) if no AssetValue
    exists yet — this matches what the user actually invested at entry,
    not the per-unit price."""
    # Market-priced assets are authoritative on (units × last_price). The
    # AssetValue history exists for the chart, but the "live" number users
    # see should reflect the most recent quote even between scheduled syncs.
    if asset.valuation_method == "market_price":
        if asset.last_price is not None and asset.units is not None:
            return float(Decimal(str(asset.last_price)) * Decimal(str(asset.units)))
        if latest_value is not None:
            return float(latest_value.amount)
        return None
    if latest_value is None:
        # Cost-basis fallback. Previous version returned `purchase_price`
        # raw, which is *per-unit* — for any asset with units != 1 (crypto
        # fractions, fractional shares, RF positions where units carries
        # face value, etc.) this displayed wildly inflated current values.
        # E.g. Bitcoin at 0.074649 units × $49,051.94/unit reported a
        # current value of $49,051.94 (R$ 241k) when the real cost basis
        # was $3,661.68 (R$ 18k). Multiply through.
        if asset.purchase_price is not None:
            units = float(asset.units) if asset.units is not None else 1.0
            return float(asset.purchase_price) * units
        return None
    return float(latest_value.amount)


def _generate_growth_values(
    asset_id: uuid.UUID,
    base_amount: float,
    base_date: date,
    growth_type: str,
    growth_rate: float,
    growth_frequency: str,
    growth_start_date: Optional[date],
) -> list[AssetValue]:
    """Generate all AssetValue rows from base_date to today using the growth rule.
    When growth_start_date is set, growth iteration begins from that date — not
    from base_date — so the asset accrues no growth for the gap between
    purchase and the configured growth start."""
    today = date.today()
    if growth_start_date and today < growth_start_date:
        return []

    values: list[AssetValue] = []
    current_amount = base_amount
    # Match the frontend preview: `growth_start_date or base_date`. Otherwise
    # backfill applied growth periods between purchase_date and
    # growth_start_date that the form said wouldn't accrue, leaving the
    # list-page total exactly N growth periods higher than the edit
    # dialog's calculated value.
    current_date = growth_start_date if growth_start_date else base_date

    while True:
        next_due = _next_due_date(current_date, growth_frequency)
        if next_due > today:
            break
        if growth_type == "percentage":
            current_amount = current_amount * (1 + growth_rate / 100)
        elif growth_type == "absolute":
            current_amount = current_amount + growth_rate
        else:
            break
        values.append(AssetValue(
            asset_id=asset_id,
            amount=Decimal(str(round(current_amount, 6))),
            date=next_due,
            source="rule",
        ))
        current_date = next_due
        if len(values) >= 10000:
            break

    return values


def _tw_capital(
    flows: list[tuple[date, str, float]], as_of: date
) -> Optional[float]:
    """Time-weighted average capital deployed (Modified Dietz denominator).

    flows: (date, type, amount) where type in BUY/DEPOSIT (money in) or
    SELL/WITHDRAWAL (money out). Each flow is weighted by the fraction of
    the holding period it was actually deployed:

        w_i = days(flow_i → as_of) / days(first_flow → as_of)

    Why: dividing the P&L by the raw sum of buys treats a dollar that
    stayed 2 days the same as one that stayed 15 months. NVDA case: two
    lots bought (US$ 2,079), one flipped 2 days later near cost, the
    survivor doubled — profit/total-deployed said +51% while the user's
    honest experience was ~+100%. Weighting the flipped lot by its 2/470
    days keeps it from diluting the metric. Income (dividends/juros) is
    NOT a capital flow — it belongs in the gain, not the denominator.

    CLOSED positions: the caller must cap `as_of` at the close date
    (sell_date). With as_of = today, every calendar day after the close
    shrinks the denominator further while the realized gain stays fixed,
    so a position sold at +15% in 2022 would display +203% today and keep
    inflating daily — then snap discontinuously to +15% when tw crosses 0.
    Capping at the close makes every outflow weight 0 at period end, so
    tw == invested and Dietz degenerates to the honest money-on-money
    return over the actual holding period.

    Returns None when there are no inflows (caller falls back to the
    plain invested basis).
    """
    if not flows:
        return None
    inflow_dates = [f[0] for f in flows if f[1] in ("BUY", "DEPOSIT")]
    if not inflow_dates:
        return None
    # Window starts at the first capital IN. A stray SELL predating the
    # first BUY (ghost rows on reconciled/archived assets) must not
    # define the window start nor carry weight > 1 — clamp w to [0, 1].
    first = min(inflow_dates)
    total_days = max((as_of - first).days, 1)
    tw = 0.0
    for fdate, ftype, amount in flows:
        w = min(max((as_of - fdate).days, 0) / total_days, 1.0)
        if ftype in ("BUY", "DEPOSIT"):
            tw += amount * w
        else:  # SELL / WITHDRAWAL
            tw -= amount * w
    return tw


def _asset_to_read(
    asset: Asset,
    latest_value: Optional[AssetValue],
    value_count: int,
    total_returned_net: float = 0.0,
    invested_txs: Optional[float] = None,
    tw_capital: Optional[float] = None,
    transaction_count: int = 0,
) -> AssetRead:
    """Convert an Asset model + computed fields to AssetRead schema.

    `gain_loss` is the absolute P&L of the position. For straight buy-and-
    hold positions (most stocks) it's `current_value - invested`. For
    positions where capital is returned over time (loans with WITHDRAWAL,
    stocks with SELL parcials, FIIs / loans with INTEREST+DIVIDEND income)
    we add `total_returned_net` so the rent display reflects the user's
    actual money-on-money return rather than treating returned capital as
    "loss". Without this, a loan that already amortised R$ 28k of a R$
    113k principal would show -R$ 27k "loss" even though the user is
    receiving exactly what they expected (capital + interest).

    total_returned_net = Σ(WITHDRAWAL + SELL).value
                       + Σ(DIVIDEND + JCP + RENDIMENTO + INTEREST + RESGATE)
                         . (value − fees)
    Net of fees so NRA tax withheld on US dividends doesn't inflate the
    return; full value otherwise (BR fees are usually 0).
    """
    current_value = _compute_current_value(asset, latest_value)
    # Money-in basis. Prefer the transaction ledger (Σ BUY+DEPOSIT,
    # value+fees) over purchase_price × units: after a partial SELL,
    # purchase_price × units only covers the REMAINING shares while
    # total_returned_net includes the gross proceeds of the sold ones —
    # the sold principal masqueraded as pure profit. Seen on NVDA: two
    # lots bought (US$ 2,078.90), one sold two days later near cost;
    # the card showed +201% on a position that had gained ~50%.
    invested_total = None
    if invested_txs is not None and invested_txs > 0:
        invested_total = invested_txs
    elif asset.purchase_price is not None:
        units = float(asset.units) if asset.units is not None else 1.0
        invested_total = float(asset.purchase_price) * units
    gain_loss = None
    if current_value is not None and invested_total is not None:
        gain_loss = current_value + total_returned_net - invested_total

    # Rent %: money-weighted (Modified Dietz) — gain over the TIME-
    # WEIGHTED capital, so a dollar deployed for 2 days doesn't dilute
    # the return of a dollar deployed for 15 months. NVDA: profit /
    # total-deployed said +51% after a 2-day near-cost flip of half the
    # capital, while the surviving lot had doubled; Dietz gives ~+108%,
    # matching the investor's experience. Falls back to plain
    # gain/invested when there's no ledger (CAIXA, manual assets) or the
    # position was opened today (tw = 0). Single-buy positions are
    # unchanged by construction (tw == invested).
    rent_pct = None
    if gain_loss is not None:
        denom = None
        if tw_capital is not None and tw_capital > 0:
            denom = tw_capital
        elif invested_total is not None and invested_total > 0:
            denom = invested_total
        if denom:
            rent_pct = round(gain_loss / denom * 100, 4)

    # For ledger-backed holdings `purchase_price` caches the cost basis of the
    # held units, so it doubles as `total_invested`. `average_price != None`
    # is the signal that the holding is driven by the transactions ledger.
    # `total_invested` do upstream e o custo das cotas em carteira. La ele
    # sai de `purchase_price`, que naquele fork guarda o custo total; aqui
    # esse campo e o preco POR UNIDADE, entao o equivalente honesto e
    # average_price x units. `average_price != None` continua sendo o sinal
    # de que a posicao e derivada do historico.
    is_ledger = asset.average_price is not None
    total_invested = (
        float(asset.average_price) * float(asset.units)
        if is_ledger and asset.units is not None
        else None
    )

    return AssetRead(
        id=asset.id,
        user_id=asset.user_id,
        name=asset.name,
        type=asset.type,
        currency=asset.currency,
        units=float(asset.units) if asset.units is not None else None,
        valuation_method=asset.valuation_method,
        purchase_date=asset.purchase_date,
        purchase_price=float(asset.purchase_price) if asset.purchase_price is not None else None,
        sell_date=asset.sell_date,
        sell_price=float(asset.sell_price) if asset.sell_price is not None else None,
        growth_type=asset.growth_type,
        growth_rate=float(asset.growth_rate) if asset.growth_rate is not None else None,
        growth_frequency=asset.growth_frequency,
        growth_start_date=asset.growth_start_date,
        is_archived=asset.is_archived,
        position=asset.position,
        current_value=current_value,
        gain_loss=gain_loss,
        value_count=value_count,
        source=asset.source,
        connection_id=asset.connection_id,
        isin=asset.isin,
        maturity_date=asset.maturity_date,
        group_id=asset.group_id,
        ticker=asset.ticker,
        ticker_exchange=asset.ticker_exchange,
        last_price=float(asset.last_price) if asset.last_price is not None else None,
        last_price_at=asset.last_price_at,
        # Prefer the locally-cached icon over the external URL. The
        # frontend renders <img src=logo_url> directly, so swapping in
        # /api/assets/<id>/icon keeps the call site identical while
        # eliminating round-trips to gstatic per asset. See
        # asset_icon_service for the cache contract. We probe with
        # `logo_content_type` (cheap string column) rather than
        # `logo_data` (deferred bytes) so the bulk SELECT doesn't have
        # to pull every favicon blob into the ORM identity map.
        logo_url=(
            f"/api/assets/{asset.id}/icon"
            if asset.logo_content_type is not None
            else asset.logo_url
        ),
        asset_class=asset.asset_class,
        custodian=asset.custodian,
        rf_indexer=asset.rf_indexer,
        rf_rate_pct=float(asset.rf_rate_pct) if asset.rf_rate_pct is not None else None,
        rf_index_offset_pct=float(asset.rf_index_offset_pct) if asset.rf_index_offset_pct is not None else None,
        rf_on_curve=bool(asset.rf_on_curve),
        invested_total=round(invested_total, 2) if invested_total is not None else None,
        rent_pct=rent_pct,
        # Derivados do historico, do upstream. Trafegam na API mas nenhuma
        # pagina do fork os le -- a decisao foi nao expor preco medio nem
        # ganho realizado por ora.
        average_price=float(asset.average_price) if asset.average_price is not None else None,
        total_invested=total_invested,
        realized_gain=float(asset.realized_gain) if asset.realized_gain is not None else None,
        transaction_count=transaction_count,
    )


async def _get_latest_value(session: AsyncSession, asset_id: uuid.UUID) -> Optional[AssetValue]:
    """Get the most recent AssetValue for an asset."""
    result = await session.execute(
        select(AssetValue)
        .where(AssetValue.asset_id == asset_id)
        .order_by(desc(AssetValue.date), desc(AssetValue.id))
        .limit(1)
    )
    return result.scalar_one_or_none()


async def _get_value_as_of(
    session: AsyncSession, asset_id: uuid.UUID, as_of_date: date
) -> Optional[AssetValue]:
    """Get the most recent AssetValue for an asset on or before as_of_date."""
    result = await session.execute(
        select(AssetValue)
        .where(AssetValue.asset_id == asset_id, AssetValue.date <= as_of_date)
        .order_by(desc(AssetValue.date), desc(AssetValue.id))
        .limit(1)
    )
    return result.scalar_one_or_none()


def build_market_value_series(
    value_rows: list[ValueRecord],
    txs: list[TxRecord],
) -> list[tuple[date, float]]:
    """Rebuild a market-priced holding's value series from the ledger.

    value(date) = quantity_held_on(date) × price(date), where quantity is the
    cumulative buys − sells up to that date (from the ledger) and price is the
    most recent stored per-share price. This keeps the chart consistent with the
    ledger even when past trades are entered after the fact. Falls back to the
    trade's own price on dates that predate any recorded market price (backdated
    trades entered before price tracking began), and to the baked amount when
    neither a price nor a later market price exists.

    A point is emitted at every stored-value date *and* every trade date, so a
    quantity change shows up on the chart at the date it happened. Without the
    trade-date points, a holding whose stored prices only start recently (the
    common case — prices are recorded daily from when the holding was added)
    collapses every backdated trade onto a single early anchor and renders one
    long straight interpolation across the gap. `value_rows` must be sorted by
    date.

    A holding with no ledger at all (e.g. a pre-ledger "no cost" position that
    still has a stored quantity) keeps its baked amounts — replaying an empty
    ledger would wrongly zero it out.
    """
    if not txs:
        return [(d, float(amount)) for d, amount, _ in value_rows]

    # Net quantity change per trade date, plus a representative per-share price
    # (the day's last trade) used to value points that predate any market price.
    tx_delta: dict[date, Decimal] = {}
    tx_price: dict[date, Decimal] = {}
    for d, kind, q, p in sorted(txs, key=lambda t: t[0]):
        tx_delta[d] = tx_delta.get(d, Decimal("0")) + (q if kind == "buy" else -q)
        if p is not None:
            tx_price[d] = p

    # Stored value points by date (last write wins on duplicate dates).
    value_by_date: dict[date, tuple[Decimal, Optional[Decimal]]] = {
        d: (amount, price) for d, amount, price in value_rows
    }

    out: list[tuple[date, float]] = []
    qty = Decimal("0")
    last_price: Optional[Decimal] = None  # most recent known per-share price
    seen_market = False  # has a stored market price been reached yet?
    for d in sorted(set(value_by_date) | set(tx_delta)):
        qty += tx_delta.get(d, Decimal("0"))
        held = qty if qty > 0 else Decimal("0")

        amount, price = value_by_date.get(d, (0.0, None))
        if price is not None:
            last_price = price  # a recorded market price always wins
            seen_market = True
        elif not seen_market and d in tx_price:
            # Before any market price is recorded, value each trade at its own
            # price so backdated points aren't flattened onto a single anchor.
            # Once market prices begin they take over and carry forward.
            last_price = tx_price[d]

        if d in value_by_date and price is None:
            out.append((d, float(amount)))  # stored point with no per-share price
        elif last_price is not None:
            out.append((d, float(last_price * held)))
        else:
            out.append((d, float(amount)))
    return out


async def _load_asset_native_values(
    session: AsyncSession,
    assets: list[Asset],
    up_to_date: Optional[date] = None,
) -> dict[str, list[tuple[date, float]]]:
    """Bulk-load AssetValue rows for all assets in one query.

    Returns {aid: [(date, amount), ...]} sorted ascending by (date, id).
    When purchase_price and purchase_date are both set and purchase_date
    predates the first recorded value, it is prepended as the earliest anchor.
    """
    if not assets:
        return {}

    asset_ids = [a.id for a in assets]
    q = (
        select(AssetValue.asset_id, AssetValue.date, AssetValue.amount)
        .where(AssetValue.asset_id.in_(asset_ids))
        .order_by(AssetValue.asset_id, AssetValue.date, AssetValue.id)
    )
    if up_to_date is not None:
        q = q.where(AssetValue.date <= up_to_date)

    rows = (await session.execute(q)).all()

    values_map: dict[str, list[tuple[date, float]]] = {str(a.id): [] for a in assets}
    for aid, d, amt in rows:
        values_map[str(aid)].append((d, float(amt)))

    for asset in assets:
        aid = str(asset.id)
        vals = values_map[aid]
        if asset.purchase_price is not None and asset.purchase_date is not None:
            if not vals or asset.purchase_date < vals[0][0]:
                # purchase_price is *per-unit* (per the Asset model); the
                # earliest anchor needs the total cost basis. Without the
                # multiplier, fractional crypto + RF positions where units
                # carry face value or fractional shares (Bitcoin at 0.07
                # units × $49 k/unit) get prepended at per-unit price and
                # the trend chart spikes to a value matching only one unit.
                units_for_basis = float(asset.units) if asset.units is not None else 1.0
                cost_basis = float(asset.purchase_price) * units_for_basis
                vals.insert(0, (asset.purchase_date, cost_basis))

    return values_map


def _fill_forward_at(
    asset: Asset,
    sorted_vals: list[tuple[date, float]],
    as_of: date,
) -> Optional[float]:
    """Return the fill-forwarded native value of asset at as_of, or None.

    Scans sorted_vals for the latest entry on or before as_of. Falls back
    to purchase_price when purchase_date is None (asset predates any known
    date) and no value history is available for the requested date.
    """
    result = None
    for d, v in sorted_vals:
        if d <= as_of:
            result = v
        else:
            break
    if result is None and asset.purchase_price is not None and asset.purchase_date is None:
        # Same per-unit fix as _load_asset_native_values: purchase_price
        # is per-unit, so the fallback to cost basis must multiply by
        # units. Without it, an asset whose purchase_date is unknown but
        # has units=8 (e.g. a Tesouro position imported without a buy
        # date) reads as 1/8th of its real cost basis at as_of_date.
        units_for_basis = float(asset.units) if asset.units is not None else 1.0
        result = float(asset.purchase_price) * units_for_basis
    return result


async def _get_value_count(session: AsyncSession, asset_id: uuid.UUID) -> int:
    """Get the number of AssetValue entries for an asset."""
    result = await session.scalar(
        select(func.count()).select_from(AssetValue).where(AssetValue.asset_id == asset_id)
    )
    return result or 0


async def _get_transaction_counts(
    session: AsyncSession, workspace_id: uuid.UUID
) -> dict[uuid.UUID, int]:
    """Number of ledger transactions per asset in a workspace (one query)."""
    result = await session.execute(
        select(AssetTransaction.asset_id, func.count())
        .where(AssetTransaction.workspace_id == workspace_id)
        .group_by(AssetTransaction.asset_id)
    )
    return {row[0]: row[1] for row in result.all()}


async def get_assets(
    session: AsyncSession, workspace_id: uuid.UUID, include_archived: bool = False
) -> list[AssetRead]:
    """List all assets in a workspace with computed current_value."""
    query = select(Asset).where(Asset.workspace_id == workspace_id)
    if not include_archived:
        query = query.where(Asset.is_archived == False)
    query = query.order_by(Asset.position, Asset.name)

    result = await session.execute(query)
    assets = list(result.scalars().all())
    asset_ids = [a.id for a in assets]

    # Bulk-aggregate transaction sums per asset so the rent display
    # accounts for returned capital (WITHDRAWAL/SELL) and net income
    # (DIVIDEND/JCP/RENDIMENTO/INTEREST/RESGATE minus fees). One query
    # for the whole portfolio rather than N+1.
    returned_by_asset: dict[uuid.UUID, float] = {}
    invested_by_asset: dict[uuid.UUID, float] = {}
    tw_by_asset: dict[uuid.UUID, Optional[float]] = {}
    if asset_ids:
        cf_in_types = ["BUY", "DEPOSIT"]
        cf_out_types = ["WITHDRAWAL", "SELL"]
        income_types = ["DIVIDEND", "JCP", "RENDIMENTO", "INTEREST", "RESGATE"]
        from app.models.asset_transaction import AssetTransaction
        # Raw rows (not GROUP BY sums): the Dietz denominator needs each
        # flow's date. Portfolio-wide this is a few hundred rows — cheaper
        # than it looks, and still a single round-trip.
        tx_rows = (await session.execute(
            select(
                AssetTransaction.asset_id,
                AssetTransaction.type,
                AssetTransaction.date,
                AssetTransaction.value,
                AssetTransaction.fees,
            )
            .where(
                AssetTransaction.asset_id.in_(asset_ids),
                AssetTransaction.type.in_(cf_in_types + cf_out_types + income_types),
            )
        )).all()
        flows_by_asset: dict[uuid.UUID, list[tuple[date, str, float]]] = {}
        for asset_id, tx_type, tx_date, value, fees in tx_rows:
            v = float(value or 0)
            f = float(fees or 0)
            if tx_type in cf_in_types:
                # Money in — the true invested basis (fees add to cost).
                # Preferred over purchase_price × units, which shrinks
                # with partial sells and turned sold principal into
                # phantom profit on the rent display.
                invested_by_asset[asset_id] = invested_by_asset.get(asset_id, 0) + v + f
                flows_by_asset.setdefault(asset_id, []).append((tx_date, tx_type, v + f))
            elif tx_type in cf_out_types:
                # Capital being returned — full value, fees ignored (e.g.
                # broker commission was already deducted from amount).
                returned_by_asset[asset_id] = returned_by_asset.get(asset_id, 0) + v
                flows_by_asset.setdefault(asset_id, []).append((tx_date, tx_type, v))
            else:
                # Income — net of fees (NRA tax withholding on US divs etc).
                # Not a capital flow: goes into the gain, not the Dietz
                # denominator.
                returned_by_asset[asset_id] = returned_by_asset.get(asset_id, 0) + (v - f)
        today_d = date.today()
        # Closed positions measure Dietz over their actual holding period
        # (first buy → sell_date), not to today — see _tw_capital's
        # docstring for why an uncapped window inflates sold assets' rent
        # a little more every calendar day.
        sell_date_by_id = {a.id: a.sell_date for a in assets}
        for aid, flows in flows_by_asset.items():
            end = today_d
            sd = sell_date_by_id.get(aid)
            if sd is not None and sd < end:
                end = sd
            tw_by_asset[aid] = _tw_capital(flows, end)

    # Bulk-fetch the latest AssetValue per asset (DISTINCT ON, single
    # round-trip) and AV counts so we don't issue 2 SELECTs per asset.
    # On a 100-asset portfolio that's ~200 fewer round-trips — biggest
    # win in get_assets latency.
    latest_by_asset: dict[uuid.UUID, AssetValue] = {}
    count_by_asset: dict[uuid.UUID, int] = {}
    if asset_ids:
        # Postgres DISTINCT ON: keep one row per asset_id, sorted so the
        # row we keep is the most recent date.
        latest_rows = (await session.execute(
            select(AssetValue)
            .where(AssetValue.asset_id.in_(asset_ids))
            .order_by(AssetValue.asset_id, AssetValue.date.desc())
            .distinct(AssetValue.asset_id)
        )).scalars().all()
        for av in latest_rows:
            latest_by_asset[av.asset_id] = av

        count_rows = await session.execute(
            select(AssetValue.asset_id, func.count(AssetValue.id))
            .where(AssetValue.asset_id.in_(asset_ids))
            .group_by(AssetValue.asset_id)
        )
        for aid, n in count_rows.all():
            count_by_asset[aid] = int(n)

    tx_counts = await _get_transaction_counts(session, workspace_id)
    reads = []
    for asset in assets:
        # Tudo vem dos dicionarios pre-carregados em lote. O upstream
        # consulta valor e contagem por ativo dentro do laco; com 155
        # ativos era esse N+1 que fazia /api/assets levar ~50 s.
        latest = latest_by_asset.get(asset.id)
        count = count_by_asset.get(asset.id, 0)
        total_returned = returned_by_asset.get(asset.id, 0.0)
        invested_txs = invested_by_asset.get(asset.id)
        tw = tw_by_asset.get(asset.id)
        reads.append(_asset_to_read(asset, latest, count, total_returned,
                                    invested_txs, tw,
                                    tx_counts.get(asset.id, 0)))
    return reads


async def _get_tx_aggregates(
    session: AsyncSession, asset_id: uuid.UUID,
    sell_date: Optional[date] = None,
) -> tuple[float, Optional[float], Optional[float]]:
    """Per-asset version of the bulk aggregate computed in get_assets.
    Returns (total_returned_net, invested_txs, tw_capital). Used by
    single-asset endpoints (get/create/update) so the gain_loss and
    rent_pct they return match the list view. invested_txs / tw_capital
    are None when the asset has no BUY/DEPOSIT rows (caller falls back
    to purchase_price × units). `sell_date` caps the Dietz window for
    closed positions (same contract as the bulk path)."""
    from app.models.asset_transaction import AssetTransaction
    cf_in_types = ["BUY", "DEPOSIT"]
    cf_out_types = ["WITHDRAWAL", "SELL"]
    income_types = ["DIVIDEND", "JCP", "RENDIMENTO", "INTEREST", "RESGATE"]
    returned = 0.0
    invested: Optional[float] = None
    flows: list[tuple[date, str, float]] = []
    rows = await session.execute(
        select(
            AssetTransaction.type,
            AssetTransaction.date,
            AssetTransaction.value,
            AssetTransaction.fees,
        )
        .where(
            AssetTransaction.asset_id == asset_id,
            AssetTransaction.type.in_(cf_in_types + cf_out_types + income_types),
        )
    )
    for tx_type, tx_date, value, fees in rows.all():
        v = float(value or 0)
        f = float(fees or 0)
        if tx_type in cf_in_types:
            invested = (invested or 0.0) + v + f
            flows.append((tx_date, tx_type, v + f))
        elif tx_type in cf_out_types:
            returned += v
            flows.append((tx_date, tx_type, v))
        else:
            returned += v - f
    end = date.today()
    if sell_date is not None and sell_date < end:
        end = sell_date
    tw = _tw_capital(flows, end) if flows else None
    return returned, invested, tw


async def get_asset(
    session: AsyncSession, asset_id: uuid.UUID, workspace_id: uuid.UUID
) -> Optional[AssetRead]:
    """Get a single asset with computed fields."""
    result = await session.execute(
        select(Asset).where(Asset.id == asset_id, Asset.workspace_id == workspace_id)
    )
    asset = result.scalar_one_or_none()
    if not asset:
        return None
    latest = await _get_latest_value(session, asset.id)
    count = await _get_value_count(session, asset.id)
    total_returned, invested_txs, tw = await _get_tx_aggregates(
        session, asset.id, asset.sell_date)
    tx_count = await session.scalar(
        select(func.count())
        .select_from(AssetTransaction)
        .where(AssetTransaction.asset_id == asset.id)
    )
    return _asset_to_read(asset, latest, count, total_returned, invested_txs, tw,
                          tx_count or 0)


async def create_asset(
    session: AsyncSession,
    workspace_id: uuid.UUID,
    user_id: uuid.UUID,
    data: AssetCreate,
    *,
    market_provider: Optional[MarketPriceProvider] = None,
) -> AssetRead:
    """Create an asset, optionally with an initial value."""
    # Market-priced path: fetch a live quote first so we can derive currency
    # and the initial value from the ticker. Validate up-front rather than
    # half-creating an asset and failing on a 5xx from Yahoo.
    quote = None
    if data.valuation_method == "market_price":
        if not data.ticker:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="ticker is required for market_price assets",
            )
        if data.units is None or data.units <= 0:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="units (quantity) must be > 0 for market_price assets",
            )
        provider = market_provider or get_market_price_provider()
        quote = await provider.get_quote(data.ticker)
        if quote is None:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Could not fetch quote for {data.ticker}",
            )

    # Default for marcação na curva: a Tesouro IPCA+ with a contracted
    # rate is held-to-maturity by nature, so it's born on-curve unless the
    # caller said otherwise. Everything else defaults to market PU. The
    # caller can always override by passing rf_on_curve explicitly.
    if data.rf_on_curve is not None:
        rf_on_curve = data.rf_on_curve
    else:
        rf_on_curve = bool(
            data.asset_class == "RENDA_FIXA"
            and (data.rf_indexer or "").upper() == "IPCA"
            and data.rf_rate_pct is not None
            and (data.name or "").startswith("Tesouro IPCA+")
        )

    asset = Asset(
        user_id=user_id,
        workspace_id=workspace_id,
        name=data.name,
        type=data.type,
        # For market_price, the quote's currency is authoritative — a user
        # entering PETR4.SA from an English-language form shouldn't end up
        # with USD just because the dropdown defaulted to USD.
        currency=quote.currency if quote else data.currency,
        units=data.units,
        valuation_method=data.valuation_method,
        purchase_date=data.purchase_date,
        purchase_price=data.purchase_price,
        sell_date=data.sell_date,
        sell_price=data.sell_price,
        growth_type=data.growth_type,
        growth_rate=data.growth_rate,
        growth_frequency=data.growth_frequency,
        growth_start_date=data.growth_start_date,
        maturity_date=data.maturity_date,
        is_archived=data.is_archived,
        position=data.position,
        group_id=data.group_id,
        ticker=data.ticker.upper() if data.ticker else None,
        ticker_exchange=data.ticker_exchange or (quote.exchange if quote else None),
        last_price=Decimal(str(quote.price)) if quote else None,
        last_price_at=datetime.now(timezone.utc) if quote else None,
        logo_url=quote.logo_url if quote else None,
        source=(
            "tesouro_direto"
            if quote and quote.exchange == "Tesouro Direto"
            else ("yfinance" if data.valuation_method == "market_price" else "manual")
        ),
        asset_class=data.asset_class,
        custodian=data.custodian,
        rf_indexer=data.rf_indexer,
        rf_rate_pct=data.rf_rate_pct,
        rf_index_offset_pct=data.rf_index_offset_pct,
        rf_on_curve=rf_on_curve,
    )
    session.add(asset)
    await session.flush()

    # Seed exactly ONE initial AssetValue dated today. Order of preference:
    # explicit current_value (from the import payload), live quote (for
    # market-priced assets), or fall through to the growth-rule seed
    # below. The previous version layered the live-quote and
    # current_value branches as two independent `if`s, so a market_price
    # create with current_value (typical push_to_securo payload) wrote
    # the same date twice — that's how the asset_values table grew 41
    # duplicate rows over multiple bulk imports.
    if data.current_value is not None:
        session.add(AssetValue(
            asset_id=asset.id,
            amount=data.current_value,
            date=date.today(),
            source="manual",
        ))
    elif (
        data.valuation_method == "manual"
        and data.purchase_price is not None
        and data.units is not None
    ):
        # Manual asset created without an explicit current_value: seed an
        # initial AssetValue at the cost basis (purchase_price × units)
        # so the timeseries / dashboard filters / Investments page can
        # actually find this asset. Without it the asset sits at V=0
        # forever in the chart and the per-class breakdown ignores it.
        cost_basis = Decimal(str(data.purchase_price)) * Decimal(str(data.units))
        seed_date = data.purchase_date or date.today()
        session.add(AssetValue(
            asset_id=asset.id,
            amount=cost_basis,
            date=seed_date,
            source="manual",
        ))
    elif data.valuation_method == "market_price" and quote is not None:
        initial_amount = Decimal(str(quote.price)) * Decimal(str(data.units))
        session.add(AssetValue(
            asset_id=asset.id,
            amount=initial_amount,
            date=date.today(),
            source="sync",
        ))
    elif data.valuation_method == "growth_rule" and data.purchase_price is not None:
        # Seed the initial value from purchase price
        base_date = data.purchase_date or data.growth_start_date or date.today()
        seed = AssetValue(
            asset_id=asset.id,
            amount=data.purchase_price,
            date=base_date,
            source="manual",
        )
        session.add(seed)

        # Backfill all growth values from the seed date to today
        if data.growth_type and data.growth_rate and data.growth_frequency:
            backfill = _generate_growth_values(
                asset_id=asset.id,
                base_amount=float(data.purchase_price),
                base_date=base_date,
                growth_type=data.growth_type,
                growth_rate=float(data.growth_rate),
                growth_frequency=data.growth_frequency,
                growth_start_date=data.growth_start_date,
            )
            for v in backfill:
                session.add(v)

    # Seed the opening buy so market-priced holdings are ledger-backed from
    # the start (issue #235): units/average_price/cost basis are then derived
    # from the transactions, consistently with later edits. `purchase_price`
    # is the total paid, so per-share = purchase_price / units; absent that we
    # fall back to the live quote (cost basis ≈ current value, gain ≈ 0).
    # O upstream passou a semear aqui uma compra de abertura quando o ativo
    # nasce com unidades. Removido: o fork já faz isso mais abaixo, com o
    # interruptor `seed_purchase_transaction`, que as pipelines de importação
    # desligam para não contar duas vezes. Manter os dois criaria dois
    # lançamentos de compra para o mesmo ativo.

    # Stamp purchase_price_primary
    if asset.purchase_price is not None:
        await stamp_primary_amount(
            session, user_id, asset,
            amount_field="purchase_price",
            primary_field="purchase_price_primary",
            rate_field="_no_rate",  # Asset has no rate field
            date_field="purchase_date",
        )

    # Auto-seed a BUY transaction at purchase_date so the timeseries treats
    # the asset's entry as a cashflow rather than a phantom gain. Without
    # this, an asset with purchase_date in the past + only a current
    # AssetValue would have its V_end jump from 0 to cost_basis on
    # purchase_date with cf=0 — Modified Dietz reads that as +∞% on day 1
    # (or a several-pp spike on the cumulative TWR line). Bulk-import
    # scripts that build their own AssetTransaction history pass
    # seed_purchase_transaction=False to avoid double-counting.
    if (
        data.seed_purchase_transaction
        and data.purchase_date is not None
        and data.purchase_price is not None
        and data.units is not None
        and float(data.units) > 0
    ):
        cost_basis = Decimal(str(data.purchase_price)) * Decimal(str(data.units))
        session.add(AssetTransaction(
            user_id=user_id,
            asset_id=asset.id,
            date=data.purchase_date,
            type="BUY",
            qty=data.units,
            price=data.purchase_price,
            value=cost_basis.quantize(Decimal("0.01")),
            fees=Decimal("0"),
            source="auto",
            external_id=f"auto-buy-{asset.id}",
        ))

    await session.commit()
    await session.refresh(asset)
    invalidate_ts_cache(user_id)
    # Best-effort fetch of the logo so the next page load can serve it
    # from /api/assets/<id>/icon instead of round-tripping to gstatic.
    # We await inline (vs a background task) because we only created
    # one asset — the latency is bounded by a single httpx call and
    # avoids the open-session-in-background-task complications. Failure
    # is silent: the cache is opportunistic.
    if asset.logo_url and not asset.logo_url.startswith("/api/"):
        try:
            from app.services.asset_icon_service import fetch_and_store_icon
            await fetch_and_store_icon(session, asset)
            await session.refresh(asset)
        except Exception as exc:
            logger.info("post-create icon fetch failed for %s: %s",
                        asset.id, exc)
    latest = await _get_latest_value(session, asset.id)
    count = await _get_value_count(session, asset.id)
    total_returned, invested_txs, tw = await _get_tx_aggregates(
        session, asset.id, asset.sell_date)
    tx_count = await session.scalar(
        select(func.count()).select_from(AssetTransaction).where(AssetTransaction.asset_id == asset.id)
    )
    return _asset_to_read(asset, latest, count, total_returned, invested_txs, tw,
                          tx_count or 0)


async def update_asset(
    session: AsyncSession,
    asset_id: uuid.UUID,
    workspace_id: uuid.UUID,
    user_id: uuid.UUID,
    data: AssetUpdate,
    regenerate_growth: bool = False,
) -> Optional[AssetRead]:
    """Partial update of an asset."""
    result = await session.execute(
        select(Asset).where(Asset.id == asset_id, Asset.workspace_id == workspace_id)
    )
    asset = result.scalar_one_or_none()
    if not asset:
        return None

    # Capture the pre-edit on-curve state so we can detect a flip and
    # re-value the asset's history na curva (below, after commit).
    prev_on_curve = bool(asset.rf_on_curve)

    update_data = data.model_dump(exclude_unset=True)
    # Prevent changing valuation_method on existing assets
    update_data.pop("valuation_method", None)
    # Not a column — handled below as an AssetValue upsert.
    new_current_value = update_data.pop("current_value", None)
    for key, value in update_data.items():
        setattr(asset, key, value)

    # "Valor atual" edited in the dialog (manual assets): upsert today's
    # AV so the card reflects it immediately. Manual source wins over the
    # daily refresh for today, which is the user's intent when they type
    # a value by hand.
    if new_current_value is not None and asset.valuation_method == "manual":
        today = date.today()
        existing_av = (await session.execute(
            select(AssetValue).where(
                AssetValue.asset_id == asset.id,
                AssetValue.date == today,
            )
        )).scalar_one_or_none()
        if existing_av is not None:
            existing_av.amount = Decimal(str(new_current_value))
            existing_av.source = "manual"
        else:
            session.add(AssetValue(
                asset_id=asset.id, date=today,
                amount=Decimal(str(new_current_value)), source="manual"))
        from app.services.portfolio_daily_snapshot_service import (
            invalidate_daily_snapshots,
        )
        await invalidate_daily_snapshots(session, user_id, from_date=today)

    # Regenerate growth-rule values if requested
    if regenerate_growth and asset.valuation_method == "growth_rule":
        # Delete all rule-generated values
        await session.execute(
            select(AssetValue)
            .where(AssetValue.asset_id == asset.id, AssetValue.source == "rule")
        )
        from sqlalchemy import delete as sa_delete
        await session.execute(
            sa_delete(AssetValue).where(
                AssetValue.asset_id == asset.id,
                AssetValue.source == "rule",
            )
        )
        # Regenerate from purchase_price
        if asset.purchase_price and asset.growth_type and asset.growth_rate and asset.growth_frequency:
            base_date = asset.purchase_date or asset.growth_start_date or date.today()
            backfill = _generate_growth_values(
                asset_id=asset.id,
                base_amount=float(asset.purchase_price),
                base_date=base_date,
                growth_type=asset.growth_type,
                growth_rate=float(asset.growth_rate),
                growth_frequency=asset.growth_frequency,
                growth_start_date=asset.growth_start_date,
            )
            for v in backfill:
                session.add(v)

    # Re-stamp purchase_price_primary if purchase_price or currency changed
    if "purchase_price" in update_data or "currency" in update_data:
        if asset.purchase_price is not None:
            await stamp_primary_amount(
                session, user_id, asset,
                amount_field="purchase_price",
                primary_field="purchase_price_primary",
                rate_field="_no_rate",
                date_field="purchase_date",
            )

    # If units change on a market-priced asset, rewrite today's AssetValue with
    # the new (units × last_price). Without this, the portfolio chart keeps
    # plotting the old position size even though the header and wallet totals
    # (computed live) already reflect the new units — the two disagree until
    # the next scheduled refresh overwrites today's row.
    if (
        "units" in update_data
        and asset.valuation_method == "market_price"
        and asset.last_price is not None
        and asset.units is not None
        and asset.units > 0
    ):
        await _apply_price_to_asset(session, asset, Decimal(str(asset.last_price)))

    await session.commit()
    await session.refresh(asset)

    # On-curve was just switched ON: re-value the asset's whole history na
    # curva so the chart shows the smooth carrego instead of a market-then-
    # curve step on the toggle day. Then drop the user's daily snapshots so
    # they rebuild with the new values. Only runs on the False→True edge to
    # avoid rewriting history on every unrelated edit.
    if asset.rf_on_curve and not prev_on_curve:
        from app.tasks.rf_tasks import backfill_on_curve_history
        try:
            n = await backfill_on_curve_history(session, asset)
            logger.info("on-curve backfill: asset=%s rewrote %d AV rows",
                        asset.id, n)
            from app.services.portfolio_daily_snapshot_service import (
                invalidate_daily_snapshots,
            )
            # Only drop snapshots from the purchase date forward — the curve
            # rewrite never touches AVs before purchase. Wiping ALL snapshots
            # would leave the next incremental rebuild with no prior day to
            # seed cum from, resetting the cumulative TWR to 0 (the cliff
            # bug). Surgical invalidation keeps the pre-purchase tail intact
            # so the rebuild chains correctly.
            await invalidate_daily_snapshots(
                session, user_id, from_date=asset.purchase_date
            )
        except Exception as exc:
            logger.warning("on-curve backfill failed for %s: %s", asset.id, exc)

    # Bust the timeseries cache so the dashboard / KPI bars / Resultado
    # table see the edit immediately instead of waiting up to 10 minutes
    # for the result cache to expire. Without this, changing purchase_date
    # or units left the chart and CRIPTO/STOCKS_US filters showing the
    # pre-edit state until the TTL ran out, which felt broken.
    invalidate_ts_cache(user_id)
    latest = await _get_latest_value(session, asset.id)
    count = await _get_value_count(session, asset.id)
    total_returned, invested_txs, tw = await _get_tx_aggregates(
        session, asset.id, asset.sell_date)
    return _asset_to_read(asset, latest, count, total_returned, invested_txs, tw)


async def delete_asset(
    session: AsyncSession, asset_id: uuid.UUID, workspace_id: uuid.UUID
) -> bool:
    """Delete an asset (cascades to values)."""
    result = await session.execute(
        select(Asset).where(Asset.id == asset_id, Asset.workspace_id == workspace_id)
    )
    asset = result.scalar_one_or_none()
    if not asset:
        return False
    # O cache de series temporais e indexado por usuario; guarda o dono
    # antes de apagar, senao nao ha de quem invalidar.
    owner_id = asset.user_id
    await session.delete(asset)
    await session.commit()
    invalidate_ts_cache(owner_id)
    return True


async def get_asset_values(
    session: AsyncSession, asset_id: uuid.UUID, workspace_id: uuid.UUID
) -> Optional[list[AssetValueRead]]:
    """Get value history for an asset, most recent first."""
    # Verify ownership
    owner_check = await session.execute(
        select(Asset.id).where(Asset.id == asset_id, Asset.workspace_id == workspace_id)
    )
    if not owner_check.scalar_one_or_none():
        return None

    result = await session.execute(
        select(AssetValue)
        .where(AssetValue.asset_id == asset_id)
        .order_by(desc(AssetValue.date), desc(AssetValue.id))
    )
    values = result.scalars().all()
    return [AssetValueRead.model_validate(v) for v in values]


async def add_asset_value(
    session: AsyncSession, asset_id: uuid.UUID, workspace_id: uuid.UUID, data: AssetValueCreate
) -> Optional[AssetValueRead]:
    """Upsert a value entry for an asset (one row per asset_id+date).

    Uses ON CONFLICT against the uq_asset_values_asset_date constraint
    so that re-running push_to_securo for the same monthly history
    rewrites the existing row instead of inserting a duplicate. The
    previous plain-INSERT path silently created duplicates that
    distorted the timeseries walk on every cashflow day."""
    # Verify ownership. Traz o dono junto: o cache de series temporais e
    # indexado por usuario, e o workspace ja vem por parametro -- nao ha
    # o que resolver aqui.
    owner_check = await session.execute(
        select(Asset.user_id).where(
            Asset.id == asset_id, Asset.workspace_id == workspace_id)
    )
    owner_id = owner_check.scalar_one_or_none()
    if owner_id is None:
        return None

    stmt = pg_insert(AssetValue).values(
        asset_id=asset_id,
        workspace_id=workspace_id,
        amount=data.amount,
        date=data.date,
        source="manual",
    ).on_conflict_do_update(
        constraint="uq_asset_values_asset_date",
        set_={"amount": data.amount, "source": "manual"},
    ).returning(AssetValue)
    result = await session.execute(stmt)
    row = result.scalar_one()
    await session.commit()
    invalidate_ts_cache(owner_id)
    return AssetValueRead.model_validate(row)


async def delete_asset_value(
    session: AsyncSession, value_id: uuid.UUID, workspace_id: uuid.UUID
) -> bool:
    """Delete a specific asset value entry."""
    result = await session.execute(
        select(AssetValue, Asset.user_id)
        .join(Asset, AssetValue.asset_id == Asset.id)
        .where(AssetValue.id == value_id, Asset.workspace_id == workspace_id)
    )
    linha = result.first()
    if not linha:
        return False
    value, owner_id = linha
    await session.delete(value)
    await session.commit()
    invalidate_ts_cache(owner_id)
    return True


async def get_asset_value_trend(
    session: AsyncSession, asset_id: uuid.UUID, workspace_id: uuid.UUID, months: int = 12
) -> Optional[list[dict]]:
    """Get value trend data for charting.

    For market-priced holdings the series is rebuilt from the ledger
    (quantity(date) × price(date)) so entering past trades reshapes the whole
    line; other assets use their stored value points.
    """
    asset = (
        await session.execute(
            select(Asset).where(Asset.id == asset_id, Asset.workspace_id == workspace_id)
        )
    ).scalar_one_or_none()
    if asset is None:
        return None

    result = await session.execute(
        select(AssetValue.date, AssetValue.amount, AssetValue.market_amount)
        .where(AssetValue.asset_id == asset_id)
        .order_by(AssetValue.date)
    )
    rows = result.all()
    # `market_amount` só existe em Tesouro marcado na curva (ver
    # AssetValue.market_amount). Vai como None no resto, e o gráfico
    # simplesmente não desenha a segunda linha.
    return [
        {
            "date": row[0].isoformat(),
            "amount": float(row[1]),
            "market_amount": float(row[2]) if row[2] is not None else None,
        }
        for row in rows
    ]


async def get_portfolio_trend(
    session: AsyncSession,
    workspace_id: uuid.UUID,
    user_id: Optional[uuid.UUID] = None,
) -> dict:
    """Get portfolio trend data for stacked area chart.
    Returns asset metadata + pivoted trend with fill-forward values.
    Sold assets are included so their pre-sell history still contributes
    to the historical total; their contribution drops to 0 the day after
    sell_date.

    `user_id` is only used to resolve the user's primary_currency for the
    chart's converted totals; it falls back to the workspace's default when
    not supplied.
    """
    result = await session.execute(
        select(Asset).where(
            Asset.workspace_id == workspace_id,
            Asset.is_archived == False,
        ).order_by(Asset.position, Asset.name)
    )
    active_assets = list(result.scalars().all())

    if not active_assets:
        return {"assets": [], "trend": [], "total": 0.0}

    # Get user's primary currency for conversion
    user = await session.get(User, user_id) if user_id is not None else None
    primary_currency = user.primary_currency if user else get_settings().default_currency

    # Use the shared helper to bulk-load native-currency AVs (and the
    # purchase_price × units anchor when purchase_date precedes them).
    # Same source-of-truth used by get_asset_values_at, so the dashboard
    # widget and the trend chart can't drift apart on prepend logic.
    values_map = await _load_asset_native_values(session, active_assets)

    asset_meta: list[dict[str, Any]] = []
    asset_currency: dict[str, str] = {}
    sell_date_by_aid: dict[str, date] = {}
    all_dates: set[date] = set()

    # Bulk-load every FX rate up-front so the per-(date, asset) loop is
    # O(1) lookups instead of N×M `convert()` calls hitting the DB. The
    # upstream version of this function calls convert() inside the trend
    # loop, which for a 100-asset × 5-year history portfolio means ~6 k
    # FX queries per request and pushed the Patrimônio page to ~400 ms
    # of pure DB chatter. We keep the in-memory `fx_at` here and let
    # convert() handle one-off lookups elsewhere.
    foreign_ccys = {a.currency for a in active_assets if a.currency != primary_currency}
    fx_by_ccy_date: dict[tuple[str, str], float] = {}
    fx_dates_by_ccy: dict[str, list[str]] = {}
    if foreign_ccys:
        from app.models.fx_rate import FxRate
        fx_rows = (await session.execute(
            select(FxRate.date, FxRate.quote_currency, FxRate.rate)
            .where(FxRate.base_currency == "USD")
        )).all()
        for d, qc, rate in fx_rows:
            fx_by_ccy_date[(qc, d.isoformat())] = float(rate)
            fx_dates_by_ccy.setdefault(qc, []).append(d.isoformat())
        for qc in fx_dates_by_ccy:
            fx_dates_by_ccy[qc].sort()

    def fx_at(from_ccy: str, to_ccy: str, on: date) -> float:
        """In-memory FX lookup with as-of fallback (last known rate ≤ on).
        Returns 1.0 if no FX data; that matches what convert() does too."""
        if from_ccy == to_ccy:
            return 1.0
        target = on.isoformat()
        # USD-quoted table: rate = USD/X. Convert via USD.
        if from_ccy == "USD" and to_ccy != "USD":
            dates = fx_dates_by_ccy.get(to_ccy, [])
            if not dates:
                return 1.0
            idx = bisect.bisect_right(dates, target) - 1
            if idx < 0:
                return 1.0
            return fx_by_ccy_date[(to_ccy, dates[idx])]
        if to_ccy == "USD" and from_ccy != "USD":
            dates = fx_dates_by_ccy.get(from_ccy, [])
            if not dates:
                return 1.0
            idx = bisect.bisect_right(dates, target) - 1
            if idx < 0:
                return 1.0
            rate = fx_by_ccy_date[(from_ccy, dates[idx])]
            return 1.0 / rate if rate else 1.0
        # Cross-ccy via USD
        return fx_at(from_ccy, "USD", on) * fx_at("USD", to_ccy, on)

    for asset in active_assets:
        aid = str(asset.id)
        asset_meta.append({
            "id": aid,
            "name": asset.name,
            "type": asset.type,
            "group_id": str(asset.group_id) if asset.group_id else None,
        })
        asset_currency[aid] = asset.currency

        # Prepend (with units multiplier) is now handled centrally inside
        # _load_asset_native_values so the dashboard widget and the chart
        # can't disagree about earliest-anchor logic.
        vals = values_map[aid]

        # If the asset was sold and a sell_price is recorded, treat it as the
        # asset's terminal value on sell_date so the chart reflects the
        # realized value before dropping to 0.
        if asset.sell_date is not None:
            sell_date_by_aid[aid] = asset.sell_date
            if asset.sell_price is not None:
                vals = [(d, v) for d, v in vals if d <= asset.sell_date]
                if not vals or vals[-1][0] != asset.sell_date:
                    vals.append((asset.sell_date, float(asset.sell_price)))
                values_map[aid] = vals

        # Convert this asset's series to primary currency up-front so the
        # trend loop below doesn't need a per-(date, asset) FX call. Each
        # AV row is FX'd at its own date — same semantics as upstream's
        # in-loop convert(), but using the bulk-loaded fx_at instead of
        # round-tripping to the DB N×M times.
        if asset.currency != primary_currency:
            vals = [(d, amt * fx_at(asset.currency, primary_currency, d))
                    for d, amt in vals]
            values_map[aid] = vals

        for d, _ in vals:
            all_dates.add(d)

    if not all_dates:
        return {"assets": asset_meta, "trend": [], "total": 0.0}

    sorted_dates = sorted(all_dates)

    # Build lookup: aid -> {date: value}
    value_lookup: dict[str, dict[date, float]] = {}
    first_date: dict[str, date] = {}
    for aid in [a["id"] for a in asset_meta]:
        value_lookup[aid] = dict(values_map[aid])
        if values_map[aid]:
            first_date[aid] = values_map[aid][0][0]

    # Build trend with fill-forward; 0 before first date (for stacking).
    # Each native-currency amount is converted at the display date `d` so that
    # fill-forwarded values reflect current FX rates — consistent with
    # get_asset_values_at(as_of_date=d).
    trend = []
    last_known: dict[str, float] = {}  # native currency amounts
    for aid in [a["id"] for a in asset_meta]:
        last_known[aid] = 0.0

    for d in sorted_dates:
        row: dict[str, object] = {"date": d.isoformat()}
        date_total = 0.0
        for aid in [a["id"] for a in asset_meta]:
            if d in value_lookup[aid]:
                last_known[aid] = value_lookup[aid][d]
            # Use 0 before asset exists (stacking needs numeric values)
            if aid in first_date and d >= first_date[aid]:
                native = last_known[aid]
            else:
                native = 0.0
            # After sell_date, the asset has been liquidated — drop to 0 so
            # it stops contributing to the portfolio total going forward.
            if aid in sell_date_by_aid and d > sell_date_by_aid[aid]:
                native = 0.0
                last_known[aid] = 0.0

            # `values_map` was already FX-converted at each (date, asset)
            # pair upstream of this loop, so `native` is in primary
            # currency. Skipping the convert() call here drops ~N_dates ×
            # N_assets DB roundtrips for portfolios with foreign positions.
            val = round(native, 2)

            row[aid] = val
            date_total += val
        row["_total"] = round(date_total, 2)
        trend.append(row)

    # The header total matches the last row's _total — both use the same
    # per-display-date conversion so no second conversion is needed.
    total: float = cast(float, trend[-1]["_total"]) if trend else 0.0

    return {"assets": asset_meta, "trend": trend, "total": round(total, 2)}


async def get_asset_values_at(
    session: AsyncSession,
    scope_id: uuid.UUID,
    as_of_date: Optional[date] = None,
    primary_currency: Optional[str] = None,
    *,
    by_workspace: bool = False,
    group_ids: Optional[list[uuid.UUID]] = None,
) -> tuple[dict[str, float], float]:
    """Return (per_currency_totals, primary_total) for all active assets.

    `scope_id` is a workspace_id when `by_workspace=True` (preferred for
    multi-tenant code paths), otherwise treated as a legacy user_id
    filter. Both branches honor the `is_archived=False` + `sell_date is None`
    filters.

    - as_of_date=None: uses live prices (current view).
    - as_of_date set: uses the latest AssetValue on or before that date,
      falling back to purchase_price only if the asset existed by that date.
    - primary_currency=None: primary_total is 0.0.
    """
    scope_filter = (
        Asset.workspace_id == scope_id if by_workspace else Asset.user_id == scope_id
    )
    # `group_ids` restricts to assets in a Collection's wallets (issue #105).
    # An empty list means "no wallets in this collection" → no assets.
    if group_ids is not None and len(group_ids) == 0:
        return {}, 0.0
    stmt = select(Asset).where(
        scope_filter,
        Asset.is_archived == False,
        Asset.sell_date.is_(None),
    )
    if group_ids:
        stmt = stmt.where(Asset.group_id.in_(group_ids))
    result = await session.execute(stmt)
    assets = list(result.scalars().all())

    totals: dict[str, float] = {}
    primary_total = 0.0

    if as_of_date is not None:
        values_map = await _load_asset_native_values(session, assets, up_to_date=as_of_date)

    for asset in assets:
        if as_of_date is not None:
            amount: Optional[float] = _fill_forward_at(asset, values_map[str(asset.id)], as_of_date)
        else:
            latest = await _get_latest_value(session, asset.id)
            amount = _compute_current_value(asset, latest)

        if not amount:
            continue

        totals[asset.currency] = totals.get(asset.currency, 0.0) + amount

        if primary_currency is not None:
            converted, _ = await convert(
                session, Decimal(str(amount)), asset.currency, primary_currency, as_of_date
            )
            primary_total += float(converted)

    return totals, primary_total


async def get_custodian_summary(
    session: AsyncSession, user_id: uuid.UUID
) -> dict:
    """Live portfolio totals grouped by (custodian, wallet) — the broker
    reconciliation view ("bater com a corretora"). Wallet matters because
    broker accounts are per CPF: BTG-Anderson and BTG-Camila are separate
    statements even though the custodian string is the same.

    Values use the same live valuation as the dashboard Patrimônio
    (_compute_current_value: units × last_price for market assets, latest
    AV otherwise) so the numbers here match what the user reconciles
    against, converted to the primary currency at the latest FX rate.
    """
    from app.models.asset_group import AssetGroup

    user = await session.get(User, user_id)
    primary = (user.primary_currency if user else None) or "BRL"

    assets = list((await session.execute(
        select(Asset).where(
            Asset.user_id == user_id,
            Asset.is_archived == False,   # noqa: E712
            Asset.sell_date.is_(None),
        )
    )).scalars().all())

    groups = {
        g.id: g.name
        for g in (await session.execute(
            select(AssetGroup).where(AssetGroup.user_id == user_id)
        )).scalars().all()
    }

    # Bulk latest AV per asset (same pattern as get_assets).
    asset_ids = [a.id for a in assets]
    latest_by_asset: dict[uuid.UUID, AssetValue] = {}
    if asset_ids:
        rows = (await session.execute(
            select(AssetValue)
            .where(AssetValue.asset_id.in_(asset_ids))
            .order_by(AssetValue.asset_id, AssetValue.date.desc())
            .distinct(AssetValue.asset_id)
        )).scalars().all()
        latest_by_asset = {r.asset_id: r for r in rows}

    agg: dict[tuple[str, str], dict] = {}
    total_primary = 0.0
    for a in assets:
        amount = _compute_current_value(a, latest_by_asset.get(a.id))
        if not amount:
            continue
        if a.currency != primary:
            converted, _ = await convert(
                session, Decimal(str(amount)), a.currency, primary, None)
            amount_primary = float(converted)
        else:
            amount_primary = float(amount)
        key = (
            (a.custodian or "").strip() or "(sem custodiante)",
            groups.get(a.group_id) or "Sem carteira",
        )
        bucket = agg.setdefault(key, {"count": 0, "total": 0.0})
        bucket["count"] += 1
        bucket["total"] += amount_primary
        total_primary += amount_primary

    out = [
        {
            "custodian": cust,
            "wallet": wallet,
            "count": v["count"],
            "total": round(v["total"], 2),
            "share_pct": round(v["total"] / total_primary * 100, 4)
            if total_primary > 0 else 0.0,
        }
        for (cust, wallet), v in agg.items()
    ]
    out.sort(key=lambda r: (r["wallet"], -r["total"]))
    return {
        "primary_currency": primary,
        "total": round(total_primary, 2),
        "rows": out,
    }


# ============================================================================
# Market-price refresh
# ============================================================================


async def _apply_price_to_asset(
    session: AsyncSession, asset: Asset, new_price: Decimal, *, value_date: date | None = None
) -> None:
    """Update the cached price and upsert today's AssetValue.

    Shared by the single-asset and batch refresh paths so both behave
    identically: price + timestamp get stamped; today's value gets
    inserted or overwritten so running the task multiple times per day
    doesn't pile up duplicate rows.
    """
    asset.last_price = new_price
    asset.last_price_at = datetime.now(timezone.utc)

    if not asset.units or asset.units <= 0:
        return

    today = value_date or date.today()
    new_amount = new_price * Decimal(str(asset.units))
    existing = await session.execute(
        select(AssetValue)
        .where(AssetValue.asset_id == asset.id, AssetValue.date == today)
        .order_by(desc(AssetValue.id))
        .limit(1)
    )
    today_value = existing.scalar_one_or_none()
    if today_value is not None:
        today_value.amount = new_amount
        today_value.price = new_price
        today_value.source = "sync"
    else:
        session.add(
            AssetValue(
                asset_id=asset.id,
                amount=new_amount,
                price=new_price,
                date=today,
                source="sync",
            )
        )


async def refresh_market_price_asset(
    session: AsyncSession,
    asset: Asset,
    *,
    market_provider: Optional[MarketPriceProvider] = None,
) -> bool:
    """Re-quote a single market-priced asset and update its cached price.

    Returns True when a new quote was persisted, False otherwise (no quote
    available, stale price unchanged, or missing fields).
    """
    if asset.valuation_method != "market_price" or not asset.ticker:
        return False

    provider = market_provider or get_market_price_provider()
    try:
        quote = await provider.get_quote(asset.ticker)
    except MarketPriceRateLimitedError:
        # Let the scheduler see this explicitly so it can back off globally.
        raise
    except Exception as e:
        logger.warning("Market price refresh failed for %s: %s", asset.ticker, e)
        return False

    if quote is None or quote.price is None:
        return False

    await _apply_price_to_asset(session, asset, Decimal(str(quote.price)))
    # Opportunistic logo backfill: assets created before Brandfetch was
    # configured have no logo_url. On the next single-asset refresh (which
    # goes through the full get_quote → website lookup), stamp it in.
    # The batch refresh path doesn't have website info so it leaves logos
    # alone; manual refreshes and creates cover the fill-in.
    if not asset.logo_url and quote.logo_url:
        asset.logo_url = quote.logo_url
        # New logo_url — drop the cached blob so the next icon fetch
        # picks up the fresh URL.
        asset.logo_data = None
        asset.logo_content_type = None
    await session.flush()
    # If we have a logo_url and no cached bytes yet, fetch them now so
    # subsequent loads come from our backend.
    if asset.logo_url and asset.logo_content_type is None \
            and not asset.logo_url.startswith("/api/"):
        try:
            from app.services.asset_icon_service import fetch_and_store_icon
            await fetch_and_store_icon(session, asset, commit=False)
        except Exception as exc:
            logger.info("post-refresh icon fetch failed for %s: %s",
                        asset.id, exc)
    return True


async def refresh_all_market_prices(
    session: AsyncSession,
    *,
    market_provider: Optional[MarketPriceProvider] = None,
) -> dict[str, int]:
    """Refresh every non-archived market-priced asset in the database.

    Uses the provider's batch ``get_latest_prices`` endpoint when possible —
    one HTTP request covers the whole portfolio via ``yfinance.download``
    instead of one call per asset. Falls back silently to per-asset refresh
    if the batch returns nothing (provider without bulk support, or a hard
    failure on Yahoo's end).

    Returns a summary counting successes, skips, and rate-limit halts —
    surfaced as the Celery task's return payload for observability.
    """
    result = await session.execute(
        select(Asset).where(
            Asset.valuation_method == "market_price",
            Asset.is_archived == False,
            Asset.sell_date.is_(None),
            Asset.ticker.isnot(None),
        )
    )
    assets = list(result.scalars().all())

    if not assets:
        return {"refreshed": 0, "skipped": 0, "rate_limited": 0}

    provider = market_provider or get_market_price_provider()
    tickers = [a.ticker for a in assets if a.ticker]

    # Batch path: one request → dict[SYMBOL, price]. On rate-limit we halt
    # immediately — retrying within the same task would just pile on 429s
    # and risk an IP-level cookie ban.
    try:
        prices = await provider.get_latest_prices(tickers)
    except MarketPriceRateLimitedError:
        logger.warning("Yahoo rate-limited the batch fetch; skipping this cycle")
        return {"refreshed": 0, "skipped": len(assets), "rate_limited": 1}
    except Exception as e:
        logger.warning("Batch price fetch failed, falling back to per-asset: %s", e)
        prices = {}

    refreshed = 0
    skipped = 0

    for asset in assets:
        if not asset.ticker:
            skipped += 1
            continue
        price = prices.get(asset.ticker.upper()) if prices else None
        if price is None:
            # Per-asset fallback: the batch missed this symbol (delisted
            # ticker, one-off provider error, etc.). Try the full quote
            # path which also populates name/currency if needed.
            try:
                ok = await refresh_market_price_asset(
                    session, asset, market_provider=provider
                )
            except MarketPriceRateLimitedError:
                logger.warning(
                    "Yahoo rate-limited mid-refresh after %d assets; halting",
                    refreshed,
                )
                await session.commit()
                return {
                    "refreshed": refreshed,
                    "skipped": skipped + (len(assets) - refreshed - skipped),
                    "rate_limited": 1,
                }
            if ok:
                refreshed += 1
            else:
                skipped += 1
            continue

        await _apply_price_to_asset(session, asset, price)
        refreshed += 1

    await session.commit()
    return {"refreshed": refreshed, "skipped": skipped, "rate_limited": 0}
