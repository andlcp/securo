"""Portfolio time series — Modified Dietz TWR computed live from
asset_transactions + asset_values + assets.

Replaces the parallel `portfolio_snapshots` table as the source for the
investments dashboard chart. Reading from the same tables that Patrimônio
edits guarantees that every change in the UI is reflected in the chart.

The math (per month, per "bucket" — could be all assets, a class, or a
specific asset id):
    V_start_m  = sum(AssetValue.amount at month_end_{m-1}) for assets in bucket
    V_end_m    = sum(AssetValue.amount at month_end_m)     for assets in bucket
    CF_m       = sum(BUY at_close) - sum(SELL at_close)    in month m
    INC_m      = sum(DIVIDEND.value + JCP.value + RENDIMENTO.value
                    + RESGATE.value)
    r_m        = (V_end_m + INC_m - V_start_m - CF_m) / (V_start_m + 0.5*CF_m)
    TWR_cum_m  = product over m of (1 + r_m) - 1

For market-priced assets BUY / SELL cashflows are valued at the day's
yfinance close (qty × close), not the broker's execution price. That
keeps the daily formula on a single basis: V_end uses close, CF uses
close, and the result captures price drift on the existing position
without flipping into a phantom -8 % "loss" the moment a BUY settles
against a low close (ASAI3 14/08/2025 ate one of those before this
fix). The actual cash you paid is preserved on the per-asset card via
asset.purchase_price × units vs last_price × units.

For window normalization the frontend rebases each line to 0% at the
start of the chosen window — we just return the cumulative series here,
the UI does the math (see investments.tsx).

NOTE: For a multi-currency portfolio (USD assets) the AssetValue.amount
must be expressed in the asset's currency. Conversion to the user's
display currency uses the FX cache (FxRate table) at the closing date
of each month. This is consistent with how Securo's Net Worth report
already handles multi-currency.
"""

import asyncio
import json
import logging
import time as _time
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional
import uuid

from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.asset import Asset
from app.models.asset_transaction import AssetTransaction
from app.models.asset_value import AssetValue
from app.models.user import User
from app.services import fx_rate_service
from app.services.investment_benchmark_service import fetch_yahoo_close_history

logger = logging.getLogger(__name__)


_INCOME_TYPES = {"DIVIDEND", "JCP", "RENDIMENTO", "RESGATE", "INTEREST"}
_BUY_TYPES = {"BUY", "DEPOSIT"}
_SELL_TYPES = {"SELL", "WITHDRAWAL"}


# Module-level CDI history cache. We fetch the monthly accumulated CDI
# from BCB SGS once per process and use it to drive the implicit-cashflow
# heuristic for CDI-tracking assets (Tesouro Selic, CDBs without explicit
# transactions). Without this the heuristic uses a fixed 0.1 %/day
# allowance which is well above CDI in low-rate years (Selic at 2 %
# = 0.005 %/day) and well below in high-rate years (Selic at 13.75 %
# = 0.04 %/day) — both miscalibrations leak organic CDI into cf_buy.
_CDI_MONTHLY: dict[tuple[int, int], float] = {}
_CDI_FETCHED = False


async def _ensure_cdi_loaded() -> None:
    global _CDI_FETCHED
    if _CDI_FETCHED:
        return
    import urllib.request
    try:
        url = ("https://api.bcb.gov.br/dados/serie/bcdata.sgs.4391/dados"
               "?formato=json&dataInicial=01/01/2018&dataFinal=01/12/2030")
        with urllib.request.urlopen(url, timeout=15) as r:
            raw = json.load(r)
        for it in raw:
            d = datetime.strptime(it["data"], "%d/%m/%Y").date()
            _CDI_MONTHLY[(d.year, d.month)] = float(it["valor"]) / 100
        _CDI_FETCHED = True
    except Exception as exc:
        logger.warning("CDI fetch failed: %s — heuristic falls back to 0.1%%/day", exc)
        _CDI_FETCHED = True  # don't keep retrying


def _cdi_factor_between(d_from: date, d_to: date) -> float:
    """Cumulative CDI multiplier between two dates (inclusive on the to side).

    Uses monthly accumulated CDI from BCB. Returns 1.0 if data is missing
    so the caller can fall back gracefully.
    """
    if not _CDI_MONTHLY or d_from >= d_to:
        return 1.0
    factor = 1.0
    y, m = d_from.year, d_from.month
    # Skip d_from's month — we count months that have CLOSED between
    # d_from and d_to. The month of d_from has already been partly lived
    # before the user got here.
    m += 1
    if m > 12:
        m = 1; y += 1
    while (y, m) <= (d_to.year, d_to.month):
        factor *= 1 + _CDI_MONTHLY.get((y, m), 0)
        m += 1
        if m > 12:
            m = 1; y += 1
    return factor


def _is_cdi_tracker(asset: Asset) -> bool:
    """Heuristic: assets whose AV growth between anchors should track
    CDI gross. Tesouro Selic and CDBs are the textbook cases. Tesouro
    IPCA+ and Tesouro Prefixado intentionally diverge (IPCA-linked or
    fixed-rate exposure to mark-to-market), so they keep the looser
    fallback allowance."""
    name = (asset.name or "").upper()
    if "SELIC" in name:
        return True
    if name.startswith("CDB"):
        return True
    return False

# Module-level result cache for get_timeseries. The walk is deterministic
# given (user, filters, window, granularity) and the underlying tables
# rarely change within a few minutes for a personal-finance app, so a
# moderate TTL collapses the repeated calls fired by the dashboard
# (chart, KPIs, /returns) into a single computation per cache window.
# Mutations that should drop entries (asset edit, transaction insert,
# dividend sync) call invalidate_ts_cache() to bust the cache.
_TS_CACHE_TTL_S = 600.0  # 10 min
_ts_cache: dict[tuple, tuple[float, list]] = {}


def _ts_cache_key(user_id, months, since_start, asset_ids, asset_classes,
                  group_ids, granularity, date_from, date_to) -> tuple:
    return (
        str(user_id),
        months,
        since_start,
        tuple(sorted(str(x) for x in (asset_ids or []))),
        tuple(sorted(asset_classes or [])),
        tuple(sorted(str(x) for x in (group_ids or []))),
        granularity,
        date_from.isoformat() if date_from else None,
        date_to.isoformat() if date_to else None,
    )


def invalidate_ts_cache(user_id=None) -> None:
    """Drop cached entries — call after mutations that could change the
    timeseries (asset edit, transaction insert, FX rate sync)."""
    if user_id is None:
        _ts_cache.clear()
        return
    target = str(user_id)
    for k in list(_ts_cache.keys()):
        if k and k[0] == target:
            del _ts_cache[k]


def _month_end(y: int, m: int) -> date:
    if m == 12:
        return date(y, 12, 31)
    return date(y, m + 1, 1) - timedelta(days=1)


def _iter_months(start: date, end: date):
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield (y, m)
        m += 1
        if m > 12:
            y, m = y + 1, 1


async def _user_primary_currency(session: AsyncSession,
                                 user: User) -> str:
    cur = (user.preferences or {}).get("currency_display") if user else None
    return cur or "BRL"


async def _fx_rate(session: AsyncSession, ccy_from: str,
                   ccy_to: str, on: date) -> float:
    """Returns the FX rate from ccy_from to ccy_to at `on`. 1.0 for parity
    or when no rate is available (the FX cache may not have a row for
    every (currency, date) pair). Delegates to fx_rate_service which
    knows how to invert / chain rates against the OER USD base."""
    if ccy_from == ccy_to:
        return 1.0
    try:
        _, rate = await fx_rate_service.convert(
            session, Decimal("1"), ccy_from, ccy_to, on)
        return float(rate)
    except Exception:
        return 1.0


async def _load_assets(session: AsyncSession, user_id: uuid.UUID,
                       asset_ids: Optional[list[uuid.UUID]] = None,
                       asset_classes: Optional[list[str]] = None,
                       group_ids: Optional[list[uuid.UUID]] = None,
                       include_archived: bool = True,
                       ) -> list[Asset]:
    """Load the user's assets for timeseries computation.

    include_archived defaults to True because the historical walk needs
    to see positions that have since been closed — otherwise a year
    where the user held R$ 200k in a Tesouro Selic that's now archived
    reads as "no return" instead of CDI growth. The carry-forward path
    in get_timeseries handles archived-without-transactions assets via
    the implicit-cashflow heuristic so abrupt AV jumps (undocumented
    buys) don't show as gains and the V→0 close-out doesn't show as a
    loss.
    """
    stmt = select(Asset).where(Asset.user_id == user_id)
    if not include_archived:
        stmt = stmt.where(Asset.is_archived == False)  # noqa: E712
    if asset_ids:
        stmt = stmt.where(Asset.id.in_(asset_ids))
    if asset_classes:
        stmt = stmt.where(Asset.asset_class.in_(asset_classes))
    if group_ids:
        stmt = stmt.where(Asset.group_id.in_(group_ids))
    return list((await session.execute(stmt)).scalars().all())


async def _value_at(session: AsyncSession, asset_id: uuid.UUID,
                    on: date) -> float:
    """AssetValue at month_end, carrying forward last known value."""
    stmt = (select(AssetValue)
            .where(AssetValue.asset_id == asset_id,
                   AssetValue.date <= on)
            .order_by(AssetValue.date.desc())
            .limit(1))
    row = (await session.execute(stmt)).scalar_one_or_none()
    return float(row.amount) if row else 0.0


async def _portfolio_start(session: AsyncSession,
                           user_id: uuid.UUID,
                           assets: list[Asset]) -> Optional[date]:
    """Earliest of: AssetValue.date, AssetTransaction.date, purchase_date."""
    if not assets:
        return None
    asset_ids = [a.id for a in assets]
    # Earliest AssetValue
    stmt_av = (select(AssetValue.date)
               .where(AssetValue.asset_id.in_(asset_ids))
               .order_by(AssetValue.date.asc()).limit(1))
    av_min = (await session.execute(stmt_av)).scalar_one_or_none()
    # Earliest AssetTransaction
    stmt_tx = (select(AssetTransaction.date)
               .where(AssetTransaction.asset_id.in_(asset_ids))
               .order_by(AssetTransaction.date.asc()).limit(1))
    tx_min = (await session.execute(stmt_tx)).scalar_one_or_none()
    # purchase_date fallback
    pd_min = min((a.purchase_date for a in assets if a.purchase_date),
                 default=None)
    candidates = [d for d in (av_min, tx_min, pd_min) if d is not None]
    return min(candidates) if candidates else None


async def get_twr_by_asset(session: AsyncSession, user: User,
                           months: Optional[int] = None,
                           since_start: bool = False,
                           granularity: str = "daily") -> dict[str, dict]:
    """Returns {asset_id: {twr_cum, v_start, v_end, invested}} for every
    non-archived asset of the user. Used by the Patrimônio list to render
    a Rent. TWR column without N+1 round-trips.

    Defaults to daily granularity because monthly Modified Dietz buckets
    cashflow and price action together in the same period and produces
    distorted percentages whenever a sizeable buy/sell collides with a
    volatile day (e.g. NVDA 2025-04 partial sale on a -6 % day reads as
    +21 % in the monthly bucket).

    Implementation note: we just iterate `get_timeseries` over each asset
    individually (the backend cost is dominated by AssetValue / FxRate
    lookups, not by HTTP/protocol overhead), but expose it as ONE network
    call so the frontend doesn't fan out 70 requests.
    """
    assets = await _load_assets(session, user.id)
    out: dict[str, dict] = {}
    for asset in assets:
        if asset.is_archived:
            continue
        try:
            r = await get_asset_twr(session, user, asset.id,
                                    months=months,
                                    since_start=since_start,
                                    granularity=granularity)
            invested = 0.0
            if asset.purchase_price is not None and asset.units is not None:
                invested = float(asset.purchase_price) * float(asset.units)
            out[str(asset.id)] = {
                "twr_cum": r["twr_cum"],
                "v_start": r["v_start"],
                "v_end": r["v_end"],
                "invested": round(invested, 2),
            }
        except Exception:
            logger.exception("twr-by-asset failed for asset %s", asset.id)
            continue
    return out


async def get_asset_twr(session: AsyncSession, user: User,
                        asset_id: uuid.UUID,
                        months: Optional[int] = None,
                        since_start: bool = False,
                        granularity: str = "daily") -> dict:
    """Modified Dietz TWR for a single asset over a window. Used by the
    "Rent. TWR" column in the Patrimônio list.

    Returns {"twr_cum": float, "v_start": float, "v_end": float}.
    """
    series = await get_timeseries(
        session, user,
        months=months, since_start=since_start,
        asset_ids=[asset_id],
        granularity=granularity,
    )
    if not series:
        return {"twr_cum": 0.0, "v_start": 0.0, "v_end": 0.0}
    last = series[-1]
    return {
        "twr_cum": last.get("twr_cum", 0.0) or 0.0,
        "v_start": series[0].get("v_end", 0.0),
        "v_end": last.get("v_end", 0.0),
    }


async def get_timeseries(session: AsyncSession, user: User,
                         months: Optional[int] = None,
                         since_start: bool = False,
                         asset_ids: Optional[list[uuid.UUID]] = None,
                         asset_classes: Optional[list[str]] = None,
                         group_ids: Optional[list[uuid.UUID]] = None,
                         granularity: str = "monthly",
                         date_from: Optional[date] = None,
                         date_to: Optional[date] = None,
                         ) -> list[dict]:
    """Return [{month_end, v_end, cashflow, income, return_month, twr_cum}]
    for the (optionally filtered) portfolio.

    months: window in months ending today (default 12)
    since_start: include from earliest data point
    granularity: "monthly" (one row per month-end) or "daily" (one row per
        calendar day in the window). Daily is useful for short windows
        (1–3 months) where a monthly TWR has too few points.
    date_from / date_to: explicit window override. When both are set, they
        take precedence over months/since_start. Daily granularity is forced
        for ranges shorter than 4 months.
    """
    # Lazy-load CDI history once per process (used by the implicit
    # cashflow heuristic for Tesouro Selic and CDBs without transactions).
    await _ensure_cdi_loaded()

    # Cache lookup before any DB work. Same (user, filters, window) hit
    # within the TTL gets the previous walk's result instantly.
    cache_key = _ts_cache_key(
        user.id, months, since_start, asset_ids, asset_classes,
        group_ids, granularity, date_from, date_to,
    )
    now_mono = _time.monotonic()
    cached = _ts_cache.get(cache_key)
    if cached is not None and cached[0] > now_mono:
        return cached[1]

    user_ccy = await _user_primary_currency(session, user)
    assets = await _load_assets(session, user.id, asset_ids, asset_classes,
                                group_ids)
    if not assets:
        _ts_cache[cache_key] = (now_mono + _TS_CACHE_TTL_S, [])
        return []

    # In-request FX cache — daily mode walks ~1800 days × N assets and
    # _fx_rate hits the DB (and on-demand sync) every call. Without
    # caching, "Início" daily for a multi-currency portfolio took ~30 s
    # and timed out the chart query. Hot path is just dict lookup.
    fx_cache: dict[tuple[str, str, str], float] = {}

    # Detect an empty FX table once. If there are no rows, every lookup
    # would degrade to "exact match miss → on-demand sync (fails when no
    # provider configured) → closest miss → 1.0 fallback" — three DB
    # round-trips for a guaranteed-1.0 answer. Skip them entirely.
    from app.models.fx_rate import FxRate
    from sqlalchemy import func as _sa_func
    _has_fx = (await session.execute(
        select(_sa_func.count()).select_from(FxRate)
    )).scalar_one() > 0

    async def fx(ccy_from: str, ccy_to: str, on: date) -> float:
        if ccy_from == ccy_to or not _has_fx:
            return 1.0
        key = (ccy_from, ccy_to, on.isoformat())
        cached = fx_cache.get(key)
        if cached is not None:
            return cached
        rate = await _fx_rate(session, ccy_from, ccy_to, on)
        fx_cache[key] = rate
        return rate

    # Determine window
    today = date.today()
    end_y, end_m = today.year, today.month
    custom_range = date_from is not None and date_to is not None
    if custom_range:
        # Clamp date_to to today; allow date_from to be any past date.
        end_d = min(date_to, today)
        end_y, end_m = end_d.year, end_d.month
        start_y, start_m = date_from.year, date_from.month
        # Auto-pick daily for short ranges to give a useful chart.
        if (end_d - date_from).days <= 120 and granularity == "monthly":
            granularity = "daily"
    elif since_start:
        start = await _portfolio_start(session, user.id, assets)
        if not start:
            return []
        start_y, start_m = start.year, start.month
    else:
        n = max(int(months or 12), 1)
        # Include n months ending current month (inclusive)
        m = end_m - (n - 1)
        y = end_y
        while m <= 0:
            m += 12
            y -= 1
        start_y, start_m = y, m

    # Pull all AssetTransactions for these assets. Index by either
    # year-month (monthly mode) or exact date (daily mode).
    asset_ids_use = [a.id for a in assets]
    asset_by_id = {a.id: a for a in assets}
    cf_idx: dict[tuple[uuid.UUID, str], dict] = defaultdict(
        lambda: {"buy": 0.0, "sell": 0.0, "income": 0.0})

    stmt_tx = select(AssetTransaction).where(
        AssetTransaction.user_id == user.id,
        AssetTransaction.asset_id.in_(asset_ids_use),
    )
    txs = list((await session.execute(stmt_tx)).scalars().all())

    # Pre-fetch yfinance close history for every market-priced asset
    # so we can both value cashflows at close on tx days (instead of at
    # the broker's execution price) AND drive V_end in the daily walk.
    # One fetch shared by both paths — the previous version called
    # fetch_yahoo_close_history twice for the same data and made the
    # Investments page noticeably slow on a portfolio with ~50 tickers.
    market_assets_for_tx = [a for a in assets
                            if a.valuation_method == "market_price" and a.ticker]
    # We need close prices that cover both the chart window AND every
    # tx date (so the BUY/SELL on a date OUTSIDE the window can still
    # be valued at close for cf_idx lookups inside the window when the
    # daily walk asks for it). The bounds below catch both.
    today_for_fetch = date.today()
    if granularity == "daily":
        if custom_range and date_from is not None:
            _start_d = date_from
        elif since_start:
            _start_d = date(start_y, start_m, 1)
        else:
            _start_d = today_for_fetch - timedelta(days=int(months or 1) * 30)
    else:
        _start_d = date(start_y, start_m, 1)
    _window_end = date_to if (custom_range and date_to is not None) else today_for_fetch
    tx_min = min((tx.date for tx in txs), default=_start_d)
    fetch_start = min(_start_d - timedelta(days=10), tx_min)
    history_results = await asyncio.gather(*[
        fetch_yahoo_close_history(a.ticker, fetch_start, _window_end)
        for a in market_assets_for_tx
    ]) if market_assets_for_tx else []
    price_history: dict[uuid.UUID, list[tuple[str, float]]] = {}
    for a, hist in zip(market_assets_for_tx, history_results):
        if hist:
            price_history[a.id] = sorted(hist.items())

    def _close_at(asset_id: uuid.UUID, on: date) -> Optional[float]:
        hist = price_history.get(asset_id)
        if not hist:
            return None
        target = on.isoformat()
        last: Optional[float] = None
        for d_iso, p in hist:
            if d_iso > target:
                break
            last = p
        return last

    # Convert to user_ccy on the transaction date. For market-priced BUY/
    # SELL with a real cash value we substitute (qty × close) for
    # tx.value so the cashflow is on the same basis as the daily V_end
    # (kills the bid-ask-spread blip on tx days).
    #
    # IMPORTANT — never override tx.value when tx.value is 0. That's
    # the signature of a reconciliation row written to make
    # asset.units = sum(BUY) - sum(SELL) again after a corporate
    # action we couldn't capture as a real trade (AERI3 grouping,
    # BLAU3 bonus, etc.). Those rows must stay at zero cashflow —
    # otherwise Modified Dietz reads `qty × close` as money flowing
    # in/out of the portfolio with no V change to absorb it, and
    # that registers as a +50 % daily return cap (AERI3 SELL 9697.6
    # × R$ 9.95 = R$ 96 k phantom sell on 2024-05-14, which inflated
    # the Renda Variável Brasil cumulative line by ~60 pp).
    for tx in txs:
        if tx.value is None:
            continue
        a = asset_by_id.get(tx.asset_id)
        if a is None:
            continue
        rate = await fx(a.currency, user_ccy, tx.date)
        tx_value_native = float(tx.value)
        cf_amount: Optional[float] = None
        if (a.valuation_method == "market_price"
                and tx.qty is not None
                and tx_value_native > 0
                and tx.type in (_BUY_TYPES | _SELL_TYPES)):
            close = _close_at(a.id, tx.date)
            if close is not None:
                cf_amount = float(tx.qty) * close * rate
        if cf_amount is None:
            cf_amount = tx_value_native * rate
        key = tx.date.isoformat() if granularity == "daily" else tx.date.strftime("%Y-%m")
        bucket = cf_idx[(a.id, key)]
        if tx.type in _BUY_TYPES:
            bucket["buy"] += cf_amount
        elif tx.type in _SELL_TYPES:
            bucket["sell"] += cf_amount
        elif tx.type in _INCOME_TYPES:
            bucket["income"] += tx_value_native * rate

    # Compute V_end per month per asset, summed across assets.
    # Optimization: pull AssetValues in one query, sort by asset+date,
    # walk forward.
    stmt_av = (select(AssetValue)
               .where(AssetValue.asset_id.in_(asset_ids_use))
               .order_by(AssetValue.asset_id, AssetValue.date))
    avs = list((await session.execute(stmt_av)).scalars().all())
    av_by_asset: dict[uuid.UUID, list[AssetValue]] = defaultdict(list)
    for av in avs:
        a = asset_by_id.get(av.asset_id)
        # Drop post-sell zero-value markers. push_to_securo writes a
        # `AV = 0 at last-of-month` row whenever an asset is liquidated
        # to flag "this position no longer exists". The walk picks up
        # those rows as legitimate AV anchors and treats the drop from
        # the last real value to zero as a giant cashflow event many
        # months after the actual sale — combined with the new
        # CDI-aware heuristic, that injects a phantom +44 % daily
        # return on the ghost-AV day. The actual SELL at the asset's
        # sell_date is handled below by an explicit cashflow injection.
        if (a is not None and a.is_archived and a.sell_date is not None
                and av.date > a.sell_date and float(av.amount) == 0):
            continue
        av_by_asset[av.asset_id].append(av)

    today_d = date.today()
    # When a custom date range is supplied, the chart window ends at
    # date_to (not today). Used as a cap below for daily walk + month-end
    # labels, and so the daily seed reads V_end at date_from - 1, not at
    # the calendar month start of `start_y/start_m`.
    window_end = date_to if (custom_range and date_to is not None) else today_d

    def value_at_for_asset(asset: Asset, on: date) -> float:
        """Return V_end at month-end `on` in the asset's native currency.

        For the current month (on >= today) we want the LIVE value, since
        AssetValue rows imported by push_to_securo can hold a future date
        with a stale snapshot. Patrimônio renders `last_price * units` for
        market-priced assets, so this code path mirrors that to keep the
        two views in sync.
        """
        # Cap the lookup at today so we never read a future-dated row that
        # the offline pipeline may have written when month_end > today.
        cap = min(on, today_d)
        rows = av_by_asset.get(asset.id, [])
        latest = None
        for r in rows:
            if r.date <= cap:
                latest = r
            else:
                break
        latest_amount = float(latest.amount) if latest else 0.0

        # For market-priced assets in the *current* month, prefer the
        # cached live quote (last_price * units). This matches Patrimônio.
        if (asset.valuation_method == "market_price"
                and asset.last_price is not None and asset.units is not None
                and on >= today_d):
            return float(asset.last_price) * float(asset.units)
        return latest_amount

    out: list[dict] = []
    cum = 1.0

    if granularity == "daily":
        # Daily mode: walk one row per calendar day. Useful for short
        # windows (1M, 3M) where monthly granularity gives too few points.
        # Use a rolling N*30-day window so "1M on May 2" really means the
        # last 30 days (Apr 2 → May 2), not just the calendar month-to-date.
        if custom_range and date_from is not None:
            start_d = date_from
        elif since_start:
            start_d = date(start_y, start_m, 1)
        else:
            start_d = today_d - timedelta(days=int(months or 1) * 30)

        # price_history was already populated upstream (shared with cf
        # indexing). Reuse it here.

        # Sort all transactions per asset once, ascending by date — used by
        # units_at(asset, d) to compute the share count owned at end of day d.
        tx_by_asset: dict[uuid.UUID, list[AssetTransaction]] = defaultdict(list)
        for tx in txs:
            if tx.type in _BUY_TYPES or tx.type in _SELL_TYPES:
                tx_by_asset[tx.asset_id].append(tx)
        for lst in tx_by_asset.values():
            lst.sort(key=lambda t: t.date)

        def units_at(asset: Asset, on: date) -> float:
            """Walk transactions backwards from current units, undoing those
            that happened after `on`. Returns share count owned at end of `on`.

            Floored at 0 because negative shares are nonsensical: when a
            corporate action (reverse split, delisting, group/ungrouping)
            silently reduced the position outside our transaction ledger,
            asset.units no longer matches sum(BUYs)-sum(SELLs) and the
            walk produces ghost short positions for pre-purchase dates
            (seen on AERI3.SA: 0.4 units now, 10208 BUYs, 510 SELLs;
            walking back to 2021 returns -9698 ghosts × R$ 200 ≈ R$ -1.9M
            of phantom historical value).

            Pre-history clamp: if `on` is before the earliest known signal
            (first transaction or, failing that, purchase_date), we have
            no evidence the user held the asset on `on` — return 0.
            Without this, ghost units (current asset.units > sum(BUY)-
            sum(SELL), e.g. user got 780 BLAU3 over time but only 600
            net qty are recorded as transactions) get projected back to
            the start of time. When yfinance starts returning a price for
            the ticker — typically at IPO — those ghost units suddenly
            multiply by the IPO price and inject a phantom V_end jump
            with no matching cashflow, producing a vertical step in the
            TWR curve (seen on 2021-04-19, the day BLAU3 IPO'd: 180
            ghost units × R$ 31.6 ≈ R$ 5.7 k of unexplained gain)."""
            txs = tx_by_asset.get(asset.id, [])
            earliest = txs[0].date if txs else asset.purchase_date
            if earliest is not None and on < earliest:
                return 0.0
            u = float(asset.units or 0)
            for tx in reversed(txs):
                if tx.date <= on:
                    break
                q = float(tx.qty or 0)
                if tx.type in _BUY_TYPES:
                    u -= q
                elif tx.type in _SELL_TYPES:
                    u += q
            return max(u, 0.0)

        def price_at(asset: Asset, on: date) -> Optional[float]:
            """Most recent close <= on. None if no history fetched/available."""
            hist = price_history.get(asset.id)
            if not hist:
                return None
            target = on.isoformat()
            last: Optional[float] = None
            for d_iso, p in hist:
                if d_iso > target:
                    break
                last = p
            return last

        def daily_v_native(asset: Asset, on: date) -> float:
            """Preferred daily-mode value: units * yfinance close. Falls back
            to the AssetValue snapshot when no history is available (RF and
            non-market-priced assets, or if the fetch failed)."""
            if asset.valuation_method == "market_price":
                p = price_at(asset, on)
                if p is not None:
                    return units_at(asset, on) * p
            return value_at_for_asset(asset, on)

        # Seed prev_v_end with the portfolio value the day before window start.
        # Per-asset "carried" state for non-market-priced assets (RF, manual
        # valuation). The AssetValue row only appears at month-end, so a
        # mid-month BUY/SELL is invisible to a naive lookup. We carry a base
        # value forward day-by-day, absorbing the day's cashflow, and jump
        # to a fresh AV only when its date is *strictly after* the AV we
        # last anchored on. Without the date check, the AV row from
        # before a sell would clobber the cashflow-adjusted base on the
        # very next iteration (seen on a 2023-08-02 RF sell of R$ 30 789
        # that read as +20 % on the cumulative TWR).
        asset_state: dict[uuid.UUID, dict] = {}
        seed_d = start_d - timedelta(days=1)
        for a in assets:
            # Sold-and-gone clamp: if the asset was sold before the
            # window opens, seed at zero. Otherwise the carry-forward
            # picks up the last AV before the sell (the position's
            # gross value the day it was liquidated) and projects it
            # into the window as phantom V_start. Tesouro Selic 2024
            # sold in 2022 was reading R$ 56 k of imaginary patrimony
            # at window start of 2025-05; combined with two other
            # archived Selics, that put R$ 395 k of ghost capital
            # into the Renda Fixa denominator and dragged the RF
            # rentabilidade visibly below CDI.
            if (a.is_archived and a.sell_date is not None
                    and a.sell_date <= seed_d):
                asset_state[a.id] = {"base": 0.0, "av_date": a.sell_date}
                continue
            rows = av_by_asset.get(a.id, [])
            initial_v = 0.0
            initial_av_date = None
            for r in rows:
                if r.date <= seed_d:
                    initial_v = float(r.amount)
                    initial_av_date = r.date
                else:
                    break
            asset_state[a.id] = {"base": initial_v, "av_date": initial_av_date}

        prev_d = start_d - timedelta(days=1)
        prev_v_end = 0.0
        for a in assets:
            v_native = daily_v_native(a, prev_d)
            if v_native == 0:
                continue
            rate = await fx(a.currency, user_ccy, prev_d)
            prev_v_end += v_native * rate

        d = start_d
        while d <= window_end:
            iso = d.isoformat()
            v_end_total = 0.0
            per_class: dict[str, float] = defaultdict(float)
            cf_buy = cf_sell = inc = 0.0

            for a in assets:
                # Per-asset cashflow on this day (already in user_ccy, but
                # for non-market we approximate as native — works when
                # asset.currency == user_ccy, which holds for every RF
                # ticker today; market_price assets don't use this path).
                bucket = cf_idx.get((a.id, iso)) or {}
                a_buy = bucket.get("buy", 0.0)
                a_sell = bucket.get("sell", 0.0)
                a_inc = bucket.get("income", 0.0)
                cf_buy += a_buy
                cf_sell += a_sell
                inc += a_inc
                a_cf_user = a_buy - a_sell

                if (a.valuation_method == "market_price"
                        and a.id in price_history):
                    # Market-priced assets: units_at × yfinance close.
                    v_native = daily_v_native(a, d)
                    rate = await fx(a.currency, user_ccy, d)
                    v_user = v_native * rate
                else:
                    # Sparse-AV assets (RF / manual): carry a base value
                    # forward, absorbing today's net cashflow. Anchor to a
                    # new AV row only when its date is strictly after the
                    # last anchor — at that point it represents fresh
                    # ground-truth (interest accrued, broker reconciled
                    # the prior buys/sells). Same-or-older AV rows are
                    # ignored so they can't undo a recent cashflow.
                    state = asset_state.setdefault(a.id, {"base": 0.0, "av_date": None})
                    state["base"] += a_cf_user
                    rows = av_by_asset.get(a.id, [])
                    last_av_date = state["av_date"]
                    new_av_amount = None
                    new_av_date = None
                    for r in rows:
                        if r.date > d:
                            break
                        if last_av_date is None or r.date > last_av_date:
                            new_av_amount = float(r.amount)
                            new_av_date = r.date
                    if new_av_amount is not None:
                        # Implausible-jump heuristic for assets without
                        # transactions (typically archived RF imported
                        # via push_to_securo where the offline pipeline
                        # populates monthly AVs but no BUY/SELL rows).
                        #
                        # When the asset is a CDI tracker (Tesouro Selic,
                        # CDB) we use the actual BCB CDI between the two
                        # AV anchors as the "expected organic growth" —
                        # any excess over CDI is a hidden cashflow. The
                        # looser fallback (0.1 %/day allowance) is only
                        # used when CDI data isn't available or for
                        # non-CDI-tracker assets (Tesouro IPCA+,
                        # Prefixado) whose AV moves driven by rate-cycle
                        # mark-to-market shouldn't be re-attributed to
                        # cashflow.
                        if not tx_by_asset.get(a.id):
                            base_before = state["base"]
                            elapsed_days = (
                                (new_av_date - last_av_date).days
                                if last_av_date else 30
                            )
                            elapsed_days = max(elapsed_days, 1)

                            if _is_cdi_tracker(a) and _CDI_MONTHLY and last_av_date:
                                cdi_growth = base_before * (
                                    _cdi_factor_between(last_av_date, new_av_date) - 1
                                )
                                expected_v = base_before + cdi_growth
                                # Allow ±2 % of base around CDI for noise
                                # (Selic spread, custódia fees, etc.).
                                tol = max(abs(base_before) * 0.02, 50.0)
                                diff_excess = new_av_amount - expected_v
                                if abs(diff_excess) > tol:
                                    if diff_excess > 0:
                                        cf_buy += diff_excess
                                    else:
                                        cf_sell += -diff_excess
                            else:
                                allow_growth = abs(base_before) * 0.001 * elapsed_days
                                allow_growth = min(allow_growth, abs(base_before) * 0.25)
                                allow_growth = max(allow_growth, 50.0)
                                diff = new_av_amount - base_before
                                if abs(diff) > allow_growth:
                                    if diff > 0:
                                        cf_buy += diff
                                    else:
                                        cf_sell += -diff
                        state["base"] = new_av_amount
                        state["av_date"] = new_av_date

                    # Explicit liquidation event on sell_date for archived
                    # assets without transactions. The AVs we keep go up
                    # to the sell month; the post-sell zero markers were
                    # filtered out above. Without this injection the
                    # asset's value would just keep carrying forward at
                    # the last AV indefinitely (Tesouro Selic 2025 sold
                    # in Feb 2025 was reading R$ 127 k of patrimony all
                    # the way through May 2026).
                    if (a.is_archived and a.sell_date is not None
                            and d == a.sell_date and not tx_by_asset.get(a.id)
                            and state["base"] > 0):
                        cf_sell += state["base"]
                        state["base"] = 0.0
                    base = state["base"]
                    rate = await fx(a.currency, user_ccy, d)
                    v_user = base * rate

                if v_user == 0:
                    continue
                v_end_total += v_user
                per_class[a.asset_class or "OUTRO"] += v_user

            cf = cf_buy - cf_sell

            denom = prev_v_end + 0.5 * cf
            if denom <= 1e-3:
                r_d: Optional[float] = 0.0 if (v_end_total + inc) <= 1e-3 else None
            else:
                r_d = (v_end_total + inc - prev_v_end - cf) / denom

            if r_d is not None:
                # Per-day cap is tighter than monthly: ±50%/day handles even
                # extreme single-day moves and clips obvious data glitches.
                cum *= (1.0 + max(min(r_d, 0.5), -0.5))

            out.append({
                "month_end": iso,
                "month": d.strftime("%Y-%m"),
                "v_end": round(v_end_total, 2),
                "cashflow": round(cf, 2),
                "income": round(inc, 2),
                "return_month": round(r_d, 6) if r_d is not None else None,
                "twr_cum": round(cum - 1.0, 6),
                "by_class": {k: round(v, 2) for k, v in per_class.items()},
            })
            prev_v_end = v_end_total
            d += timedelta(days=1)

        _ts_cache[cache_key] = (now_mono + _TS_CACHE_TTL_S, out)
        return out

    # Monthly mode (default): walk one row per month-end.
    # Seed prev_v_end with the portfolio value at the month-end *before* the
    # window starts. Otherwise, the first month's Modified Dietz return uses
    # prev_v_end=0 even though the user already owned assets, which inflates
    # the cumulative TWR by the entire pre-window value.
    if start_m == 1:
        prev_y, prev_m = start_y - 1, 12
    else:
        prev_y, prev_m = start_y, start_m - 1
    prev_me = _month_end(prev_y, prev_m)
    prev_v_end = 0.0
    for a in assets:
        v_native = value_at_for_asset(a, prev_me)
        if v_native == 0:
            continue
        rate = await fx(a.currency, user_ccy, prev_me)
        prev_v_end += v_native * rate

    for (y, m) in _iter_months(date(start_y, start_m, 1),
                                date(end_y, end_m, 1)):
        me = _month_end(y, m)
        # Cap the chart label at today so we don't render a future
        # month-end as if it had already happened. The cashflow / value
        # math above uses `me` (and value_at_for_asset already caps at
        # today), so only the display label needs trimming here.
        label_date = min(me, window_end)
        ym = me.strftime("%Y-%m")
        # Sum V_end across assets, converting each to user_ccy
        v_end_total = 0.0
        per_class: dict[str, float] = defaultdict(float)
        for a in assets:
            v_native = value_at_for_asset(a, me)
            if v_native == 0:
                continue
            rate = await fx(a.currency, user_ccy, me)
            v_user = v_native * rate
            v_end_total += v_user
            per_class[a.asset_class or "OUTRO"] += v_user

        # Sum cashflows in this month for our assets
        cf_buy = cf_sell = inc = 0.0
        for a in assets:
            b = cf_idx.get((a.id, ym))
            if b:
                cf_buy += b["buy"]
                cf_sell += b["sell"]
                inc += b["income"]
        cf = cf_buy - cf_sell

        # Modified Dietz return
        denom = prev_v_end + 0.5 * cf
        if denom <= 1e-3:
            r_m: Optional[float] = 0.0 if (v_end_total + inc) <= 1e-3 else None
        else:
            r_m = (v_end_total + inc - prev_v_end - cf) / denom

        if r_m is not None:
            cum *= (1.0 + max(min(r_m, 5.0), -0.95))

        out.append({
            "month_end": label_date.isoformat(),
            "month": me.strftime("%Y-%m"),
            "v_end": round(v_end_total, 2),
            "cashflow": round(cf, 2),
            "income": round(inc, 2),
            "return_month": round(r_m, 6) if r_m is not None else None,
            "twr_cum": round(cum - 1.0, 6),
            "by_class": {k: round(v, 2) for k, v in per_class.items()},
        })
        prev_v_end = v_end_total

    _ts_cache[cache_key] = (now_mono + _TS_CACHE_TTL_S, out)
    return out
