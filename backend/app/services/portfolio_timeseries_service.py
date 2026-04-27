"""Portfolio time series — Modified Dietz TWR computed live from
asset_transactions + asset_values + assets.

Replaces the parallel `portfolio_snapshots` table as the source for the
investments dashboard chart. Reading from the same tables that Patrimônio
edits guarantees that every change in the UI is reflected in the chart.

The math (per month, per "bucket" — could be all assets, a class, or a
specific asset id):
    V_start_m  = sum(AssetValue.amount at month_end_{m-1}) for assets in bucket
    V_end_m    = sum(AssetValue.amount at month_end_m)     for assets in bucket
    CF_m       = sum(BUY.value)  - sum(SELL.value)         in month m
    INC_m      = sum(DIVIDEND.value + JCP.value + RENDIMENTO.value
                    + RESGATE.value)
    r_m        = (V_end_m + INC_m - V_start_m - CF_m) / (V_start_m + 0.5*CF_m)
    TWR_cum_m  = product over m of (1 + r_m) - 1

For window normalization the frontend rebases each line to 0% at the
start of the chosen window — we just return the cumulative series here,
the UI does the math (see investments.tsx).

NOTE: For a multi-currency portfolio (USD assets) the AssetValue.amount
must be expressed in the asset's currency. Conversion to the user's
display currency uses the FX cache (FxRate table) at the closing date
of each month. This is consistent with how Securo's Net Worth report
already handles multi-currency.
"""

import logging
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

logger = logging.getLogger(__name__)


_INCOME_TYPES = {"DIVIDEND", "JCP", "RENDIMENTO", "RESGATE", "INTEREST"}
_BUY_TYPES = {"BUY", "DEPOSIT"}
_SELL_TYPES = {"SELL", "WITHDRAWAL"}


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
                       group_ids: Optional[list[uuid.UUID]] = None
                       ) -> list[Asset]:
    stmt = select(Asset).where(
        Asset.user_id == user_id,
        Asset.is_archived == False,    # noqa: E712
    )
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
                           since_start: bool = False) -> dict[str, dict]:
    """Returns {asset_id: {twr_cum, v_start, v_end, invested}} for every
    non-archived asset of the user. Used by the Patrimônio list to render
    a Rent. TWR column without N+1 round-trips.

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
                                    months=months, since_start=since_start)
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
                        since_start: bool = False) -> dict:
    """Modified Dietz TWR for a single asset over a window. Used by the
    "Rent. TWR" column in the Patrimônio list.

    Returns {"twr_cum": float, "v_start": float, "v_end": float}.
    """
    series = await get_timeseries(
        session, user,
        months=months, since_start=since_start,
        asset_ids=[asset_id],
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
                         ) -> list[dict]:
    """Return [{month_end, v_end, cashflow, income, return_month, twr_cum}]
    for the (optionally filtered) portfolio.

    months: window in months ending today (default 12)
    since_start: include from earliest data point
    """
    user_ccy = await _user_primary_currency(session, user)
    assets = await _load_assets(session, user.id, asset_ids, asset_classes,
                                group_ids)
    if not assets:
        return []

    # Determine window
    today = date.today()
    end_y, end_m = today.year, today.month
    if since_start:
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

    # Pull all AssetTransactions in window for these assets, indexed by
    # (asset_id, ym).
    asset_ids_use = [a.id for a in assets]
    asset_by_id = {a.id: a for a in assets}
    cf_idx: dict[tuple[uuid.UUID, str], dict] = defaultdict(
        lambda: {"buy": 0.0, "sell": 0.0, "income": 0.0})

    stmt_tx = select(AssetTransaction).where(
        AssetTransaction.user_id == user.id,
        AssetTransaction.asset_id.in_(asset_ids_use),
    )
    txs = list((await session.execute(stmt_tx)).scalars().all())
    # Convert to user_ccy on the transaction date
    for tx in txs:
        if tx.value is None:
            continue
        a = asset_by_id.get(tx.asset_id)
        if a is None:
            continue
        rate = await _fx_rate(session, a.currency, user_ccy, tx.date)
        amount = float(tx.value) * rate
        ym = tx.date.strftime("%Y-%m")
        bucket = cf_idx[(a.id, ym)]
        if tx.type in _BUY_TYPES:
            bucket["buy"] += amount
        elif tx.type in _SELL_TYPES:
            bucket["sell"] += amount
        elif tx.type in _INCOME_TYPES:
            bucket["income"] += amount

    # Compute V_end per month per asset, summed across assets.
    # Optimization: pull AssetValues in one query, sort by asset+date,
    # walk forward.
    stmt_av = (select(AssetValue)
               .where(AssetValue.asset_id.in_(asset_ids_use))
               .order_by(AssetValue.asset_id, AssetValue.date))
    avs = list((await session.execute(stmt_av)).scalars().all())
    av_by_asset: dict[uuid.UUID, list[AssetValue]] = defaultdict(list)
    for av in avs:
        av_by_asset[av.asset_id].append(av)

    today_d = date.today()

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

    # Walk months
    out: list[dict] = []
    cum = 1.0
    prev_v_end = 0.0

    for (y, m) in _iter_months(date(start_y, start_m, 1),
                                date(end_y, end_m, 1)):
        me = _month_end(y, m)
        ym = me.strftime("%Y-%m")
        # Sum V_end across assets, converting each to user_ccy
        v_end_total = 0.0
        per_class: dict[str, float] = defaultdict(float)
        for a in assets:
            v_native = value_at_for_asset(a, me)
            if v_native == 0:
                continue
            rate = await _fx_rate(session, a.currency, user_ccy, me)
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
            "month_end": me.isoformat(),
            "month": me.strftime("%Y-%m"),
            "v_end": round(v_end_total, 2),
            "cashflow": round(cf, 2),
            "income": round(inc, 2),
            "return_month": round(r_m, 6) if r_m is not None else None,
            "twr_cum": round(cum - 1.0, 6),
            "by_class": {k: round(v, 2) for k, v in per_class.items()},
        })
        prev_v_end = v_end_total

    return out
