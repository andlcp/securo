"""Benchmark and portfolio-return calculations for the Investments page.

Fetches CDI (BACEN), IBOV and S&P 500 (Yahoo Finance) and computes
cumulative percentage returns so the frontend can overlay them on a chart.
Also computes per-group and per-asset-class returns from the assets table.
"""

import asyncio
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.asset import Asset
from app.models.asset_group import AssetGroup

logger = logging.getLogger(__name__)

BACEN_CDI_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados?dataInicial={start}&dataFinal={end}&formato=json"
YAHOO_BASE = "https://query1.finance.yahoo.com/v8/finance/chart"
YAHOO_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# Known ETF tickers (B3) — everything else ending in 11 is treated as FII
_ETF_TICKERS_B3 = {
    "IVVB11", "BOVA11", "SMAL11", "SPXI11", "HASH11", "GOLD11",
    "NTNB11", "IRFM11", "DIVO11", "FIND11", "GOVE11", "MATB11",
    "BOVB11", "BOVS11", "BOVV11", "ECOO11", "ISUS11", "PIBB11",
}


def detect_asset_class(ticker: Optional[str], name: str) -> str:
    """Heuristic asset class from ticker / name."""
    if not ticker:
        return "Fundo/RF"
    t = ticker.upper().replace(".SA", "")
    if t in ("BTC-USD", "ETH-USD", "BTC", "ETH"):
        return "Cripto"
    if t in _ETF_TICKERS_B3:
        return "ETF"
    if t.endswith("11"):
        return "FII"
    return "Ação"


# ─── External data fetching ───────────────────────────────────────────────────

async def _fetch_cdi(start: date, end: date) -> list[dict]:
    """Cumulative CDI % return series from BACEN for a date range."""
    url = BACEN_CDI_URL.format(
        start=start.strftime("%d/%m/%Y"),
        end=end.strftime("%d/%m/%Y"),
    )
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url)
            r.raise_for_status()
            raw = r.json()
        if not raw:
            return []
        cumulative = 1.0
        result = []
        for entry in raw:
            cumulative *= 1 + float(entry["valor"]) / 100
            result.append({"date": entry["data"], "value": round((cumulative - 1) * 100, 4)})
        return result
    except Exception as exc:
        logger.warning("BACEN CDI fetch failed: %s", exc)
        return []


async def fetch_yahoo_dividends(symbol: str, start: date, end: date) -> list[dict]:
    """Historical dividend events from Yahoo Finance for a symbol/date range.

    Returns [{date: "YYYY-MM-DD", amount: float}, ...] sorted chronologically.
    Yahoo lumps Brazilian JCP and ordinary dividends together into the
    `dividends` event stream — that's fine for TWR purposes since Modified
    Dietz only cares about the cash amount, not the tax label.
    """
    period1 = int(datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc).timestamp())
    period2 = int(datetime.combine(end + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc).timestamp())
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            r = await client.get(
                f"{YAHOO_BASE}/{symbol}",
                params={
                    "interval": "1d",
                    "events": "div",
                    "period1": period1,
                    "period2": period2,
                },
                headers=YAHOO_HEADERS,
            )
            r.raise_for_status()
            data = r.json()
        result = data.get("chart", {}).get("result") or []
        if not result:
            return []
        events = result[0].get("events", {}) or {}
        divs = events.get("dividends", {}) or {}
        out: list[dict] = []
        for entry in divs.values():
            ts = entry.get("date")
            amount = entry.get("amount")
            if ts is None or amount is None:
                continue
            try:
                d = datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat()
                out.append({"date": d, "amount": float(amount)})
            except (TypeError, ValueError):
                continue
        out.sort(key=lambda e: e["date"])
        return out
    except Exception as exc:
        logger.warning("Yahoo dividends %s fetch failed: %s", symbol, exc)
        return []


async def fetch_yahoo_close_history(symbol: str, start: date, end: date) -> dict[str, float]:
    """Daily unadjusted close prices for a symbol over a date range.

    Returns {iso_date: close} keyed on the trading day. Non-business days
    are absent — callers should hold-last-known when filling gaps.
    Used by the portfolio timeseries to compute daily V_end for
    market-priced assets without relying on sparse AssetValue snapshots.
    """
    period1 = int(datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc).timestamp())
    period2 = int(datetime.combine(end + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc).timestamp())
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            r = await client.get(
                f"{YAHOO_BASE}/{symbol}",
                params={"interval": "1d", "period1": period1, "period2": period2},
                headers=YAHOO_HEADERS,
            )
            r.raise_for_status()
            data = r.json()
        chart = data["chart"]["result"][0]
        timestamps = chart["timestamp"]
        closes = chart["indicators"]["quote"][0]["close"]
        out: dict[str, float] = {}
        for ts, close in zip(timestamps, closes):
            if close is None:
                continue
            d = datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
            out[d] = float(close)
        return out
    except Exception as exc:
        logger.warning("Yahoo history %s fetch failed: %s", symbol, exc)
        return {}


async def _fetch_yahoo_index(symbol: str, start: date, end: date) -> list[dict]:
    """Normalised % return series (base=0) from Yahoo Finance using Unix timestamps."""
    period1 = int(datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc).timestamp())
    period2 = int(datetime.combine(end, datetime.min.time(), tzinfo=timezone.utc).timestamp())
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            r = await client.get(
                f"{YAHOO_BASE}/{symbol}",
                params={"interval": "1d", "period1": period1, "period2": period2},
                headers=YAHOO_HEADERS,
            )
            r.raise_for_status()
            data = r.json()
        chart = data["chart"]["result"][0]
        timestamps = chart["timestamp"]
        closes = chart["indicators"]["quote"][0]["close"]
        valid = [(ts, c) for ts, c in zip(timestamps, closes) if c is not None]
        if not valid:
            return []
        base = valid[0][1]
        return [
            {
                "date": datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%d/%m/%Y"),
                "value": round((close - base) / base * 100, 4),
            }
            for ts, close in valid
        ]
    except Exception as exc:
        logger.warning("Yahoo %s fetch failed: %s", symbol, exc)
        return []


async def get_portfolio_start_date(session: AsyncSession, user_id) -> Optional[date]:
    """Earliest purchase_date across ALL of the user's assets (including
    sold/archived). Used to anchor the benchmark fetch for the lifetime
    chart — we want IBOV/CDI/S&P to span the entire walked history,
    even for periods when the user only held positions that have since
    been closed."""
    result = await session.execute(
        select(func.min(Asset.purchase_date)).where(
            Asset.user_id == user_id,
            Asset.purchase_date.is_not(None),
        )
    )
    return result.scalar()


async def get_benchmark_series(
    months: int = 12,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> dict:
    """Return CDI, IBOV and S&P 500 cumulative return series (fetched concurrently)."""
    today = date.today()
    end = min(end_date, today) if end_date else today
    start = start_date if start_date else today - timedelta(days=months * 31)
    cdi, ibov, sp500 = await asyncio.gather(
        _fetch_cdi(start, end),
        _fetch_yahoo_index("%5EBVSP", start, end),
        _fetch_yahoo_index("%5EGSPC", start, end),
    )
    return {"cdi": cdi, "ibov": ibov, "sp500": sp500}


# ─── Portfolio return calculations ───────────────────────────────────────────

def _asset_return(asset: Asset) -> tuple[float, float]:
    """Return (total_invested, current_value) for one asset in its native currency."""
    units = float(asset.units or 0)
    avg = float(asset.purchase_price or 0)
    invested = units * avg

    if asset.valuation_method == "market_price" and asset.last_price is not None:
        current = units * float(asset.last_price)
    else:
        current = invested
    return invested, current


def _window_return_from_series(series: list[dict]) -> dict:
    """Pick V_start, V_end and rebased TWR from a portfolio_timeseries series.

    The series is the same monthly Modified-Dietz output used by the chart.
    return_pct = ((1 + last.twr_cum) / (1 + first.twr_cum) - 1) * 100, which
    is the window-rebased percentage that matches the chart's last point."""
    if not series:
        return {"invested": 0.0, "current": 0.0, "return_pct": None}
    first = series[0]
    last = series[-1]
    base_factor = 1.0 + float(first.get("twr_cum") or 0.0)
    end_factor = 1.0 + float(last.get("twr_cum") or 0.0)
    if base_factor <= 1e-9:
        return {
            "invested": round(float(first.get("v_end") or 0.0), 2),
            "current": round(float(last.get("v_end") or 0.0), 2),
            "return_pct": None,
        }
    rebased = (end_factor / base_factor - 1.0) * 100.0
    return {
        "invested": round(float(first.get("v_end") or 0.0), 2),
        "current": round(float(last.get("v_end") or 0.0), 2),
        "return_pct": round(rebased, 2),
    }


async def get_portfolio_returns(
    session: AsyncSession,
    user_id,
    user=None,
    group_ids: Optional[list[str]] = None,
    months: int = 12,
    since_start: bool = False,
    date_from=None,
    date_to=None,
) -> dict:
    """Compute window-aware TWR returns for the consolidated portfolio,
    each AssetGroup, and each distinct asset_class in the user's
    holdings. Reuses portfolio_timeseries.get_timeseries (Modified
    Dietz) so the percentages match what the chart line shows.

    Per-group/class TWR comes from one timeseries call per bucket. The
    cost is N+M+1 calls, but each is a small in-process computation —
    fine for ~5–10 groups + ~5 classes typical of a personal portfolio.
    """
    # Local import to avoid a hard cycle with portfolio_timeseries_service,
    # which imports `fetch_yahoo_close_history` from this file.
    from app.services import portfolio_timeseries_service
    from app.models.user import User

    if user is None:
        # Older callers passed only user_id. Fetch the User row so the
        # timeseries service can read the FX-display preference.
        user = (await session.execute(
            select(User).where(User.id == user_id)
        )).scalar_one_or_none()
        if user is None:
            return {
                "consolidated": {"invested": 0.0, "current": 0.0, "return_pct": None},
                "by_group": [],
                "by_class": [],
            }

    import uuid as _uuid
    parsed_group_ids = None
    if group_ids:
        parsed_group_ids = [_uuid.UUID(g) for g in group_ids if g]

    # Discover the assets that match the (optional) group filter so we can
    # enumerate the relevant groups/classes for the per-bucket calls.
    asset_stmt = (
        select(Asset)
        .where(Asset.user_id == user_id, Asset.is_archived.is_(False),
               Asset.sell_date.is_(None))
    )
    if parsed_group_ids:
        asset_stmt = asset_stmt.where(Asset.group_id.in_(parsed_group_ids))
    assets = list((await session.execute(asset_stmt)).scalars().all())

    group_rows = await session.execute(
        select(AssetGroup).where(AssetGroup.user_id == user_id)
    )
    groups_by_id = {str(g.id): g.name for g in group_rows.scalars().all()}

    # Distinct group_ids and asset_classes actually present in the filter.
    present_group_ids: list[str] = []
    seen_groups: set[str] = set()
    present_classes: list[str] = []
    seen_classes: set[str] = set()
    for a in assets:
        if a.group_id:
            gid = str(a.group_id)
            if gid not in seen_groups:
                seen_groups.add(gid)
                present_group_ids.append(gid)
        cls = a.asset_class or "OUTRO"
        if cls not in seen_classes:
            seen_classes.add(cls)
            present_classes.append(cls)

    async def _series_for(filter_groups=None, filter_classes=None):
        return await portfolio_timeseries_service.get_timeseries(
            session, user,
            months=months,
            since_start=since_start,
            asset_classes=filter_classes,
            group_ids=filter_groups,
            date_from=date_from,
            date_to=date_to,
            granularity="daily",
        )

    # Consolidated (respect the incoming group_ids filter — same as before).
    consolidated_series = await _series_for(filter_groups=parsed_group_ids)
    consolidated = _window_return_from_series(consolidated_series)

    # By group — one call per group present in the filtered set.
    by_group_out = []
    for gid in present_group_ids:
        try:
            g_uuid = _uuid.UUID(gid)
        except ValueError:
            continue
        s = await _series_for(filter_groups=[g_uuid])
        result = _window_return_from_series(s)
        by_group_out.append({
            "id": gid,
            "name": groups_by_id.get(gid, "Sem carteira"),
            **result,
        })

    # By class — one call per asset_class present in the filtered set.
    by_class_out = []
    for cls in present_classes:
        s = await _series_for(filter_classes=[cls])
        result = _window_return_from_series(s)
        by_class_out.append({"name": cls, **result})
    # Largest current value first, matching the previous ordering.
    by_class_out.sort(key=lambda x: -(x.get("current") or 0))

    return {
        "consolidated": consolidated,
        "by_group": by_group_out,
        "by_class": by_class_out,
    }
