"""Benchmark and portfolio-return calculations for the Investments page.

Fetches CDI (BACEN), IBOV and S&P 500 (Yahoo Finance) and computes
cumulative percentage returns so the frontend can overlay them on a chart.
Also computes per-group and per-asset-class returns from the assets table.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.asset import Asset
from app.models.asset_group import AssetGroup

logger = logging.getLogger(__name__)

BACEN_CDI_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados/ultimos/{days}?formato=json"
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

async def _fetch_cdi(months: int) -> list[dict]:
    """Cumulative CDI % return series from BACEN."""
    days = min(months * 23, 500)
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(BACEN_CDI_URL.format(days=days))
            r.raise_for_status()
            raw = r.json()
        cumulative = 1.0
        result = []
        for entry in raw:
            cumulative *= 1 + float(entry["valor"]) / 100
            result.append({"date": entry["data"], "value": round((cumulative - 1) * 100, 4)})
        return result
    except Exception as exc:
        logger.warning("BACEN CDI fetch failed: %s", exc)
        return []


async def _fetch_yahoo_index(symbol: str, months: int) -> list[dict]:
    """Normalised % return series (base=0) from Yahoo Finance."""
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            r = await client.get(
                f"{YAHOO_BASE}/{symbol}",
                params={"interval": "1d", "range": f"{months}mo"},
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


async def get_benchmark_series(months: int = 12) -> dict:
    """Return CDI, IBOV and S&P 500 cumulative return series (fetched concurrently)."""
    cdi, ibov, sp500 = await asyncio.gather(
        _fetch_cdi(months),
        _fetch_yahoo_index("%5EBVSP", months),
        _fetch_yahoo_index("%5EGSPC", months),
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
        # Fall back to most recent manual value via purchase_price (approximation)
        current = invested
    return invested, current


async def get_portfolio_returns(
    session: AsyncSession,
    user_id,
    group_ids: Optional[list[str]] = None,
) -> dict:
    """Compute returns per group, per asset class, and consolidated.

    Returns a structure ready for the frontend to display reference lines
    alongside the benchmark time series.
    """
    # Fetch all active assets for the user
    stmt = (
        select(Asset)
        .where(Asset.user_id == user_id, Asset.is_archived.is_(False), Asset.sell_date.is_(None))
    )
    if group_ids:
        import uuid as _uuid
        ids = [_uuid.UUID(g) for g in group_ids if g]
        stmt = stmt.where(Asset.group_id.in_(ids))

    result = await session.execute(stmt)
    assets = list(result.scalars().all())

    # Fetch group names
    group_rows = await session.execute(
        select(AssetGroup).where(AssetGroup.user_id == user_id)
    )
    groups_by_id = {str(g.id): g.name for g in group_rows.scalars().all()}

    # Aggregate
    by_group: dict[str, dict] = {}
    by_class: dict[str, dict] = {}
    total_invested = total_current = 0.0

    for asset in assets:
        invested, current = _asset_return(asset)
        total_invested += invested
        total_current += current

        # By group
        gid = str(asset.group_id) if asset.group_id else "_ungrouped"
        gname = groups_by_id.get(gid, "Sem carteira")
        if gid not in by_group:
            by_group[gid] = {"id": gid, "name": gname, "invested": 0.0, "current": 0.0}
        by_group[gid]["invested"] += invested
        by_group[gid]["current"] += current

        # By asset class
        cls = detect_asset_class(asset.ticker, asset.name)
        if cls not in by_class:
            by_class[cls] = {"name": cls, "invested": 0.0, "current": 0.0}
        by_class[cls]["invested"] += invested
        by_class[cls]["current"] += current

    def _pct(inv: float, cur: float) -> Optional[float]:
        if inv <= 0:
            return None
        return round((cur - inv) / inv * 100, 2)

    groups_out = [
        {
            "id": v["id"],
            "name": v["name"],
            "invested": round(v["invested"], 2),
            "current": round(v["current"], 2),
            "return_pct": _pct(v["invested"], v["current"]),
        }
        for v in by_group.values()
    ]

    classes_out = [
        {
            "name": v["name"],
            "invested": round(v["invested"], 2),
            "current": round(v["current"], 2),
            "return_pct": _pct(v["invested"], v["current"]),
        }
        for v in sorted(by_class.values(), key=lambda x: -x["current"])
    ]

    return {
        "consolidated": {
            "invested": round(total_invested, 2),
            "current": round(total_current, 2),
            "return_pct": _pct(total_invested, total_current),
        },
        "by_group": groups_out,
        "by_class": classes_out,
    }
