"""Per-asset historical price endpoint — feeds the cotação chart in the
asset detail panel.

Hits Yahoo Finance's public chart API directly (same source the rest of
the app uses for live quotes). Cached at the route level by FastAPI's
default behavior; given the volume (one chart open at a time) and the
typical ~50-200 KB response, no extra caching layer is necessary.

Returns an empty array for non-market_priced assets.
"""

import datetime as dt
import json
import logging
import urllib.error
import urllib.request
import uuid
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import current_active_user
from app.core.database import get_async_session
from app.models.asset import Asset
from app.models.user import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/assets", tags=["asset-prices"])

YAHOO_URL = ("https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
             "?period1={p1}&period2={p2}&interval={interval}")
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; Securo/1.0)"}


_RANGE_TO_INTERVAL = {
    "1mo": ("1d", 32),
    "3mo": ("1d", 95),
    "6mo": ("1d", 190),
    "1y":  ("1d", 366),
    "2y":  ("1wk", 366 * 2),
    "5y":  ("1wk", 366 * 5),
    "max": ("1mo", 366 * 30),
}


def _fetch_yahoo_history(symbol: str, days: int, interval: str
                         ) -> list[dict]:
    end = dt.datetime.now(dt.timezone.utc)
    start = end - dt.timedelta(days=days)
    p1 = int(start.timestamp())
    p2 = int(end.timestamp())
    url = YAHOO_URL.format(symbol=symbol, p1=p1, p2=p2, interval=interval)
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            j = json.load(r)
    except urllib.error.HTTPError as e:
        if e.code in (404, 400):
            return []
        raise
    res = j.get("chart", {}).get("result") or [{}]
    res = res[0]
    if not res:
        return []
    timestamps = res.get("timestamp") or []
    quote = (res.get("indicators", {}).get("quote") or [{}])[0]
    closes = quote.get("close") or []
    out = []
    for ts, c in zip(timestamps, closes):
        if c is None:
            continue
        d = dt.datetime.fromtimestamp(int(ts), dt.timezone.utc).date()
        out.append({"date": d.isoformat(), "close": round(float(c), 4)})
    return out


@router.get("/{asset_id}/price-history")
async def get_price_history(
    asset_id: uuid.UUID,
    range: str = Query("1y", description="1mo|3mo|6mo|1y|2y|5y|max"),
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    asset = (await session.execute(
        select(Asset).where(Asset.id == asset_id, Asset.user_id == user.id)
    )).scalar_one_or_none()
    if not asset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Asset not found")

    if not asset.ticker or asset.valuation_method != "market_price":
        return {"asset_id": str(asset_id), "ticker": asset.ticker,
                "currency": asset.currency,
                "range": range, "data": []}

    interval, days = _RANGE_TO_INTERVAL.get(range, _RANGE_TO_INTERVAL["1y"])
    try:
        data = _fetch_yahoo_history(asset.ticker, days, interval)
    except Exception:
        logger.exception("price-history fetch failed for %s", asset.ticker)
        data = []
    return {
        "asset_id": str(asset_id),
        "ticker": asset.ticker,
        "currency": asset.currency,
        "range": range,
        "data": data,
    }
