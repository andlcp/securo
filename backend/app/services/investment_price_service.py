import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.investment_price_cache import InvestmentPriceCache

logger = logging.getLogger(__name__)

CACHE_TTL_SECONDS = 15 * 60  # 15 minutes
BRAPI_BASE = "https://brapi.dev/api"
AWESOMEAPI_URL = "https://economia.awesomeapi.com.br/json/last/USD-BRL"
YAHOO_BASE = "https://query1.finance.yahoo.com/v8/finance/chart"
BACEN_CDI_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados/ultimos/{days}?formato=json"
TD_API_URL = "https://www.tesourodireto.com.br/json/br/com/b3/teasourodireto/apitd/1/listing.json"

YAHOO_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


async def _fetch_brapi(ticker: str) -> Optional[dict]:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{BRAPI_BASE}/quote/{ticker}")
            r.raise_for_status()
            data = r.json()
            results = data.get("results", [])
            if results:
                res = results[0]
                return {
                    "price": res.get("regularMarketPrice"),
                    "currency": "BRL",
                    "change_pct": res.get("regularMarketChangePercent"),
                    "long_name": res.get("longName") or res.get("shortName") or ticker,
                }
    except Exception as exc:
        logger.warning("brapi fetch failed for %s: %s", ticker, exc)
    return None


async def _fetch_yahoo(ticker: str) -> Optional[dict]:
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            r = await client.get(
                f"{YAHOO_BASE}/{ticker}",
                params={"interval": "1d", "range": "1d"},
                headers=YAHOO_HEADERS,
            )
            r.raise_for_status()
            data = r.json()
            meta = data["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice")
            prev = meta.get("previousClose") or meta.get("chartPreviousClose")
            change_pct = ((price - prev) / prev * 100) if price and prev else None
            return {
                "price": price,
                "currency": meta.get("currency", "USD"),
                "change_pct": change_pct,
                "long_name": meta.get("longName") or meta.get("shortName") or ticker,
            }
    except Exception as exc:
        logger.warning("Yahoo Finance fetch failed for %s: %s", ticker, exc)
    return None


async def _fetch_tesouro(ticker: str) -> Optional[dict]:
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(TD_API_URL, headers=YAHOO_HEADERS)
            r.raise_for_status()
            data = r.json()
            securities = data.get("response", {}).get("TrsrBdTradgList", [])
            for sec in securities:
                bd = sec.get("TrsrBd", {})
                name = bd.get("nm", "")
                if ticker.upper() in name.upper():
                    price = bd.get("untrRedVal")
                    return {
                        "price": float(price) if price else None,
                        "currency": "BRL",
                        "change_pct": None,
                        "long_name": name,
                    }
    except Exception as exc:
        logger.warning("Tesouro Direto fetch failed for %s: %s", ticker, exc)
    return None


def _is_fresh(cached: InvestmentPriceCache) -> bool:
    now = datetime.now(timezone.utc)
    updated = cached.updated_at
    if updated.tzinfo is None:
        updated = updated.replace(tzinfo=timezone.utc)
    return (now - updated).total_seconds() < CACHE_TTL_SECONDS


async def _update_cache(
    session: AsyncSession,
    ticker: str,
    data: dict,
) -> None:
    now = datetime.now(timezone.utc)
    cached = await session.get(InvestmentPriceCache, ticker)
    if cached:
        cached.price = Decimal(str(data["price"])) if data.get("price") else None
        cached.currency = data.get("currency", "BRL")
        cached.change_pct = Decimal(str(round(data["change_pct"], 4))) if data.get("change_pct") else None
        cached.long_name = data.get("long_name")
        cached.updated_at = now
    else:
        session.add(InvestmentPriceCache(
            ticker=ticker,
            price=Decimal(str(data["price"])) if data.get("price") else None,
            currency=data.get("currency", "BRL"),
            change_pct=Decimal(str(round(data["change_pct"], 4))) if data.get("change_pct") else None,
            long_name=data.get("long_name"),
            updated_at=now,
        ))
    await session.commit()


async def get_price(session: AsyncSession, ticker: str, asset_type: str) -> Optional[dict]:
    cache_key = ticker.upper()
    cached = await session.get(InvestmentPriceCache, cache_key)

    if cached and cached.price is not None and _is_fresh(cached):
        return {
            "ticker": cached.ticker,
            "price": float(cached.price),
            "currency": cached.currency,
            "change_pct": float(cached.change_pct) if cached.change_pct else None,
            "long_name": cached.long_name,
            "updated_at": cached.updated_at.isoformat(),
        }

    fetched = None
    if asset_type in ("stock_br", "fii", "etf_br", "bitcoin"):
        fetched = await _fetch_brapi(cache_key)
    elif asset_type in ("stock_us", "etf_us"):
        fetched = await _fetch_yahoo(cache_key)
    elif asset_type == "tesouro":
        fetched = await _fetch_tesouro(cache_key)

    if fetched and fetched.get("price"):
        await _update_cache(session, cache_key, fetched)
        return {
            "ticker": cache_key,
            "price": fetched["price"],
            "currency": fetched.get("currency", "BRL"),
            "change_pct": fetched.get("change_pct"),
            "long_name": fetched.get("long_name"),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    # Return stale cache if fetch failed
    if cached and cached.price:
        return {
            "ticker": cached.ticker,
            "price": float(cached.price),
            "currency": cached.currency,
            "change_pct": float(cached.change_pct) if cached.change_pct else None,
            "long_name": cached.long_name,
            "updated_at": cached.updated_at.isoformat(),
        }
    return None


async def get_usd_brl_rate(session: AsyncSession) -> float:
    cache_key = "USD-BRL"
    cached = await session.get(InvestmentPriceCache, cache_key)

    if cached and cached.price and _is_fresh(cached):
        return float(cached.price)

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(AWESOMEAPI_URL)
            r.raise_for_status()
            rate = float(r.json()["USDBRL"]["bid"])
            await _update_cache(session, cache_key, {"price": rate, "currency": "BRL"})
            return rate
    except Exception as exc:
        logger.warning("AwesomeAPI fetch failed: %s", exc)

    return float(cached.price) if cached and cached.price else 5.80


async def get_benchmark_history(months: int = 12) -> dict:
    """Fetch historical benchmark data: CDI (cumulative %), IBOV, S&P 500."""
    result: dict = {"cdi": [], "ibov": [], "sp500": []}

    # CDI from BACEN — daily rates, we cumulate them
    try:
        days = min(months * 23, 500)
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(BACEN_CDI_URL.format(days=days))
            r.raise_for_status()
            raw = r.json()
            cumulative = 1.0
            points = []
            for entry in raw:
                cumulative *= 1 + float(entry["valor"]) / 100
                points.append({
                    "date": entry["data"],
                    "value": round((cumulative - 1) * 100, 4),
                })
            result["cdi"] = points
    except Exception as exc:
        logger.warning("BACEN CDI fetch failed: %s", exc)

    # IBOV and S&P 500 from Yahoo Finance
    period = f"{months}mo"
    for key, symbol in (("ibov", "%5EBVSP"), ("sp500", "%5EGSPC")):
        try:
            async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
                r = await client.get(
                    f"{YAHOO_BASE}/{symbol}",
                    params={"interval": "1d", "range": period},
                    headers=YAHOO_HEADERS,
                )
                r.raise_for_status()
                data = r.json()
                chart_result = data["chart"]["result"][0]
                timestamps = chart_result["timestamp"]
                closes = chart_result["indicators"]["quote"][0]["close"]
                valid = [(ts, c) for ts, c in zip(timestamps, closes) if c is not None]
                if not valid:
                    continue
                base = valid[0][1]
                result[key] = [
                    {
                        "date": datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%d/%m/%Y"),
                        "value": round((close - base) / base * 100, 4),
                    }
                    for ts, close in valid
                ]
        except Exception as exc:
            logger.warning("Yahoo %s fetch failed: %s", key, exc)

    return result
