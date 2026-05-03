"""BRAPI (brapi.dev) — Brazilian-market dividend events.

Why BRAPI instead of Yahoo for *.SA tickers:
  Yahoo's ``events=div`` payload lumps every cash distribution into a
  single "dividends" stream (DIVIDENDO + JCP + RENDIMENTO). Yahoo also
  returns gross values, so JCP (15% withholding for individuals) ends up
  inflated and indistinguishable from tax-free DIVIDENDO.

BRAPI separates each cash event with a ``label`` field
(DIVIDENDO / JCP / RENDIMENTO), letting us store the correct
``type`` on AssetTransaction and apply the right withholding factor.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

BRAPI_BASE = "https://brapi.dev/api/quote"


def _classify_label(label: str) -> str:
    """Map BRAPI's free-text label to our AssetTransaction.type vocabulary."""
    s = (label or "").upper()
    if "JCP" in s or "JUROS" in s:
        return "JCP"
    if "RENDIMENTO" in s:
        return "RENDIMENTO"
    # Fallthrough — DIVIDENDO and any unknown ordinary cash dividend.
    return "DIVIDEND"


async def fetch_brapi_dividends(
    ticker: str,
    token: str,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> list[dict]:
    """Return cash-distribution events for a Brazilian ticker.

    Each entry: ``{date, type, amount}`` where:
      - ``date`` is the payment date (matches what XP records),
      - ``type`` ∈ {DIVIDEND, JCP, RENDIMENTO},
      - ``amount`` is the BRL per-share value.

    Filters by [start, end] inclusive when both are provided. Returns []
    on any error (network, missing token, unknown ticker) — caller can
    fall through to other sources.
    """
    if not token:
        logger.warning("BRAPI token missing — skipping %s", ticker)
        return []

    params = {"token": token, "dividends": "true"}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(f"{BRAPI_BASE}/{ticker}", params=params)
            r.raise_for_status()
            data = r.json()
    except Exception as exc:
        logger.warning("BRAPI dividends %s fetch failed: %s", ticker, exc)
        return []

    results = data.get("results") or []
    if not results:
        return []
    div_data = results[0].get("dividendsData") or {}
    cash = div_data.get("cashDividends") or []
    out: list[dict] = []
    for ev in cash:
        # Prefer paymentDate (cash actually hits the account) so events
        # align with the XP CSV history. Fall back to lastDatePrior
        # (ex-date) or approvedOn if the API ever omits paymentDate.
        raw = (ev.get("paymentDate")
               or ev.get("lastDatePrior")
               or ev.get("approvedOn"))
        if not raw:
            continue
        try:
            iso = raw.split("T")[0]
            d_obj = date.fromisoformat(iso)
        except (AttributeError, TypeError, ValueError):
            continue
        if start and d_obj < start:
            continue
        if end and d_obj > end:
            continue

        rate = ev.get("rate")
        if rate is None:
            continue
        try:
            amount = float(rate)
        except (TypeError, ValueError):
            continue
        if amount <= 0:
            continue

        out.append({
            "date": iso,
            "type": _classify_label(ev.get("label") or ""),
            "amount": amount,
        })
    out.sort(key=lambda e: e["date"])
    return out
