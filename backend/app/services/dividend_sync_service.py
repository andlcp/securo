"""Dividend auto-sync — route by ticker, dedupe against the user's CSV history.

Source routing:
  ``*.SA`` (B3 tickers) → BRAPI (brapi.dev). Returns events with proper
    type labels (DIVIDENDO / JCP / RENDIMENTO) and uses paymentDate, which
    matches the dates the XP CSV records.
  Anything else → Yahoo Finance ``events=div``. Yahoo doesn't separate
    JCP from ordinary dividends, so we always store these as DIVIDEND.

All factors are 1.0 — we store GROSS values so portfolio rentabilidade
sits on the same basis as the CDI benchmark line drawn alongside it.
The earlier × 0.85 for JCP was producing a misleading apples-to-oranges
comparison (net portfolio vs gross CDI) that the user read as
underperformance.

  DIVIDEND BR    × 1.00
  RENDIMENTO     × 1.00
  JCP            × 1.00
  DIVIDEND US    × 1.00

Dedupe:
  1. Idempotent re-runs use ``external_id`` keyed by source+ticker+date.
  2. ±5 day window against any existing DIVIDEND/JCP/RENDIMENTO row on
     this asset, regardless of source. This catches the off-by-N-day
     mismatch between Yahoo's ex-date and XP's payment-date.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.models.asset import Asset
from app.models.asset_transaction import AssetTransaction
from app.services.brapi_service import fetch_brapi_dividends
from app.services.investment_benchmark_service import fetch_yahoo_dividends

logger = logging.getLogger(__name__)

_BUY_TYPES = {"BUY", "DEPOSIT"}
_SELL_TYPES = {"SELL", "WITHDRAWAL"}
_DIVIDEND_LIKE = {"DIVIDEND", "JCP", "RENDIMENTO"}

# Window (in days) around an existing dividend within which a new event
# is treated as the same payout for dedupe purposes. Brazilian FIIs pay
# the rendimento ~10–15 days after the ex-date, so anything tighter than
# this misses the cross-source duplicates between Yahoo (ex-date) and the
# XP CSV (payment date). 20 days handles every common monthly cadence.
_DEDUPE_WINDOW_DAYS = 20

# Two events are considered "the same payout" only if their per-share
# amount is within this absolute *and* relative tolerance. Stops a
# legitimate JCP near a separate dividend from being suppressed when
# the values differ.
_DEDUPE_VALUE_ABS = 0.50  # R$
_DEDUPE_VALUE_REL = 0.05  # 5 %

# Per-type withholding factor applied to amount × units before storing.
# All set to 1.0 — we store GROSS values to keep portfolio rentabilidade
# on the same basis as the gross CDI benchmark line. Mixing net (after IR
# retido na fonte for JCP, etc.) cashflows with a gross CDI baseline gives
# an apples-to-oranges comparison, and the user reads the resulting gap
# as portfolio underperformance even when it's just a tax artifact.
_FACTOR_BY_TYPE = {
    "DIVIDEND": 1.00,
    "JCP": 1.00,
    "RENDIMENTO": 1.00,
}


def _is_brazilian_ticker(ticker: str) -> bool:
    """B3 tickers we expect to be served by BRAPI use the .SA suffix
    (Yahoo convention: PETR4.SA). FIIs and BDRs follow the same pattern."""
    return bool(ticker) and ticker.upper().endswith(".SA")


def _bare_ticker(ticker: str) -> str:
    """BRAPI expects the ticker without the .SA suffix (PETR4, not PETR4.SA)."""
    if not ticker:
        return ticker
    upper = ticker.upper()
    return upper[:-3] if upper.endswith(".SA") else upper


def _units_at(asset_units: float, txs: list[AssetTransaction],
              on: date,
              purchase_date: Optional[date] = None) -> float:
    """Walk transactions newest-first, undoing buys/sells dated AFTER `on`.
    Returns share count owned at end of day `on`. txs must be sorted ascending.

    Pre-history clamp: if `on` is before the earliest known signal (first
    tx, or `purchase_date` when there are no txs), return 0. Without this,
    when current `asset.units` doesn't match sum(BUY)-sum(SELL) (the
    "ghost units" pattern: corporate actions that bumped the position
    silently, pre-CSV holdings), the walk projects the discrepancy
    backwards in time. Multiplied by yfinance's per-share dividend rate
    that gets added back as a "received" cashflow on a date the user
    didn't actually own the asset, polluting the ledger with synthetic
    DIVIDENDs (BLAU3 had three of those for jun/24, sep/24, mar/25).
    """
    earliest = txs[0].date if txs else purchase_date
    if earliest is not None and on < earliest:
        return 0.0
    u = asset_units
    for tx in reversed(txs):
        if tx.date <= on:
            break
        q = float(tx.qty or 0)
        if tx.type in _BUY_TYPES:
            u -= q
        elif tx.type in _SELL_TYPES:
            u += q
    return u


def _values_match(a: float, b: float) -> bool:
    """True if two per-share dividend values look like the same payout —
    within an absolute floor (±R$ 0.50) AND a relative tolerance (±5 %).
    Both checks have to pass so we don't incorrectly merge a small
    R$ 0.05 dividend with an unrelated R$ 0.10 one."""
    diff = abs(a - b)
    if diff > _DEDUPE_VALUE_ABS:
        return False
    base = max(abs(a), abs(b), 1e-9)
    return (diff / base) <= _DEDUPE_VALUE_REL


def _has_nearby_existing_dividend(
    target_date: date,
    target_value: float,
    existing_events: list[tuple[date, float]],
) -> bool:
    """True if any existing dividend-like row falls within both
    ±_DEDUPE_WINDOW_DAYS of target_date AND has a per-share value matching
    target_value (per `_values_match`). Catches FII payouts where the
    Yahoo ex-date is 10-15 days off the XP payment-date but the cash
    amount is identical."""
    for d, v in existing_events:
        if abs((target_date - d).days) <= _DEDUPE_WINDOW_DAYS and _values_match(target_value, v):
            return True
    return False


async def _process_asset_events(
    session: AsyncSession,
    asset: Asset,
    events: list[dict],
    source_label: str,
) -> dict:
    """Apply pre-fetched dividend events for one asset. Caller awaits this
    sequentially across assets — AsyncSession isn't concurrency-safe."""
    if not events:
        return {"created": 0, "skipped": 0, "fetched": 0}

    stmt = select(AssetTransaction).where(
        AssetTransaction.asset_id == asset.id,
    ).order_by(AssetTransaction.date)
    txs = list((await session.execute(stmt)).scalars().all())

    # (date, total_value) pairs for every existing dividend-like row on
    # this asset — used for the cross-source dedupe check below.
    existing_div_events: list[tuple[date, float]] = [
        (tx.date, float(tx.value or 0))
        for tx in txs
        if tx.type in _DIVIDEND_LIKE and tx.value is not None
    ]
    existing_external_ids = {tx.external_id for tx in txs if tx.external_id}
    asset_units_today = float(asset.units or 0)

    # CSV-first authority: if the user imported a dividend history via
    # csv_import (i.e. push_to_securo from the broker statement), treat
    # it as ground truth for the past and skip any auto-sync event that
    # falls in a calendar month already covered by the CSV. This catches
    # the case where Yahoo's ex-date is one day after the CSV's payment
    # date (different days, same month, same payout) and avoids the
    # gross-vs-net value discrepancy that lets duplicates slip past the
    # ±20-day / 5 %-value dedupe.
    csv_covered_months: set[tuple[int, int]] = {
        (tx.date.year, tx.date.month)
        for tx in txs
        if tx.type in _DIVIDEND_LIKE and tx.source == "csv_import"
    }

    created = 0
    skipped = 0
    new_rows: list[AssetTransaction] = []
    for ev in events:
        try:
            ev_date = date.fromisoformat(ev["date"])
        except (TypeError, ValueError, KeyError):
            continue
        amount_per_share = float(ev.get("amount") or 0)
        if amount_per_share <= 0:
            skipped += 1
            continue

        # Yahoo always returns DIVIDEND (no type info). BRAPI sets it.
        ev_type = ev.get("type") or "DIVIDEND"
        factor = _FACTOR_BY_TYPE.get(ev_type, 1.00)

        external_id = f"{source_label}-{ev_type.lower()}-{asset.ticker}-{ev['date']}"
        if external_id in existing_external_ids:
            skipped += 1
            continue

        # CSV-first: skip anything in a calendar month already covered
        # by the user's csv_import dividend history.
        if (ev_date.year, ev_date.month) in csv_covered_months:
            skipped += 1
            continue

        units = _units_at(asset_units_today, txs, ev_date,
                          purchase_date=asset.purchase_date)
        if units <= 0:
            skipped += 1
            continue

        net_amount = amount_per_share * factor
        new_total = round(net_amount * units, 2)
        if new_total <= 0:
            skipped += 1
            continue

        # Cross-source dedupe: skip when an existing dividend-like row on
        # the same asset has both a nearby date AND a similar total. This
        # catches FII payouts where Yahoo's ex-date and XP's payment date
        # are 10-15 days apart but represent the same R$ amount.
        if _has_nearby_existing_dividend(ev_date, new_total, existing_div_events):
            skipped += 1
            continue

        value = new_total

        notes_bits = [
            f"{source_label}: {amount_per_share:g}/share × {units:g}",
        ]
        if factor != 1.00:
            notes_bits.append(f"× factor {factor}")
        notes = " ".join(notes_bits)

        row = AssetTransaction(
            user_id=asset.user_id,
            asset_id=asset.id,
            date=ev_date,
            type=ev_type,
            qty=None,
            price=Decimal(str(round(net_amount, 6))),
            value=Decimal(str(value)),
            fees=Decimal("0"),
            notes=notes,
            source=source_label,
            external_id=external_id,
        )
        new_rows.append(row)
        existing_external_ids.add(external_id)
        existing_div_events.append((ev_date, value))
        created += 1

    if new_rows:
        session.add_all(new_rows)
        await session.flush()

    return {"created": created, "skipped": skipped, "fetched": len(events)}


async def _fetch_for_asset(
    asset: Asset,
    brapi_token: str,
    brapi_enabled: bool,
) -> tuple[list[dict], str]:
    """Fetch dividend events for one asset, picking the source by ticker.

    For .SA tickers we *prefer* BRAPI (proper JCP/DIVIDEND/RENDIMENTO
    separation) but transparently fall back to Yahoo if BRAPI is
    unavailable — common cases:

      - BRAPI dividend module gated behind a paid plan (free tier
        returns 403 on the ?dividends=true endpoint),
      - no BRAPI_API_KEY configured,
      - transient network failure.

    Yahoo lumps every BR cash event into a single "dividends" stream
    without a type label, so fallbacks come through as DIVIDEND.

    Returns (events, source_label) for AssetTransaction.source so the
    events log can show where each row originated.
    """
    today = date.today()
    start = asset.purchase_date or date(today.year - 2, today.month, today.day)
    if start > today:
        start = today

    if _is_brazilian_ticker(asset.ticker):
        if brapi_enabled and brapi_token:
            events = await fetch_brapi_dividends(_bare_ticker(asset.ticker),
                                                 brapi_token, start, today)
            if events:
                return events, "brapi"
            # Empty either because the asset really has no dividends or
            # the request was gated/blocked — fall through to Yahoo.
        events = await fetch_yahoo_dividends(asset.ticker, start, today)
        return events, "yfinance"

    # US stocks/ETFs, crypto — Yahoo is fine.
    events = await fetch_yahoo_dividends(asset.ticker, start, today)
    return events, "yfinance"


async def _probe_brapi_dividends(token: str) -> bool:
    """One-shot probe: does this BRAPI key have access to the dividends
    endpoint? Returns False on 403 (free tier), network errors, or any
    other non-success. Caches the result for the rest of the sync run so
    we don't issue 30+ doomed requests."""
    if not token:
        return False
    try:
        # PETR4 is on every plan's allowlist; if dividends are gated, we
        # get a 403 here and skip BRAPI for the whole batch.
        events = await fetch_brapi_dividends("PETR4", token,
                                             date.today() - timedelta(days=30),
                                             date.today())
    except Exception:
        return False
    # fetch_brapi_dividends swallows errors and returns []. We
    # additionally accept any non-empty result as proof the key works.
    return bool(events)


async def sync_user_dividends(
    session: AsyncSession,
    user_id: uuid.UUID,
) -> dict:
    """Sync dividends for every market-priced asset of one user. Yahoo +
    BRAPI HTTP calls run in parallel (the slow part); DB writes are
    sequential (AsyncSession isn't concurrency-safe).
    """
    settings = get_settings()
    brapi_token = settings.brapi_api_key

    stmt = select(Asset).where(
        and_(
            Asset.user_id == user_id,
            Asset.valuation_method == "market_price",
            Asset.ticker.is_not(None),
            Asset.sell_date.is_(None),
        )
    )
    assets = list((await session.execute(stmt)).scalars().all())
    if not assets:
        return {"created": 0, "skipped": 0, "fetched": 0, "assets": []}

    # Probe once before the per-asset fan-out: if the BRAPI key isn't
    # cleared for the dividend endpoint (free plan returns 403), we'd
    # otherwise burn a request per BR ticker before falling back. The
    # probe collapses that to a single wasted call.
    brapi_enabled = await _probe_brapi_dividends(brapi_token)
    if not brapi_enabled and brapi_token:
        logger.info("BRAPI dividend endpoint unavailable for this key — "
                    "falling back to Yahoo for all .SA tickers.")

    fetch_results = await asyncio.gather(*[
        _fetch_for_asset(a, brapi_token, brapi_enabled) for a in assets
    ])

    results: list[dict] = []
    for asset, (events, source_label) in zip(assets, fetch_results):
        results.append(await _process_asset_events(session, asset, events, source_label))
    await session.commit()

    total_created = sum(r["created"] for r in results)
    total_skipped = sum(r["skipped"] for r in results)
    total_fetched = sum(r["fetched"] for r in results)
    per_asset = [
        {
            "asset_id": str(a.id),
            "ticker": a.ticker,
            "name": a.name,
            "source": src,
            **r,
        }
        for a, (_, src), r in zip(assets, fetch_results, results)
        if r["fetched"] > 0 or r["created"] > 0
    ]
    return {
        "created": total_created,
        "skipped": total_skipped,
        "fetched": total_fetched,
        "assets": per_asset,
    }


async def sync_all_users_dividends(session_maker) -> dict:
    """Per-user sync, used by the daily Celery beat task. Isolates each
    user in its own session so one failure doesn't poison the rest."""
    async with session_maker() as session:
        stmt = select(Asset.user_id).where(
            Asset.valuation_method == "market_price",
            Asset.ticker.is_not(None),
        ).distinct()
        user_ids = [row[0] for row in (await session.execute(stmt)).all()]

    totals: dict[str, int] = defaultdict(int)
    failures = 0
    for uid in user_ids:
        try:
            async with session_maker() as session:
                r = await sync_user_dividends(session, uid)
                totals["created"] += r["created"]
                totals["skipped"] += r["skipped"]
                totals["fetched"] += r["fetched"]
        except Exception as exc:
            failures += 1
            logger.exception("dividend sync failed for user %s: %s", uid, exc)
    return {
        "users": len(user_ids),
        "failures": failures,
        **dict(totals),
    }
