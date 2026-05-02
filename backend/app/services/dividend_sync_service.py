"""Yahoo Finance dividend auto-sync.

For every market-priced asset with a Yahoo ticker, fetch the dividend
history and create AssetTransaction(type=DIVIDEND) rows for events that
aren't already in the database. Used both by the daily Celery task and
the manual "Sincronizar dividendos" button in the events page.

Dedupe strategy:
  1. Idempotent re-runs use a deterministic external_id of
     ``yahoo-div-{ticker}-{ex_date}`` (UNIQUE upsert key).
  2. Asset already had dividends imported via push_to_securo or by hand?
     We also skip when an existing DIVIDEND/JCP transaction matches
     (asset_id, date) regardless of source — preserves the user's
     existing rows so the values they curated don't get rewritten.

Value calculation: ``amount_per_share * units_at(ex_date)`` where
units_at walks transactions backwards from the asset's current units.
Stored in the asset's native currency (matches Modified Dietz)."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict
from datetime import date
from decimal import Decimal
from typing import Optional

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.asset import Asset
from app.models.asset_transaction import AssetTransaction
from app.services.investment_benchmark_service import fetch_yahoo_dividends

logger = logging.getLogger(__name__)

# Match what's in asset_transaction_service / portfolio_timeseries_service.
_BUY_TYPES = {"BUY", "DEPOSIT"}
_SELL_TYPES = {"SELL", "WITHDRAWAL"}
# Existing DIVIDEND-equivalent rows we DON'T want to clobber if Yahoo
# returns an event for the same (asset, date).
_DIVIDEND_LIKE = {"DIVIDEND", "JCP", "RENDIMENTO"}


def _units_at(asset_units: float, txs: list[AssetTransaction], on: date) -> float:
    """Walk transactions newest-first, undoing those after `on`. Returns
    units owned at end of `on`. txs must be sorted ascending by date."""
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


async def _process_asset_events(
    session: AsyncSession,
    asset: Asset,
    events: list[dict],
) -> dict:
    """Apply already-fetched Yahoo dividend events for one asset to the
    database. Caller is responsible for awaiting this sequentially across
    assets — SQLAlchemy AsyncSession is NOT safe to use from multiple
    coroutines concurrently (you get "Session is already flushing")."""
    if not events:
        return {"created": 0, "skipped": 0, "fetched": 0}

    # Pull existing transactions for this asset to (a) dedupe and (b)
    # compute units_at(ex_date) for each event.
    stmt = select(AssetTransaction).where(
        AssetTransaction.asset_id == asset.id,
    ).order_by(AssetTransaction.date)
    txs = list((await session.execute(stmt)).scalars().all())

    # Index existing dividends by (date) so we can skip Yahoo events that
    # are already represented by manual / csv_import rows.
    existing_div_dates = {
        tx.date for tx in txs
        if tx.type in _DIVIDEND_LIKE
    }
    existing_external_ids = {
        tx.external_id for tx in txs if tx.external_id
    }

    asset_units_today = float(asset.units or 0)

    created = 0
    skipped = 0
    new_rows: list[AssetTransaction] = []
    for ev in events:
        try:
            ex_date = date.fromisoformat(ev["date"])
        except (TypeError, ValueError):
            continue
        amount_per_share = float(ev.get("amount") or 0)
        if amount_per_share <= 0:
            continue

        external_id = f"yahoo-div-{asset.ticker}-{ev['date']}"

        if external_id in existing_external_ids:
            skipped += 1
            continue
        if ex_date in existing_div_dates:
            # Already have a manual or CSV-imported dividend on this date —
            # keep the user's row intact, don't duplicate.
            skipped += 1
            continue

        # Compute units owned on ex_date.
        units = _units_at(asset_units_today, txs, ex_date)
        if units <= 0:
            # User didn't own this asset on the ex-date (e.g. the position
            # was opened later). Skip.
            skipped += 1
            continue

        value = round(amount_per_share * units, 2)
        if value <= 0:
            skipped += 1
            continue

        row = AssetTransaction(
            user_id=asset.user_id,
            asset_id=asset.id,
            date=ex_date,
            type="DIVIDEND",
            qty=None,
            price=Decimal(str(amount_per_share)),
            value=Decimal(str(value)),
            fees=Decimal("0"),
            notes=f"Yahoo: {amount_per_share}/share × {units:g}",
            source="yfinance",
            external_id=external_id,
        )
        new_rows.append(row)
        existing_external_ids.add(external_id)
        existing_div_dates.add(ex_date)
        created += 1

    if new_rows:
        session.add_all(new_rows)
        await session.flush()

    return {"created": created, "skipped": skipped, "fetched": len(events)}


async def sync_user_dividends(
    session: AsyncSession,
    user_id: uuid.UUID,
) -> dict:
    """Sync dividends for every market-priced asset owned by a user.

    Returns aggregate counts plus a per-asset breakdown the UI can show
    after a manual run. Yahoo HTTP fetches run in parallel (the slow
    part), then the DB writes are applied sequentially against the
    shared session (SQLAlchemy AsyncSession isn't concurrency-safe).
    """
    stmt = select(Asset).where(
        and_(
            Asset.user_id == user_id,
            Asset.valuation_method == "market_price",
            Asset.ticker.is_not(None),
            Asset.sell_date.is_(None),  # skip fully-closed positions
        )
    )
    assets = list((await session.execute(stmt)).scalars().all())
    if not assets:
        return {"created": 0, "skipped": 0, "fetched": 0, "assets": []}

    today = date.today()

    async def _fetch_one(a: Asset) -> list[dict]:
        start = a.purchase_date or date(today.year - 2, today.month, today.day)
        if start > today:
            start = today
        return await fetch_yahoo_dividends(a.ticker, start, today)

    events_per_asset = await asyncio.gather(*[_fetch_one(a) for a in assets])

    results: list[dict] = []
    for asset, events in zip(assets, events_per_asset):
        results.append(await _process_asset_events(session, asset, events))
    await session.commit()

    total_created = sum(r["created"] for r in results)
    total_skipped = sum(r["skipped"] for r in results)
    total_fetched = sum(r["fetched"] for r in results)
    per_asset = [
        {
            "asset_id": str(a.id),
            "ticker": a.ticker,
            "name": a.name,
            **r,
        }
        for a, r in zip(assets, results)
        if r["fetched"] > 0 or r["created"] > 0
    ]
    return {
        "created": total_created,
        "skipped": total_skipped,
        "fetched": total_fetched,
        "assets": per_asset,
    }


async def sync_all_users_dividends(session_maker) -> dict:
    """Run sync_user_dividends for every distinct user_id with assets.
    Used by the Celery beat task. Each user runs in its own session so a
    failure on one doesn't poison the rest."""
    # Find user_ids with at least one syncable asset.
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
