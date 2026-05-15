"""Portfolio daily snapshots — materialized cache of the timeseries output.

Reading from `portfolio_daily_snapshots` is a single indexed SELECT, vs.
the live recompute path which is a ~2-3s walk over every transaction and
AV the user has. Pages that render the full portfolio (Patrimônio,
Investments, Dashboard's monthly growth chart and Resultado table)
benefit the most.

Contract:
  - Only the "no-filter" view is materialized (asset_ids / asset_classes /
    group_ids unset). Filtered queries fall through to the legacy compute
    path — filters are rare and would 10x the row count if pre-stored.
  - `payload` JSONB matches one row of get_timeseries' daily output:
    {month_end, month, v_end, cashflow, income, return_month, twr_cum,
     by_class}. Monthly granularity is reaggregated from daily on read.
  - Mutations call `invalidate_daily_snapshots(user_id, from_date)` to
    DELETE rows >= from_date so the next read triggers a partial rebuild.
    A nightly Celery beat sweeps any missed invalidations.

NB: this file is named `portfolio_daily_snapshot_service` to avoid
clobbering the older `portfolio_snapshot_service` which handles the
offline-pipeline-import flow (separate concern).
"""
import logging
import time as _time
import uuid
from datetime import date, timedelta
from typing import Optional

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.portfolio_daily_snapshot import PortfolioDailySnapshot
from app.models.user import User

logger = logging.getLogger(__name__)


# Per-user snapshot "version stamp" — the MAX(computed_at) across the
# user's snapshot rows. Used by the timeseries cache to detect when an
# out-of-process rebuild has happened (e.g. a maintenance script running
# `rebuild_daily_snapshots` directly in psql) and silently invalidate
# the backend's in-memory result cache without needing a Redis pub/sub.
#
# Cached in-process for 5 seconds so the lookup doesn't add a round-trip
# to every timeseries request; that window is short enough that
# real-world latency between an external rebuild and the next page load
# already exceeds it.
_SNAPSHOT_VERSION_CACHE: dict[uuid.UUID, tuple[float, str]] = {}
_SNAPSHOT_VERSION_TTL_S = 5.0


async def get_snapshot_version(
    session: AsyncSession, user_id: uuid.UUID,
) -> str:
    """Return a stable, monotonic-ish version stamp for the user's
    snapshot table. Two snapshots with the same MAX(computed_at) are
    treated as identical for caching purposes.

    Empty snapshots map to the sentinel "none" so a first-read cache
    entry written before any rebuild still busts after the rebuild
    fills the table.
    """
    now = _time.monotonic()
    cached = _SNAPSHOT_VERSION_CACHE.get(user_id)
    if cached is not None and cached[0] > now:
        return cached[1]
    latest = await session.scalar(
        select(func.max(PortfolioDailySnapshot.computed_at))
        .where(PortfolioDailySnapshot.user_id == user_id)
    )
    version = latest.isoformat() if latest is not None else "none"
    _SNAPSHOT_VERSION_CACHE[user_id] = (now + _SNAPSHOT_VERSION_TTL_S, version)
    return version


def invalidate_snapshot_version_cache(user_id: Optional[uuid.UUID] = None) -> None:
    """Drop the cached version stamp so the next read hits the DB.
    Called from the same paths that mutate snapshots in-process so the
    next request immediately sees the new version without waiting on
    the 5s TTL."""
    if user_id is None:
        _SNAPSHOT_VERSION_CACHE.clear()
    else:
        _SNAPSHOT_VERSION_CACHE.pop(user_id, None)


async def read_daily_snapshot_rows(
    session: AsyncSession,
    user_id: uuid.UUID,
    start_d: Optional[date] = None,
    end_d: Optional[date] = None,
) -> list[dict]:
    """Return the cached daily payloads as a list[dict], ordered by date.

    Empty result means "no cache" — caller should rebuild and retry.
    """
    stmt = (
        select(PortfolioDailySnapshot.date, PortfolioDailySnapshot.payload)
        .where(PortfolioDailySnapshot.user_id == user_id)
    )
    if start_d is not None:
        stmt = stmt.where(PortfolioDailySnapshot.date >= start_d)
    if end_d is not None:
        stmt = stmt.where(PortfolioDailySnapshot.date <= end_d)
    stmt = stmt.order_by(PortfolioDailySnapshot.date)
    rows = (await session.execute(stmt)).all()
    return [r.payload for r in rows]


async def write_daily_snapshot_rows(
    session: AsyncSession,
    user_id: uuid.UUID,
    rows: list[dict],
) -> None:
    """Bulk-upsert daily rows. `rows` must be the daily-granularity output
    of get_timeseries (each item carries `month_end` ISO date).

    Uses ON CONFLICT DO UPDATE so re-running rebuild is idempotent and
    repairs any stale data in place.
    """
    if not rows:
        return
    values = [
        {
            "user_id": user_id,
            "date": date.fromisoformat(r["month_end"]),
            "payload": r,
        }
        for r in rows
    ]
    stmt = pg_insert(PortfolioDailySnapshot).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["user_id", "date"],
        set_={
            "payload": stmt.excluded.payload,
            "computed_at": stmt.excluded.computed_at,
        },
    )
    await session.execute(stmt)
    await session.commit()
    # Bust the version cache so callers re-read the new MAX(computed_at)
    # on their next lookup rather than waiting for the 5s TTL.
    invalidate_snapshot_version_cache(user_id)


async def invalidate_daily_snapshots(
    session: AsyncSession,
    user_id: uuid.UUID,
    from_date: Optional[date] = None,
) -> int:
    """Delete cached rows from `from_date` onwards (or all rows if None).

    Use case: a user just edited a 2024-03 AssetValue — every snapshot
    from 2024-03-01 to today needs to be recomputed. Call this with
    `from_date=2024-03-01`.

    Returns rows deleted.
    """
    stmt = delete(PortfolioDailySnapshot).where(
        PortfolioDailySnapshot.user_id == user_id
    )
    if from_date is not None:
        stmt = stmt.where(PortfolioDailySnapshot.date >= from_date)
    result = await session.execute(stmt)
    await session.commit()
    invalidate_snapshot_version_cache(user_id)
    return result.rowcount or 0


async def latest_daily_snapshot_date(
    session: AsyncSession, user_id: uuid.UUID
) -> Optional[date]:
    """Return the most recent date present in the cache, or None if empty."""
    stmt = (
        select(PortfolioDailySnapshot.date)
        .where(PortfolioDailySnapshot.user_id == user_id)
        .order_by(PortfolioDailySnapshot.date.desc())
        .limit(1)
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def rebuild_daily_snapshots(
    session: AsyncSession,
    user: User,
    from_date: Optional[date] = None,
) -> int:
    """Recompute and persist daily snapshots for `user`.

    If `from_date` is None, rebuilds from the user's first transaction —
    full backfill, slow (~3-5s).
    If `from_date` is set, only the window from that date onward is
    recomputed; the rows are upserted, preserving older snapshots. Used
    after partial invalidations so we don't redo work that's still valid.

    Returns number of daily rows written.
    """
    # Local import to break the circular dep — portfolio_timeseries_service
    # also imports from this module for read-through.
    from app.services.portfolio_timeseries_service import (
        _compute_timeseries_uncached,
    )

    if from_date is None:
        rows = await _compute_timeseries_uncached(
            session, user,
            since_start=True,
            granularity="daily",
        )
    else:
        rows = await _compute_timeseries_uncached(
            session, user,
            date_from=from_date,
            date_to=date.today(),
            granularity="daily",
        )
    await write_daily_snapshot_rows(session, user.id, rows)
    logger.info(
        "Rebuilt portfolio daily snapshots: user=%s from=%s rows=%d",
        user.id, from_date, len(rows),
    )
    return len(rows)


def aggregate_monthly(daily_rows: list[dict]) -> list[dict]:
    """Reduce daily rows to one per month-end.

    Mirrors what the legacy `get_timeseries(granularity="monthly")` returns
    so callers can swap freely. Uses the last daily row of each YYYY-MM as
    the month's snapshot, with cashflow/income summed across days and
    monthly TWR chained from daily returns.
    """
    if not daily_rows:
        return []
    by_month: dict[str, list[dict]] = {}
    for r in daily_rows:
        ym = r["month"]
        by_month.setdefault(ym, []).append(r)

    out: list[dict] = []
    cum = 1.0
    for ym in sorted(by_month.keys()):
        days = by_month[ym]
        cf_sum = sum(d.get("cashflow", 0) or 0 for d in days)
        inc_sum = sum(d.get("income", 0) or 0 for d in days)
        month_factor = 1.0
        for d in days:
            r_m = d.get("return_month")
            if r_m is None:
                continue
            month_factor *= (1.0 + r_m)
        ret_month = month_factor - 1.0
        cum *= month_factor

        last = days[-1]
        out.append({
            "month_end": last["month_end"],
            "month": ym,
            "v_end": last["v_end"],
            "cashflow": round(cf_sum, 2),
            "income": round(inc_sum, 2),
            "return_month": round(ret_month, 6),
            "twr_cum": round(cum - 1.0, 6),
            "by_class": last.get("by_class", {}),
        })
    return out


def window_daily(
    daily_rows: list[dict],
    months: Optional[int],
    since_start: bool,
    date_from: Optional[date],
    date_to: Optional[date],
) -> list[dict]:
    """Trim the lifetime daily series to the requested window and re-base
    the cumulative TWR so the first point is 0%, matching what the live
    compute path returns when called with the same window."""
    if not daily_rows:
        return daily_rows

    today = date.today()
    if date_from is not None and date_to is not None:
        lo, hi = date_from, date_to
    elif since_start:
        return [_clone(r) for r in daily_rows]
    else:
        m = months or 12
        lo = today - timedelta(days=m * 30)
        hi = today

    lo_iso = lo.isoformat()
    hi_iso = hi.isoformat()
    out = [_clone(r) for r in daily_rows if lo_iso <= r["month_end"] <= hi_iso]
    if out:
        base = 1.0 + (out[0].get("twr_cum") or 0)
        if base > 0:
            for r in out:
                r["twr_cum"] = round(
                    ((1.0 + (r.get("twr_cum") or 0)) / base) - 1.0, 6
                )
    return out


def _clone(d: dict) -> dict:
    """Shallow copy a payload dict — we mutate twr_cum during windowing
    and don't want to scribble on the snapshot rows we just SELECTed."""
    return dict(d)
