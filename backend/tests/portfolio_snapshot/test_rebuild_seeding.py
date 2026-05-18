"""Tests for the incremental rebuild seeding logic in
`rebuild_daily_snapshots`.

Why this file exists: the user repeatedly reported a "queda súbita" /
"vertical cliff" in the rentabilidade chart that reappeared after every
midnight snapshot rebuild. Root cause: the daily walk inside
`_compute_timeseries_uncached` initialises its cumulative TWR multiplier
at 1.0, so an incremental rebuild restarts the curve at 0 % even when
yesterday's cached snapshot is already at +87 %. The fix is to seed
`initial_cum` from the previous day's snapshot before the walk; these
tests pin that contract so a future refactor can't regress it silently.
"""
from datetime import date, timedelta
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from app.models.user import User
from app.services.portfolio_daily_snapshot_service import (
    rebuild_daily_snapshots,
)


def _user() -> User:
    """Bare User stub — the seed-lookup path only needs `.id`."""
    u = User()
    u.id = uuid4()
    u.email = "x@y.z"
    u.hashed_password = "x"
    return u


def _payload(d: date, twr_cum: float) -> dict:
    """Minimal snapshot payload shaped like the real one. The seed loop
    only reads `twr_cum`, but we include `month_end` because
    `write_daily_snapshot_rows` needs it on the way back."""
    return {
        "month_end": d.isoformat(),
        "month": d.strftime("%Y-%m"),
        "v_end": 1_000_000.0,
        "cashflow": 0.0,
        "income": 0.0,
        "return_month": 0.0,
        "twr_cum": twr_cum,
        "by_class": {},
    }


@pytest.mark.asyncio
async def test_incremental_rebuild_seeds_cum_from_previous_snapshot():
    """When `from_date` has a cached snapshot on the prior day, the
    rebuild must pass `initial_cum = 1.0 + prev_twr_cum` so the chart
    continues from where it left off."""
    user = _user()
    from_d = date(2026, 5, 16)
    prev_d = from_d - timedelta(days=1)
    prev_twr = 0.866029  # +86.6 % cumulative

    session = AsyncMock()
    # session.execute(...).scalar_one_or_none() => the prior day's payload.
    exec_result = AsyncMock()
    exec_result.scalar_one_or_none = lambda: _payload(prev_d, prev_twr)
    session.execute.return_value = exec_result

    compute_mock = AsyncMock(return_value=[_payload(from_d, 0.0)])
    with patch(
        "app.services.portfolio_timeseries_service._compute_timeseries_uncached",
        compute_mock,
    ), patch(
        "app.services.portfolio_daily_snapshot_service.write_daily_snapshot_rows",
        AsyncMock(),
    ):
        await rebuild_daily_snapshots(session, user, from_date=from_d)

    assert compute_mock.called, "compute should have been called"
    kwargs = compute_mock.call_args.kwargs
    assert kwargs.get("date_from") == from_d
    assert kwargs.get("initial_cum") == pytest.approx(1.0 + prev_twr), (
        "incremental rebuild must seed cum from previous snapshot's twr_cum; "
        "otherwise the chart shows a vertical cliff at the join."
    )


@pytest.mark.asyncio
async def test_incremental_rebuild_with_no_previous_snapshot_uses_default_cum():
    """Cold cache case: no snapshot for prev_day. The rebuild still
    proceeds, defaulting initial_cum to 1.0 (i.e. cum left at default)."""
    user = _user()
    from_d = date(2026, 5, 16)

    session = AsyncMock()
    exec_result = AsyncMock()
    exec_result.scalar_one_or_none = lambda: None
    session.execute.return_value = exec_result

    compute_mock = AsyncMock(return_value=[])
    with patch(
        "app.services.portfolio_timeseries_service._compute_timeseries_uncached",
        compute_mock,
    ), patch(
        "app.services.portfolio_daily_snapshot_service.write_daily_snapshot_rows",
        AsyncMock(),
    ):
        await rebuild_daily_snapshots(session, user, from_date=from_d)

    kwargs = compute_mock.call_args.kwargs
    # Either explicit 1.0 or omitted (uses the function default of 1.0).
    seeded = kwargs.get("initial_cum", 1.0)
    assert seeded == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_full_rebuild_does_not_pass_initial_cum():
    """`from_date=None` is a from-scratch backfill — the default
    `initial_cum=1.0` is correct, and we should NOT do the seed lookup
    (saves a DB roundtrip and avoids the corrupted-payload fallback)."""
    user = _user()

    session = AsyncMock()
    compute_mock = AsyncMock(return_value=[])
    with patch(
        "app.services.portfolio_timeseries_service._compute_timeseries_uncached",
        compute_mock,
    ), patch(
        "app.services.portfolio_daily_snapshot_service.write_daily_snapshot_rows",
        AsyncMock(),
    ):
        await rebuild_daily_snapshots(session, user, from_date=None)

    # Full rebuild path uses since_start=True and does NOT pass initial_cum.
    kwargs = compute_mock.call_args.kwargs
    assert kwargs.get("since_start") is True
    assert "initial_cum" not in kwargs
    # No prior-day SELECT should have happened on the full path.
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_corrupted_prev_payload_falls_back_to_full_rebuild():
    """If the prior snapshot's `twr_cum` is missing or unparseable, we
    must NOT silently default to cum=1.0 (that's the exact bug this
    file pins). Fall through to a full rebuild — slower but correct."""
    user = _user()
    from_d = date(2026, 5, 16)
    prev_d = from_d - timedelta(days=1)

    # Payload exists but `twr_cum` is something we can't float() — e.g.
    # a string "n/a" injected by a buggy import.
    bad_payload = _payload(prev_d, 0.0)
    bad_payload["twr_cum"] = "n/a"

    session = AsyncMock()
    exec_result = AsyncMock()
    exec_result.scalar_one_or_none = lambda: bad_payload
    session.execute.return_value = exec_result

    compute_mock = AsyncMock(return_value=[])
    with patch(
        "app.services.portfolio_timeseries_service._compute_timeseries_uncached",
        compute_mock,
    ), patch(
        "app.services.portfolio_daily_snapshot_service.write_daily_snapshot_rows",
        AsyncMock(),
    ):
        await rebuild_daily_snapshots(session, user, from_date=from_d)

    # Should have done the full-rebuild fallback (since_start=True, no date_from).
    kwargs = compute_mock.call_args.kwargs
    assert kwargs.get("since_start") is True
    assert kwargs.get("date_from") is None
