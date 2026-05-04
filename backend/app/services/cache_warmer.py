"""Periodic cache warmer for portfolio timeseries.

The Investments dashboard's "Início" window walks ~1800 daily Modified
Dietz iterations × ~58 assets and takes ~4-5 s on a cold cache.
get_timeseries has a 10-minute in-process result cache, so once warm
every dashboard interaction is instant — but the FIRST load of a
session still pays the full cost.

This module fires get_timeseries for every active user across the
common preset windows shortly after backend startup and again every
9 minutes (just before the cache entry expires). The warm-up runs
inside the FastAPI process so the populated cache lives in the same
memory the API requests read from. It deliberately sleeps a few
seconds between users / windows to avoid hogging the event loop —
the warming is "best effort", a slow run is fine.
"""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.config import get_settings
from app.models.asset import Asset
from app.models.user import User
from app.services import portfolio_timeseries_service as pts

logger = logging.getLogger(__name__)

# Warm-up cycle. Just below the 600 s cache TTL so entries renew
# before they age out.
_WARM_INTERVAL_S = 9 * 60

# Windows to warm. Matches the presets the Investments page exposes.
_WINDOWS: list[dict] = [
    {"since_start": True},
    {"months": 24},
    {"months": 12},
    {"months": 6},
    {"months": 3},
    {"months": 1},
]


def _make_session_maker():
    settings = get_settings()
    engine = create_async_engine(settings.database_url)
    return async_sessionmaker(engine, expire_on_commit=False)


async def _warm_user(session, user: User) -> None:
    """Run get_timeseries for each preset window for one user."""
    for win in _WINDOWS:
        try:
            await pts.get_timeseries(
                session, user,
                granularity="daily",
                **win,
            )
        except Exception:
            logger.warning("cache warm failed for user=%s window=%s",
                           user.id, win, exc_info=True)
        # Tiny breath between windows so we don't starve incoming HTTP.
        await asyncio.sleep(0.05)


async def _warm_once() -> None:
    """One pass through every user that has at least one asset."""
    Session = _make_session_maker()
    async with Session() as session:
        # Load only users with assets — no point warming an empty portfolio.
        # SELECT DISTINCT on User row fails because User has a JSON column;
        # query distinct user_ids first, then resolve to User objects.
        ids_stmt = select(Asset.user_id).distinct()
        user_ids = [row[0] for row in (await session.execute(ids_stmt)).all()]
        if not user_ids:
            return
        users = list((await session.execute(
            select(User).where(User.id.in_(user_ids))
        )).scalars().all())

    if not users:
        return

    logger.info("Warming portfolio timeseries cache for %d user(s)", len(users))
    for u in users:
        # Each user gets its own session — the timeseries walk loads a
        # lot of data and we don't want it lingering across users.
        async with Session() as session:
            await _warm_user(session, u)
        await asyncio.sleep(0.5)
    logger.info("Cache warm pass complete")


async def periodic_warm_loop() -> None:
    """Run forever, warming the cache every _WARM_INTERVAL_S seconds.

    Caller is expected to put this on the event loop with
    asyncio.create_task(periodic_warm_loop()) and let it run.
    Cancellation (during shutdown) is handled cleanly.
    """
    # Small initial delay so the first warm doesn't fight startup.
    await asyncio.sleep(5)
    while True:
        try:
            await _warm_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("periodic_warm_loop iteration failed")
        try:
            await asyncio.sleep(_WARM_INTERVAL_S)
        except asyncio.CancelledError:
            raise
