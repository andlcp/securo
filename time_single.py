import asyncio, uuid, time
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.core.config import get_settings
from app.models.user import User
from app.services.portfolio_timeseries_service import get_timeseries, _ts_cache

USER_ID = uuid.UUID('8bcb95a1-fce0-4de3-8952-15d04343e3f8')
s = get_settings(); engine = create_async_engine(s.database_url)
Session = async_sessionmaker(engine, expire_on_commit=False)

async def main():
    async with Session() as session:
        user = (await session.execute(select(User).where(User.id == USER_ID))).scalar_one()
        # Drop the cache for cold timing
        _ts_cache.clear()

        t0 = time.time()
        ts = await get_timeseries(session, user, since_start=True, granularity='daily')
        t1 = time.time()
        print(f'COLD lifetime call: {t1-t0:.2f}s, {len(ts)} points')

        t0 = time.time()
        ts = await get_timeseries(session, user, since_start=True, granularity='daily')
        t1 = time.time()
        print(f'WARM lifetime call: {t1-t0:.4f}s')

asyncio.run(main())
