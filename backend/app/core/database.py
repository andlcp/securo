from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

from app.core.config import get_settings

settings = get_settings()

# Pool sizing: the Investments / Patrimônio pages fire ~8 parallel
# queries on load. The default 5+10 starts queueing under that load,
# pushing perceived request time to tens of seconds when one of them
# happens to be a slow path (snapshot rebuild on first read of the day).
# 30 saturating + 20 burst gives every parallel request a connection
# even when the snapshot warmer is mid-pass too.
engine = create_async_engine(
    settings.database_url,
    echo=settings.debug,
    pool_size=30,
    max_overflow=20,
    pool_pre_ping=True,   # cheap noop on healthy conns, recycles dead ones
    pool_recycle=1800,    # 30 min — avoids hitting idle timeouts on the db side
)
async_session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_async_session() -> AsyncSession:
    async with async_session_maker() as session:
        yield session
