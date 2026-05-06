import asyncio, uuid
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.core.config import get_settings
from app.models.user import User
from app.services.portfolio_timeseries_service import get_timeseries

USER_ID = uuid.UUID('8bcb95a1-fce0-4de3-8952-15d04343e3f8')
s = get_settings(); engine = create_async_engine(s.database_url)
Session = async_sessionmaker(engine, expire_on_commit=False)

async def main():
    async with Session() as session:
        user = (await session.execute(select(User).where(User.id == USER_ID))).scalar_one()
        for label, kwargs in [
            ('Stocks US', {'asset_classes': ['STOCKS_US']}),
            ('RV BR', {'asset_classes': ['RENDA_VARIAVEL_BR']}),
            ('Renda Fixa', {'asset_classes': ['RENDA_FIXA']}),
            ('FIIs', {'asset_classes': ['FIIS']}),
            ('Carteira total', {}),
        ]:
            ts = await get_timeseries(session, user, since_start=True,
                                       granularity='daily', **kwargs)
            if not ts: continue
            last = ts[-1]
            v = last['v_end']
            twr = (last['twr_cum'] or 0) * 100
            print(f'{label:<20} V = R$ {v:>12,.2f}  rent_inicio = {twr:+7.2f}%')

asyncio.run(main())
