import asyncio, uuid
from datetime import date
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.core.config import get_settings
from app.models.asset import Asset
from app.models.user import User
from app.services.portfolio_timeseries_service import get_timeseries

USER_ID = uuid.UUID('8bcb95a1-fce0-4de3-8952-15d04343e3f8')
s = get_settings(); engine = create_async_engine(s.database_url)
Session = async_sessionmaker(engine, expire_on_commit=False)

async def main():
    async with Session() as session:
        user = (await session.execute(select(User).where(User.id == USER_ID))).scalar_one()
        asai = (await session.execute(
            select(Asset).where(Asset.ticker == 'ASAI3.SA',
                                Asset.user_id == USER_ID))).scalar_one()
        ts = await get_timeseries(session, user, asset_ids=[asai.id],
                                  date_from=date(2024,4,22), date_to=date(2026,4,22),
                                  granularity='daily')
        print(f'Total days: {len(ts)}')
        # Sample at key dates
        keydates = ['2024-04-22', '2025-08-13', '2025-08-14', '2025-08-15',
                    '2026-01-05', '2026-01-06', '2026-01-07', '2026-04-22']
        for kd in keydates:
            for r in ts:
                if r['month_end'] == kd:
                    print(f"{kd}: V_end={r['v_end']:>10,.2f} cf={r['cashflow']:>10,.2f} inc={r['income']:>8,.2f} twr={r['twr_cum']*100:>+7.2f}%")
                    break
        print()
        # Final
        first, last = ts[0], ts[-1]
        base = 1 + (first['twr_cum'] or 0)
        top = 1 + (last['twr_cum'] or 0)
        print(f'First twr_cum: {first["twr_cum"]*100:+.4f}%')
        print(f'Last twr_cum:  {last["twr_cum"]*100:+.4f}%')
        print(f'Rebased: {(top/base - 1)*100:+.4f}%')

asyncio.run(main())
