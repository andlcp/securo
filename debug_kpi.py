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
        ts = await get_timeseries(session, user, since_start=True, granularity='daily')
        print(f'Total points: {len(ts)}')
        print('FIRST 10:')
        for r in ts[:10]:
            print(f'  {r["month_end"]}: V={r["v_end"]:>14,.2f} cf={r["cashflow"]:>10,.2f} twr={r["twr_cum"]*100:>+7.2f}%')
        print('\nLAST 5:')
        for r in ts[-5:]:
            print(f'  {r["month_end"]}: V={r["v_end"]:>14,.2f} cf={r["cashflow"]:>10,.2f} twr={r["twr_cum"]*100:>+7.2f}%')
        print('\nNegative v_end days:')
        neg = [r for r in ts if r["v_end"] < 0]
        print(f'  {len(neg)} days with negative V_end')
        if neg:
            for r in neg[:5]:
                print(f'  {r["month_end"]}: V={r["v_end"]:,.2f}')

        first = ts[0]
        last = ts[-1]
        delta = last["v_end"] - first["v_end"]
        pct = (1 + (last["twr_cum"] or 0)) / (1 + (first["twr_cum"] or 0)) - 1
        print(f'\nKPI: first.v_end={first["v_end"]:,.2f}  last.v_end={last["v_end"]:,.2f}')
        print(f'KPI: delta={delta:,.2f}  twrPeriod={pct*100:+.2f}%')

asyncio.run(main())
