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
        # Same params as the frontend KPI call: months=12, sinceStart=True, granularity=default(monthly)
        ts = await get_timeseries(session, user, months=12, since_start=True, granularity='monthly')
        print(f'Total points: {len(ts)}')
        print('FIRST 5:')
        for r in ts[:5]:
            print(f'  {r["month"]}: V_end={r["v_end"]:>14,.2f} cf={r["cashflow"]:>10,.2f} inc={r["income"]:>8,.2f} twr_cum={(r["twr_cum"] or 0)*100:>+7.2f}%')
        print('\nLAST 3:')
        for r in ts[-3:]:
            print(f'  {r["month"]}: V_end={r["v_end"]:>14,.2f} cf={r["cashflow"]:>10,.2f} inc={r["income"]:>8,.2f} twr_cum={(r["twr_cum"] or 0)*100:>+7.2f}%')
        first = ts[0]
        last = ts[-1]
        delta = last["v_end"] - first["v_end"]
        ftw = first["twr_cum"] or 0
        ltw = last["twr_cum"] or 0
        pct = (1 + ltw) / (1 + ftw) - 1
        print(f'\nKPI calc:')
        print(f'  first.v_end = {first["v_end"]:,.2f}')
        print(f'  last.v_end  = {last["v_end"]:,.2f}')
        print(f'  delta       = {delta:,.2f}')
        print(f'  first.twr_cum = {ftw*100:+.4f}%')
        print(f'  last.twr_cum  = {ltw*100:+.4f}%')
        print(f'  twrPeriod (rebase) = {pct*100:+.2f}%')

asyncio.run(main())
