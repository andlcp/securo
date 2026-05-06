import asyncio, uuid
from datetime import date
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.core.config import get_settings
from app.models.asset import Asset
from app.models.user import User
from app.services.portfolio_timeseries_service import get_timeseries

USER_ID = uuid.UUID('8bcb95a1-fce0-4de3-8952-15d04343e3f8')

PDF = {
    'ARML3.SA': 1.0299, 'ASAI3.SA': 0.1836, 'BLAU3.SA': 0.2734, 'CMIN3.SA': 0.1297,
    'GRND3.SA': 0.2376, 'ISAE4.SA': 0.3303, 'IVVB11.SA': 0.3858, 'JHSF3.SA': 2.8300,
    'KEPL3.SA': 0.0276, 'LEVE3.SA': 0.2591, 'PETR4.SA': 0.5015, 'PLPL3.SA': 0.1956,
    'POSI3.SA': -0.3806, 'RECV3.SA': 0.2256, 'TTEN3.SA': 0.2680, 'VALE3.SA': 0.5983,
    'VAMO3.SA': 0.6001, 'VLID3.SA': 0.4687,
}

# Earlier baseline (Option A from previous run)
OPT_A = {
    'ARML3.SA': 0.6578, 'ASAI3.SA': -0.0604, 'BLAU3.SA': 0.2624, 'CMIN3.SA': 0.2954,
    'GRND3.SA': 0.0025, 'ISAE4.SA': 0.5076, 'IVVB11.SA': 0.3849, 'JHSF3.SA': 2.9838,
    'KEPL3.SA': 0.0464, 'LEVE3.SA': 0.3863, 'PETR4.SA': 0.6866, 'PLPL3.SA': 0.1614,
    'POSI3.SA': -0.3537, 'RECV3.SA': 0.2115, 'TTEN3.SA': 0.2073, 'VALE3.SA': 0.6925,
    'VAMO3.SA': 0.2411, 'VLID3.SA': 0.5609,
}

YF_BAD = {'ARML3.SA','VAMO3.SA','RECV3.SA','ASAI3.SA','GRND3.SA','TTEN3.SA'}

s = get_settings(); engine = create_async_engine(s.database_url)
Session = async_sessionmaker(engine, expire_on_commit=False)

async def main():
    async with Session() as session:
        user = (await session.execute(select(User).where(User.id == USER_ID))).scalar_one()
        rows = (await session.execute(
            select(Asset).where(Asset.asset_class == 'RENDA_VARIAVEL_BR',
                                Asset.user_id == USER_ID,
                                Asset.is_archived == False))).scalars().all()
        ours = {a.ticker: a for a in rows if a.ticker}

        d_from = date(2024, 4, 22); d_to = date(2026, 4, 22)
        print(f"{'Ticker':<10} {'PDF':>8} {'OptA-old':>9} {'OptB-now':>9} {'Gap-A':>7} {'Gap-B':>7} {'Tag'}")
        print('-' * 80)
        for tk, pdf in PDF.items():
            if tk not in ours: continue
            ts = await get_timeseries(session, user, asset_ids=[ours[tk].id],
                                      date_from=d_from, date_to=d_to,
                                      granularity='daily')
            if not ts: continue
            base = 1 + (ts[0].get('twr_cum') or 0)
            top = 1 + (ts[-1].get('twr_cum') or 0)
            opt_b = top/base - 1 if base > 0 else 0
            opt_a = OPT_A[tk]
            gap_a = (opt_a - pdf) * 100
            gap_b = (opt_b - pdf) * 100
            tag = '[yf bad]' if tk in YF_BAD else ''
            improved = '✓' if abs(gap_b) < abs(gap_a) else ('=' if abs(gap_b - gap_a) < 0.5 else '×')
            print(f'{tk:<10} {pdf*100:>+7.2f}% {opt_a*100:>+8.2f}% {opt_b*100:>+8.2f}% {gap_a:>+6.2f} {gap_b:>+6.2f} {improved} {tag}')

asyncio.run(main())
