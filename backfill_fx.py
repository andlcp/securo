"""Backfill USD/BRL daily history from BCB SGS series 1.

Pulls every business day from 2019-01-01 to today and upserts into
fx_rates with source='bcb_sgs1'. Runs once after deploy.
"""
import asyncio, uuid
from datetime import date
from decimal import Decimal
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.core.config import get_settings
from app.models.fx_rate import FxRate
from app.providers.bcb_ptax import BcbPtaxProvider

s = get_settings(); engine = create_async_engine(s.database_url)
Session = async_sessionmaker(engine, expire_on_commit=False)

async def main():
    provider = BcbPtaxProvider()
    rates = await provider.fetch_range(date(2019, 1, 1), date.today())
    print(f'Got {len(rates)} daily USD/BRL rates from BCB')

    async with Session() as session:
        count = 0
        for d, rate in rates.items():
            stmt = pg_insert(FxRate).values(
                base_currency='USD',
                quote_currency='BRL',
                date=d,
                rate=rate,
                source='bcb_sgs1',
            ).on_conflict_do_update(
                constraint='uq_fx_rate_base_quote_date',
                set_={'rate': rate, 'source': 'bcb_sgs1'},
            )
            await session.execute(stmt)
            count += 1
        await session.commit()
        print(f'Upserted {count} rows.')

    # Sanity check
    async with Session() as session:
        rows = (await session.execute(
            select(FxRate).where(FxRate.quote_currency == 'BRL')
            .order_by(FxRate.date.desc()).limit(5)
        )).scalars().all()
        print('\nMost recent rates:')
        for r in rows:
            print(f'  {r.date}: R$ {float(r.rate):.4f} ({r.source})')

asyncio.run(main())
