"""Backfill purchase_price (preço médio) for US stock assets.

Brazilian "preço médio" convention:
  - BUY: new_avg = (old_avg × old_qty + buy_qty × buy_price) / (old_qty + buy_qty)
  - SELL: avg unchanged, qty -= sell_qty

Walks each US stock's BUY/SELL transactions in chronological order,
computes the running avg, and stores the final value in asset.purchase_price.
"""
import asyncio, uuid
from decimal import Decimal
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.core.config import get_settings
from app.models.asset import Asset
from app.models.asset_transaction import AssetTransaction

USER_ID = uuid.UUID('8bcb95a1-fce0-4de3-8952-15d04343e3f8')

s = get_settings(); engine = create_async_engine(s.database_url)
Session = async_sessionmaker(engine, expire_on_commit=False)


async def main():
    async with Session() as session:
        rows = (await session.execute(
            select(Asset).where(
                Asset.user_id == USER_ID,
                Asset.asset_class == 'STOCKS_US',
                Asset.is_archived == False,
            ).order_by(Asset.ticker)
        )).scalars().all()

        print(f"{'Ticker':<8} {'Cotas':>8} {'AvgCost':>10} {'LastPx':>10} {'Investido':>12} {'Saldo':>12} {'Rent%':>8}")
        print('-' * 75)

        for asset in rows:
            txs = (await session.execute(
                select(AssetTransaction).where(
                    AssetTransaction.asset_id == asset.id,
                    AssetTransaction.type.in_(['BUY', 'SELL']),
                ).order_by(AssetTransaction.date)
            )).scalars().all()

            avg_cost = 0.0
            qty = 0.0
            for tx in txs:
                if tx.qty is None or tx.value is None:
                    continue
                tx_qty = float(tx.qty)
                tx_value = float(tx.value)
                if tx_qty <= 0:
                    continue
                if tx.type == 'BUY':
                    new_qty = qty + tx_qty
                    if new_qty > 0:
                        avg_cost = (qty * avg_cost + tx_value) / new_qty
                    qty = new_qty
                elif tx.type == 'SELL':
                    qty -= tx_qty
                    # avg unchanged

            if qty <= 0 or avg_cost <= 0:
                print(f'{asset.ticker:<8} {float(asset.units):>8.2f} {"-":>10} {"-":>10} sem BUYs')
                continue

            last = float(asset.last_price or 0)
            invested = qty * avg_cost
            saldo = qty * last
            rent_pct = (saldo - invested) / invested * 100 if invested > 0 else 0
            print(f'{asset.ticker:<8} {qty:>8.2f} ${avg_cost:>9.2f} ${last:>9.2f} ${invested:>11.2f} ${saldo:>11.2f} {rent_pct:>+7.2f}%')

            # Update the asset
            asset.purchase_price = Decimal(str(round(avg_cost, 4)))

        await session.commit()
        print('\nCommitted.')


asyncio.run(main())
