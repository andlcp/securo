"""Simula Modified Dietz com 3 opções diferentes pra cf_amount no dia da
compra:
  - A (atual): cf = qty × close. V_end = units × close.
  - B (raw cash): cf = qty × user_price. V_end = units × close.
  - C (hibrido): cf = qty × user_price. V_end no dia = old_units × close + new_qty × user_price.

Compara com a Rentabilidade 24M do PDF da XP (22/04/2024 → 22/04/2026).
"""
import asyncio, uuid
from datetime import date, timedelta
from collections import defaultdict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.core.config import get_settings
from app.models.asset import Asset
from app.models.asset_transaction import AssetTransaction
from app.models.user import User
from app.services.investment_benchmark_service import fetch_yahoo_close_history

USER_ID = uuid.UUID('8bcb95a1-fce0-4de3-8952-15d04343e3f8')

# 22/04/2024 -> 22/04/2026 (PDF reference period)
D_FROM = date(2024, 4, 22)
D_TO = date(2026, 4, 22)

PDF_RENT = {
    'ARML3.SA': 1.0299, 'ASAI3.SA': 0.1836, 'BLAU3.SA': 0.2734, 'CMIN3.SA': 0.1297,
    'GRND3.SA': 0.2376, 'ISAE4.SA': 0.3303, 'IVVB11.SA': 0.3858, 'JHSF3.SA': 2.8300,
    'KEPL3.SA': 0.0276, 'LEVE3.SA': 0.2591, 'PETR4.SA': 0.5015, 'PLPL3.SA': 0.1956,
    'POSI3.SA': -0.3806, 'RECV3.SA': 0.2256, 'TTEN3.SA': 0.2680, 'VALE3.SA': 0.5983,
    'VAMO3.SA': 0.6001, 'VLID3.SA': 0.4687,
}

# Tickers com yfinance dados ruins (skip pra análise metodológica pura)
YF_BAD = {'ARML3.SA','VAMO3.SA','RECV3.SA','ASAI3.SA','GRND3.SA','TTEN3.SA'}

s = get_settings(); engine = create_async_engine(s.database_url)
Session = async_sessionmaker(engine, expire_on_commit=False)


def closes_at(hist, on):
    """Closest close on or before `on`."""
    if not hist: return None
    target = on.isoformat()
    last = None
    for d_iso, p in sorted(hist.items()):
        if d_iso > target: break
        last = p
    return last


def sim_option(option, txs_by_date, units_by_date, hist, d_from, d_to):
    """Walk daily. Return cum return at d_to.

    txs_by_date: {date: [(type, qty, value)]} (only BUY/SELL)
    units_by_date: callable(date) -> units owned at end of day
    hist: {iso_date: close}
    """
    cum = 1.0
    prev_v = 0.0
    d = d_from
    while d <= d_to:
        iso = d.isoformat()
        close = closes_at(hist, d)
        units = units_by_date(d)
        # V_end (with option C tweak on tx day)
        v_end = (units * close) if close is not None else prev_v
        cf_buy = 0.0; cf_sell = 0.0; inc = 0.0
        for (tp, qty, val) in txs_by_date.get(d, []):
            user_price = (val / qty) if qty else 0
            if tp == 'BUY':
                if option == 'A':
                    cf_buy += qty * (close or user_price)
                else:  # B or C
                    cf_buy += val  # real cash
                if option == 'C' and close is not None:
                    # Adjust V_end: replace this tx's close-based portion
                    # with user_price-based portion.
                    v_end = v_end - qty * close + qty * user_price
            elif tp == 'SELL':
                if option == 'A':
                    cf_sell += qty * (close or user_price)
                else:
                    cf_sell += val
                if option == 'C' and close is not None:
                    v_end = v_end + qty * close - qty * user_price
            else:  # INCOME
                inc += val
        cf = cf_buy - cf_sell
        denom = prev_v + 0.5 * cf
        if denom > 1e-3:
            r = (v_end + inc - prev_v - cf) / denom
            r = max(min(r, 0.5), -0.5)
            cum *= (1 + r)
        prev_v = v_end
        d += timedelta(days=1)
    return cum - 1


async def main():
    async with Session() as session:
        user = (await session.execute(select(User).where(User.id == USER_ID))).scalar_one()
        rows = (await session.execute(
            select(Asset).where(Asset.asset_class == 'RENDA_VARIAVEL_BR',
                                Asset.user_id == USER_ID,
                                Asset.is_archived == False))).scalars().all()
        ours = {a.ticker: a for a in rows if a.ticker}

        print(f'{"Ticker":<10} {"PDF":>8} {"OptA":>8} {"OptB":>8} {"OptC":>8} {"BestGap":>8} {"Tag"}')
        print('-' * 75)
        for tk, pdf in PDF_RENT.items():
            if tk not in ours:
                print(f'{tk:<10} {pdf*100:>+7.2f}% {"FALTA":>8}')
                continue
            a = ours[tk]
            # Fetch yfinance close history
            hist = await fetch_yahoo_close_history(tk, D_FROM - timedelta(days=10), D_TO)
            if not hist:
                print(f'{tk:<10} sem dados')
                continue

            # Get all txs for this asset
            txs = (await session.execute(
                select(AssetTransaction).where(AssetTransaction.asset_id == a.id)
                .order_by(AssetTransaction.date))).scalars().all()
            txs_by_date = defaultdict(list)
            for tx in txs:
                if tx.value is None: continue
                if tx.type in ('BUY','DEPOSIT'):
                    if tx.qty and float(tx.value) > 0:
                        txs_by_date[tx.date].append(('BUY', float(tx.qty), float(tx.value)))
                elif tx.type in ('SELL','WITHDRAWAL'):
                    if tx.qty and float(tx.value) > 0:
                        txs_by_date[tx.date].append(('SELL', float(tx.qty), float(tx.value)))
                elif tx.type in ('DIVIDEND','JCP','RENDIMENTO','RESGATE','INTEREST'):
                    txs_by_date[tx.date].append(('INC', 0, float(tx.value)))

            # units_at function (walk-back)
            sorted_buy_sell = sorted(
                [(t.date, t.type, float(t.qty or 0)) for t in txs
                 if t.type in ('BUY','DEPOSIT','SELL','WITHDRAWAL') and t.qty],
                key=lambda x: x[0])
            current_units = float(a.units)
            def units_at(on):
                u = current_units
                for (d_, tp, q) in reversed(sorted_buy_sell):
                    if d_ <= on: break
                    if tp in ('BUY','DEPOSIT'): u -= q
                    else: u += q
                return max(u, 0)

            ra = sim_option('A', txs_by_date, units_at, hist, D_FROM, D_TO)
            rb = sim_option('B', txs_by_date, units_at, hist, D_FROM, D_TO)
            rc = sim_option('C', txs_by_date, units_at, hist, D_FROM, D_TO)

            gaps = {'A': abs(ra - pdf), 'B': abs(rb - pdf), 'C': abs(rc - pdf)}
            best = min(gaps, key=gaps.get)

            tag = '[yf bad]' if tk in YF_BAD else ''
            print(f'{tk:<10} {pdf*100:>+7.2f}% {ra*100:>+7.2f}% {rb*100:>+7.2f}% {rc*100:>+7.2f}% {gaps[best]*100:>+7.2f}pp {best:<3} {tag}')

asyncio.run(main())
