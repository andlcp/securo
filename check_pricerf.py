import yfinance as yf

PDF_RENT = {
    'ARML3.SA': 1.0299, 'ASAI3.SA': 0.1836, 'BLAU3.SA': 0.2734, 'CMIN3.SA': 0.1297,
    'GRND3.SA': 0.2376, 'ISAE4.SA': 0.3303, 'IVVB11.SA': 0.3858, 'JHSF3.SA': 2.8300,
    'KEPL3.SA': 0.0276, 'LEVE3.SA': 0.2591, 'PETR4.SA': 0.5015, 'PLPL3.SA': 0.1956,
    'POSI3.SA': -0.3806, 'RECV3.SA': 0.2256, 'TTEN3.SA': 0.2680, 'VALE3.SA': 0.5983,
    'VAMO3.SA': 0.6001, 'VLID3.SA': 0.4687,
}

print('Ticker      C24      C26   PricePerf    PDF     Diff   Tag')
print('-' * 65)
for tk, pdf in PDF_RENT.items():
    t = yf.Ticker(tk)
    df = t.history(start='2024-04-15', end='2026-04-30', auto_adjust=False)
    if len(df) == 0:
        print(tk, ' sem dados'); continue
    c24 = None; c26 = None
    for d, c in df['Close'].items():
        ds = d.date().isoformat()
        if ds <= '2024-04-22': c24 = c
        if ds <= '2026-04-22': c26 = c
    if c24 is None or c26 is None or c24 == 0:
        print(tk, ' sem closes'); continue
    pp = c26/c24 - 1
    diff = (pp - pdf) * 100
    tag = 'MATCH' if abs(diff) < 3 else ('~' if abs(diff) < 8 else 'NO')
    print(f'{tk:<10}  {c24:>6.2f}  {c26:>6.2f}  {pp*100:>+7.2f}%  {pdf*100:>+7.2f}%  {diff:>+6.2f}  {tag}')
