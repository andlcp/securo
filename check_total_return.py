import yfinance as yf

PDF_RENT = {
    'PETR4.SA': 0.5015, 'VALE3.SA': 0.5983, 'JHSF3.SA': 2.8300,
    'IVVB11.SA': 0.3858, 'BLAU3.SA': 0.2734, 'POSI3.SA': -0.3806,
    'KEPL3.SA': 0.0276, 'PLPL3.SA': 0.1956, 'RECV3.SA': 0.2256,
    'ARML3.SA': 1.0299, 'ASAI3.SA': 0.1836, 'GRND3.SA': 0.2376,
    'CMIN3.SA': 0.1297, 'ISAE4.SA': 0.3303, 'LEVE3.SA': 0.2591,
    'TTEN3.SA': 0.2680, 'VAMO3.SA': 0.6001, 'VLID3.SA': 0.4687,
}

print('Ticker      C24      C26     Divs   Total Ret   PDF      Diff   Tag')
print('-' * 75)
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

    # Sum dividends per share between 22/04/2024 and 22/04/2026
    divs = t.dividends
    div_sum = 0.0
    for d, v in divs.items():
        ds = d.date().isoformat()
        if '2024-04-22' < ds <= '2026-04-22':
            div_sum += float(v)

    total_ret = (c26 + div_sum) / c24 - 1
    diff = (total_ret - pdf) * 100
    tag = 'MATCH' if abs(diff) < 5 else ('~' if abs(diff) < 12 else 'NO')
    print(f'{tk:<10}  {c24:>6.2f}  {c26:>6.2f}  {div_sum:>6.2f}  {total_ret*100:>+8.2f}%  {pdf*100:>+7.2f}%  {diff:>+6.2f}  {tag}')
