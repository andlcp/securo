#!/usr/bin/env python3
"""
fetch_benchmarks.py

Fetches monthly returns for benchmarks to compare against the personal TWR:
    - IBOV  : Yahoo ^BVSP (Ibovespa, BRL)
    - IVVB  : Yahoo IVVB11.SA (S&P 500 hedged BRL — what user actually holds)
    - SP500 : Yahoo ^GSPC (USD, optional)
    - CDI   : computed monthly compounding from BCB SGS series 12 (CDI daily)

Output: benchmarks_monthly.csv with columns
    month_end, bench, return_month, index_value (rebased to 1.0 at start)

Usage:
    python fetch_benchmarks.py --start 2020-10
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
import time
import urllib.request


def fetch_yahoo_monthly_close(symbol: str, start: str, end: str) -> dict[str, float]:
    p1 = int(dt.datetime.fromisoformat(start).timestamp())
    p2 = int(dt.datetime.fromisoformat(end).timestamp())
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
           f"?period1={p1}&period2={p2}&interval=1d")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        j = json.load(r)
    res = j["chart"]["result"][0]
    ts = res.get("timestamp", [])
    close = res["indicators"]["quote"][0].get("close", [])
    by_ym: dict[str, dict] = {}
    for t, c in zip(ts, close):
        if c is None:
            continue
        d = dt.date.fromtimestamp(int(t))
        ym = d.strftime("%Y-%m")
        if ym not in by_ym or d.isoformat() > by_ym[ym]["date"]:
            by_ym[ym] = {"date": d.isoformat(), "close": float(c)}
    # Convert to month_end keyed
    out = {}
    for ym, row in by_ym.items():
        y, m = map(int, ym.split("-"))
        if m == 12:
            me = dt.date(y, 12, 31)
        else:
            me = dt.date(y, m + 1, 1) - dt.timedelta(days=1)
        out[me.isoformat()] = row["close"]
    return out


def fetch_bcb_cdi(start: str, end: str) -> dict[str, float]:
    """Returns {month_end_iso -> monthly_factor}. Series 12 = CDI daily.
    Compounds daily factors to a monthly factor."""
    sd = dt.datetime.fromisoformat(start).strftime("%d/%m/%Y")
    ed = dt.datetime.fromisoformat(end).strftime("%d/%m/%Y")
    url = (f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados"
           f"?formato=json&dataInicial={sd}&dataFinal={ed}")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        rows = json.load(r)
    # Rows: [{'data': 'dd/mm/yyyy', 'valor': '0.0...'}]  (daily % rate)
    # Compound by month
    monthly_factor: dict[str, float] = {}
    for row in rows:
        d = dt.datetime.strptime(row["data"], "%d/%m/%Y").date()
        rate = float(row["valor"]) / 100.0  # to decimal
        ym = d.strftime("%Y-%m")
        monthly_factor[ym] = monthly_factor.get(ym, 1.0) * (1.0 + rate)
    # Convert to month_end keys
    out = {}
    for ym, f in monthly_factor.items():
        y, m = map(int, ym.split("-"))
        if m == 12:
            me = dt.date(y, 12, 31)
        else:
            me = dt.date(y, m + 1, 1) - dt.timedelta(days=1)
        out[me.isoformat()] = f - 1.0  # monthly return
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", default="2020-10")
    ap.add_argument("--end", default=None)
    ap.add_argument("--out", default="benchmarks_monthly.csv")
    args = ap.parse_args()

    if not args.end:
        today = dt.date.today()
        args.end = f"{today.year:04d}-{today.month:02d}"

    # Date ranges for fetch (give some slack on the start)
    sy, sm = map(int, args.start.split("-"))
    fetch_start = f"{sy}-{max(sm-1,1):02d}-01"
    ey, em = map(int, args.end.split("-"))
    if em == 12:
        fetch_end = f"{ey+1}-01-31"
    else:
        fetch_end = f"{ey}-{em+1:02d}-28"

    print(f"Fetching benchmarks {fetch_start} -> {fetch_end}")

    benches = {}

    # Yahoo: IBOV, IVVB11, S&P500
    yh = {
        "IBOV": "^BVSP",
        "IVVB11": "IVVB11.SA",
        "SP500_USD": "^GSPC",
    }
    for name, sym in yh.items():
        print(f"  fetching {name} ({sym})...")
        try:
            closes = fetch_yahoo_monthly_close(sym, fetch_start, fetch_end)
            benches[name] = closes
            print(f"    OK: {len(closes)} monthly closes")
        except Exception as e:
            print(f"    ERR: {e}")
        time.sleep(0.4)

    # BCB CDI
    print(f"  fetching CDI (BCB SGS 12)...")
    try:
        cdi_returns = fetch_bcb_cdi(fetch_start, fetch_end)
        benches["CDI"] = cdi_returns
        print(f"    OK: {len(cdi_returns)} monthly returns")
    except Exception as e:
        print(f"    ERR: {e}")

    # Build month-end iterator
    def iter_months(s, e):
        sy, sm = map(int, s.split("-"))
        ey, em = map(int, e.split("-"))
        y, m = sy, sm
        while (y, m) <= (ey, em):
            if m == 12:
                me = dt.date(y, 12, 31)
            else:
                me = dt.date(y, m + 1, 1) - dt.timedelta(days=1)
            yield me.isoformat()
            m += 1
            if m > 12:
                y, m = y + 1, 1

    out_rows = []
    bench_index: dict[str, float] = {b: 1.0 for b in benches}
    prev_close: dict[str, float] = {}

    for me in iter_months(args.start, args.end):
        for name, data in benches.items():
            if name == "CDI":
                # data already monthly returns
                r = data.get(me, 0.0)
            else:
                # data is closes; compute monthly return
                c = data.get(me)
                if c is None:
                    r = 0.0
                else:
                    pc = prev_close.get(name)
                    r = (c / pc - 1.0) if pc else 0.0
                    prev_close[name] = c
            bench_index[name] *= (1.0 + r)
            out_rows.append({
                "month_end": me,
                "bench": name,
                "return_month": round(r, 6),
                "index_value": round(bench_index[name], 6),
            })

    fields = ["month_end", "bench", "return_month", "index_value"]
    with open(args.out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in out_rows:
            w.writerow(r)

    # Summary
    print(f"\nWrote {args.out}")
    print(f"\n{'BENCH':<12} {'CUMUL':>10} {'ANNUAL':>10}")
    months_count = sum(1 for _ in iter_months(args.start, args.end))
    years = months_count / 12
    for b in benches:
        idx = bench_index[b]
        ann = idx**(1/years) - 1 if years > 0 else 0
        print(f"{b:<12} {(idx-1)*100:>+9.2f}% {ann*100:>+9.2f}%")

    return 0


if __name__ == "__main__":
    sys.exit(main())
