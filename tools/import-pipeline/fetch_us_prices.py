#!/usr/bin/env python3
"""
fetch_us_prices.py

Fetches monthly closing prices (USD) from Yahoo Finance for all US tickers
present in us_trades.csv / us_positions_final.csv. Caches in us_prices_cache.csv.

For each ticker we fetch daily candles, then pick the last trading day of
each month (so close reflects month-end mark-to-market).

Output: us_prices_cache.csv with columns
    ticker, month_end, close_usd
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict


def to_yahoo(ticker: str) -> str:
    """IBKR uses 'BRK B' / our normalize -> 'BRK-B'. Yahoo expects 'BRK-B'."""
    return ticker.strip()


def fetch_daily(yahoo_ticker: str, date_from: str, date_to: str) -> list[dict]:
    p1 = int(dt.datetime.fromisoformat(date_from).timestamp())
    p2 = int(dt.datetime.fromisoformat(date_to).timestamp())
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_ticker}"
           f"?period1={p1}&period2={p2}&interval=1d")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        j = json.load(r)
    res = j.get("chart", {}).get("result", [{}])[0]
    if not res:
        return []
    ts = res.get("timestamp", []) or []
    quote = (res.get("indicators", {}).get("quote") or [{}])[0]
    closes = quote.get("close") or []
    out = []
    for t, c in zip(ts, closes):
        if c is None:
            continue
        d = dt.date.fromtimestamp(int(t))
        out.append({"date": d.isoformat(), "close": float(c)})
    return out


def last_trading_day_per_month(daily: list[dict]) -> dict[str, dict]:
    by_ym: dict[str, dict] = {}
    for row in daily:
        ym = row["date"][:7]
        if ym not in by_ym or row["date"] > by_ym[ym]["date"]:
            by_ym[ym] = row
    return by_ym


def month_end_date(ym: str) -> str:
    y, m = ym.split("-")
    if m == "12":
        return dt.date(int(y), 12, 31).isoformat()
    return (dt.date(int(y), int(m) + 1, 1) - dt.timedelta(days=1)).isoformat()


def collect_tickers(*paths: str) -> list[str]:
    s: set[str] = set()
    for p in paths:
        if not os.path.exists(p):
            continue
        for r in csv.DictReader(open(p, encoding="utf-8")):
            tk = (r.get("ticker") or "").strip()
            if tk:
                s.add(tk)
    return sorted(s)


def load_existing_cache(path: str) -> dict[tuple[str, str], float]:
    if not os.path.exists(path):
        return {}
    out: dict[tuple[str, str], float] = {}
    for r in csv.DictReader(open(path, encoding="utf-8")):
        out[(r["ticker"], r["month_end"])] = float(r["close_usd"])
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--trades", default="us_trades.csv")
    ap.add_argument("--positions", default="us_positions_final.csv")
    ap.add_argument("--cache", default="us_prices_cache.csv")
    ap.add_argument("--start", default="2025-03",
                    help="YYYY-MM, primeiro mês para buscar")
    ap.add_argument("--end", default=None)
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--sleep", type=float, default=0.4)
    args = ap.parse_args()

    if not args.end:
        today = dt.date.today()
        args.end = f"{today.year:04d}-{today.month:02d}"

    tickers = collect_tickers(args.trades, args.positions)
    print(f"{len(tickers)} tickers US: {tickers}")

    cache = {} if args.force else load_existing_cache(args.cache)

    date_from = f"{args.start}-01"
    y, m = map(int, args.end.split("-"))
    if m == 12:
        date_to = f"{y+1}-01-01"
    else:
        date_to = f"{y}-{m+1:02d}-01"

    fetched = updated = 0
    for tk in tickers:
        ysym = to_yahoo(tk)
        try:
            daily = fetch_daily(ysym, date_from, date_to)
        except urllib.error.HTTPError as e:
            print(f"  [ERR {e.code}] {tk}")
            time.sleep(args.sleep)
            continue
        except Exception as e:
            print(f"  [ERR] {tk}: {e}")
            time.sleep(args.sleep)
            continue

        if not daily:
            print(f"  [empty] {tk}")
            time.sleep(args.sleep)
            continue

        by_ym = last_trading_day_per_month(daily)
        added = 0
        for ym, row in by_ym.items():
            me = month_end_date(ym)
            key = (tk, me)
            if key in cache and not args.force:
                continue
            cache[key] = row["close"]
            added += 1
        fetched += 1
        if added > 0:
            updated += added
            print(f"  [OK]  {tk:<8} {len(by_ym)} months ({added} new)")
        time.sleep(args.sleep)

    rows = sorted([{"ticker": k[0], "month_end": k[1], "close_usd": v}
                   for k, v in cache.items()],
                  key=lambda r: (r["ticker"], r["month_end"]))
    with open(args.cache, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["ticker", "month_end", "close_usd"])
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"\nFetched {fetched} tickers, {updated} new month-prices.")
    print(f"Cache: {len(cache)} entries em {args.cache}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
