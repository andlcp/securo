#!/usr/bin/env python3
"""
fetch_prices.py

Fetch monthly closing prices from Yahoo Finance for all tickers that ever
appeared in holdings_monthly.csv. Caches in prices_cache.csv.

For each ticker we fetch daily candles, then pick the last trading day of
each month (so that the close reflects the actual mark-to-market value of
holdings on the month-end snapshot).

For tickers that 404 (delisted/incorporated), we fall back to:
  - last known price from trades.csv (last trade price)
  - kept constant going forward until the holding goes to zero

Output: prices_cache.csv with columns
    ticker, month_end, close
where month_end is the last calendar day of each month (YYYY-MM-DD).

Usage:
    python fetch_prices.py [--force] [--start 2020-09]
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
    return ticker if ticker.endswith(".SA") else f"{ticker}.SA"


def fetch_daily(yahoo_ticker: str, date_from: str, date_to: str) -> list[dict]:
    """Returns list of {date, close} for every trading day."""
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
    """Group daily by YYYY-MM and keep the latest day's close per month."""
    by_ym: dict[str, dict] = {}
    for row in daily:
        ym = row["date"][:7]  # YYYY-MM
        if ym not in by_ym or row["date"] > by_ym[ym]["date"]:
            by_ym[ym] = row
    return by_ym


def month_end_date(ym: str) -> str:
    """YYYY-MM -> YYYY-MM-DD (last calendar day)."""
    y, m = ym.split("-")
    if m == "12":
        last = dt.date(int(y), 12, 31)
    else:
        last = dt.date(int(y), int(m) + 1, 1) - dt.timedelta(days=1)
    return last.isoformat()


def load_holdings_tickers(holdings_csv: str) -> list[str]:
    rows = list(csv.DictReader(open(holdings_csv, encoding="utf-8")))
    return sorted({r["ticker"] for r in rows})


def load_last_trade_prices(trades_csv: str) -> dict[str, dict]:
    """Last trade price per ticker, used as fallback for delisted ones."""
    rows = list(csv.DictReader(open(trades_csv, encoding="utf-8")))
    last: dict[str, dict] = {}
    for r in rows:
        if r.get("categoria") not in ("VISTA", "FRACIONARIO", "EXERCICIO_CALL"):
            continue
        tk = r["ticker"]
        d = r["data"]
        if tk not in last or d > last[tk]["date"]:
            last[tk] = {"date": d, "preco": float(r["preco"])}
    return last


def load_existing_cache(path: str) -> dict[tuple[str, str], float]:
    if not os.path.exists(path):
        return {}
    out: dict[tuple[str, str], float] = {}
    for r in csv.DictReader(open(path, encoding="utf-8")):
        out[(r["ticker"], r["month_end"])] = float(r["close"])
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--holdings", default="holdings_monthly.csv")
    ap.add_argument("--trades", default="trades.csv")
    ap.add_argument("--cache", default="prices_cache.csv")
    ap.add_argument("--start", default="2020-09",
                    help="YYYY-MM, primeiro mês para buscar (default 2020-09)")
    ap.add_argument("--end", default=None,
                    help="YYYY-MM, último mês (default: mês atual)")
    ap.add_argument("--force", action="store_true",
                    help="Refetch tudo (ignora cache)")
    ap.add_argument("--sleep", type=float, default=0.4)
    args = ap.parse_args()

    if not args.end:
        today = dt.date.today()
        args.end = f"{today.year:04d}-{today.month:02d}"

    tickers = load_holdings_tickers(args.holdings)
    print(f"{len(tickers)} tickers para buscar preços de {args.start} a {args.end}")

    last_trade_prices = load_last_trade_prices(args.trades)
    cache = {} if args.force else load_existing_cache(args.cache)

    date_from = f"{args.start}-01"
    y, m = map(int, args.end.split("-"))
    if m == 12:
        date_to = f"{y+1}-01-01"
    else:
        date_to = f"{y}-{m+1:02d}-01"

    notfound: list[str] = []
    fetched = updated = 0

    for tk in tickers:
        ysym = to_yahoo(tk)
        try:
            daily = fetch_daily(ysym, date_from, date_to)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                notfound.append(tk)
                print(f"  [404] {tk}  (vai usar último preço de trade)")
            else:
                print(f"  [ERR] {tk}: {e}")
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
            print(f"  [OK]  {tk:<10} {len(by_ym)} months ({added} new)")
        time.sleep(args.sleep)

    # Fallback for 404s: use last trade price, hold constant from then on
    for tk in notfound:
        last = last_trade_prices.get(tk)
        if not last:
            continue
        last_date = last["date"]
        price = last["preco"]
        # Generate flat price from last trade onwards through end of period
        ym_start = last_date[:7]
        y, m = map(int, ym_start.split("-"))
        end_y, end_m = map(int, args.end.split("-"))
        added = 0
        while (y, m) <= (end_y, end_m):
            me = month_end_date(f"{y:04d}-{m:02d}")
            key = (tk, me)
            if key not in cache or args.force:
                cache[key] = price
                added += 1
            m += 1
            if m > 12:
                m = 1
                y += 1
        if added:
            print(f"  [fb]  {tk:<10} flat @ R${price:.4f} ({added} months)")
            updated += added

    # Write cache
    rows = sorted([{"ticker": k[0], "month_end": k[1], "close": v}
                   for k, v in cache.items()],
                  key=lambda r: (r["ticker"], r["month_end"]))
    with open(args.cache, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["ticker", "month_end", "close"])
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"\nFetched {fetched} tickers, {updated} new month-prices.")
    print(f"Cache: {len(cache)} (ticker,month) entries em {args.cache}")
    if notfound:
        print(f"Tickers 404 ({len(notfound)}): {', '.join(notfound)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
