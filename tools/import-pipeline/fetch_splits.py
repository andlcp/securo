#!/usr/bin/env python3
"""
fetch_splits.py

For every distinct ticker in trades.csv, fetch corporate events (splits and
bonifications) from Yahoo Finance and write splits.csv.

Output columns:
    ticker, date, numerator, denominator, ratio_str, factor

`factor` = numerator / denominator. Apply by:
    qty_after_split = qty_before * factor

Yahoo conventions for B3:
    "1:20"  -> num=1,  den=20  -> factor 0.05  (agrupamento — 20 viram 1)
    "13:10" -> num=13, den=10  -> factor 1.30  (bonificação 30%)
    "2:1"   -> num=2,  den=1   -> factor 2.0   (desdobramento)

Manual overrides: ticker_aliases.csv lets you map e.g. TRPL4 -> ISAE4 (rename).
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
import time
import urllib.error
import urllib.request


def fetch_yahoo_events(yahoo_ticker: str,
                       date_from: str = "2018-01-01",
                       date_to: str = "2026-12-31") -> list[dict]:
    p1 = int(dt.datetime.fromisoformat(date_from).timestamp())
    p2 = int(dt.datetime.fromisoformat(date_to).timestamp())
    url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_ticker}"
           f"?period1={p1}&period2={p2}&interval=1d&events=split")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        j = json.load(r)
    res = j.get("chart", {}).get("result", [{}])[0]
    events = (res.get("events") or {}).get("splits", {}) or {}
    out = []
    for ts, ev in events.items():
        d = dt.date.fromtimestamp(int(ts)).isoformat()
        num = ev.get("numerator", 1)
        den = ev.get("denominator", 1)
        ratio = ev.get("splitRatio", f"{num}:{den}")
        out.append({
            "ticker": yahoo_ticker.replace(".SA", ""),
            "date": d,
            "numerator": num,
            "denominator": den,
            "ratio_str": ratio,
            "factor": float(num) / float(den) if den else 1.0,
        })
    return out


def to_yahoo(ticker: str) -> str:
    """B3 ticker -> Yahoo symbol (e.g. PETR4 -> PETR4.SA)."""
    return ticker if ticker.endswith(".SA") else f"{ticker}.SA"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--trades", default="trades.csv")
    ap.add_argument("--out", default="splits.csv")
    ap.add_argument("--sleep", type=float, default=0.4,
                    help="Delay between Yahoo requests (sec)")
    args = ap.parse_args()

    rows = list(csv.DictReader(open(args.trades, encoding="utf-8")))
    tickers = sorted({r["ticker"] for r in rows
                      if r.get("ticker") and r.get("categoria") in
                      ("VISTA", "FRACIONARIO", "EXERCICIO_CALL")})
    print(f"Buscando splits para {len(tickers)} tickers...")

    all_events: list[dict] = []
    notfound: list[str] = []
    for tk in tickers:
        ysym = to_yahoo(tk)
        try:
            evs = fetch_yahoo_events(ysym)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                notfound.append(tk)
                print(f"  [404] {tk}  (descontinuado/incorporado?)")
            else:
                print(f"  [ERR] {tk}: {e}")
            time.sleep(args.sleep)
            continue
        except Exception as e:
            print(f"  [ERR] {tk}: {e}")
            time.sleep(args.sleep)
            continue

        if evs:
            for e in evs:
                print(f"  [OK]  {tk:<8} {e['date']}  {e['ratio_str']:<12}  factor={e['factor']:.4f}")
            all_events.extend(evs)
        time.sleep(args.sleep)

    fields = ["ticker", "date", "numerator", "denominator", "ratio_str", "factor"]
    with open(args.out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for e in sorted(all_events, key=lambda x: (x["ticker"], x["date"])):
            w.writerow(e)

    print(f"\nWrote {len(all_events)} eventos em {args.out}")
    if notfound:
        print(f"\nTickers 404 ({len(notfound)}): {', '.join(notfound)}")
        print("Podem precisar de tratamento manual (incorporação/encerramento).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
