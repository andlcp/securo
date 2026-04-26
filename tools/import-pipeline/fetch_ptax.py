#!/usr/bin/env python3
"""
fetch_ptax.py

Fetches daily PTAX (USD/BRL) from BCB. Uses series 1 for "USD compra" and
series 10813 for "USD venda" via SGS public API. Caches in ptax_daily.csv.

PTAX is the official rate published by the Central Bank used for tax purposes
on foreign exchange operations.

Output: ptax_daily.csv with columns
    data, cot_compra, cot_venda

Usage:
    python fetch_ptax.py [--start 2019-01]
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import sys
import urllib.request


def fetch_bcb_series(serie: int, start: str, end: str) -> dict[str, float]:
    sd = dt.datetime.fromisoformat(start).strftime("%d/%m/%Y")
    ed = dt.datetime.fromisoformat(end).strftime("%d/%m/%Y")
    url = (f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie}/dados"
           f"?formato=json&dataInicial={sd}&dataFinal={ed}")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=120) as r:
        rows = json.load(r)
    out = {}
    for row in rows:
        d = dt.datetime.strptime(row["data"], "%d/%m/%Y").date()
        out[d.isoformat()] = float(row["valor"])
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--start", default="2019-01-01")
    ap.add_argument("--end", default=None)
    ap.add_argument("--out", default="ptax_daily.csv")
    args = ap.parse_args()

    if not args.end:
        args.end = dt.date.today().isoformat()

    print(f"Fetching PTAX  {args.start} -> {args.end}")
    print("  serie 10813 (USD venda)...")
    venda = fetch_bcb_series(10813, args.start, args.end)
    print(f"    {len(venda)} business days")
    print("  serie 1 (USD compra)...")
    compra = fetch_bcb_series(1, args.start, args.end)
    print(f"    {len(compra)} business days")

    days = sorted(set(venda) | set(compra))
    with open(args.out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["data", "cot_compra", "cot_venda"])
        w.writeheader()
        for d in days:
            w.writerow({
                "data": d,
                "cot_compra": round(compra.get(d, ""), 6) if d in compra else "",
                "cot_venda": round(venda.get(d, ""), 6) if d in venda else "",
            })
    print(f"\nWrote {args.out} ({len(days)} business days)")
    if days:
        latest = days[-1]
        print(f"\nÚltimas 5 cotações:")
        for d in days[-5:]:
            print(f"  {d}  compra=R$ {compra.get(d, 0):.4f}  "
                  f"venda=R$ {venda.get(d, 0):.4f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
