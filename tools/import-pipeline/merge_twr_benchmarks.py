#!/usr/bin/env python3
"""
merge_twr_benchmarks.py

Joins twr_monthly.csv with benchmarks_monthly.csv into a single wide CSV
suitable for plotting (twr_full.csv).

Output columns:
    month, month_end, v_end, return_month, return_cum,
    ibov_cum, ivvb11_cum, sp500_cum, cdi_cum
"""
from __future__ import annotations

import csv
import sys


def main() -> int:
    twr = list(csv.DictReader(open("twr_monthly.csv", encoding="utf-8")))
    bench = list(csv.DictReader(open("benchmarks_monthly.csv", encoding="utf-8")))

    # bench: month_end -> {bench -> index_value}
    by_me: dict[str, dict[str, float]] = {}
    for r in bench:
        me = r["month_end"]
        by_me.setdefault(me, {})[r["bench"]] = float(r["index_value"])

    out = []
    for r in twr:
        me = r["month_end"]
        b = by_me.get(me, {})
        out.append({
            "month": r["month"],
            "month_end": me,
            "v_end_rv": r.get("v_end_rv", "0"),
            "v_end_rf": r.get("v_end_rf_liquido", "0"),
            "v_end_us": r.get("v_end_us_liquido", "0"),
            "v_end": r.get("v_end_liquido", r["v_end"]),
            "cashflow_month": r["cashflow"],
            "income_month": r.get("income", "0"),
            "return_month": r["return_month"],
            "twr_cum": float(r["return_cum"]) if r["return_cum"] else 0,
            "twr_cum_bruto": float(r.get("return_cum_bruto", 0) or 0),
            "ibov_cum": (b.get("IBOV", 1.0) - 1) if b else 0,
            "ivvb11_cum": (b.get("IVVB11", 1.0) - 1) if b else 0,
            "sp500_cum": (b.get("SP500_USD", 1.0) - 1) if b else 0,
            "cdi_cum": (b.get("CDI", 1.0) - 1) if b else 0,
        })

    fields = ["month", "month_end", "v_end_rv", "v_end_rf", "v_end_us", "v_end",
              "cashflow_month", "income_month",
              "return_month", "twr_cum", "twr_cum_bruto",
              "ibov_cum", "ivvb11_cum", "sp500_cum", "cdi_cum"]
    with open("twr_full.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in out:
            w.writerow(r)
    print(f"Wrote twr_full.csv ({len(out)} months)")

    # Print compact summary every 6 months
    print(f"\n{'MONTH':<8} {'V_end':>11} {'TWR':>8} {'IBOV':>8} {'IVVB':>8} {'SP500':>8} {'CDI':>8}")
    for i, r in enumerate(out):
        if i % 6 != 0 and i != len(out) - 1:
            continue
        print(f"{r['month']:<8} {float(r['v_end']):>11,.0f} "
              f"{r['twr_cum']*100:>+7.1f}% "
              f"{r['ibov_cum']*100:>+7.1f}% "
              f"{r['ivvb11_cum']*100:>+7.1f}% "
              f"{r['sp500_cum']*100:>+7.1f}% "
              f"{r['cdi_cum']*100:>+7.1f}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
