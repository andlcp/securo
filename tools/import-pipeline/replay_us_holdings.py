#!/usr/bin/env python3
"""
replay_us_holdings.py

Reconstrói posições mensais (em USD e em BRL) da conta IBKR.

Estratégia:
    Mantém DUAS quantidades: stock holdings (qty por ticker) e CASH (USD).
    Eventos:
        DEPOSIT     -> cash += valor (CF EXTERNO entrando)
        WITHDRAWAL  -> cash += valor (negativo) (CF EXTERNO saindo)
        BUY         -> qty[t] += q;  cash += proceeds + commission  (interno)
        SELL        -> qty[t] -= q;  cash += proceeds + commission  (interno)
        DIVIDEND    -> cash += valor (líquido considera withholding tax retido)
        WITHHOLDING -> cash += valor (negativo)
        INTEREST    -> cash += valor (pequeno; ignoramos no overall)

    V_end_USD_liquido = sum(qty * close_USD) + cash_USD
    V_end_USD_bruto   = V_end_USD_liquido + |withholding_acumulado|
                        (gross-up: imagina que o IR não tivesse sido retido)

    V_end_BRL = V_end_USD * PTAX_venda(month_end)

Inputs:
    us_trades.csv, us_dividends.csv, us_withholding.csv, us_deposits.csv
    us_prices_cache.csv
    ptax_daily.csv

Outputs:
    us_holdings_monthly.csv   -- snapshot mensal por ticker (qty, valor)
    us_summary_monthly.csv    -- por mês: V_stock_USD, Cash_USD, PTAX,
                                 V_end_USD_liq/brt, V_end_BRL_liq/brt,
                                 CF_USD, CF_BRL, INC_USD, INC_BRL,
                                 IR_acumulado_USD
    us_final.csv              -- snapshot final
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta


def parse_iso(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def month_end(y: int, m: int) -> date:
    if m == 12:
        return date(y, 12, 31)
    return date(y, m + 1, 1) - timedelta(days=1)


def iter_months(start_iso: str, end_iso: str):
    sy, sm = int(start_iso[:4]), int(start_iso[5:7])
    ey, em = int(end_iso[:4]), int(end_iso[5:7])
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield (y, m)
        m += 1
        if m > 12:
            y, m = y + 1, 1


def load_csv_dict(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    return list(csv.DictReader(open(path, encoding="utf-8")))


def load_ptax(path: str) -> dict[str, float]:
    """Returns {date_iso -> cot_venda}. Note: only business days."""
    out = {}
    for r in csv.DictReader(open(path, encoding="utf-8")):
        v = r.get("cot_venda", "")
        if v not in ("", None):
            try:
                out[r["data"]] = float(v)
            except ValueError:
                pass
    return out


def ptax_at(daily: dict[str, float], iso: str) -> float:
    """Returns PTAX at iso if exists, else last available before."""
    if iso in daily:
        return daily[iso]
    past = [d for d in daily if d <= iso]
    if not past:
        return 0.0
    return daily[max(past)]


def load_prices(path: str) -> dict[str, dict[str, float]]:
    """Returns {ticker -> {month_end_iso -> close_usd}}."""
    out: dict[str, dict[str, float]] = defaultdict(dict)
    for r in csv.DictReader(open(path, encoding="utf-8")):
        out[r["ticker"]][r["month_end"]] = float(r["close_usd"])
    return out


def get_close_usd(prices: dict, ticker: str, me_iso: str) -> float | None:
    series = prices.get(ticker)
    if not series:
        return None
    if me_iso in series:
        return series[me_iso]
    past = [m for m in series if m <= me_iso]
    if past:
        return series[max(past)]
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--trades", default="us_trades.csv")
    ap.add_argument("--dividends", default="us_dividends.csv")
    ap.add_argument("--withholding", default="us_withholding.csv")
    ap.add_argument("--deposits", default="us_deposits.csv")
    ap.add_argument("--prices", default="us_prices_cache.csv")
    ap.add_argument("--ptax", default="ptax_daily.csv")
    ap.add_argument("--monthly-out", default="us_holdings_monthly.csv")
    ap.add_argument("--summary-out", default="us_summary_monthly.csv")
    ap.add_argument("--final-out", default="us_final.csv")
    args = ap.parse_args()

    trades = load_csv_dict(args.trades)
    divs = load_csv_dict(args.dividends)
    whs = load_csv_dict(args.withholding)
    deps = load_csv_dict(args.deposits)
    prices = load_prices(args.prices)
    ptax = load_ptax(args.ptax)

    print(f"Loaded: {len(trades)} trades, {len(divs)} divs, {len(whs)} wh, "
          f"{len(deps)} deps, {len(ptax)} PTAX days")

    # Build event list: (date, kind, payload)
    events: list[dict] = []
    for t in trades:
        events.append({
            "date": t["data"], "kind": "TRADE",
            "ticker": t["ticker"], "side": t["operacao"],
            "qty": float(t["qty"]),
            "proceeds_usd": float(t["proceeds_usd"]),
            "commission_usd": float(t["commission_usd"] or 0),
            "basis_usd": float(t["basis_usd"] or 0),
        })
    for d in divs:
        events.append({
            "date": d["data"], "kind": "DIVIDEND",
            "ticker": d["ticker"], "valor_usd": float(d["valor_usd"]),
            "tipo": d.get("tipo", "DIVIDENDO"),
        })
    for w in whs:
        events.append({
            "date": w["data"], "kind": "WITHHOLDING",
            "ticker": w["ticker"], "valor_usd": float(w["valor_usd"]),
        })
    for d in deps:
        events.append({
            "date": d["data"], "kind": "DEPOSIT",
            "valor_usd": float(d["valor_usd"]),
            "tipo": d.get("tipo", "DEPOSIT"),
        })
    events.sort(key=lambda e: (e["date"], e["kind"]))
    if not events:
        print("Nenhum evento. Abortando.")
        return 1

    start = events[0]["date"]
    end = date.today().isoformat()

    # State
    qty_by_ticker: dict[str, float] = defaultdict(float)
    cash_usd = 0.0
    wh_acum_usd = 0.0  # acumulado sempre negativo

    # Aggregations per month_end
    cf_by_me: dict[str, dict] = defaultdict(
        lambda: {"deposit_usd": 0.0, "withdrawal_usd": 0.0,
                 "deposit_brl": 0.0, "withdrawal_brl": 0.0,
                 "div_gross_usd": 0.0, "div_gross_brl": 0.0,
                 "wh_usd": 0.0, "wh_brl": 0.0,
                 "buy_usd": 0.0, "sell_usd": 0.0})

    monthly_rows: list[dict] = []
    summary_rows: list[dict] = []

    months = list(iter_months(start, end))
    ev_idx = 0
    n_events = len(events)

    print(f"\n{'MONTH':<8} {'PTAX':>8}  {'V_stock':>10} {'Cash':>9} "
          f"{'V_USD_liq':>11} {'V_BRL_liq':>13} {'V_BRL_brt':>13}  "
          f"{'CF_BRL':>10} {'INC_BRL_brt':>11}")
    print("-" * 130)

    for (y, m) in months:
        me = month_end(y, m)
        me_iso = me.isoformat()
        ym_prefix = f"{y:04d}-{m:02d}"

        # Apply all events with date <= me_iso not yet applied
        cf_buy_usd = cf_sell_usd = 0.0
        cf_dep_usd = cf_wd_usd = 0.0
        cf_dep_brl = cf_wd_brl = 0.0
        div_gross_usd = wh_usd = 0.0
        div_gross_brl = wh_brl = 0.0

        while ev_idx < n_events and events[ev_idx]["date"] <= me_iso:
            ev = events[ev_idx]
            ev_idx += 1
            ev_date = ev["date"]
            ev_ptax = ptax_at(ptax, ev_date)
            in_month = ev_date.startswith(ym_prefix)

            if ev["kind"] == "TRADE":
                if ev["side"] == "BUY":
                    qty_by_ticker[ev["ticker"]] += ev["qty"]
                else:
                    qty_by_ticker[ev["ticker"]] -= ev["qty"]
                # cash flows: proceeds + commission (both signed)
                cash_usd += ev["proceeds_usd"] + ev["commission_usd"]
                if in_month:
                    if ev["side"] == "BUY":
                        cf_buy_usd += ev["basis_usd"]
                    else:
                        cf_sell_usd += abs(ev["proceeds_usd"])
            elif ev["kind"] == "DEPOSIT":
                v = ev["valor_usd"]
                cash_usd += v
                if in_month:
                    if v >= 0:
                        cf_dep_usd += v
                        cf_dep_brl += v * ev_ptax
                    else:
                        cf_wd_usd += v
                        cf_wd_brl += v * ev_ptax
            elif ev["kind"] == "DIVIDEND":
                cash_usd += ev["valor_usd"]
                if in_month:
                    div_gross_usd += ev["valor_usd"]
                    div_gross_brl += ev["valor_usd"] * ev_ptax
            elif ev["kind"] == "WITHHOLDING":
                cash_usd += ev["valor_usd"]  # negative
                wh_acum_usd += ev["valor_usd"]
                if in_month:
                    wh_usd += ev["valor_usd"]
                    wh_brl += ev["valor_usd"] * ev_ptax

        # Snapshot
        ptax_me = ptax_at(ptax, me_iso)
        v_stock_usd = 0.0
        for tk, q in qty_by_ticker.items():
            if abs(q) < 1e-9:
                continue
            close = get_close_usd(prices, tk, me_iso)
            if close is None:
                continue
            v_stock_usd += q * close
            monthly_rows.append({
                "month_end": me_iso,
                "ticker": tk,
                "qty": round(q, 6),
                "close_usd": round(close, 4),
                "valor_usd": round(q * close, 2),
                "valor_brl": round(q * close * ptax_me, 2),
                "ptax": round(ptax_me, 4),
            })

        v_end_usd_liq = v_stock_usd + cash_usd
        v_end_usd_brt = v_end_usd_liq + abs(wh_acum_usd)  # gross-up cumulativo
        v_end_brl_liq = v_end_usd_liq * ptax_me
        v_end_brl_brt = v_end_usd_brt * ptax_me

        cf_brl = cf_dep_brl + cf_wd_brl  # withdrawal já negativo
        inc_brl_brt = div_gross_brl  # bruto = dividendo bruto cheio
        inc_brl_liq = div_gross_brl + wh_brl  # líquido = bruto + (wh negativo)

        summary_rows.append({
            "month_end": me_iso,
            "ptax_venda": round(ptax_me, 6),
            "v_stock_usd": round(v_stock_usd, 2),
            "cash_usd": round(cash_usd, 2),
            "wh_acumulado_usd": round(wh_acum_usd, 2),
            "v_end_usd_liquido": round(v_end_usd_liq, 2),
            "v_end_usd_bruto": round(v_end_usd_brt, 2),
            "v_end_brl_liquido": round(v_end_brl_liq, 2),
            "v_end_brl_bruto": round(v_end_brl_brt, 2),
            "deposit_usd": round(cf_dep_usd, 2),
            "withdrawal_usd": round(cf_wd_usd, 2),
            "deposit_brl": round(cf_dep_brl, 2),
            "withdrawal_brl": round(cf_wd_brl, 2),
            "cashflow_brl": round(cf_brl, 2),
            "div_gross_usd": round(div_gross_usd, 2),
            "div_gross_brl": round(div_gross_brl, 2),
            "wh_mes_usd": round(wh_usd, 2),
            "wh_mes_brl": round(wh_brl, 2),
            "income_brl_liquido": round(inc_brl_liq, 2),
            "income_brl_bruto": round(inc_brl_brt, 2),
            "buy_usd": round(cf_buy_usd, 2),
            "sell_usd": round(cf_sell_usd, 2),
        })

        if v_stock_usd > 0 or abs(cash_usd) > 1:
            print(f"{ym_prefix:<8} {ptax_me:>8.4f}  "
                  f"{v_stock_usd:>10,.0f} {cash_usd:>9,.0f} "
                  f"{v_end_usd_liq:>11,.0f} "
                  f"{v_end_brl_liq:>13,.2f} {v_end_brl_brt:>13,.2f}  "
                  f"{cf_brl:>10,.0f} {inc_brl_brt:>11,.2f}")

    # Final
    last_me = months[-1]
    last_iso = month_end(last_me[0], last_me[1]).isoformat()
    final_rows = [r for r in monthly_rows if r["month_end"] == last_iso]

    # Write
    with open(args.monthly_out, "w", encoding="utf-8", newline="") as f:
        fields = ["month_end", "ticker", "qty", "close_usd", "valor_usd",
                  "valor_brl", "ptax"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in monthly_rows:
            w.writerow(r)
    with open(args.summary_out, "w", encoding="utf-8", newline="") as f:
        fields = list(summary_rows[0].keys())
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in summary_rows:
            w.writerow(r)
    with open(args.final_out, "w", encoding="utf-8", newline="") as f:
        fields = ["month_end", "ticker", "qty", "close_usd", "valor_usd",
                  "valor_brl", "ptax"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in final_rows:
            w.writerow(r)

    print(f"\nWrote {args.monthly_out}, {args.summary_out}, {args.final_out}")
    if summary_rows:
        last = summary_rows[-1]
        print(f"\n=== Posição final ({last['month_end']}) ===")
        print(f"  V_stock USD:        {last['v_stock_usd']:>14,.2f}")
        print(f"  Cash USD:           {last['cash_usd']:>14,.2f}")
        print(f"  WH acumulado USD:   {last['wh_acumulado_usd']:>14,.2f}")
        print(f"  V_end USD líquido:  {last['v_end_usd_liquido']:>14,.2f}")
        print(f"  V_end USD bruto:    {last['v_end_usd_bruto']:>14,.2f}")
        print(f"  PTAX venda:         R$ {last['ptax_venda']:.4f}")
        print(f"  V_end BRL líquido:  R$ {last['v_end_brl_liquido']:>11,.2f}")
        print(f"  V_end BRL bruto:    R$ {last['v_end_brl_bruto']:>11,.2f}")

        tot_dep_usd = sum(r["deposit_usd"] for r in summary_rows)
        tot_dep_brl = sum(r["deposit_brl"] for r in summary_rows)
        tot_div_brl = sum(r["div_gross_brl"] for r in summary_rows)
        print(f"\n  Total depositado USD: {tot_dep_usd:,.2f}")
        print(f"  Total depositado BRL: R$ {tot_dep_brl:,.2f}")
        print(f"  Total dividendos BRL (bruto): R$ {tot_div_brl:,.2f}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
