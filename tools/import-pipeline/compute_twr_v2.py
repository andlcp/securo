#!/usr/bin/env python3
"""
compute_twr_v2.py

Computes Time-Weighted Return (TWR) using the Modified Dietz formula on
monthly portfolio snapshots, based on the CLEAN negociacao-based replay.

Inputs:
    holdings_monthly.csv  — qty per ticker per month-end (split-adjusted basis)
    prices_cache.csv      — Yahoo monthly closes (split-adjusted)
    trades.csv            — for monthly cashflow (BUY - SELL net)

For each month i (Total Return convention — dividends/JCP/FII rendimentos
treated as portfolio income, equivalent to immediate reinvestment):
    V_start  = sum(qty[t, m-1] * price[t, m-1])
    V_end    = sum(qty[t, m]   * price[t, m])
    CF       = sum(BUY values in month) - sum(SELL values in month)
                + net option premium received in month
    INCOME   = dividends + JCP + FII rendimentos + resgates received in month
    r_i      = (V_end + INCOME - V_start - CF) / (V_start + 0.5 * CF)
    TWR_cum  = product of (1 + r_i) - 1

Output:
    twr_monthly.csv with columns:
        month, v_start, v_end, cashflow, income, return_month, return_cum, n_tickers
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta


def month_end(y: int, m: int) -> date:
    if m == 12:
        return date(y, 12, 31)
    return date(y, m + 1, 1) - timedelta(days=1)


def iter_months(start_ym: str, end_ym: str):
    sy, sm = map(int, start_ym.split("-"))
    ey, em = map(int, end_ym.split("-"))
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield (y, m)
        m += 1
        if m > 12:
            y, m = y + 1, 1


def load_holdings(path: str) -> dict[str, dict[str, float]]:
    """Returns {month_end_iso -> {ticker -> qty}}."""
    out: dict[str, dict[str, float]] = defaultdict(dict)
    for r in csv.DictReader(open(path, encoding="utf-8")):
        out[r["date"]][r["ticker"]] = float(r["qty"])
    return out


def load_prices(path: str) -> dict[str, dict[str, float]]:
    """Returns {ticker -> {month_end_iso -> close}}."""
    out: dict[str, dict[str, float]] = defaultdict(dict)
    for r in csv.DictReader(open(path, encoding="utf-8")):
        out[r["ticker"]][r["month_end"]] = float(r["close"])
    return out


def load_trades(path: str) -> list[dict]:
    out = []
    for r in csv.DictReader(open(path, encoding="utf-8")):
        out.append({
            "date": r["data"],  # YYYY-MM-DD
            "ticker": r["ticker"],
            "side": r["operacao"],
            "valor": float(r["valor"]),
            "categoria": r["categoria"],
            "operacao_origem": r.get("operacao_origem", ""),
        })
    return out


def load_proventos(path: str) -> list[dict]:
    """Loads proventos.csv (columns: data, ticker, tipo, valor, ...)."""
    import os
    if not os.path.exists(path):
        return []
    out = []
    for r in csv.DictReader(open(path, encoding="utf-8")):
        out.append({
            "date": r["data"],
            "ticker": r["ticker"],
            "tipo": r["tipo"],
            "valor": float(r["valor"]),
        })
    return out


def income_in_month(year: int, month: int,
                    proventos: list[dict]) -> tuple[float, dict[str, float]]:
    """Sum of all proventos in this month, plus breakdown by tipo."""
    ymp = f"{year:04d}-{month:02d}"
    total = 0.0
    by_tipo: dict[str, float] = {}
    for p in proventos:
        if not p["date"].startswith(ymp):
            continue
        total += p["valor"]
        by_tipo[p["tipo"]] = by_tipo.get(p["tipo"], 0.0) + p["valor"]
    return total, by_tipo


def load_rf_holdings_value(path: str) -> dict[str, dict[str, float]]:
    """Returns {month_end_iso -> {"liq": V_total, "brt": V_total_bruto}}."""
    import os
    out: dict[str, dict[str, float]] = defaultdict(
        lambda: {"liq": 0.0, "brt": 0.0})
    if not os.path.exists(path):
        return {}
    for r in csv.DictReader(open(path, encoding="utf-8")):
        out[r["month_end"]]["liq"] += float(r["valor_mtm_liquido"] or 0)
        out[r["month_end"]]["brt"] += float(r["valor_mtm_bruto"] or 0)
    return dict(out)


def load_rf_cashflow(path: str) -> dict[str, dict[str, float]]:
    """Returns {month_end_iso -> {buy, sell_liquido, sell_bruto, ir_estimado, ...}}."""
    import os
    out: dict[str, dict[str, float]] = {}
    if not os.path.exists(path):
        return {}
    for r in csv.DictReader(open(path, encoding="utf-8")):
        out[r["month_end"]] = {
            "buy": float(r["buy"] or 0),
            "sell_liquido": float(r["sell_liquido"] or 0),
            "sell_bruto": float(r["sell_bruto"] or 0),
            "ir_estimado": float(r["ir_estimado"] or 0),
            "ganho_bruto_realizado": float(r.get("ganho_bruto_realizado", 0)),
            "ganho_liquido_realizado": float(r.get("ganho_liquido_realizado", 0)),
        }
    return out


def income_gross_up(by_tipo: dict[str, float]) -> float:
    """Gross-up reverso do JCP (15% retido na fonte). Demais já são isentos."""
    div = by_tipo.get("DIVIDENDO", 0)
    jcp = by_tipo.get("JCP", 0)
    rend = by_tipo.get("RENDIMENTO", 0)
    res = by_tipo.get("RESGATE", 0)
    return div + (jcp / 0.85) + rend + res


def load_us_summary(path: str) -> dict[str, dict[str, float]]:
    """Returns {month_end -> dict with v_end_brl_liquido/bruto, cashflow_brl,
    income_brl_liq/bruto}."""
    import os
    if not os.path.exists(path):
        return {}
    out: dict[str, dict[str, float]] = {}
    for r in csv.DictReader(open(path, encoding="utf-8")):
        out[r["month_end"]] = {
            "v_end_brl_liquido": float(r["v_end_brl_liquido"] or 0),
            "v_end_brl_bruto": float(r["v_end_brl_bruto"] or 0),
            "cashflow_brl": float(r["cashflow_brl"] or 0),
            "income_brl_liquido": float(r.get("income_brl_liquido", 0) or 0),
            "income_brl_bruto": float(r.get("income_brl_bruto", 0) or 0),
            "buy_usd": float(r.get("buy_usd", 0) or 0),
            "sell_usd": float(r.get("sell_usd", 0) or 0),
            "ptax": float(r.get("ptax_venda", 0) or 0),
        }
    return out


def value_at(month_iso: str,
             holdings: dict[str, dict[str, float]],
             prices: dict[str, dict[str, float]]) -> tuple[float, int, list[str]]:
    """Value the snapshot at month_iso. Returns (total, n_priced, missing)."""
    holding = holdings.get(month_iso, {})
    total = 0.0
    n = 0
    missing: list[str] = []
    for tk, qty in holding.items():
        if abs(qty) < 1e-6:
            continue
        # Find price: same month, else carry the most recent prior month's
        # price for this ticker (helps for delisted/incorporated tickers).
        p = prices.get(tk, {}).get(month_iso)
        if p is None:
            # carry-forward
            past_months = [m for m in prices.get(tk, {}) if m <= month_iso]
            if past_months:
                p = prices[tk][max(past_months)]
            else:
                missing.append(tk)
                continue
        total += qty * p
        n += 1
    return total, n, missing


def cashflow_in_month(year: int, month: int,
                      trades: list[dict]) -> tuple[float, float, float, float]:
    """Return (buy_total, sell_total, premium_net, cashflow_for_dietz).
    cashflow = buy_value - sell_value - premium_received_net
       (premium reduces external CF — it is internal income, not a deposit)
    """
    ymp = f"{year:04d}-{month:02d}"
    buy_v = sell_v = prem_recv = prem_paid = 0.0
    for t in trades:
        if not t["date"].startswith(ymp):
            continue
        if t["categoria"] in ("VISTA", "FRACIONARIO", "EXERCICIO_CALL"):
            if t["side"] == "BUY":
                buy_v += t["valor"]
            elif t["side"] == "SELL":
                sell_v += t["valor"]
        elif t["categoria"] == "OPCAO_PREMIO_IGNORE":
            op = (t.get("operacao_origem") or "").lower()
            if op == "venda":
                prem_recv += t["valor"]
            elif op == "compra":
                prem_paid += t["valor"]
    prem_net = prem_recv - prem_paid
    cf = buy_v - sell_v - prem_net
    return buy_v, sell_v, prem_net, cf


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--holdings", default="holdings_monthly.csv")
    ap.add_argument("--prices", default="prices_cache.csv")
    ap.add_argument("--trades", default="trades.csv")
    ap.add_argument("--proventos", default="proventos.csv")
    ap.add_argument("--rf-holdings", default="rf_holdings_monthly.csv")
    ap.add_argument("--rf-cashflow", default="rf_cashflow_monthly.csv")
    ap.add_argument("--us-summary", default="us_summary_monthly.csv")
    ap.add_argument("--out", default="twr_monthly.csv")
    ap.add_argument("--start", default="2019-06")
    ap.add_argument("--end", default=None)
    args = ap.parse_args()

    holdings = load_holdings(args.holdings)
    prices = load_prices(args.prices)
    trades = load_trades(args.trades)
    proventos = load_proventos(args.proventos)
    rf_value_by_me = load_rf_holdings_value(args.rf_holdings)
    rf_cf_by_me = load_rf_cashflow(args.rf_cashflow)
    us_by_me = load_us_summary(args.us_summary)
    print(f"Loaded {len(proventos)} provento entries from {args.proventos}")
    print(f"Loaded RF value for {len(rf_value_by_me)} months from {args.rf_holdings}")
    print(f"Loaded RF cashflow for {len(rf_cf_by_me)} months from {args.rf_cashflow}")
    print(f"Loaded US summary for {len(us_by_me)} months from {args.us_summary}")

    if not args.end:
        today = date.today()
        args.end = f"{today.year:04d}-{today.month:02d}"

    months = list(iter_months(args.start, args.end))
    out_rows = []
    cum_liq = 1.0
    cum_brt = 1.0
    prev_v_liq = 0.0
    prev_v_brt = 0.0
    prev_iso = None

    print(f"\n{'MONTH':<8} {'V_RV':>11} {'V_RF':>11} {'V_TOT':>12} "
          f"{'CF_RV':>10} {'CF_RF':>10} {'INC':>8} "
          f"{'r_liq':>7} {'r_brt':>7} {'TWR_liq':>10} {'TWR_brt':>10}")
    print("-" * 130)

    for (y, m) in months:
        me = month_end(y, m).isoformat()

        # === Renda Variável ===
        v_end_rv, n_priced, _ = value_at(me, holdings, prices)

        # === Renda Fixa ===
        rfv = rf_value_by_me.get(me, {"liq": 0.0, "brt": 0.0})
        v_end_rf_liq = rfv["liq"]
        v_end_rf_brt = rfv["brt"]
        rfcf = rf_cf_by_me.get(me, {"buy": 0, "sell_liquido": 0,
                                     "sell_bruto": 0, "ir_estimado": 0})

        # === Ações Americanas (IBKR) ===
        usrec = us_by_me.get(me, {"v_end_brl_liquido": 0.0,
                                   "v_end_brl_bruto": 0.0,
                                   "cashflow_brl": 0.0})
        v_end_us_liq = usrec["v_end_brl_liquido"]
        v_end_us_brt = usrec["v_end_brl_bruto"]
        cf_us = usrec["cashflow_brl"]

        # === Totais ===
        v_end_liq = v_end_rv + v_end_rf_liq + v_end_us_liq
        v_end_brt = v_end_rv + v_end_rf_brt + v_end_us_brt

        # === Cashflows ===
        # RV (já bruto: compras/vendas de ações são valores cheios)
        buy_rv, sell_rv, prem, cf_rv = cashflow_in_month(y, m, trades)
        # RF (líquido vs bruto se distinguem nas SAÍDAS)
        cf_rf_liq = rfcf["buy"] - rfcf["sell_liquido"]
        cf_rf_brt = rfcf["buy"] - rfcf["sell_bruto"]
        # US: já em BRL, deposits - withdrawals
        cf_liq = cf_rv + cf_rf_liq + cf_us
        cf_brt = cf_rv + cf_rf_brt + cf_us

        # === Income (proventos RV — US dividends já em V_end_us) ===
        income_liq, income_by_tipo = income_in_month(y, m, proventos)
        income_brt = income_gross_up(income_by_tipo)

        # === Modified Dietz (líquido) ===
        v_start_liq = prev_v_liq if prev_iso else 0.0
        denom_liq = v_start_liq + 0.5 * cf_liq
        if denom_liq <= 1e-3:
            r_liq = 0.0 if (v_end_liq + income_liq) <= 1e-3 else None
        else:
            r_liq = (v_end_liq + income_liq - v_start_liq - cf_liq) / denom_liq

        # === Modified Dietz (bruto) ===
        v_start_brt = prev_v_brt if prev_iso else 0.0
        denom_brt = v_start_brt + 0.5 * cf_brt
        if denom_brt <= 1e-3:
            r_brt = 0.0 if (v_end_brt + income_brt) <= 1e-3 else None
        else:
            r_brt = (v_end_brt + income_brt - v_start_brt - cf_brt) / denom_brt

        # Encadeamento
        if r_liq is not None:
            cum_liq *= (1.0 + max(min(r_liq, 5.0), -0.95))
        if r_brt is not None:
            cum_brt *= (1.0 + max(min(r_brt, 5.0), -0.95))

        r_liq_str = f"{r_liq*100:>+6.2f}%" if r_liq is not None else "   -"
        r_brt_str = f"{r_brt*100:>+6.2f}%" if r_brt is not None else "   -"

        print(f"{y}-{m:02d}  {v_end_rv:>11,.0f} {v_end_rf_liq:>11,.0f} "
              f"{v_end_liq:>12,.0f} {cf_rv:>10,.0f} {cf_rf_liq:>10,.0f} "
              f"{income_liq:>8,.0f} {r_liq_str:>7} {r_brt_str:>7} "
              f"{(cum_liq-1)*100:>+9.2f}% {(cum_brt-1)*100:>+9.2f}%")

        out_rows.append({
            "month": f"{y:04d}-{m:02d}",
            "month_end": me,
            "v_end_rv": round(v_end_rv, 2),
            "v_end_rf_liquido": round(v_end_rf_liq, 2),
            "v_end_rf_bruto": round(v_end_rf_brt, 2),
            "v_end_us_liquido": round(v_end_us_liq, 2),
            "v_end_us_bruto": round(v_end_us_brt, 2),
            "cf_us_brl": round(cf_us, 2),
            "v_end_liquido": round(v_end_liq, 2),
            "v_end_bruto": round(v_end_brt, 2),
            "v_start": round(v_start_liq, 2),
            "v_end": round(v_end_liq, 2),  # legacy column
            "buy_value": round(buy_rv, 2),
            "sell_value": round(sell_rv, 2),
            "premium_net": round(prem, 2),
            "rf_buy": round(rfcf["buy"], 2),
            "rf_sell_liquido": round(rfcf["sell_liquido"], 2),
            "rf_sell_bruto": round(rfcf["sell_bruto"], 2),
            "rf_ir_estimado": round(rfcf["ir_estimado"], 2),
            "cashflow": round(cf_liq, 2),
            "cashflow_bruto": round(cf_brt, 2),
            "income": round(income_liq, 2),
            "income_bruto": round(income_brt, 2),
            "income_dividendo": round(income_by_tipo.get("DIVIDENDO", 0), 2),
            "income_jcp": round(income_by_tipo.get("JCP", 0), 2),
            "income_rendimento": round(income_by_tipo.get("RENDIMENTO", 0), 2),
            "income_resgate": round(income_by_tipo.get("RESGATE", 0), 2),
            "return_month": round(r_liq, 6) if r_liq is not None else "",
            "return_month_bruto": round(r_brt, 6) if r_brt is not None else "",
            "return_cum": round(cum_liq - 1, 6),
            "return_cum_bruto": round(cum_brt - 1, 6),
            "n_tickers_priced": n_priced,
        })

        prev_v_liq = v_end_liq
        prev_v_brt = v_end_brt
        prev_iso = me

    fields = ["month", "month_end",
              "v_end_rv", "v_end_rf_liquido", "v_end_rf_bruto",
              "v_end_us_liquido", "v_end_us_bruto", "cf_us_brl",
              "v_end_liquido", "v_end_bruto",
              "v_start", "v_end",
              "buy_value", "sell_value", "premium_net",
              "rf_buy", "rf_sell_liquido", "rf_sell_bruto", "rf_ir_estimado",
              "cashflow", "cashflow_bruto",
              "income", "income_bruto",
              "income_dividendo", "income_jcp",
              "income_rendimento", "income_resgate",
              "return_month", "return_month_bruto",
              "return_cum", "return_cum_bruto",
              "n_tickers_priced"]
    with open(args.out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in out_rows:
            w.writerow(r)

    print(f"\nWrote {args.out} ({len(out_rows)} months)")
    if out_rows:
        last = out_rows[-1]
        total_income = sum(r['income'] for r in out_rows)
        total_ir = sum(r['rf_ir_estimado'] for r in out_rows)
        print(f"\n=== Resumo final ===")
        print(f"V_end RV (BR):           R$ {last['v_end_rv']:>14,.2f}")
        print(f"V_end RF:                R$ {last['v_end_rf_liquido']:>14,.2f}")
        print(f"V_end US (BRL líq):      R$ {last.get('v_end_us_liquido', 0):>14,.2f}")
        print(f"V_end TOTAL (líquido):   R$ {last['v_end_liquido']:>14,.2f}")
        print(f"Aporte líq. acumulado:   R$ {sum(r['cashflow'] for r in out_rows):>14,.2f}")
        print(f"Proventos recebidos:     R$ {total_income:>14,.2f}")
        print(f"IR estimado retido (RF): R$ {total_ir:>14,.2f}")
        n = len(out_rows)
        years = n / 12
        ann_liq = (1 + last['return_cum'])**(1/years) - 1 if years > 0 else 0
        ann_brt = (1 + last['return_cum_bruto'])**(1/years) - 1 if years > 0 else 0
        print(f"\nTWR cumulativo (líq):    {last['return_cum']*100:>+10.2f}%")
        print(f"TWR anualizado (líq):    {ann_liq*100:>+10.2f}% (over {years:.1f} years)")
        print(f"TWR cumulativo (brt):    {last['return_cum_bruto']*100:>+10.2f}%")
        print(f"TWR anualizado (brt):    {ann_brt*100:>+10.2f}% (over {years:.1f} years)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
