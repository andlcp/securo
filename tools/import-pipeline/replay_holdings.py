#!/usr/bin/env python3
"""
replay_holdings.py

Reconstructs day-by-day holdings from trades.csv + splits.csv + ticker_aliases.csv.

Algorithm:
    For each day in chronological order:
        1. Apply all corporate events of that day (splits/bonifications):
              qty[ticker] *= factor
              cost_basis[ticker] stays the same (split doesn't change cost)
              avg_price[ticker] /= factor (price adjusts inversely)
        2. Apply all trades of that day:
              BUY:  qty += q,  cost_basis += valor
              SELL: qty -= q,  cost_basis -= avg_price * q (FIFO-like proportional)
        3. Apply ticker aliases (e.g. TRPL4 -> ISAE4) on the alias date
              merge holdings under new ticker

Outputs:
    holdings_monthly.csv  — last-day-of-month snapshot per ticker (qty, cost_basis)
    holdings_final.csv    — final state (today)
    holdings_validation.txt — comparison vs target_position.csv if provided
"""
from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Optional


def load_trades(path: str) -> list[dict]:
    rows = list(csv.DictReader(open(path, encoding="utf-8")))
    out = []
    for r in rows:
        if r.get("categoria") not in ("VISTA", "FRACIONARIO", "EXERCICIO_CALL"):
            continue
        if not r.get("operacao"):
            continue
        out.append({
            "date": datetime.strptime(r["data"], "%Y-%m-%d").date(),
            "ticker": r["ticker"],
            "side": r["operacao"],  # BUY or SELL
            "qty": float(r["quantidade"]),
            "preco": float(r["preco"]),
            "valor": float(r["valor"]),
            "cat": r["categoria"],
        })
    return out


def load_splits(path: str) -> list[dict]:
    try:
        rows = list(csv.DictReader(open(path, encoding="utf-8")))
    except FileNotFoundError:
        return []
    out = []
    for r in rows:
        out.append({
            "date": datetime.strptime(r["date"], "%Y-%m-%d").date(),
            "ticker": r["ticker"],
            "factor": float(r["factor"]),
            "ratio": r.get("ratio_str", ""),
        })
    return out


def load_aliases(path: str) -> list[dict]:
    """ticker_aliases.csv: from_ticker, to_ticker, date, ratio (default 1.0)"""
    try:
        rows = list(csv.DictReader(open(path, encoding="utf-8")))
    except FileNotFoundError:
        return []
    out = []
    for r in rows:
        out.append({
            "date": datetime.strptime(r["date"], "%Y-%m-%d").date(),
            "from": r["from_ticker"],
            "to": r["to_ticker"],
            "ratio": float(r.get("ratio") or 1.0),
        })
    return out


def month_end(d: date) -> date:
    nxt = d.replace(day=28) + timedelta(days=4)
    return nxt - timedelta(days=nxt.day)


class Portfolio:
    def __init__(self):
        # ticker -> {qty, cost_basis, realized_pnl, total_buy_value}
        self.h: dict[str, dict] = defaultdict(
            lambda: {"qty": 0.0, "cost_basis": 0.0,
                     "realized": 0.0, "buy_value": 0.0})

    def apply_split(self, ticker: str, factor: float):
        if ticker not in self.h:
            return
        h = self.h[ticker]
        if abs(h["qty"]) < 1e-9:
            return
        h["qty"] *= factor
        # cost_basis stays the same (you didn't pay anything)

    def apply_alias(self, frm: str, to: str, ratio: float = 1.0):
        if frm not in self.h:
            return
        src = self.h[frm]
        if abs(src["qty"]) < 1e-9:
            return
        dst = self.h[to]
        dst["qty"] += src["qty"] * ratio
        dst["cost_basis"] += src["cost_basis"]
        dst["buy_value"] += src["buy_value"]
        dst["realized"] += src["realized"]
        # zero out source
        self.h[frm] = {"qty": 0.0, "cost_basis": 0.0,
                       "realized": 0.0, "buy_value": 0.0}

    def buy(self, ticker: str, qty: float, valor: float):
        h = self.h[ticker]
        h["qty"] += qty
        h["cost_basis"] += valor
        h["buy_value"] += valor

    def sell(self, ticker: str, qty: float, valor: float):
        h = self.h[ticker]
        # proportional cost out
        if h["qty"] > 1e-9:
            avg = h["cost_basis"] / h["qty"]
            cost_out = avg * min(qty, h["qty"])
        else:
            cost_out = 0.0
        h["qty"] -= qty
        h["cost_basis"] -= cost_out
        h["realized"] += (valor - cost_out)
        if h["qty"] < 1e-6:
            # avoid drifting negatives from rounding
            if h["qty"] > -1e-3:
                h["qty"] = 0.0
                h["cost_basis"] = 0.0

    def snapshot(self, dt: date) -> list[dict]:
        out = []
        for tk, h in self.h.items():
            if abs(h["qty"]) < 1e-9 and abs(h["cost_basis"]) < 1e-9:
                continue
            out.append({
                "date": dt.isoformat(),
                "ticker": tk,
                "qty": round(h["qty"], 6),
                "cost_basis": round(h["cost_basis"], 2),
                "buy_value_total": round(h["buy_value"], 2),
                "realized_pnl": round(h["realized"], 2),
            })
        return out


def adjust_trades_for_splits(trades: list[dict], splits: list[dict]) -> list[dict]:
    """Pre-multiply each trade's qty by the cumulative factor of all FUTURE
    splits (splits after the trade date). This expresses every share count
    in 'today's post-split-equivalent' units, matching Yahoo's split-adjusted
    `close` field.

    Example: AERI3 has split 1:20 (factor 0.05) on 2024-05-14.
        BUY 5000 on 2023-10-20  -> effective qty = 5000 * 0.05 = 250
        SELL 200 on 2024-06-21  -> effective qty = 200       (no future split)
    """
    splits_by_ticker: dict[str, list[dict]] = defaultdict(list)
    for s in splits:
        splits_by_ticker[s["ticker"]].append(s)

    out = []
    for t in trades:
        future = [s for s in splits_by_ticker.get(t["ticker"], [])
                  if s["date"] > t["date"]]
        factor = 1.0
        for s in future:
            factor *= s["factor"]
        adj_qty = t["qty"] * factor
        adj_preco = t["preco"] / factor if factor else t["preco"]
        out.append({
            **t,
            "qty": adj_qty,
            "preco": adj_preco,  # valor stays the same (qty*preco invariant)
            "qty_raw": t["qty"],
            "split_factor_applied": factor,
        })
    return out


def replay(trades, splits, aliases, monthly_out: str, final_out: str):
    if not trades:
        print("no trades")
        return None

    # Pre-adjust trade qty by future-split factors (so all qty in current basis)
    trades_adj = adjust_trades_for_splits(trades, splits)

    # Index events by date
    by_date_trades: dict[date, list[dict]] = defaultdict(list)
    for t in trades_adj:
        by_date_trades[t["date"]].append(t)
    by_date_aliases: dict[date, list[dict]] = defaultdict(list)
    for a in aliases:
        by_date_aliases[a["date"]].append(a)

    start = min(t["date"] for t in trades_adj)
    end = max(t["date"] for t in trades_adj)
    if aliases:
        end = max(end, max(a["date"] for a in aliases))
    # Extend to month_end of current real-world date so that every month
    # between start and today gets a snapshot (TWR needs continuous coverage).
    today = date.today()
    end = max(end, month_end(today))

    p = Portfolio()
    monthly_rows: list[dict] = []

    # Iterate day by day. Splits NOT applied here (already in qty_adj).
    d = start
    last_month_end = None
    while d <= end:
        # 1. aliases (rename merges) at their date
        for a in by_date_aliases.get(d, []):
            p.apply_alias(a["from"], a["to"], a["ratio"])
        # 2. trades (qty already in current-basis units)
        for t in by_date_trades.get(d, []):
            if t["side"] == "BUY":
                p.buy(t["ticker"], t["qty"], t["valor"])
            else:
                p.sell(t["ticker"], t["qty"], t["valor"])

        # Snapshot at end of month (always emit a row, even if empty, so
        # downstream consumers can detect "this month was covered, position 0")
        me = month_end(d)
        if d == me and last_month_end != me:
            snap = p.snapshot(me)
            if not snap:
                # sentinel row so the month is detectable in holdings_monthly
                snap = [{"date": me.isoformat(), "ticker": "__EMPTY__",
                         "qty": 0.0, "cost_basis": 0.0,
                         "buy_value_total": 0.0, "realized_pnl": 0.0}]
            monthly_rows.extend(snap)
            last_month_end = me

        d += timedelta(days=1)

    # Final snapshot
    final_rows = p.snapshot(end)

    # Write
    fields = ["date", "ticker", "qty", "cost_basis", "buy_value_total", "realized_pnl"]
    with open(monthly_out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in monthly_rows:
            w.writerow(r)
    with open(final_out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in final_rows:
            w.writerow(r)

    return p, final_rows


def validate(final_rows, target_csv: Optional[str]):
    if not target_csv:
        return
    try:
        target = list(csv.DictReader(open(target_csv, encoding="utf-8")))
    except FileNotFoundError:
        print(f"\n[validate] {target_csv} not found, skipping.")
        return
    target_qty = {r["ticker"]: float(r["qty"]) for r in target}
    calc_qty = {r["ticker"]: float(r["qty"]) for r in final_rows}

    all_tk = sorted(set(target_qty) | set(calc_qty))
    print(f"\n=== Validation: calculated vs target ({len(all_tk)} tickers) ===")
    print(f"  {'TICKER':<10} {'CALC':>12} {'TARGET':>12} {'DIFF':>10}  status")
    ok = mismatch = only_calc = only_tgt = 0
    for tk in all_tk:
        c = calc_qty.get(tk, 0.0)
        t = target_qty.get(tk, 0.0)
        diff = c - t
        if abs(diff) < 0.5:
            status = "OK"
            ok += 1
        elif tk not in target_qty:
            status = "ONLY_CALC (descontinuado?)"
            only_calc += 1
        elif tk not in calc_qty:
            status = "ONLY_TARGET (origem fora do extrato?)"
            only_tgt += 1
        else:
            status = f"DIFF"
            mismatch += 1
        if status == "OK" and abs(c) < 0.5 and abs(t) < 0.5:
            continue
        print(f"  {tk:<10} {c:>12.2f} {t:>12.2f} {diff:>10.2f}  {status}")
    print(f"\nResumo: {ok} OK, {mismatch} divergentes, {only_calc} só calc, {only_tgt} só target")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--trades", default="trades.csv")
    ap.add_argument("--splits", default="splits.csv")
    ap.add_argument("--aliases", default="ticker_aliases.csv")
    ap.add_argument("--target", default="target_position.csv",
                    help="Posição esperada (ticker,qty) para validação")
    ap.add_argument("--monthly-out", default="holdings_monthly.csv")
    ap.add_argument("--final-out", default="holdings_final.csv")
    args = ap.parse_args()

    trades = load_trades(args.trades)
    splits = load_splits(args.splits)
    aliases = load_aliases(args.aliases)
    print(f"Loaded {len(trades)} trades, {len(splits)} splits, {len(aliases)} aliases")

    res = replay(trades, splits, aliases, args.monthly_out, args.final_out)
    if res is None:
        return 1
    p, final_rows = res

    print(f"\nFinal: {len(final_rows)} tickers com posição != 0")
    print(f"Wrote {args.monthly_out} e {args.final_out}")

    validate(final_rows, args.target)
    return 0


if __name__ == "__main__":
    sys.exit(main())
