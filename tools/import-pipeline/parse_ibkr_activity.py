#!/usr/bin/env python3
"""
parse_ibkr_activity.py

Parses Interactive Brokers Activity Statement CSV (multi-section format).

The IBKR CSV is a single file with many sections. Every line starts with the
section name as the first column, and the second column tells if it is a
Header / Data / SubTotal / Total / Notes / etc.

Sections we care about:
    Trades                  -> us_trades.csv
    Dividends               -> us_dividends.csv
    Withholding Tax         -> us_withholding.csv
    Deposits & Withdrawals  -> us_deposits.csv
    Open Positions          -> us_positions_final.csv (snapshot at statement end)

Multiple statements (different periods) can be merged: dedup by (section, key).

Usage:
    python parse_ibkr_activity.py path/to/UXXXXXXXX_YYYYMMDD_YYYYMMDD.csv [--out-prefix us]
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import defaultdict
from datetime import datetime
from typing import Iterable


def normalize_ticker(t: str) -> str:
    """IBKR uses 'BRK B' for Berkshire B; Yahoo expects 'BRK-B'."""
    return t.strip().replace(" ", "-")


def parse_iso_datetime(s: str) -> str:
    """IBKR Date/Time format: '2025-07-08, 14:45:59'  -> '2025-07-08'."""
    s = s.strip().strip('"')
    if "," in s:
        s = s.split(",")[0].strip()
    return s  # already YYYY-MM-DD


def parse_iso_date(s: str) -> str:
    return s.strip().strip('"')


def section_iter(csv_paths: list[str]):
    """Yield (section, kind, row) tuples across all files."""
    for path in csv_paths:
        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row or len(row) < 2:
                    continue
                section = row[0].strip()
                kind = row[1].strip()
                yield path, section, kind, row


def parse_trades(rows: list[list[str]]) -> list[dict]:
    """Trades section.

    Header: DataDiscriminator, Asset Category, Currency, Symbol, Date/Time,
            Quantity, T. Price, C. Price, Proceeds, Comm/Fee, Basis,
            Realized P/L, MTM P/L, Code
    """
    out = []
    header = None
    for kind, row in rows:
        if kind == "Header":
            # row[2:] are field names
            header = [c.strip() for c in row[2:]]
            continue
        if kind != "Data":
            continue
        if not header:
            continue
        # row[2:] line up with header
        d = dict(zip(header, row[2:]))
        if d.get("DataDiscriminator", "").strip() != "Order":
            continue
        asset = d.get("Asset Category", "").strip()
        if asset != "Stocks":
            continue
        try:
            qty = float(d.get("Quantity", "0") or 0)
            price = float(d.get("T. Price", "0") or 0)
            proceeds = float(d.get("Proceeds", "0") or 0)
            comm = float(d.get("Comm/Fee", "0") or 0)
            basis = float(d.get("Basis", "0") or 0)
            rpnl = float(d.get("Realized P/L", "0") or 0)
        except ValueError:
            continue
        side = "BUY" if qty > 0 else "SELL"
        out.append({
            "data": parse_iso_datetime(d.get("Date/Time", "")),
            "ticker": normalize_ticker(d.get("Symbol", "")),
            "ticker_ibkr": d.get("Symbol", "").strip(),
            "currency": d.get("Currency", "USD").strip(),
            "operacao": side,
            "qty": abs(qty),
            "preco_usd": price,
            "proceeds_usd": proceeds,        # negativo p/ compra, positivo p/ venda
            "commission_usd": comm,          # sempre <= 0
            "basis_usd": basis,              # cost basis com comissão (compra)
            "realized_pnl_usd": rpnl,
            "code": d.get("Code", "").strip(),
        })
    return out


def parse_dividends(rows: list[list[str]]) -> list[dict]:
    """Dividends section.

    Header: Currency, Date, Description, Amount
    Description format: 'TICKER(US...) ... USD X.XX per Share (Type)'
                        also 'TICKER(US...) Payment in Lieu of Dividend'
    """
    out = []
    header = None
    desc_re = re.compile(r"^([A-Z][A-Z0-9 ._-]*?)\s*\(US[A-Z0-9]+\)")
    for kind, row in rows:
        if kind == "Header":
            header = [c.strip() for c in row[2:]]
            continue
        if kind != "Data":
            continue
        if not header:
            continue
        d = dict(zip(header, row[2:]))
        date = parse_iso_date(d.get("Date", ""))
        desc = d.get("Description", "").strip()
        amount = d.get("Amount", "")
        if not date or not desc or amount == "":
            continue
        try:
            amt = float(amount)
        except ValueError:
            continue
        m = desc_re.match(desc)
        ticker = normalize_ticker(m.group(1)) if m else ""
        is_pil = "Payment in Lieu" in desc
        out.append({
            "data": date,
            "ticker": ticker,
            "tipo": "PIL" if is_pil else "DIVIDENDO",
            "valor_usd": amt,
            "currency": d.get("Currency", "USD").strip(),
            "descricao": desc,
        })
    return out


def parse_withholding(rows: list[list[str]]) -> list[dict]:
    """Withholding Tax section. Same layout as Dividends."""
    out = []
    header = None
    desc_re = re.compile(r"^([A-Z][A-Z0-9 ._-]*?)\s*\(US[A-Z0-9]+\)")
    for kind, row in rows:
        if kind == "Header":
            header = [c.strip() for c in row[2:]]
            continue
        if kind != "Data":
            continue
        if not header:
            continue
        d = dict(zip(header, row[2:]))
        date = parse_iso_date(d.get("Date", ""))
        desc = d.get("Description", "").strip()
        amount = d.get("Amount", "")
        if not date or not desc or amount == "":
            continue
        try:
            amt = float(amount)  # negative
        except ValueError:
            continue
        m = desc_re.match(desc)
        ticker = normalize_ticker(m.group(1)) if m else ""
        out.append({
            "data": date,
            "ticker": ticker,
            "valor_usd": amt,
            "descricao": desc,
        })
    return out


def parse_deposits(rows: list[list[str]]) -> list[dict]:
    """Deposits & Withdrawals section.

    Header: Currency, Settle Date, Description, Amount
    """
    out = []
    header = None
    for kind, row in rows:
        if kind == "Header":
            header = [c.strip() for c in row[2:]]
            continue
        if kind != "Data":
            continue
        if not header:
            continue
        d = dict(zip(header, row[2:]))
        date = parse_iso_date(d.get("Settle Date", "") or d.get("Date", ""))
        desc = d.get("Description", "").strip()
        amount = d.get("Amount", "")
        if not date or amount == "":
            continue
        try:
            amt = float(amount)
        except ValueError:
            continue
        out.append({
            "data": date,
            "tipo": "DEPOSIT" if amt > 0 else "WITHDRAWAL",
            "valor_usd": amt,
            "currency": d.get("Currency", "USD").strip(),
            "descricao": desc,
        })
    return out


def parse_open_positions(rows: list[list[str]]) -> list[dict]:
    """Open Positions section (snapshot at statement end)."""
    out = []
    header = None
    for kind, row in rows:
        if kind == "Header":
            header = [c.strip() for c in row[2:]]
            continue
        if kind != "Data":
            continue
        if not header:
            continue
        d = dict(zip(header, row[2:]))
        if d.get("DataDiscriminator", "").strip() not in ("Summary", ""):
            continue
        if d.get("Asset Category", "").strip() != "Stocks":
            continue
        try:
            out.append({
                "ticker": normalize_ticker(d.get("Symbol", "")),
                "qty": float(d.get("Quantity", "0") or 0),
                "cost_price_usd": float(d.get("Cost Price", "0") or 0),
                "cost_basis_usd": float(d.get("Cost Basis", "0") or 0),
                "close_price_usd": float(d.get("Close Price", "0") or 0),
                "value_usd": float(d.get("Value", "0") or 0),
                "unrealized_pnl_usd": float(d.get("Unrealized P/L", "0") or 0),
            })
        except ValueError:
            continue
    return out


def parse_period(rows: list[list[str]]) -> tuple[str, str]:
    """Statement,Data,Period,'April 7, 2025 - April 6, 2026'"""
    for kind, row in rows:
        if kind == "Data" and len(row) > 3 and row[2] == "Period":
            period = row[3].strip().strip('"')
            return period
    return ""


def write_csv(path: str, rows: list[dict], fields: list[str]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})
    print(f"  wrote {path}  ({len(rows)} rows)")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("csv_paths", nargs="+", help="Path(s) to IBKR Activity Statement CSV")
    ap.add_argument("--prefix", default="us")
    args = ap.parse_args()

    by_section: dict[str, list[tuple[str, list[str]]]] = defaultdict(list)
    period_by_path: dict[str, str] = {}

    for path, section, kind, row in section_iter(args.csv_paths):
        if section == "Statement":
            period_by_path.setdefault(path,
                row[3].strip().strip('"') if (kind == "Data" and len(row) > 3 and row[2] == "Period")
                else period_by_path.get(path, ""))
        by_section[section].append((kind, row))

    print("Files processed:")
    for p, per in period_by_path.items():
        print(f"  {p}  [{per}]")
    print()

    trades = parse_trades(by_section.get("Trades", []))
    divs = parse_dividends(by_section.get("Dividends", []))
    whs = parse_withholding(by_section.get("Withholding Tax", []))
    deps = parse_deposits(by_section.get("Deposits & Withdrawals", []))
    pos = parse_open_positions(by_section.get("Open Positions", []))

    # Dedup trades by (data, ticker_ibkr, qty, preco_usd) — defensive in case
    # of multi-file overlap.
    seen = set()
    trades_dedup = []
    for t in trades:
        key = (t["data"], t["ticker_ibkr"], t["operacao"],
               round(t["qty"], 6), round(t["preco_usd"], 6))
        if key in seen:
            continue
        seen.add(key)
        trades_dedup.append(t)
    trades = sorted(trades_dedup, key=lambda r: (r["data"], r["ticker"]))

    # Dedup dividends/withholding/deposits by (data, ticker, valor)
    def dedup(rows, key_fn):
        s = set()
        out = []
        for r in rows:
            k = key_fn(r)
            if k in s:
                continue
            s.add(k)
            out.append(r)
        return out

    divs = sorted(dedup(divs, lambda r: (r["data"], r["ticker"], r["tipo"], round(r["valor_usd"], 4))),
                  key=lambda r: (r["data"], r["ticker"]))
    whs = sorted(dedup(whs, lambda r: (r["data"], r["ticker"], round(r["valor_usd"], 4), r["descricao"])),
                 key=lambda r: (r["data"], r["ticker"]))
    deps = sorted(dedup(deps, lambda r: (r["data"], round(r["valor_usd"], 4), r["descricao"])),
                  key=lambda r: r["data"])

    # --- Print summary ---
    print("=== Summary ===")
    print(f"  Trades:           {len(trades)}")
    print(f"  Dividends:        {len(divs)}  total US$ {sum(r['valor_usd'] for r in divs):,.2f}")
    print(f"  Withholding tax:  {len(whs)}  total US$ {sum(r['valor_usd'] for r in whs):,.2f}")
    print(f"  Deposits:         {len(deps)}  total US$ {sum(r['valor_usd'] for r in deps):,.2f}")
    print(f"  Open positions:   {len(pos)}  total US$ {sum(r['value_usd'] for r in pos):,.2f}")
    if trades:
        print(f"  Date range:       {trades[0]['data']} -> {trades[-1]['data']}")

    # --- Write output ---
    write_csv(f"{args.prefix}_trades.csv", trades, [
        "data", "ticker", "ticker_ibkr", "currency", "operacao",
        "qty", "preco_usd", "proceeds_usd", "commission_usd",
        "basis_usd", "realized_pnl_usd", "code"])
    write_csv(f"{args.prefix}_dividends.csv", divs, [
        "data", "ticker", "tipo", "valor_usd", "currency", "descricao"])
    write_csv(f"{args.prefix}_withholding.csv", whs, [
        "data", "ticker", "valor_usd", "descricao"])
    write_csv(f"{args.prefix}_deposits.csv", deps, [
        "data", "tipo", "valor_usd", "currency", "descricao"])
    write_csv(f"{args.prefix}_positions_final.csv", pos, [
        "ticker", "qty", "cost_price_usd", "cost_basis_usd",
        "close_price_usd", "value_usd", "unrealized_pnl_usd"])

    # Per-ticker net summary
    if trades:
        net = defaultdict(lambda: {"buy": 0.0, "sell": 0.0, "buy_v": 0.0, "sell_v": 0.0})
        for t in trades:
            if t["operacao"] == "BUY":
                net[t["ticker"]]["buy"] += t["qty"]
                net[t["ticker"]]["buy_v"] += t["basis_usd"]
            else:
                net[t["ticker"]]["sell"] += t["qty"]
                net[t["ticker"]]["sell_v"] += t["proceeds_usd"]
        print(f"\n=== Net por ticker ===")
        print(f"  {'TICKER':<8} {'BUY qty':>9} {'SELL qty':>9} {'NET':>9}  "
              f"{'BUY US$':>11} {'SELL US$':>11}")
        for tk in sorted(net):
            d = net[tk]
            netq = d["buy"] - d["sell"]
            print(f"  {tk:<8} {d['buy']:>9.2f} {d['sell']:>9.2f} {netq:>9.2f}  "
                  f"{d['buy_v']:>11,.2f} {d['sell_v']:>11,.2f}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
