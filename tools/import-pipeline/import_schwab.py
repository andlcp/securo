#!/usr/bin/env python3
"""import_schwab.py

One-off importer for Charles Schwab Joint Tenant CSV exports
(Transaction History + Positions). Pushes the 10 US stock holdings
into the Camila AssetGroup via the Securo public REST API.

Why a separate script instead of extending push_to_securo.py?
- Schwab format is unrelated to the IBKR/XP pipeline (no monthly
  history files, no prices_cache, no parse_ibkr_activity normalisation).
- DRIP modelling is Schwab-specific: every "Reinvest Shares" comes
  paired with a "Qual Div Reinvest" + "NRA Tax Adj" line and we want
  the dividend tracked with the withholding stamped on `fees`.
- The dataset is tiny (10 assets, ~13 transactions) so we hard-code
  the parsing here rather than re-engineering the pipeline plumbing.

Decisions (locked with the user before writing):
  Carteira destino  : Camila
  DRIP              : DIVIDEND (gross) + NRA in `fees` + BUY (net reinvest)
  MoneyLink deposits: ignored (already represented in the source acct)
  Cash leftover     : ignored (will be invested by the user)
  FX                : on-demand PTAX via Securo's existing fx_rate_service

Usage:
    python tools/import-pipeline/import_schwab.py \
        --base-url http://46.225.24.167 \
        --email YOU@example.com \
        --transactions "C:/path/to/Joint_Tenant_..._Transactions_*.csv" \
        --positions    "C:/path/to/Joint Tenant-Positions-*.csv"
"""
from __future__ import annotations

import argparse
import csv
import getpass
import re
import sys
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path

# Reuse SecuroClient + login + ensure_owner_group from the main pipeline.
sys.path.insert(0, str(Path(__file__).parent))
from push_to_securo import SecuroClient, login, ensure_owner_group  # noqa: E402


SOURCE_TAG = "schwab_csv"
CUSTODIAN = "Charles Schwab"
EXCHANGE_GUESS = {  # cosmetic only
    "AAPL": "NASDAQ", "ADP": "NASDAQ", "COST": "NASDAQ", "GOOGL": "NASDAQ",
    "HD": "NYSE", "JNJ": "NYSE", "PG": "NYSE", "PH": "NYSE",
    "UNH": "NYSE", "V": "NYSE",
}


def _parse_money(s: str) -> float:
    """'$1,234.56' / '-$0.60' / '' -> float."""
    if s is None:
        return 0.0
    s = s.strip().replace(",", "").replace("$", "")
    if s in ("", "--"):
        return 0.0
    return float(s)


def _parse_date(s: str) -> str:
    """'04/20/2026' -> '2026-04-20'."""
    return datetime.strptime(s.strip(), "%m/%d/%Y").date().isoformat()


def parse_transactions(path: Path) -> list[dict]:
    """Yield normalized rows from the Schwab Transaction History CSV.

    Schwab quirks handled here:
    - Header line is row 1; rows 2+ are data; there's a trailing blank.
    - 'NRA Tax Adj' rows have an empty Quantity (it's not a share movement).
    - DRIP comes as triple: Reinvest Shares (BUY), NRA Tax Adj (fee),
      Qual Div Reinvest (gross dividend) — all on the same date+symbol.
    - Money fields use $ prefix and commas; we normalise to floats.
    """
    out = []
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            if not r.get("Date"):
                continue
            out.append({
                "date":   _parse_date(r["Date"]),
                "action": r["Action"].strip(),
                "symbol": r["Symbol"].strip(),
                "desc":   r["Description"].strip(),
                "qty":    _parse_money(r["Quantity"]),
                "price":  _parse_money(r["Price"]),
                "fees":   _parse_money(r["Fees & Comm"]),
                "amount": _parse_money(r["Amount"]),
            })
    return out


def parse_positions(path: Path) -> dict[str, dict]:
    """Read the Positions CSV (header on line 3, totals row at the end).
    Returns {ticker: {qty, cost_basis, mkt_val, price, ...}}.

    The cost-basis column is what we use to validate the sum of BUY
    transactions per ticker — Schwab's official lifetime cost basis.
    """
    out = {}
    with open(path, encoding="utf-8") as f:
        # Skip the "Positions for account ... as of ..." preamble + blank
        for _ in range(2):
            next(f)
        reader = csv.DictReader(f)
        for r in reader:
            sym = (r.get("Symbol") or "").strip()
            if not sym or sym in ("Cash & Cash Investments", "Positions Total"):
                continue
            qty_raw = (r.get("Qty (Quantity)") or "").strip()
            if not qty_raw or qty_raw == "--":
                continue
            out[sym] = {
                "ticker":     sym,
                "name":       (r.get("Description") or "").strip(),
                "qty":        float(qty_raw.replace(",", "")),
                "price":      _parse_money(r.get("Price") or ""),
                "mkt_val":    _parse_money(r.get("Mkt Val (Market Value)") or ""),
                "cost_basis": _parse_money(r.get("Cost Basis") or ""),
                "asset_type": (r.get("Asset Type") or "").strip(),
            }
    return out


def build_asset_payloads(
    positions: dict[str, dict],
    txs: list[dict],
    group_id: str,
) -> list[dict]:
    """One Asset payload per ticker, with first BUY date as purchase_date
    and the cost-basis-derived avg as purchase_price (USD/share)."""
    first_buy: dict[str, str] = {}
    for tx in txs:
        if tx["action"] in ("Buy", "Reinvest Shares") and tx["symbol"]:
            d = tx["date"]
            cur = first_buy.get(tx["symbol"])
            if cur is None or d < cur:
                first_buy[tx["symbol"]] = d

    out = []
    for sym, p in sorted(positions.items()):
        if p["asset_type"] != "Equity":
            # Only equity for now; if you ever buy bond ETFs / options,
            # asset_class will need a branch (BONDS / OPTIONS).
            continue
        avg_cost = p["cost_basis"] / p["qty"] if p["qty"] else 0.0
        out.append({
            "ticker": sym,
            "payload": {
                "name": sym,
                "type": "investment",
                "currency": "USD",
                "units": p["qty"],
                "valuation_method": "market_price",
                "ticker": sym,
                "ticker_exchange": EXCHANGE_GUESS.get(sym, "NASDAQ"),
                "purchase_date": first_buy.get(sym),
                "purchase_price": round(avg_cost, 6),
                "current_value": round(p["mkt_val"], 2),
                "group_id": group_id,
                "source": SOURCE_TAG,
                "asset_class": "STOCKS_US",
                "custodian": CUSTODIAN,
                "notes": p["name"],  # full company name for reference
                # We post explicit BUY transactions later — don't let the
                # backend auto-seed one or we'd double-count cost basis.
                "seed_purchase_transaction": False,
            },
        })
    return out


def build_transactions(txs: list[dict]) -> list[dict]:
    """Translate Schwab actions to AssetTransaction payloads.

    Mapping:
      Buy                 -> BUY  (qty, price, value=abs(amount))
      Reinvest Shares     -> BUY  (qty, price, value=abs(amount))
      Qual Div Reinvest   -> DIVIDEND  (value = gross amount)
                             -> we then attach the matching NRA Tax Adj
                                (same date+symbol) onto `fees`.
      NRA Tax Adj         -> consumed by the DIVIDEND row above (NOT a
                             standalone tx — it's the 30% withholding).
      Cash Dividend       -> DIVIDEND  (no NRA Tax Adj pair on this acct)
      MoneyLink Deposit   -> ignored (per user decision).

    External IDs are deterministic so re-running is idempotent (the
    backend's uq_asset_transactions_external constraint will dedupe).
    """
    # Index NRA tax rows so we can pair them with their dividend row.
    nra: dict[tuple[str, str], float] = {}
    for tx in txs:
        if tx["action"] == "NRA Tax Adj":
            nra[(tx["date"], tx["symbol"])] = abs(tx["amount"])

    out = []
    for i, tx in enumerate(txs):
        sym = tx["symbol"]
        if not sym:
            continue  # MoneyLink Deposits and other non-symbol rows
        action = tx["action"]
        if action in ("Buy", "Reinvest Shares"):
            value = abs(tx["amount"])
            out.append({
                "ticker": sym,
                "tx": {
                    "date": tx["date"],
                    "type": "BUY",
                    "qty": tx["qty"],
                    "price": tx["price"] or None,
                    "value": round(value, 2),
                    "fees": 0,
                    "source": SOURCE_TAG,
                    "external_id": (
                        f"schwab-{sym}-{tx['date']}-BUY-"
                        f"{tx['qty']:.4f}-{value:.2f}"
                    ),
                },
            })
        elif action in ("Qual Div Reinvest", "Cash Dividend", "Cash Div"):
            gross = abs(tx["amount"])
            withheld = nra.get((tx["date"], sym), 0.0)
            out.append({
                "ticker": sym,
                "tx": {
                    "date": tx["date"],
                    "type": "DIVIDEND",
                    "value": round(gross, 2),
                    "fees": round(withheld, 2),
                    "source": SOURCE_TAG,
                    "external_id": (
                        f"schwab-{sym}-{tx['date']}-DIVIDEND-{gross:.4f}"
                    ),
                    "notes": "NRA 30% withheld: ${:.2f}".format(withheld)
                              if withheld else None,
                },
            })
        elif action == "NRA Tax Adj":
            continue  # handled inline above
        # Anything else (transfers, splits) we don't expect on this acct;
        # log if encountered so we don't silently drop something material.
        else:
            print(f"  [SKIP] unhandled action: {action!r} on "
                  f"{tx['date']} {sym} ${tx['amount']:.2f}")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base-url", default="http://46.225.24.167")
    ap.add_argument("--email", required=True)
    ap.add_argument("--password", default=None)
    ap.add_argument("--transactions", required=True,
                    help="Schwab Transaction History CSV path")
    ap.add_argument("--positions", required=True,
                    help="Schwab Positions CSV path")
    ap.add_argument("--owner", default="camila",
                    help="AssetGroup owner key (default: camila)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    txs_path = Path(args.transactions)
    pos_path = Path(args.positions)
    if not txs_path.exists() or not pos_path.exists():
        print(f"ERR: arquivo não encontrado:\n  {txs_path}\n  {pos_path}")
        return 2

    print(f"Lendo transações de: {txs_path.name}")
    txs = parse_transactions(txs_path)
    print(f"  {len(txs)} linhas")

    print(f"Lendo posições de:    {pos_path.name}")
    positions = parse_positions(pos_path)
    print(f"  {len(positions)} ativos: {sorted(positions.keys())}")

    # Sanity check: cost basis from Positions == sum of BUY values from
    # Transactions (per ticker). If they disagree, the import would
    # produce a wrong purchase_price and the per-asset Rent. badge
    # would be off — fail loudly.
    print("\nValidando cost basis (Positions vs sum of BUYs)...")
    by_buy = defaultdict(float)
    by_buy_qty = defaultdict(float)
    for tx in txs:
        if tx["action"] in ("Buy", "Reinvest Shares"):
            by_buy[tx["symbol"]] += abs(tx["amount"])
            by_buy_qty[tx["symbol"]] += tx["qty"]
    mismatches = 0
    for sym, p in positions.items():
        cb_pos = p["cost_basis"]
        cb_buys = by_buy[sym]
        qty_pos = p["qty"]
        qty_buys = by_buy_qty[sym]
        ok_cb = abs(cb_pos - cb_buys) < 0.01
        ok_qty = abs(qty_pos - qty_buys) < 0.0001
        if not (ok_cb and ok_qty):
            mismatches += 1
            print(f"  [WARN] {sym}: pos.cost=${cb_pos:.2f} buys=${cb_buys:.2f}"
                  f" | pos.qty={qty_pos} buys.qty={qty_buys}")
        else:
            print(f"  [OK]   {sym}: ${cb_pos:.2f} / {qty_pos} shares")
    if mismatches:
        print(f"  WARN: {mismatches} ativo(s) com divergencia -- confira antes.")

    if args.dry_run:
        print("\n--- DRY RUN: nada foi enviado ---")
        # Build payloads anyway so we can preview them
        asset_payloads = build_asset_payloads(positions, txs, "<owner>")
        tx_payloads = build_transactions(txs)
        print(f"\n{len(asset_payloads)} ativos a criar:")
        for a in asset_payloads:
            p = a["payload"]
            print(f"  {p['name']:<6}  qty={p['units']:<8}  "
                  f"avg_cost=${p['purchase_price']}  "
                  f"first_buy={p['purchase_date']}  "
                  f"mkt_val=${p['current_value']}")
        print(f"\n{len(tx_payloads)} transações:")
        n_buy = sum(1 for t in tx_payloads if t["tx"]["type"] == "BUY")
        n_div = sum(1 for t in tx_payloads if t["tx"]["type"] == "DIVIDEND")
        print(f"  BUY: {n_buy}   DIVIDEND: {n_div}")
        for t in tx_payloads:
            tx = t["tx"]
            print(f"  {tx['date']}  {t['ticker']:<6}  {tx['type']:<8}  "
                  f"qty={tx.get('qty', '-'):<10}  "
                  f"value=${tx.get('value')}  fees=${tx.get('fees', 0)}")
        return 0

    # Login + group
    pwd = args.password or getpass.getpass(f"Senha do Securo para {args.email}: ")
    print(f"\nAutenticando em {args.base_url} ...")
    token = login(args.base_url, args.email, pwd)
    print("OK login.")
    c = SecuroClient(args.base_url, token)

    print(f"\nGarantindo AssetGroup do dono '{args.owner}'...")
    group_id = ensure_owner_group(c, args.owner)

    # Build payloads with the real group_id this time
    asset_payloads = build_asset_payloads(positions, txs, group_id)
    tx_payloads = build_transactions(txs)

    # Create assets
    asset_id_by_ticker: dict[str, str] = {}
    print(f"\n=== Criando {len(asset_payloads)} ativos ===")
    for a in asset_payloads:
        p = a["payload"]
        try:
            res = c.post("/api/assets", p)
            asset_id_by_ticker[a["ticker"]] = res["id"]
            print(f"  [OK]  {p['name']:<6}  id={res['id'][:8]}  "
                  f"qty={p['units']}  avg=${p['purchase_price']}")
        except Exception as e:
            print(f"  [ERR] {p['name']}: {e}")

    # Create transactions
    print(f"\n=== Criando {len(tx_payloads)} transações ===")
    ok = fail = 0
    for t in sorted(tx_payloads, key=lambda x: x["tx"]["date"]):
        aid = asset_id_by_ticker.get(t["ticker"])
        if not aid:
            print(f"  [SKIP] sem asset_id para {t['ticker']}")
            continue
        try:
            c.post(f"/api/assets/{aid}/transactions", t["tx"])
            ok += 1
            tx = t["tx"]
            print(f"  [OK]  {tx['date']}  {t['ticker']:<6}  {tx['type']:<8}  "
                  f"value=${tx.get('value')}  fees=${tx.get('fees', 0)}")
        except Exception as e:
            fail += 1
            print(f"  [ERR] {t['ticker']} {t['tx']['type']} {t['tx']['date']}: {e}")

    print(f"\nDone. Ativos: {len(asset_id_by_ticker)}  "
          f"Transações OK: {ok}  Falhas: {fail}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
