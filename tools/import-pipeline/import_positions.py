#!/usr/bin/env python3
"""
import_positions.py

Import IN positions from positions.csv into Securo via the backend API.

Two run modes:

1) HTTP API (local dev, recommended)
       python import_positions.py --base-url http://localhost:8000 \
           --email andersonlcpereira@gmail.com --password XXX
   Logs in via /auth/jwt/login, then POSTs each position to /api/assets.

2) Direct DB (server-side, run on Hetzner)
       cd backend && python -m app.cli import-assets \
           --email andersonlcpereira@gmail.com --csv ../positions.csv
   See cli.py — uses the service layer directly.

CSV expected columns (from net_positions.py):
    key, nome, status, qty_display, qty_buy, qty_sell, ...,
    avg_buy_price_brl, buy_value_total_brl, valuation_method, ticker_yahoo

Only rows with status == 'IN' are imported.
"""
from __future__ import annotations

import argparse
import csv
import getpass
import re
import sys

try:
    import requests
except ImportError:
    print("Missing 'requests' package. Install with: pip install requests")
    sys.exit(1)


B3_TICKER = re.compile(r"^[A-Z]{4}\d{1,2}[A-Z]?$")


def classify_asset(row: dict) -> dict:
    """Map a positions.csv row to an AssetCreate-shaped dict."""
    key = row["key"]
    nome = row["nome"] or key
    qty = float(row["qty_display"])
    avg_price = float(row["avg_buy_price_brl"])
    buy_total = float(row["buy_value_total_brl"])
    first_buy = row["first_buy"] or None

    payload: dict = {
        "name": nome[:255],
        "type": "investment",
        "currency": "BRL",
        "units": qty,
        "purchase_date": first_buy,
        "purchase_price": avg_price if avg_price > 0 else None,
        "is_archived": False,
        "position": 0,
    }

    # 1. B3 stock / ETF / FII (PETR4, BOVA11, IVVB11, MCHF11, etc)
    if B3_TICKER.match(key):
        payload.update({
            "valuation_method": "market_price",
            "ticker": key + ".SA",
            "ticker_exchange": "SAO",
        })
        return payload

    # 2. Tesouro Direto
    if key.lower().startswith("tesouro"):
        payload.update({
            "valuation_method": "manual",
            "current_value": buy_total,  # face value approximation
        })
        return payload

    # 3. CDB / LCI / LCA / CRI / CRA / Debêntures / Fundos (CFF) — fixed income
    payload.update({
        "valuation_method": "manual",
        "current_value": buy_total,
    })
    return payload


def login(base_url: str, email: str, password: str) -> str:
    """Returns Bearer token after logging in."""
    url = base_url.rstrip("/") + "/auth/jwt/login"
    r = requests.post(
        url,
        data={"username": email, "password": password},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def create_asset(base_url: str, token: str, payload: dict) -> dict:
    url = base_url.rstrip("/") + "/api/assets"
    r = requests.post(
        url,
        json=payload,
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type": "application/json"},
        timeout=30,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
    return r.json()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--csv", default="positions.csv")
    ap.add_argument("--base-url", default="http://localhost:8000")
    ap.add_argument("--email", required=True)
    ap.add_argument("--password", default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only-status", default="IN",
                    help="Filtra status (default: IN). 'all' importa todos.")
    args = ap.parse_args()

    rows = list(csv.DictReader(open(args.csv, encoding="utf-8")))
    if args.only_status != "all":
        rows = [r for r in rows if r["status"] == args.only_status]
    print(f"Encontrados {len(rows)} ativo(s) com status='{args.only_status}'.")

    if args.dry_run:
        print("\n--- DRY RUN: payloads ---")
        for r in rows:
            p = classify_asset(r)
            print(f"  {p['name'][:40]:40s}  {p['valuation_method']:13s} "
                  f"qty={p['units']:>10.4f}  "
                  f"ticker={p.get('ticker') or '-':<12s}  "
                  f"current_value={p.get('current_value') or '-'}")
        return 0

    password = args.password or getpass.getpass(f"Password for {args.email}: ")
    print(f"Autenticando em {args.base_url} ...")
    token = login(args.base_url, args.email, password)
    print("OK login.")

    ok = fail = 0
    for r in rows:
        payload = classify_asset(r)
        try:
            res = create_asset(args.base_url, token, payload)
            print(f"  [OK] {payload['name'][:40]:40s} id={res['id']}")
            ok += 1
        except Exception as e:
            print(f"  [ERR] {payload['name'][:40]:40s}: {e}")
            fail += 1

    print(f"\nResumo: {ok} criado(s), {fail} falha(s).")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
