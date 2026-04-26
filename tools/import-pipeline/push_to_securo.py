#!/usr/bin/env python3
"""
push_to_securo.py

Sync the offline pipeline outputs (holdings_final.csv, rf_final.csv,
us_final.csv + monthly history files + prices_cache.csv) INTO Securo's
existing Asset / AssetValue tables, via the public REST API.

After running this, the Patrimônio page populates with current positions
and the Patrimônio Líquido (Net Worth) historical chart renders the full
month-by-month curve — same data that powers the standalone TWR view.

Three AssetGroups are created (idempotent):
    "Renda Variável BR"  – B3 stocks/ETFs/FIIs with valuation_method="market_price"
    "Renda Fixa"         – Tesouro & CDBs with valuation_method="manual"
    "Ações US"           – IBKR holdings with valuation_method="market_price"

For each Asset, monthly AssetValue rows are created from the history files.
The B3 RV history is valued at qty × close (from prices_cache.csv).
RF history uses valor_mtm_liquido directly. US history uses valor_brl
(already converted at month-end PTAX).

Usage:
    python tools/import-pipeline/push_to_securo.py \
        --base-url http://46.225.24.167 \
        --email YOU@example.com \
        [--reset]    # delete existing investments before importing
        [--no-history]  # skip AssetValue monthly rows (current snapshot only)

Run from the repo root so the default --csv-dir=. picks up all the CSVs.
"""
from __future__ import annotations

import argparse
import csv
import getpass
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Optional

try:
    import requests
except ImportError:
    print("Missing 'requests' package. Install with: pip install requests")
    sys.exit(1)


# --- AssetGroup definitions (created idempotently) -----------------------

GROUPS = {
    "rv_br": {"name": "Renda Variável BR", "icon": "trending-up", "color": "#6366F1"},
    "rf":    {"name": "Renda Fixa",        "icon": "landmark",   "color": "#10B981"},
    "us":    {"name": "Ações US",          "icon": "globe",      "color": "#F59E0B"},
}


# --- Helpers --------------------------------------------------------------

def _f(s, default=0.0) -> float:
    try:
        return float(s) if s not in (None, "") else default
    except (TypeError, ValueError):
        return default


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        print(f"  WARN: {path} não encontrado — pulando.")
        return []
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


# --- API client -----------------------------------------------------------

class SecuroClient:
    def __init__(self, base_url: str, token: str):
        self.base = base_url.rstrip("/")
        self.h = {"Authorization": f"Bearer {token}",
                  "Content-Type": "application/json"}

    def get(self, path: str):
        r = requests.get(self.base + path, headers=self.h, timeout=30)
        r.raise_for_status()
        return r.json()

    def post(self, path: str, body: dict):
        r = requests.post(self.base + path, json=body, headers=self.h, timeout=60)
        if r.status_code >= 400:
            raise RuntimeError(f"POST {path} -> HTTP {r.status_code}: {r.text}")
        return r.json()

    def delete(self, path: str):
        r = requests.delete(self.base + path, headers=self.h, timeout=30)
        if r.status_code >= 400:
            raise RuntimeError(f"DELETE {path} -> HTTP {r.status_code}: {r.text}")
        return r.status_code


def login(base_url: str, email: str, password: str) -> str:
    r = requests.post(
        base_url.rstrip("/") + "/api/auth/jwt/login",
        data={"username": email, "password": password},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


# --- Group resolution -----------------------------------------------------

def ensure_groups(c: SecuroClient) -> dict[str, str]:
    """Return {key -> group_id}. Creates missing groups."""
    existing = {g["name"]: g["id"] for g in c.get("/api/asset-groups")}
    out = {}
    for key, spec in GROUPS.items():
        if spec["name"] in existing:
            out[key] = existing[spec["name"]]
            print(f"  AssetGroup '{spec['name']}' já existe (id={out[key]})")
        else:
            g = c.post("/api/asset-groups", spec)
            out[key] = g["id"]
            print(f"  AssetGroup '{spec['name']}' criado (id={out[key]})")
    return out


# --- Optional reset -------------------------------------------------------

def reset_investments(c: SecuroClient, group_ids: list[str]) -> None:
    """Delete every Asset whose group_id is one of ours."""
    assets = c.get("/api/assets")
    targets = [a for a in assets
               if a.get("group_id") in group_ids
               or (a.get("type") == "investment"
                   and a.get("source", "") in ("manual", "csv_import"))]
    print(f"  --reset: removendo {len(targets)} ativo(s) existentes...")
    for a in targets:
        try:
            c.delete(f"/api/assets/{a['id']}")
        except Exception as e:
            print(f"    [ERR] delete {a['name']}: {e}")


# --- Build assets from pipeline CSVs --------------------------------------

def build_b3_rv_assets(holdings_final: list[dict],
                       prices: dict[str, dict[str, float]],
                       group_id: str) -> list[dict]:
    """One Asset per ticker in holdings_final. ticker.SA + market_price."""
    out = []
    for r in holdings_final:
        tk = r.get("ticker", "").strip()
        qty = _f(r.get("qty"))
        if tk in ("", "__EMPTY__") or abs(qty) < 1e-6:
            continue
        cost = _f(r.get("cost_basis"))
        avg_price = (cost / qty) if qty > 0 else None
        # last close from prices cache (most recent month)
        series = prices.get(tk, {})
        last_close = series[max(series)] if series else None
        cur_value = qty * last_close if last_close else cost
        out.append({
            "_history_key": ("rv", tk),
            "payload": {
                "name": tk,
                "type": "investment",
                "currency": "BRL",
                "units": qty,
                "valuation_method": "market_price",
                "ticker": tk + ".SA",
                "ticker_exchange": "SAO",
                "purchase_price": round(avg_price, 2) if avg_price else None,
                "current_value": round(cur_value, 2),
                "group_id": group_id,
                "source": "csv_import",
            }
        })
    return out


def build_rf_assets(rf_final: list[dict], group_id: str) -> list[dict]:
    """One Asset per RF position. Manual valuation with current MTM."""
    out = []
    for r in rf_final:
        titulo = (r.get("titulo") or "").strip()
        codigo = (r.get("codigo") or "").strip()
        qty = _f(r.get("qty"))
        if not titulo or abs(qty) < 1e-6:
            continue
        v_atual = _f(r.get("valor_mtm_liquido"))
        out.append({
            "_history_key": ("rf", titulo),
            "payload": {
                "name": titulo[:255],
                "type": "investment",
                "currency": "BRL",
                "units": qty,
                "valuation_method": "manual",
                "current_value": round(v_atual, 2),
                "maturity_date": r.get("vencimento") or None,
                "group_id": group_id,
                "source": "csv_import",
                "external_id": codigo or None,
            }
        })
    return out


def build_us_assets(us_final: list[dict], group_id: str) -> list[dict]:
    """One Asset per US ticker. ticker + market_price + currency=USD."""
    out = []
    for r in us_final:
        tk = r.get("ticker", "").strip()
        qty = _f(r.get("qty"))
        if not tk or abs(qty) < 1e-6:
            continue
        close_usd = _f(r.get("close_usd"))
        cur_usd = qty * close_usd
        out.append({
            "_history_key": ("us", tk),
            "payload": {
                "name": tk,
                "type": "investment",
                "currency": "USD",
                "units": qty,
                "valuation_method": "market_price",
                "ticker": tk,
                "ticker_exchange": "NASDAQ",
                "current_value": round(cur_usd, 2),
                "group_id": group_id,
                "source": "csv_import",
            }
        })
    return out


# --- History (AssetValue rows) --------------------------------------------

def build_history_rv(holdings_monthly: list[dict],
                     prices: dict[str, dict[str, float]]
                     ) -> dict[str, list[tuple[str, float]]]:
    """{ticker -> [(date_iso, amount_brl)]}.
    Valor mensal = qty(month_end) × close(month_end)."""
    out: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for r in holdings_monthly:
        tk = r.get("ticker", "").strip()
        if tk in ("", "__EMPTY__"):
            continue
        d = r.get("date", "")
        qty = _f(r.get("qty"))
        if abs(qty) < 1e-6:
            continue
        series = prices.get(tk, {})
        close = series.get(d)
        if close is None:
            past = [m for m in series if m <= d]
            if past:
                close = series[max(past)]
        if close is None:
            continue
        amount = qty * close
        if amount <= 0:
            continue
        out[tk].append((d, round(amount, 2)))
    return out


def build_history_rf(rf_monthly: list[dict]
                     ) -> dict[str, list[tuple[str, float]]]:
    """{titulo -> [(month_end_iso, valor_mtm_liquido_brl)]}."""
    out: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for r in rf_monthly:
        titulo = (r.get("titulo") or "").strip()
        if not titulo:
            continue
        v = _f(r.get("valor_mtm_liquido"))
        if v <= 0:
            continue
        out[titulo].append((r["month_end"], round(v, 2)))
    return out


def build_history_us(us_monthly: list[dict]
                     ) -> dict[str, list[tuple[str, float]]]:
    """{ticker -> [(month_end_iso, valor_brl)]}.

    NOTA: AssetValue.amount está sempre na CURRENCY do Asset. Para US
    (Asset.currency=USD), gravamos valor_usd; para BR, valor em BRL.
    """
    out: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for r in us_monthly:
        tk = r.get("ticker", "").strip()
        if not tk:
            continue
        v_usd = _f(r.get("valor_usd"))
        if v_usd <= 0:
            continue
        out[tk].append((r["month_end"], round(v_usd, 2)))
    return out


def load_prices_cache(path: Path) -> dict[str, dict[str, float]]:
    """{ticker -> {month_end_iso: close_brl}} (B3 .SA prices)."""
    out: dict[str, dict[str, float]] = defaultdict(dict)
    if not path.exists():
        return out
    for r in csv.DictReader(open(path, encoding="utf-8")):
        out[r["ticker"]][r["month_end"]] = float(r["close"])
    return out


# --- Main flow ------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base-url", default="http://localhost:8000")
    ap.add_argument("--email", required=True)
    ap.add_argument("--password", default=None)
    ap.add_argument("--csv-dir", default=".",
                    help="Diretório com os CSVs do pipeline (default: cwd)")
    ap.add_argument("--reset", action="store_true",
                    help="Apaga investimentos existentes nos grupos alvo antes de importar")
    ap.add_argument("--no-history", action="store_true",
                    help="Pula a criação dos AssetValue mensais (só posição atual)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    base = Path(args.csv_dir).resolve()
    print(f"CSV dir: {base}")

    holdings_final = read_csv(base / "holdings_final.csv")
    rf_final = read_csv(base / "rf_final.csv")
    us_final = read_csv(base / "us_final.csv")
    prices = load_prices_cache(base / "prices_cache.csv")
    holdings_monthly = read_csv(base / "holdings_monthly.csv") if not args.no_history else []
    rf_monthly = read_csv(base / "rf_holdings_monthly.csv") if not args.no_history else []
    us_monthly = read_csv(base / "us_holdings_monthly.csv") if not args.no_history else []

    print(f"\nDados de origem:")
    print(f"  holdings_final:    {len(holdings_final)} linhas")
    print(f"  rf_final:          {len(rf_final)} linhas")
    print(f"  us_final:          {len(us_final)} linhas")
    print(f"  prices_cache:      {len(prices)} tickers")
    print(f"  holdings_monthly:  {len(holdings_monthly)} linhas")
    print(f"  rf_holdings_monthly: {len(rf_monthly)} linhas")
    print(f"  us_holdings_monthly: {len(us_monthly)} linhas")

    # In --dry-run we don't hit the API at all; just preview.
    if args.dry_run:
        rv_assets = build_b3_rv_assets(holdings_final, prices, "<rv-group-id>")
        rf_assets = build_rf_assets(rf_final, "<rf-group-id>")
        us_assets = build_us_assets(us_final, "<us-group-id>")
        print(f"\nAtivos a criar: RV={len(rv_assets)}  RF={len(rf_assets)}  US={len(us_assets)}")
        print("\n--- DRY RUN — primeiros exemplos por bucket ---")
        for label, lst in [("RV", rv_assets), ("RF", rf_assets), ("US", us_assets)]:
            print(f"\n[{label}]")
            for x in lst[:3]:
                p = x['payload']
                print(f"  {p['name']:<55}  "
                      f"{p['valuation_method']:<13}  "
                      f"qty={p.get('units'):<8}  "
                      f"value={p.get('current_value', '-')}  "
                      f"ccy={p['currency']}")
        if not args.no_history:
            hist_rv = build_history_rv(holdings_monthly, prices)
            hist_rf = build_history_rf(rf_monthly)
            hist_us = build_history_us(us_monthly)
            total = (sum(len(v) for v in hist_rv.values())
                     + sum(len(v) for v in hist_rf.values())
                     + sum(len(v) for v in hist_us.values()))
            print(f"\nHistórico (AssetValue) total: {total} pontos mensais")
            print(f"  RV: {sum(len(v) for v in hist_rv.values())}  "
                  f"({len(hist_rv)} ativos)")
            print(f"  RF: {sum(len(v) for v in hist_rf.values())}  "
                  f"({len(hist_rf)} ativos)")
            print(f"  US: {sum(len(v) for v in hist_us.values())}  "
                  f"({len(hist_us)} ativos)")
        return 0

    # Login
    pwd = args.password or getpass.getpass(f"Senha do Securo para {args.email}: ")
    print(f"\nAutenticando em {args.base_url} ...")
    token = login(args.base_url, args.email, pwd)
    print("OK login.")
    c = SecuroClient(args.base_url, token)

    # Groups
    print("\nGarantindo AssetGroups...")
    group_ids = ensure_groups(c)

    # Reset (optional)
    if args.reset:
        reset_investments(c, list(group_ids.values()))

    # Build asset payloads
    rv_assets = build_b3_rv_assets(holdings_final, prices, group_ids["rv_br"])
    rf_assets = build_rf_assets(rf_final, group_ids["rf"])
    us_assets = build_us_assets(us_final, group_ids["us"])
    print(f"\nAtivos a criar: RV={len(rv_assets)}  RF={len(rf_assets)}  US={len(us_assets)}")

    # Histories
    hist_rv = build_history_rv(holdings_monthly, prices) if not args.no_history else {}
    hist_rf = build_history_rf(rf_monthly) if not args.no_history else {}
    hist_us = build_history_us(us_monthly) if not args.no_history else {}

    # Create assets + populate history
    ok = fail = 0
    val_ok = val_fail = 0
    for bucket, assets, history in [
        ("RV", rv_assets, hist_rv),
        ("RF", rf_assets, hist_rf),
        ("US", us_assets, hist_us),
    ]:
        print(f"\n=== {bucket} ({len(assets)} ativos) ===")
        for x in assets:
            payload = x["payload"]
            try:
                a = c.post("/api/assets", payload)
                ok += 1
                aid = a["id"]
                # History
                hist_key = x["_history_key"][1]
                series = history.get(hist_key, [])
                if series:
                    # avoid duplicating the initial AssetValue (created from
                    # current_value); skip month_end equal to today's last
                    last = max(series, key=lambda kv: kv[0])
                    for d, amount in series:
                        # write all monthly points; the create endpoint already
                        # seeded one near "today" but a separate dated row in
                        # the past doesn't conflict.
                        try:
                            c.post(f"/api/assets/{aid}/values",
                                   {"amount": amount, "date": d})
                            val_ok += 1
                        except Exception as e:
                            val_fail += 1
                            if val_fail <= 5:
                                print(f"    [VAL ERR] {payload['name']} {d}: {e}")
                    print(f"  [OK]  {payload['name']:<50}  "
                          f"id={aid[:8]}  +{len(series)} valores históricos")
                else:
                    print(f"  [OK]  {payload['name']:<50}  id={aid[:8]}  (sem histórico)")
            except Exception as e:
                fail += 1
                print(f"  [ERR] {payload['name']:<50}  {e}")

    print(f"\n=== Resumo ===")
    print(f"  Ativos criados:           {ok}  (falhas: {fail})")
    print(f"  Valores históricos:       {val_ok}  (falhas: {val_fail})")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
