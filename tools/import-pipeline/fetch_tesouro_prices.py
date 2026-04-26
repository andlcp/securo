#!/usr/bin/env python3
"""
fetch_tesouro_prices.py

Downloads the public Tesouro Direto historical prices CSV from the
Tesouro Transparente portal and extracts monthly close PUs per
(Tipo Titulo, Data Vencimento).

Source: https://www.tesourotransparente.gov.br/ckan/dataset/precos-e-taxas-historicas-dos-titulos-ofertados
Direct CSV: PrecoTaxaTesouroDireto.csv (semi-colon separated, comma decimals).

Output: tesouro_prices_cache.csv with columns:
    titulo, vencimento, month_end, pu_close
where pu_close is the PU Base Manhã of the last business day of the month.

Usage:
    python fetch_tesouro_prices.py [--start 2019-01]
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta


CSV_URL = (
    "https://www.tesourotransparente.gov.br/ckan/dataset/"
    "df56aa42-484a-4a59-8184-7676580c81e3/resource/"
    "796d2059-14e9-44e3-80c9-2d9e30b405c1/download/PrecoTaxaTesouroDireto.csv"
)


# Map our internal label -> (Tipo Titulo no CSV oficial). Vencimento vem do extrato.
TIPO_MAP = {
    "TESOURO_SELIC":     "Tesouro Selic",
    "TESOURO_PREFIXADO": "Tesouro Prefixado",
    "TESOURO_IPCA":      "Tesouro IPCA+",
    "TESOURO_IPCA_JS":   "Tesouro IPCA+ com Juros Semestrais",
}

# Reverse map (so we can convert Tipo Titulo -> our label)
LABEL_FROM_TIPO = {v: k for k, v in TIPO_MAP.items()}


def download_csv(dest_path: str) -> None:
    print(f"Downloading {CSV_URL}\n        -> {dest_path}")
    req = urllib.request.Request(CSV_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=120) as r:
        data = r.read()
    with open(dest_path, "wb") as f:
        f.write(data)
    size_mb = os.path.getsize(dest_path) / 1024 / 1024
    print(f"Downloaded {size_mb:.1f} MB")


def parse_brazil_date(s: str) -> str:
    return datetime.strptime(s.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")


def parse_brazil_number(s: str) -> float:
    return float(s.strip().replace(".", "").replace(",", "."))


def month_end_iso(ym: str) -> str:
    y, m = ym.split("-")
    if m == "12":
        return date(int(y), 12, 31).isoformat()
    return (date(int(y), int(m) + 1, 1) - timedelta(days=1)).isoformat()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--raw", default="tesouro_raw.csv",
                    help="Local cache do CSV oficial (será baixado se ausente)")
    ap.add_argument("--out", default="tesouro_prices_cache.csv")
    ap.add_argument("--start", default="2019-01",
                    help="YYYY-MM, primeiro mês a manter no cache final")
    ap.add_argument("--force-download", action="store_true")
    args = ap.parse_args()

    if args.force_download or not os.path.exists(args.raw):
        download_csv(args.raw)
    else:
        size_mb = os.path.getsize(args.raw) / 1024 / 1024
        print(f"Using cached {args.raw} ({size_mb:.1f} MB)  "
              f"(use --force-download to refresh)")

    # The official CSV uses ';' separator and comma decimals.
    # Encoding: latin-1 historicamente; checa header.
    encs = ["utf-8-sig", "utf-8", "latin-1"]
    fh = None
    for enc in encs:
        try:
            fh = open(args.raw, encoding=enc)
            head = fh.readline()
            if "Tipo Titulo" in head:
                fh.seek(0)
                print(f"Encoding: {enc}")
                break
            fh.close()
            fh = None
        except UnicodeDecodeError:
            if fh:
                fh.close()
            fh = None
    if fh is None:
        print("Could not open CSV with known encodings", file=sys.stderr)
        return 1

    reader = csv.DictReader(fh, delimiter=";")
    # Aggregate: (label, vencimento_iso, ym) -> latest day's PU
    by_key: dict[tuple[str, str, str], dict] = {}
    n_rows = 0
    n_kept = 0
    for r in reader:
        n_rows += 1
        tipo_full = r.get("Tipo Titulo", "").strip()
        label = LABEL_FROM_TIPO.get(tipo_full)
        if not label:
            continue
        try:
            venc_iso = parse_brazil_date(r["Data Vencimento"])
            base_iso = parse_brazil_date(r["Data Base"])
        except Exception:
            continue
        ym = base_iso[:7]
        if ym < args.start:
            continue
        # Use PU Base Manhã (the canonical reference quote of the day)
        pu_str = r.get("PU Base Manha") or r.get("PU Base Manhã") \
                 or r.get("PU Venda Manha") or r.get("PU Venda Manhã") \
                 or r.get("PU Compra Manha") or r.get("PU Compra Manhã")
        if not pu_str:
            continue
        try:
            pu = parse_brazil_number(pu_str)
        except Exception:
            continue
        if pu <= 0:
            continue

        key = (label, venc_iso, ym)
        prev = by_key.get(key)
        if prev is None or base_iso > prev["base"]:
            by_key[key] = {"base": base_iso, "pu": pu}
        n_kept += 1
    fh.close()

    print(f"Read {n_rows:,} rows; kept {n_kept:,}; "
          f"distinct (titulo,venc,ym): {len(by_key):,}")

    # Convert to month-end output rows
    out_rows = []
    for (label, venc, ym), info in by_key.items():
        out_rows.append({
            "titulo_tipo": label,
            "vencimento": venc,
            "month_end": month_end_iso(ym),
            "data_base": info["base"],
            "pu_close": round(info["pu"], 6),
        })
    out_rows.sort(key=lambda r: (r["titulo_tipo"], r["vencimento"], r["month_end"]))

    fields = ["titulo_tipo", "vencimento", "month_end", "data_base", "pu_close"]
    with open(args.out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in out_rows:
            w.writerow(r)

    # Summary by titulo
    by_label: dict[str, set] = defaultdict(set)
    for r in out_rows:
        by_label[r["titulo_tipo"]].add(r["vencimento"])
    print(f"\nWrote {args.out} ({len(out_rows):,} rows)")
    print(f"\n=== Cobertura ===")
    for label, vencs in sorted(by_label.items()):
        print(f"  {label:<22s} {len(vencs):>3d} vencimentos distintos")
    return 0


if __name__ == "__main__":
    sys.exit(main())
