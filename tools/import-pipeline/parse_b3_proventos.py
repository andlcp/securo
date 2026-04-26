#!/usr/bin/env python3
"""
parse_b3_proventos.py

Parses the B3 "Movimentação" extract filtered for income events (Rendimento,
Dividendo, JCP, Resgate). The trades themselves come from a separate extract
(`negociacao-*.xlsx`), so we only keep credit/debit cashflow income here.

Input columns (with mojibake on B3 export):
    Entrada/Saída | Data | Movimentação | Produto | Instituição |
    Quantidade | Preço unitário | Valor da Operação

Movimentação values of interest:
    Rendimento       -> FII monthly income
    Dividendo        -> dividends
    Juros Sobre Capital Próprio (JCP)
    Resgate          -> partial liquidation (FII)

Output: proventos.csv with columns
    data, ticker, tipo, valor, instituicao, descricao
where tipo ∈ {RENDIMENTO, DIVIDENDO, JCP, RESGATE}.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime

try:
    import openpyxl
except ImportError:
    print("Missing openpyxl. pip install openpyxl")
    sys.exit(1)


def _norm(s: str) -> str:
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFKD", str(s))
    ascii_only = nfkd.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-zA-Z0-9]", "", ascii_only).lower()


HEADER_PREFIXES = [
    ("entrada",      ["entrada"]),       # Entrada/Saída
    ("data",         ["data"]),
    ("movimentacao", ["movimenta"]),
    ("produto",      ["produto"]),
    ("instituicao",  ["institui"]),
    ("quantidade",   ["quantidade"]),
    ("preco",        ["preco", "prec"]),
    ("valor",        ["valor"]),
]

# Map "Movimentação" string -> standard tipo. Includes mojibake-tolerant matching.
TYPE_MAP = [
    ("rendimento", "RENDIMENTO"),
    ("dividendo",  "DIVIDENDO"),
    ("juros",      "JCP"),       # "Juros Sobre Capital Próprio"
    ("jcp",        "JCP"),
    ("resgate",    "RESGATE"),
]


def parse_brazil_date(v) -> str:
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, str):
        s = v.strip()
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
            try:
                return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
    raise ValueError(f"Cannot parse date: {v!r}")


def extract_ticker(produto: str) -> str:
    """Produto vem como 'TICKER - Nome longo da empresa'. Returns the TICKER."""
    if not produto:
        return ""
    s = produto.strip()
    # take first chunk before " - "
    head = s.split(" - ")[0].strip().upper()
    # sanity: ticker has 4 letters + 1-2 digits (XXXXN or XXXXNN)
    if re.match(r"^[A-Z]{4}\d{1,2}$", head):
        return head
    return head


def classify_tipo(movimentacao: str) -> str:
    n = _norm(movimentacao)
    for needle, label in TYPE_MAP:
        if needle in n:
            return label
    return "OUTRO"


def parse_proventos(xlsx_path: str) -> list[dict]:
    wb = openpyxl.load_workbook(xlsx_path, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return []

    header_norm = [_norm(str(c)) for c in rows[0]]
    col: dict[str, int] = {}
    for key, prefixes in HEADER_PREFIXES:
        for i, h in enumerate(header_norm):
            if any(h.startswith(p) for p in prefixes):
                col[key] = i
                break

    missing = [k for k, _ in HEADER_PREFIXES if k not in col]
    if missing:
        raise RuntimeError(f"Missing columns: {missing}. Header: {rows[0]}")

    out = []
    for raw in rows[1:]:
        if raw is None or all(c is None for c in raw):
            continue
        try:
            data = parse_brazil_date(raw[col["data"]])
        except Exception:
            continue
        movimentacao = str(raw[col["movimentacao"]] or "").strip()
        tipo = classify_tipo(movimentacao)
        if tipo == "OUTRO":
            continue
        produto = str(raw[col["produto"]] or "").strip()
        ticker = extract_ticker(produto)
        instituicao = str(raw[col["instituicao"]] or "").strip()
        try:
            valor = float(raw[col["valor"]] or 0)
        except (TypeError, ValueError):
            continue
        if valor <= 0:
            continue

        out.append({
            "data": data,
            "ticker": ticker,
            "tipo": tipo,
            "valor": valor,
            "instituicao": instituicao,
            "descricao": movimentacao,
        })

    return out


def write_csv(rows: list[dict], path: str) -> None:
    fields = ["data", "ticker", "tipo", "valor", "instituicao", "descricao"]
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


def print_summary(rows: list[dict]) -> None:
    by_tipo = Counter(r["tipo"] for r in rows)
    print("\n=== Tipos ===")
    for k, v in by_tipo.most_common():
        total = sum(r["valor"] for r in rows if r["tipo"] == k)
        print(f"  {k:<12} {v:>4d}  R${total:>12,.2f}")
    total = sum(r["valor"] for r in rows)
    print(f"  {'TOTAL':<12} {len(rows):>4d}  R${total:>12,.2f}")

    if rows:
        datas = sorted(r["data"] for r in rows)
        print(f"\nPeríodo: {datas[0]} -> {datas[-1]}")

    by_ticker: dict[str, float] = defaultdict(float)
    for r in rows:
        by_ticker[r["ticker"]] += r["valor"]
    top = sorted(by_ticker.items(), key=lambda kv: -kv[1])[:15]
    print(f"\n=== Top 15 tickers (recebidos) ===")
    for tk, v in top:
        print(f"  {tk:<10} R${v:>10,.2f}")

    # Monthly aggregation
    by_month: dict[str, float] = defaultdict(float)
    for r in rows:
        ym = r["data"][:7]
        by_month[ym] += r["valor"]
    months = sorted(by_month)
    print(f"\n=== Por mês (últimos 12) ===")
    for m in months[-12:]:
        print(f"  {m}  R${by_month[m]:>10,.2f}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("xlsx", help="Path to movimentacao-*.xlsx")
    ap.add_argument("--out", default="proventos.csv")
    args = ap.parse_args()

    rows = parse_proventos(args.xlsx)
    print(f"Parsed {len(rows)} provento rows from {args.xlsx}")
    write_csv(rows, args.out)
    print(f"Wrote {args.out}")
    print_summary(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
