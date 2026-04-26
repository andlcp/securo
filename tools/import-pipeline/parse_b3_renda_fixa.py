#!/usr/bin/env python3
"""
parse_b3_renda_fixa.py

Parses the B3 "Movimentação" extract filtered for Tesouro Direto + CDBs.

Input columns (mojibake-tolerant):
    Entrada/Saída | Data | Movimentação | Produto | Instituição |
    Quantidade | Preço unitário | Valor da Operação

Movimentação values seen for RF:
    Compra            -> aplicação
    Venda             -> resgate (antecipado ou vencimento)
    COMPRA / VENDA    -> usado para CDB (Credito = aplicação, Debito = resgate)

Output: rf_trades.csv with columns:
    data, titulo, codigo, tipo, vencimento, operacao, qty, pu, valor,
    instituicao, holding_days, descricao

Where:
    tipo ∈ {TESOURO_SELIC, TESOURO_PREFIXADO, TESOURO_IPCA, TESOURO_IPCA_JS, CDB}
    vencimento = YYYY-MM-DD (NULL for CDB without metadata)
    holding_days = filled by replay (left empty here)
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


def _norm(s) -> str:
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFKD", str(s))
    ascii_only = nfkd.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-zA-Z0-9]", "", ascii_only).lower()


HEADER_PREFIXES = [
    ("entrada",      ["entrada"]),
    ("data",         ["data"]),
    ("movimentacao", ["movimenta"]),
    ("produto",      ["produto"]),
    ("instituicao",  ["institui"]),
    ("quantidade",   ["quantidade"]),
    ("preco",        ["preco", "prec"]),
    ("valor",        ["valor"]),
]


# Tesouro vencimentos (hardcoded for known series the user holds; extend as needed).
# Datas oficiais do Tesouro: Selic = 01/03; Prefixado LTN = 01/01; NTN-B (IPCA+) = 15/05 ou 15/08.
TESOURO_VENCIMENTOS = {
    "Tesouro Selic 2024": "2024-09-01",  # Selic 01/09/2024 (não 01/03)
    "Tesouro Selic 2025": "2025-03-01",
    "Tesouro Selic 2026": "2026-03-01",
    "Tesouro Selic 2027": "2027-03-01",
    "Tesouro Selic 2028": "2028-03-01",
    "Tesouro Selic 2029": "2029-03-01",
    "Tesouro Selic 2030": "2030-03-01",
    "Tesouro Selic 2031": "2031-03-01",
    "Tesouro Prefixado 2026": "2026-01-01",
    "Tesouro Prefixado 2027": "2027-01-01",
    "Tesouro Prefixado 2028": "2028-01-01",
    "Tesouro Prefixado 2029": "2029-01-01",
    "Tesouro IPCA+ 2026": "2026-08-15",
    "Tesouro IPCA+ 2029": "2029-05-15",
    "Tesouro IPCA+ 2035": "2035-05-15",
    "Tesouro IPCA+ 2040": "2040-08-15",
    "Tesouro IPCA+ 2045": "2045-05-15",
    "Tesouro IPCA+ com Juros Semestrais 2030": "2030-08-15",
    "Tesouro IPCA+ com Juros Semestrais 2035": "2035-05-15",
    "Tesouro IPCA+ com Juros Semestrais 2040": "2040-08-15",
    "Tesouro IPCA+ com Juros Semestrais 2045": "2045-05-15",
    "Tesouro IPCA+ com Juros Semestrais 2050": "2050-08-15",
    "Tesouro IPCA+ com Juros Semestrais 2055": "2055-05-15",
}


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


def classify_titulo(produto: str) -> dict:
    """Returns {tipo, codigo, vencimento}."""
    s = (produto or "").strip()
    if not s:
        return {"tipo": "OUTRO", "codigo": "", "vencimento": ""}

    # CDB: format usually "CDB - CDB123XYZ - BANCO XYZ" or "CDB - CDB123XYZ"
    if s.upper().startswith("CDB "):
        # extract code (second token if any)
        parts = [p.strip() for p in s.split(" - ")]
        codigo = ""
        if len(parts) >= 2:
            codigo = parts[1].upper()
        return {"tipo": "CDB", "codigo": codigo, "vencimento": ""}

    # Tesouro
    if s.lower().startswith("tesouro"):
        venc = TESOURO_VENCIMENTOS.get(s, "")
        if "selic" in s.lower():
            tipo = "TESOURO_SELIC"
        elif "prefixado" in s.lower():
            tipo = "TESOURO_PREFIXADO"
        elif "juros semestrais" in s.lower():
            tipo = "TESOURO_IPCA_JS"
        elif "ipca" in s.lower():
            tipo = "TESOURO_IPCA"
        else:
            tipo = "TESOURO_OUTRO"
        return {"tipo": tipo, "codigo": s, "vencimento": venc}

    return {"tipo": "OUTRO", "codigo": s, "vencimento": ""}


def parse_renda_fixa(xlsx_path: str) -> list[dict]:
    wb = openpyxl.load_workbook(xlsx_path, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return []

    header_norm = [_norm(c) for c in rows[0]]
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
        es = str(raw[col["entrada"]] or "").strip().lower()
        mov = str(raw[col["movimentacao"]] or "").strip()
        produto = str(raw[col["produto"]] or "").strip()
        instituicao = str(raw[col["instituicao"]] or "").strip()
        try:
            qty = float(raw[col["quantidade"]] or 0)
            pu = float(raw[col["preco"]] or 0)
            valor = float(raw[col["valor"]] or 0)
        except (TypeError, ValueError):
            continue

        # Determine BUY vs SELL
        # "Credito"/"Crédito" = aplicação = BUY (entrou ativo na carteira)
        # "Debito"/"Débito"  = resgate    = SELL (saiu da carteira)
        if es.startswith("cred") or _norm(es).startswith("credito"):
            operacao = "BUY"
        elif es.startswith("deb") or _norm(es).startswith("debito"):
            operacao = "SELL"
        else:
            continue

        cls = classify_titulo(produto)
        if cls["tipo"] == "OUTRO":
            continue

        out.append({
            "data": data,
            "titulo": produto,
            "codigo": cls["codigo"],
            "tipo": cls["tipo"],
            "vencimento": cls["vencimento"],
            "operacao": operacao,
            "qty": qty,
            "pu": pu,
            "valor": valor,
            "instituicao": instituicao,
            "descricao": mov,
        })

    return out


def write_csv(rows: list[dict], path: str) -> None:
    fields = ["data", "titulo", "codigo", "tipo", "vencimento", "operacao",
              "qty", "pu", "valor", "instituicao", "descricao"]
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


def print_summary(rows: list[dict]) -> None:
    print(f"\n=== Tipos ===")
    tipos = Counter(r["tipo"] for r in rows)
    for k, v in tipos.most_common():
        print(f"  {k:<22s} {v:>4d}")

    print(f"\n=== Net por título ===")
    net: dict[str, dict] = defaultdict(
        lambda: {"buy_qty": 0, "sell_qty": 0, "buy_val": 0, "sell_val": 0,
                 "first": "9999", "last": "0000", "tipo": "", "venc": ""})
    for r in rows:
        key = r["titulo"]
        n = net[key]
        n["tipo"] = r["tipo"]
        n["venc"] = r["vencimento"]
        if r["operacao"] == "BUY":
            n["buy_qty"] += r["qty"]
            n["buy_val"] += r["valor"]
        else:
            n["sell_qty"] += r["qty"]
            n["sell_val"] += r["valor"]
        n["first"] = min(n["first"], r["data"])
        n["last"] = max(n["last"], r["data"])

    print(f"  {'TÍTULO':<55} {'TIPO':<18} {'VENC':<11} "
          f"{'NET_qty':>10} {'BUY_R$':>11} {'SELL_R$':>11}")
    for k in sorted(net, key=lambda x: -net[x]["buy_val"]):
        n = net[k]
        nq = n["buy_qty"] - n["sell_qty"]
        venc = n["venc"] or "-"
        print(f"  {k:<55s} {n['tipo']:<18s} {venc:<11} "
              f"{nq:>10.2f} {n['buy_val']:>11,.0f} {n['sell_val']:>11,.0f}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("xlsx", help="Path to movimentacao-*.xlsx (renda fixa)")
    ap.add_argument("--out", default="rf_trades.csv")
    args = ap.parse_args()

    rows = parse_renda_fixa(args.xlsx)
    print(f"Parsed {len(rows)} RF rows from {args.xlsx}")
    write_csv(rows, args.out)
    print(f"Wrote {args.out}")
    print_summary(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
