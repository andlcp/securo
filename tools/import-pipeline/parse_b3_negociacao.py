#!/usr/bin/env python3
"""
parse_b3_negociacao.py

Parses the B3 "Negociação" extract (clean trade-only export, vs the noisier
"Movimentação" extract). Source of truth for buy/sell history since 2020-10.

Input columns (Portuguese, with accented headers):
    Data do Negócio | Tipo de Movimentação | Mercado | Prazo/Vencimento |
    Instituição | Código de Negociação | Quantidade | Preço | Valor

Tipo de Movimentação: Compra | Venda
Mercado:
    Mercado à Vista                    -> trade real do underlying
    Mercado Fracionário                -> trade real (normaliza XXXXF -> XXXX)
    Opção de Compra sobre Ações        -> IGNORE (premium, não muda qty)
    Opção de Venda sobre Ações         -> IGNORE (premium, não muda qty)
    Exercício de Opção de Compra       -> Venda do underlying ao strike
                                          (call coberta exercida pelo titular)

Output: trades.csv with columns:
    data,instituicao,ticker,operacao,quantidade,preco,valor,
    mercado_origem,ticker_origem,categoria,nota

categoria ∈ {VISTA, FRACIONARIO, EXERCICIO_CALL, OPCAO_PREMIO_IGNORE}
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from datetime import datetime
from typing import Optional

try:
    import openpyxl
except ImportError:
    print("Missing openpyxl. pip install openpyxl")
    sys.exit(1)


# Header normalization: openpyxl sometimes returns mojibake'd accents on this
# specific export (e.g. "Negócio" -> "Negcio" with stray bytes). Strip non-
# alphanumeric AND keep only ASCII letters/digits to make matching robust.
import unicodedata

def _norm(s: str) -> str:
    if not s:
        return ""
    # Try to fold accents; if mojibake, the accented byte is dropped here too.
    nfkd = unicodedata.normalize("NFKD", str(s))
    ascii_only = nfkd.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-zA-Z0-9]", "", ascii_only).lower()

# Match by FUZZY substring: the header may be "datadonegcio" (mojibake drops
# 'ó') or "datadonegocio" (clean). We check that all chars of the expected key
# appear in order. To keep it simple, we just match by leading prefix.
EXPECTED_PREFIXES = [
    ("data",         ["datadoneg"]),                     # Data do Negócio
    ("tipo",         ["tipodemov"]),                     # Tipo de Movimentação
    ("mercado",      ["mercado"]),                       # Mercado
    ("prazo",        ["prazo"]),                         # Prazo/Vencimento
    ("instituicao",  ["institui"]),                      # Instituição
    ("codigo",       ["codigodeneg"]),                   # Código de Negociação
    ("quantidade",   ["quantidade"]),                    # Quantidade
    ("preco",        ["preco", "prec"]),                 # Preço
    ("valor",        ["valor"]),                         # Valor
]

# Map option ticker -> underlying. B3 option tickers are 4-letter root +
# series letter (A-L = call jan-dec, M-X = put jan-dec) + strike code.
# Sometimes a trailing "E" denotes the exercise leg (e.g. VALEI568E).
OPTION_ROOT_TO_UNDERLYING = {
    "VALE": "VALE3",
    "PETR": "PETR4",  # user trades the PN; map ON if needed
    # add more if discovered
}

OPTION_RE = re.compile(r"^([A-Z]{4})([A-X])(\d{2,4})E?$")
FRAC_RE = re.compile(r"^([A-Z]{4}\d{1,2})F$")  # ASAI3F -> ASAI3


def parse_brazil_date(v) -> str:
    """Normalize date cell into YYYY-MM-DD."""
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


def classify(mercado: str, tipo: str, codigo: str) -> dict:
    """Classify a row -> returns dict with keys:
        categoria, ticker_normalizado, operacao_efetiva, nota
    or {'categoria': 'IGNORE', ...} if the row should not affect holdings.
    """
    m = _norm(mercado)
    t = _norm(tipo)
    cod = (codigo or "").strip().upper()

    # 1. Mercado à Vista -> trade real
    if "vista" in m and "fracion" not in m:
        return {
            "categoria": "VISTA",
            "ticker_normalizado": cod,
            "operacao_efetiva": "BUY" if t == "compra" else "SELL",
            "nota": "",
        }

    # 2. Mercado Fracionário -> trade real, ticker XXXXF -> XXXX
    if "fracion" in m:
        frac_m = FRAC_RE.match(cod)
        underlying = frac_m.group(1) if frac_m else cod
        return {
            "categoria": "FRACIONARIO",
            "ticker_normalizado": underlying,
            "operacao_efetiva": "BUY" if t == "compra" else "SELL",
            "nota": f"frac {cod}->{underlying}" if frac_m else "",
        }

    # 3. Exercício de Opção de Compra -> você foi exercido (vendeu underlying)
    if "exercicio" in m:
        opt_m = OPTION_RE.match(cod)
        if not opt_m:
            return {"categoria": "IGNORE",
                    "ticker_normalizado": cod,
                    "operacao_efetiva": None,
                    "nota": f"exercise but ticker not parseable: {cod}"}
        root = opt_m.group(1)
        underlying = OPTION_ROOT_TO_UNDERLYING.get(root, root + "3")
        # Only "Venda" makes sense here (covered call exercised -> you deliver)
        return {
            "categoria": "EXERCICIO_CALL",
            "ticker_normalizado": underlying,
            "operacao_efetiva": "SELL",
            "nota": f"covered call {cod} exercised -> entregou {underlying}",
        }

    # 4. Opções (premium recebido/pago, não muda quantidade do underlying)
    if "opcao" in m or "opção" in m:
        return {
            "categoria": "OPCAO_PREMIO_IGNORE",
            "ticker_normalizado": cod,
            "operacao_efetiva": None,
            "nota": f"option premium {tipo} {cod}",
        }

    # 5. Unknown
    return {"categoria": "IGNORE",
            "ticker_normalizado": cod,
            "operacao_efetiva": None,
            "nota": f"unknown mercado: {mercado}"}


def parse_negociacao(xlsx_path: str) -> list[dict]:
    wb = openpyxl.load_workbook(xlsx_path, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return []

    # Build column index from header (tolerate mojibake by prefix matching)
    header_norm = [_norm(str(c)) for c in rows[0]]
    col: dict[str, int] = {}
    for key, prefixes in EXPECTED_PREFIXES:
        for i, h in enumerate(header_norm):
            if any(h.startswith(p) for p in prefixes):
                col[key] = i
                break

    missing = [k for k, _ in EXPECTED_PREFIXES if k not in col]
    if missing:
        raise RuntimeError(
            f"Missing columns: {missing}. Got header: {rows[0]} -> norm={header_norm}")

    out = []
    for raw in rows[1:]:
        if raw is None or all(c is None for c in raw):
            continue
        try:
            data = parse_brazil_date(raw[col["data"]])
        except Exception:
            continue
        tipo = str(raw[col["tipo"]] or "").strip()
        mercado = str(raw[col["mercado"]] or "").strip()
        codigo = str(raw[col["codigo"]] or "").strip().upper()
        instituicao = str(raw[col["instituicao"]] or "").strip()
        try:
            qty = float(raw[col["quantidade"]] or 0)
            preco = float(raw[col["preco"]] or 0)
            valor = float(raw[col["valor"]] or 0)
        except (TypeError, ValueError):
            continue

        cls = classify(mercado, tipo, codigo)
        out.append({
            "data": data,
            "instituicao": instituicao,
            "ticker_origem": codigo,
            "ticker": cls["ticker_normalizado"],
            "operacao_origem": tipo,
            "operacao": cls["operacao_efetiva"] or "",
            "quantidade": qty,
            "preco": preco,
            "valor": valor,
            "mercado_origem": mercado,
            "categoria": cls["categoria"],
            "nota": cls["nota"],
        })

    return out


def write_trades_csv(rows: list[dict], path: str) -> None:
    if not rows:
        print("Nenhuma linha.")
        return
    fields = ["data", "instituicao", "ticker", "operacao", "quantidade",
              "preco", "valor", "mercado_origem", "ticker_origem",
              "operacao_origem", "categoria", "nota"]
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


def print_summary(rows: list[dict]) -> None:
    from collections import Counter, defaultdict
    cat = Counter(r["categoria"] for r in rows)
    print("\n=== Categorias ===")
    for k, v in cat.most_common():
        print(f"  {k:25s} {v:>4d}")

    print("\n=== Período ===")
    datas = sorted(r["data"] for r in rows)
    print(f"  {datas[0]} -> {datas[-1]}")

    # Per-ticker net qty (real trades only)
    real = [r for r in rows if r["categoria"] in ("VISTA", "FRACIONARIO", "EXERCICIO_CALL")]
    by_ticker: dict[str, dict[str, float]] = defaultdict(
        lambda: {"buy_qty": 0.0, "sell_qty": 0.0, "buy_val": 0.0, "sell_val": 0.0})
    for r in real:
        side = "buy" if r["operacao"] == "BUY" else "sell"
        by_ticker[r["ticker"]][f"{side}_qty"] += r["quantidade"]
        by_ticker[r["ticker"]][f"{side}_val"] += r["valor"]

    print(f"\n=== Saldo líquido por ticker ({len(by_ticker)} tickers) ===")
    print(f"  {'TICKER':<10} {'BUY qty':>10} {'SELL qty':>10} {'NET qty':>10}  "
          f"{'BUY R$':>12} {'SELL R$':>12}  {'NET R$':>12}")
    rows_sorted = sorted(by_ticker.items(),
                        key=lambda kv: kv[1]["buy_qty"] - kv[1]["sell_qty"],
                        reverse=True)
    for tk, d in rows_sorted:
        net_q = d["buy_qty"] - d["sell_qty"]
        net_v = d["buy_val"] - d["sell_val"]
        marker = ""
        if abs(net_q) < 0.001 and abs(net_v) > 100:
            marker = "  (zerado, P&L=)"
        print(f"  {tk:<10} {d['buy_qty']:>10.0f} {d['sell_qty']:>10.0f} "
              f"{net_q:>10.0f}  "
              f"R${d['buy_val']:>10.0f} R${d['sell_val']:>10.0f}  "
              f"R${net_v:>10.0f}{marker}")

    # Premium summary (ignored from holdings, but shown for transparency)
    prems = [r for r in rows if r["categoria"] == "OPCAO_PREMIO_IGNORE"]
    if prems:
        prem_recv = sum(r["valor"] for r in prems if r["operacao_origem"].lower() == "venda")
        prem_paid = sum(r["valor"] for r in prems if r["operacao_origem"].lower() == "compra")
        print(f"\n=== Prêmios de opção (ignorados) ===")
        print(f"  Recebidos (venda calls): R${prem_recv:,.2f}")
        print(f"  Pagos (compra puts):     R${prem_paid:,.2f}")
        print(f"  Net premium:             R${prem_recv - prem_paid:,.2f}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("xlsx", help="Path to negociacao-*.xlsx")
    ap.add_argument("--out", default="trades.csv")
    args = ap.parse_args()

    rows = parse_negociacao(args.xlsx)
    print(f"Parsed {len(rows)} rows from {args.xlsx}")
    write_trades_csv(rows, args.out)
    print(f"Wrote {args.out}")
    print_summary(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
