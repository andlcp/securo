#!/usr/bin/env python3
"""
Parse B3 Extrato de Movimentação Excel files into a clean ledger CSV.

Input:  F:/Investimentos/Historico Rentabilidade/Extratos B3/Excel/*.xlsx
Output: ledger_b3.csv

Columns per row in source:
    Entrada/Saída | Data | Movimentação | Produto | Instituição |
    Quantidade | Preço unitário | Valor da Operação

Output CSV columns:
    data, ticker, nome, categoria, tipo_b3, entrada_saida, quantidade,
    preco, valor, instituicao, arquivo

Categoria values:
    BUY, SELL, INCOME, PORTABILITY_IN, PORTABILITY_OUT, BONUS, SPLIT, FEE, IGNORE
"""
from __future__ import annotations

import csv
import re
from datetime import datetime
from pathlib import Path

import openpyxl

SRC_DIR = Path("F:/Investimentos/Historico Rentabilidade/Extratos B3/Excel")
OUT_PATH = Path("E:/Desenvolvimento/securo/ledger_b3.csv")

# Exact mapping of (entrada_saida, movimentação) → categoria
# Keys are (direction, normalized-label) where direction is 'C' or 'D'.
# '*' as direction means any direction.
CATEGORY_MAP: dict[tuple[str, str], str] = {
    # BUY ------------------------------------------------------------
    ("C", "Compra"): "BUY",
    ("C", "APLICAÇÃO"): "BUY",
    ("C", "APLICACAO"): "BUY",
    ("C", "APLICAÇÃO/DEPÓSITO"): "BUY",
    ("C", "Deposito"): "BUY",
    ("C", "COMPRA / VENDA"): "BUY",
    ("C", "COMPRA/VENDA DEFINITIVA/CESSAO"): "BUY",
    ("C", "MDA COMPRA/VENDA DEFINITIVA MERCADO PRIMARIO"): "BUY",
    # 'Transferência - Liquidação' with credit+val>0 is a real BUY settlement.
    ("C", "Transferência - Liquidação"): "BUY",
    # SELL -----------------------------------------------------------
    ("D", "Venda"): "SELL",
    ("D", "VENCIMENTO"): "SELL",
    ("D", "RESGATE ANTECIPADO"): "SELL",
    ("D", "Resgate"): "SELL",
    ("D", "COMPRA / VENDA"): "SELL",
    ("D", "COMPRA/VENDA DEFINITIVA/CESSAO"): "SELL",
    ("D", "Retirada"): "SELL",
    ("D", "Transferência - Liquidação"): "SELL",
    # PORTABILITY (custody-only) -------------------------------------
    ("C", "Transferência"): "PORTABILITY_IN",
    ("D", "Transferência"): "PORTABILITY_OUT",
    ("C", "Transferencia"): "PORTABILITY_IN",
    ("D", "Transferencia"): "PORTABILITY_OUT",
    ("C", "TRANSFERENCIA SEM FINANCEIRO"): "PORTABILITY_IN",
    ("D", "TRANSFERENCIA SEM FINANCEIRO"): "PORTABILITY_OUT",
    # INCOME (cash-like returns on asset) ----------------------------
    ("C", "Rendimento"): "INCOME",
    ("C", "Dividendo"): "INCOME",
    ("C", "Juros Sobre Capital Próprio"): "INCOME",
    ("C", "Juros"): "INCOME",
    ("C", "Reembolso"): "INCOME",
    ("C", "AMORTIZAÇÃO"): "INCOME",
    # Resgate on CREDITO side is typically reinvested rendimentos on FIIs.
    ("C", "Resgate"): "INCOME",
    # CFF pay-out (prêmio/rendimentos) -- asset debited but cash to investor.
    ("D", "PAGAMENTO DE PRÊMIO/RENDIMENTOS"): "INCOME",
    # FEES (decrease cash, qty unchanged) ----------------------------
    ("D", "Cobrança de Taxa Semestral"): "FEE",
    # Corporate actions changing qty, val=0 --------------------------
    ("C", "Bonificação em Ativos"): "BONUS",
    ("C", "Desdobro"): "SPLIT",
    ("C", "Grupamento"): "SPLIT",
    ("D", "Grupamento"): "SPLIT",
    # IGNORE (wildcards come last) -----------------------------------
    ("*", "Empréstimo"): "IGNORE",            # securities lending
    ("*", "Direito de Subscrição"): "IGNORE",
    ("*", "Direitos de Subscrição - Não Exercido"): "IGNORE",
    ("*", "Cessão de Direitos"): "IGNORE",
    ("*", "Cessão de Direitos - Solicitada"): "IGNORE",
    ("*", "Atualização"): "IGNORE",
    ("*", "Leilão de Fração"): "IGNORE",
    ("*", "Fração em Ativos"): "IGNORE",
    ("*", "BLOQUEIO DE CUSTÓDIA"): "IGNORE",
}


def _direction(entrada_saida: str) -> str:
    es = (entrada_saida or "").strip().lower()
    if es.startswith("cred"):
        return "C"
    if es.startswith("deb"):
        return "D"
    return "?"


def _categorize(direction: str, mov: str) -> str:
    mov = (mov or "").strip()
    # Any suffix " - Transferido" or " - Cancelado" is a reversal -> IGNORE.
    if re.search(r"-\s*(Transferido|Cancelado)$", mov, re.IGNORECASE):
        return "IGNORE"
    key = (direction, mov)
    if key in CATEGORY_MAP:
        return CATEGORY_MAP[key]
    wild = ("*", mov)
    if wild in CATEGORY_MAP:
        return CATEGORY_MAP[wild]
    return "UNKNOWN"


# Ticker normalization -----------------------------------------------
TICKER_PATTERNS = [
    # "BOVA11 - ISHARES IBOVESPA..." -> BOVA11
    re.compile(r"^([A-Z]{4}\d{1,2}[A-Z]?)\s*-\s*(.+)$"),
    # "CDB - CDB320AATJ3 - REALIZE..." -> CDB320AATJ3
    re.compile(r"^CDB\s*-\s*([A-Z0-9]+)\s*-\s*(.+)$"),
    # "LCI - LCI3250XYZ - BANCO..." -> LCI3250XYZ
    re.compile(r"^(?:LCI|LCA|LF|LFT|LFS|CRI|CRA|DEB)\s*-\s*([A-Z0-9]+)\s*-\s*(.+)$"),
    # "CFF - 4985023UN1 - AZ QUEST..." -> CFF_4985023UN1
    re.compile(r"^CFF\s*-\s*([A-Z0-9]+)\s*-\s*(.+)$"),
]


def _split_product(produto: str) -> tuple[str, str]:
    """Return (ticker, nome) from a raw Produto string."""
    p = (produto or "").strip()
    if not p:
        return "", ""

    # Tesouro Direto: "Tesouro Selic 2025" - no dash. Ticker = the whole thing.
    if p.lower().startswith("tesouro"):
        return p, p

    # Try generic B3 ticker first.
    m = re.match(r"^([A-Z]{4}\d{1,2}[A-Z]?)\s*-\s*(.+)$", p)
    if m:
        return m.group(1), m.group(2).strip()

    # CDB / LCI / LCA / CRI / CRA / DEB / LF / LFT (3 segments)
    m = re.match(
        r"^(CDB|LCI|LCA|LF[TS]?|CRI|CRA|DEB|NTN|LTN|LFTN|CFF|LC)\s*-\s*([^-]+?)\s*-\s*(.+)$",
        p,
    )
    if m:
        prefix, code, issuer = m.groups()
        return code.strip(), f"{prefix} {issuer.strip()}"

    # Same prefixes but only 2 segments: "CDB - CDB9255VFGP"
    m = re.match(
        r"^(CDB|LCI|LCA|LF[TS]?|CRI|CRA|DEB|NTN|LTN|LFTN|CFF|LC)\s*-\s*(.+)$",
        p,
    )
    if m:
        prefix, code = m.groups()
        return code.strip(), f"{prefix} {code.strip()}"

    # Fallback: first token.
    parts = [x.strip() for x in p.split("-")]
    if len(parts) >= 2:
        return parts[0], " - ".join(parts[1:])
    return p, p


def _num(x) -> float:
    if x is None or x == "-" or x == "":
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _date(x) -> str:
    if x is None:
        return ""
    if isinstance(x, datetime):
        return x.strftime("%Y-%m-%d")
    s = str(x).strip()
    # 22/01/2020
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", s)
    if m:
        d, mo, y = m.groups()
        return f"{y}-{mo}-{d}"
    # 2020-01-22 already
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return m.group(0)
    return s


def parse_all() -> list[dict]:
    rows: list[dict] = []
    unknown: dict[tuple[str, str], int] = {}
    files = sorted(SRC_DIR.glob("*.xlsx"))
    for f in files:
        wb = openpyxl.load_workbook(f, data_only=True)
        ws = wb.active
        header_seen = False
        for r in ws.iter_rows(min_row=1, values_only=True):
            if not r or r[0] is None:
                continue
            # Skip header
            if str(r[0]).strip() == "Entrada/Saída":
                header_seen = True
                continue
            if not header_seen:
                continue
            es, data, mov, prod, inst, qtd, pu, val = (list(r) + [None] * 8)[:8]
            if not mov:
                continue
            direction = _direction(es)
            cat = _categorize(direction, mov)
            if cat == "UNKNOWN":
                unknown[(direction, mov)] = unknown.get((direction, mov), 0) + 1
            ticker, nome = _split_product(prod)
            rows.append(
                {
                    "data": _date(data),
                    "ticker": ticker,
                    "nome": nome,
                    "categoria": cat,
                    "tipo_b3": mov,
                    "entrada_saida": direction,
                    "quantidade": _num(qtd),
                    "preco": _num(pu),
                    "valor": _num(val),
                    "instituicao": (inst or "").strip(),
                    "arquivo": f.name,
                }
            )
    if unknown:
        print("WARNING: unknown movement types found:")
        for (d, m), c in sorted(unknown.items(), key=lambda x: -x[1]):
            print(f"  [{d}] {c:4d}  {m}")
    return rows


def refine_by_value(rows: list[dict]) -> list[dict]:
    """Refine categories using the actual operation value:

    - BUY/SELL with val≈0 are not real trades. They are duplicate ledger
      entries for 'Transferência - Liquidação' that already appear as a
      'Transferência' (custody move) on the same day. IGNORE them so we
      don't double-count or distort PORTABILITY balances.
    """
    for row in rows:
        if row["categoria"] in ("BUY", "SELL") and abs(row["valor"]) < 1.0:
            row["categoria"] = "IGNORE"
    return rows


def drop_liquidations_with_same_day_portability(rows: list[dict]) -> list[dict]:
    """Mark 'Transferência - Liquidação' as IGNORE when there's a matching
    'Transferência' (PORTABILITY_IN/OUT) on the same date, same ticker,
    same quantity. This is the strongest signal it's a portability
    settlement, not a real market trade.

    B3 records inter-broker portability as THREE events on the same day:
        Transferência               (C, qty)  — physical receipt
        Transferência - Liquidação  (C, qty, valor) — financial settlement
        Transferência               (D, qty)  — physical exit at origin

    If we count the middle one as a BUY, we double the position. Same
    pattern for the OUT side.
    """
    from collections import defaultdict
    # Index Transferência (PORT) events by (date, ticker, qty rounded)
    port_keys: set[tuple[str, str, int]] = set()
    for r in rows:
        if r["tipo_b3"] != "Transferência":
            continue
        if r["categoria"] not in ("PORTABILITY_IN", "PORTABILITY_OUT"):
            continue
        if not r["ticker"]:
            continue
        port_keys.add((r["data"], r["ticker"], int(round(r["quantidade"]))))

    cancelled = 0
    for r in rows:
        if r["tipo_b3"] != "Transferência - Liquidação":
            continue
        if r["categoria"] not in ("BUY", "SELL"):
            continue
        if not r["ticker"]:
            continue
        key = (r["data"], r["ticker"], int(round(r["quantidade"])))
        if key in port_keys:
            r["categoria"] = "IGNORE"
            cancelled += 1
    print(f"  cancelled {cancelled} Transferência-Liquidação event(s) "
          f"matching same-day Transferência (portability settlement)")
    return rows


def pair_portability_liquidations(rows: list[dict]) -> list[dict]:
    """Detect 'Transferência - Liquidação' D/C pairs that are inter-broker
    portability with financial settlement (NOT real market trades).

    These pairs are:
      - same ticker
      - opposite direction (one D, one C)
      - same (or very close) quantity
      - within ~60 days
      - close prices (within ~5%) — a real day-trade may also pair, but
        the price gap typically widens with time

    When a pair is detected, BOTH events are reclassified as
    PORTABILITY_OUT / PORTABILITY_IN (custody-only) so they cancel out
    in net_qty and don't inflate purchase totals.

    Greedy algorithm: per ticker, scan chronologically. For each D
    (debit/SELL), look ahead up to 60 days for the first C (credit/BUY)
    with matching qty (±0.5%). If found, mark both as portability and
    move on.
    """
    from collections import defaultdict
    from datetime import date as _date, timedelta

    def _to_date(s: str) -> _date | None:
        try:
            y, m, d = s.split("-")
            return _date(int(y), int(m), int(d))
        except Exception:
            return None

    by_ticker: dict[str, list[int]] = defaultdict(list)
    for i, r in enumerate(rows):
        if r["tipo_b3"] != "Transferência - Liquidação":
            continue
        if r["categoria"] not in ("BUY", "SELL"):
            continue
        if r["valor"] < 1.0:
            continue
        if not r["ticker"]:
            continue
        by_ticker[r["ticker"]].append(i)

    paired = 0
    for ticker, idxs in by_ticker.items():
        # Sort indices by date
        idxs_sorted = sorted(idxs, key=lambda i: rows[i]["data"])
        used: set[int] = set()
        # First pass: pair D → later C (sell-then-rebuy = portability out then in)
        for ai in idxs_sorted:
            if ai in used:
                continue
            a = rows[ai]
            if a["categoria"] != "SELL":
                continue
            a_d = _to_date(a["data"])
            if not a_d:
                continue
            for bi in idxs_sorted:
                if bi in used or bi == ai:
                    continue
                b = rows[bi]
                if b["categoria"] != "BUY":
                    continue
                b_d = _to_date(b["data"])
                if not b_d or b_d < a_d:
                    continue
                if (b_d - a_d) > timedelta(days=60):
                    break  # idxs sorted by date
                # qty within 0.5%
                aq, bq = a["quantidade"], b["quantidade"]
                if aq <= 0 or abs(aq - bq) / aq > 0.005:
                    continue
                # price within 5%
                ap, bp = a.get("preco", 0) or 0, b.get("preco", 0) or 0
                if ap > 0 and bp > 0 and abs(ap - bp) / ap > 0.05:
                    continue
                # Match!
                a["categoria"] = "PORTABILITY_OUT"
                b["categoria"] = "PORTABILITY_IN"
                used.add(ai)
                used.add(bi)
                paired += 1
                break
        # Second pass: pair C → later D (rare: buy-then-sell same qty/price)
        for ai in idxs_sorted:
            if ai in used:
                continue
            a = rows[ai]
            if a["categoria"] != "BUY":
                continue
            a_d = _to_date(a["data"])
            if not a_d:
                continue
            for bi in idxs_sorted:
                if bi in used or bi == ai:
                    continue
                b = rows[bi]
                if b["categoria"] != "SELL":
                    continue
                b_d = _to_date(b["data"])
                if not b_d or b_d < a_d:
                    continue
                if (b_d - a_d) > timedelta(days=60):
                    break
                aq, bq = a["quantidade"], b["quantidade"]
                if aq <= 0 or abs(aq - bq) / aq > 0.005:
                    continue
                ap, bp = a.get("preco", 0) or 0, b.get("preco", 0) or 0
                if ap > 0 and bp > 0 and abs(ap - bp) / ap > 0.05:
                    continue
                a["categoria"] = "PORTABILITY_IN"
                b["categoria"] = "PORTABILITY_OUT"
                used.add(ai)
                used.add(bi)
                paired += 1
                break

    print(f"  paired {paired} Transferência-Liquidação event(s) as portability "
          f"(reclassified to PORTABILITY_IN/OUT)")
    return rows


def write_csv(rows: list[dict], path: Path) -> None:
    fields = [
        "data",
        "ticker",
        "nome",
        "categoria",
        "tipo_b3",
        "entrada_saida",
        "quantidade",
        "preco",
        "valor",
        "instituicao",
        "arquivo",
    ]
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in sorted(rows, key=lambda x: (x["data"], x["ticker"])):
            w.writerow(r)


def summary(rows: list[dict]) -> None:
    from collections import Counter

    c = Counter(r["categoria"] for r in rows)
    print("\n=== CATEGORY SUMMARY ===")
    for k in sorted(c, key=lambda x: -c[x]):
        print(f"  {c[k]:5d}  {k}")
    print(f"  {sum(c.values()):5d}  TOTAL")


def main() -> None:
    rows = parse_all()
    rows = refine_by_value(rows)
    rows = drop_liquidations_with_same_day_portability(rows)
    rows = pair_portability_liquidations(rows)
    write_csv(rows, OUT_PATH)
    summary(rows)
    print(f"\nWrote {len(rows)} rows to {OUT_PATH}")


if __name__ == "__main__":
    main()
