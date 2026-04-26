#!/usr/bin/env python3
"""
Parse XP Posição Detalhada Histórica monthly snapshots (2017-12 .. 2019-10).

Each XLSX has a single sheet 'Sua carteira' with:
  - Row 4 col 0: total invested patrimônio (R$ xxx,xx)
  - Row 1 col 5: header text containing "Data da Posição Histórica: dd/mm/yyyy"
  - Then sections:
      * Tesouro Direto: cols [descricao, posicao, %, total_aplicado, qtd, disponivel, vencimento]
      * Renda Fixa:     cols [descricao, posicao_mkt, %, valor_aplicado, valor_aplicado_orig,
                              taxa_mkt, data_aplic, data_venc, qtd, pu, IR, IOF, valor_liquido]
      * Fundos:         cols [descricao, posicao, %, rent_liq, rent_bruta, valor_aplicado, valor_liquido]

Outputs:
  xp_snapshots.csv  — monthly patrimônio                (data, patrimonio)
  xp_holdings.csv   — line items per snapshot           (data, categoria, descricao,
                       posicao, qtd, valor_aplicado, valor_liquido,
                       data_aplicacao, vencimento)
"""
from __future__ import annotations

import csv
import re
from pathlib import Path

import openpyxl

SRC_DIR = Path("F:/Investimentos/Historico Rentabilidade/Extratos XP")
OUT_SNAPSHOTS = Path("E:/Desenvolvimento/securo/xp_snapshots.csv")
OUT_HOLDINGS = Path("E:/Desenvolvimento/securo/xp_holdings.csv")


def _money(x) -> float:
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    s = s.replace("R$", "").replace("\xa0", "").strip()
    if not s or s == "-":
        return 0.0
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _num(x) -> float:
    if x is None or x == "":
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
    s = str(x).strip()
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", s)
    if m:
        d, mo, y = m.groups()
        return f"{y}-{mo}-{d}"
    return s


def _section_total(row) -> float | None:
    """Detect a section header row: col 0 has the section name and col 6 has 'R$ ...'."""
    c0 = (str(row[0]).strip() if row[0] else "")
    c6 = (str(row[6]).strip() if len(row) > 6 and row[6] else "")
    if not c0 or not c6:
        return None
    if not (row[1] in (None, "")) or not (row[2] in (None, "")):
        return None
    if c6.startswith("R$"):
        return _money(c6)
    return None


def parse_one(path: Path) -> tuple[dict, list[dict]]:
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))

    # Header date
    header_text = str(rows[0][5]) if len(rows[0]) > 5 and rows[0][5] else ""
    m = re.search(r"Data da Posição Histórica:\s*(\d{2}/\d{2}/\d{4})", header_text)
    data = _date(m.group(1)) if m else ""

    # Patrimônio total
    patrim = _money(rows[3][0]) if len(rows) > 3 else 0.0

    snap = {"data": data, "patrimonio": patrim, "arquivo": path.name}
    holdings: list[dict] = []

    current_section: str | None = None
    current_header: list[str] | None = None

    for r in rows[1:]:  # skip first row
        if r is None:
            continue
        # Detect section heading (e.g., 'Tesouro Direto', 'Renda Fixa', 'Fundos de Investimentos')
        c0 = (str(r[0]).strip() if r[0] else "")
        if c0 in ("Tesouro Direto", "Renda Fixa", "Fundos de Investimentos"):
            if _section_total(r) is not None:
                current_section = c0
                current_header = None
                continue
        # Detect header row inside a section
        c1 = (str(r[1]).strip() if len(r) > 1 and r[1] else "")
        if c1 in ("Posição", "Posição a mercado") and current_section:
            current_header = [str(c).strip() if c else "" for c in r]
            continue

        if not current_section or not current_header:
            continue

        # Skip empty rows
        if c0 == "" or c0 == current_section:
            continue
        # Skip "Saldos" or similar non-asset rows: must have a position value in col 1
        c1_val = r[1]
        if c1_val is None or str(c1_val).strip() == "":
            continue

        # Parse common fields
        descricao = c0
        posicao = _money(r[1])

        item: dict = {
            "data": data,
            "categoria": current_section,
            "descricao": descricao,
            "posicao": posicao,
            "qtd": 0.0,
            "valor_aplicado": 0.0,
            "valor_liquido": 0.0,
            "data_aplicacao": "",
            "vencimento": "",
            "arquivo": path.name,
        }

        if current_section == "Tesouro Direto":
            # ['descr', 'Posição', '%', 'Total aplicado', 'Qtd.', 'Disponível', 'Vencimento', ...]
            item["valor_aplicado"] = _money(r[3]) if len(r) > 3 else 0.0
            item["qtd"] = _num(r[4]) if len(r) > 4 else 0.0
            item["valor_liquido"] = posicao
            item["vencimento"] = _date(r[6]) if len(r) > 6 else ""
        elif current_section == "Renda Fixa":
            # ['descr', 'Posição a mercado', '%', 'Valor aplicado', 'Valor aplicado original',
            #  'Taxa', 'Data aplicação', 'Data vencimento', 'Quantidade', 'PU', 'IR', 'IOF', 'Valor Líquido']
            item["valor_aplicado"] = _money(r[3]) if len(r) > 3 else 0.0
            item["data_aplicacao"] = _date(r[6]) if len(r) > 6 else ""
            item["vencimento"] = _date(r[7]) if len(r) > 7 else ""
            item["qtd"] = _num(r[8]) if len(r) > 8 else 0.0
            item["valor_liquido"] = _money(r[12]) if len(r) > 12 else posicao
        elif current_section == "Fundos de Investimentos":
            # ['descr', 'Posição', '%', 'Rent.Liq', 'Rent.Bruta', 'Valor aplicado', 'Valor líquido']
            item["valor_aplicado"] = _money(r[5]) if len(r) > 5 else 0.0
            item["valor_liquido"] = _money(r[6]) if len(r) > 6 else posicao
            item["qtd"] = 0.0  # not provided in Fundos section

        holdings.append(item)

    return snap, holdings


def main() -> None:
    files = sorted(SRC_DIR.glob("*.xlsx"))
    snapshots: list[dict] = []
    all_holdings: list[dict] = []
    for f in files:
        snap, holds = parse_one(f)
        snapshots.append(snap)
        all_holdings.extend(holds)

    snapshots.sort(key=lambda x: x["data"])
    all_holdings.sort(key=lambda x: (x["data"], x["categoria"], x["descricao"]))

    # Write snapshots
    with OUT_SNAPSHOTS.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["data", "patrimonio", "arquivo"])
        w.writeheader()
        for s in snapshots:
            w.writerow(s)

    # Write holdings
    fields = [
        "data",
        "categoria",
        "descricao",
        "posicao",
        "qtd",
        "valor_aplicado",
        "valor_liquido",
        "data_aplicacao",
        "vencimento",
        "arquivo",
    ]
    with OUT_HOLDINGS.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for h in all_holdings:
            w.writerow(h)

    print(f"Wrote {len(snapshots)} snapshots to {OUT_SNAPSHOTS}")
    print(f"Wrote {len(all_holdings)} holdings to {OUT_HOLDINGS}")
    print()
    print("=== PATRIMÔNIO POR DATA ===")
    for s in snapshots:
        print(f"  {s['data']}  R$ {s['patrimonio']:>12,.2f}")
    print()
    # Distinct holdings by descricao
    distinct = {h["descricao"] for h in all_holdings}
    print(f"=== ATIVOS ÚNICOS: {len(distinct)} ===")
    for d in sorted(distinct):
        print(f"  {d}")


if __name__ == "__main__":
    main()
