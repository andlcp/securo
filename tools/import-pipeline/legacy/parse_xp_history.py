"""
parse_xp_history.py
Lê os relatórios mensais de Posição Consolidada da XP e extrai
(data, patrimônio_total) de cada arquivo PDF.

Uso:
    pip install pdfplumber
    python parse_xp_history.py --pasta ./relatorios_xp
    python parse_xp_history.py --pasta ./relatorios_xp --output meu_historico.csv
"""

import argparse
import csv
import os
import re
import sys
from datetime import datetime

try:
    import pdfplumber
except ImportError:
    print("Dependência ausente. Instale com:")
    print("    pip install pdfplumber")
    sys.exit(1)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def parse_br_money(s: str) -> float:
    """'R$ 524.379,99' ou '524.379,99' → 524379.99"""
    s = re.sub(r'R\$\s*', '', s).strip()
    s = s.replace('.', '').replace(',', '.')
    return float(s)


def extract_date_from_filename(filename: str):
    """historico_31_01_2026.pdf → date(2026, 1, 31)"""
    m = re.search(r'(\d{2})_(\d{2})_(\d{4})', filename)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime(y, mo, d).date()
        except ValueError:
            return None
    return None


def extract_date_from_text(text: str):
    """'Data de referência: 31/01/2026' → date"""
    m = re.search(r'Data de referência[:\s]+(\d{2}/\d{2}/\d{4})', text, re.IGNORECASE)
    if m:
        try:
            return datetime.strptime(m.group(1), '%d/%m/%Y').date()
        except ValueError:
            return None
    return None


def extract_total(text: str):
    """'PATRIMÔNIO TOTAL   R$ 524.379,99' → 524379.99"""
    m = re.search(r'PATRIMÔNIO TOTAL\s+R\$\s*([\d.,]+)', text, re.IGNORECASE)
    if m:
        try:
            return parse_br_money(m.group(1))
        except ValueError:
            return None
    return None


# ─── PDF processing ───────────────────────────────────────────────────────────

def process_pdf(filepath: str):
    filename = os.path.basename(filepath)
    try:
        with pdfplumber.open(filepath) as pdf:
            full_text = '\n'.join(page.extract_text() or '' for page in pdf.pages)
    except Exception as e:
        print(f"  ERRO ao ler {filename}: {e}")
        return None, None

    date = extract_date_from_text(full_text) or extract_date_from_filename(filename)
    total = extract_total(full_text)
    return date, total


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Extrai histórico de patrimônio dos PDFs da XP")
    parser.add_argument('--pasta',  default='.', help='Pasta com os PDFs (padrão: diretório atual)')
    parser.add_argument('--output', default='historico_portfolio.csv', help='Arquivo CSV de saída')
    args = parser.parse_args()

    pdfs = sorted(f for f in os.listdir(args.pasta) if f.lower().endswith('.pdf'))
    if not pdfs:
        print(f"Nenhum PDF encontrado em: {args.pasta}")
        sys.exit(1)

    print(f"Encontrei {len(pdfs)} PDFs em '{args.pasta}'\n")

    rows = []
    erros = []

    for filename in pdfs:
        filepath = os.path.join(args.pasta, filename)
        date, total = process_pdf(filepath)

        if date and total is not None:
            rows.append({'data': str(date), 'patrimonio_total': round(total, 2)})
            print(f"  ✓  {date}    R$ {total:>14,.2f}")
        else:
            erros.append(filename)
            motivo = []
            if not date:
                motivo.append("data não encontrada")
            if total is None:
                motivo.append("patrimônio não encontrado")
            print(f"  ✗  {filename}  →  {', '.join(motivo)}")

    if not rows:
        print("\nNenhum dado extraído. Verifique os arquivos.")
        sys.exit(1)

    # Ordena por data
    rows.sort(key=lambda r: r['data'])

    with open(args.output, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['data', 'patrimonio_total'])
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n{'─'*50}")
    print(f"✓ {len(rows)} meses exportados → {args.output}")

    if erros:
        print(f"✗ {len(erros)} arquivo(s) com erro: {', '.join(erros)}")

    # Resumo rápido
    if len(rows) >= 2:
        primeiro = rows[0]
        ultimo = rows[-1]
        retorno = (ultimo['patrimonio_total'] - primeiro['patrimonio_total']) / primeiro['patrimonio_total'] * 100
        print(f"\nPeríodo: {primeiro['data']}  →  {ultimo['data']}")
        print(f"Patrimônio inicial : R$ {primeiro['patrimonio_total']:>12,.2f}")
        print(f"Patrimônio final   : R$ {ultimo['patrimonio_total']:>12,.2f}")
        print(f"Variação total     : {retorno:>+.1f}%  (inclui aportes)")


if __name__ == '__main__':
    main()
