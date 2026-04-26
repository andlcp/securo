"""
parse_b3_history.py — v2
Lê Extratos de Movimentação anuais da B3 (investidor.b3.com.br).

O PDF da B3 não tem tabelas detectáveis — apenas texto plano com datas no
formato "22 de janeiro de 2020" e colunas achatadas numa linha só ou
quebradas em 2-3 linhas. Este parser usa uma máquina de estados.

Uso:
    pip install pdfplumber
    python parse_b3_history.py --pasta ./relatorios_b3
    python parse_b3_history.py --pasta ./relatorios_b3 --output ledger_b3.csv --verbose
    python parse_b3_history.py --pasta ./relatorios_b3 --debug   (diagnóstico)
"""

import argparse
import csv
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

try:
    import pdfplumber
except ImportError:
    print("Dependência ausente. Instale com:")
    print("    pip install pdfplumber")
    sys.exit(1)


# ─── Data model ───────────────────────────────────────────────────────────────

@dataclass
class Transaction:
    date: str
    tipo_b3: str
    categoria: str
    produto: str
    quantidade: float
    preco: float
    valor: float

    def to_dict(self):
        return {
            'data': self.date,
            'tipo_b3': self.tipo_b3,
            'categoria': self.categoria,
            'produto': self.produto,
            'quantidade': self.quantidade,
            'preco': self.preco,
            'valor': self.valor,
        }


# ─── Constants ────────────────────────────────────────────────────────────────

MESES = {
    'janeiro': 1, 'fevereiro': 2, 'marco': 3, 'março': 3,
    'abril': 4, 'maio': 5, 'junho': 6, 'julho': 7,
    'agosto': 8, 'setembro': 9, 'outubro': 10,
    'novembro': 11, 'dezembro': 12,
}

DATE_LONG_RE = re.compile(
    r'^(\d{1,2})\s+de\s+'
    r'(janeiro|fevereiro|mar[cç]o|abril|maio|junho|julho|agosto|setembro|outubro|novembro|dezembro)'
    r'\s+de\s+(\d{4})$',
    re.IGNORECASE,
)

MONEY_RE = re.compile(r'R\$\s*([\d.]+,\d+|-)')

# At least 2 of these words → column header line (skip)
HEADER_WORDS = {
    'movimentação', 'movimentacao', 'produto', 'instituição', 'instituicao',
    'unitário', 'unitario', 'operação', 'operacao',
}

# Ordered longest-first so greedy matching works
KNOWN_TIPOS = [
    'Transferência Sem Financeiro',
    'Transferencia Sem Financeiro',
    'Transferência - Liquidação',
    'Transferencia - Liquidacao',
    'Cobrança de Taxa',
    'Cobranca de Taxa',
    'Juros Sobre Capital Próprio',
    'Juros Sobre Capital Proprio',
    'Juros Sobre Capital',
    'Lançamento de Oferta',
    'Lancamento de Oferta',
    'Bonificação em Ativos',
    'Bonificacao em Ativos',
    'Aplicação',
    'Aplicacao',
    'Amortização',
    'Amortizacao',
    'Atualização',
    'Atualizacao',
    'Desdobramento',
    'Grupamento',
    'Dividendo',
    'Rendimento',
    'Compra',
    'Venda',
    'Resgate',
    'Crédito',
    'Credito',
    'Débito',
    'Debito',
]

TIPO_STARTERS = {t.lower().split()[0] for t in KNOWN_TIPOS}

_TIPO_MAP = [
    (['compra', 'aplicação', 'aplicacao', 'crédito', 'credito', 'deposito',
      'leilão', 'leilao',
      'transferência - liquidação', 'transferencia - liquidacao'], 'BUY'),
    (['venda', 'resgate', 'débito', 'debito', 'retirada', 'vencimento'], 'SELL'),
    # Dividendos e JCP são caixa externo que entra na conta — impactam TWR
    (['dividendo', 'juros sobre capital', 'pagamento', 'reembolso'], 'INCOME'),
    # Tudo que NÃO é fluxo de caixa externo: taxas, eventos corporativos,
    # portabilidade, bloqueios, artefatos do PDF
    (['transferência sem financeiro', 'transferencia sem financeiro',
      'transferência', 'transferencia', 'transferencia',
      'cobrança', 'cobranca',      # taxa de custódia — já no valor do ativo
      'juros',                      # juros embutidos no preço (não JCP)
      'rendimento',                 # rendimento já no NAV do fundo
      'amortização', 'amortizacao', # reduz principal, já no preço
      'lançamento', 'lancamento', 'desdobramento', 'desdobro', 'grupamento',
      'bonificação', 'bonificacao', 'atualização', 'atualizacao',
      'direito', 'direitos', 'cessão', 'cessao',
      'bloqueio', 'fração', 'fracao', 'empréstimo', 'emprestimo',
      'unitário', 'unitario', 'acesse'], 'IGNORE'),
]


# ─── Helpers ──────────────────────────────────────────────────────────────────

def classify(tipo: str) -> str:
    t = tipo.lower()
    for keywords, cat in _TIPO_MAP:
        if any(k in t for k in keywords):
            return cat
    return 'UNKNOWN'


def parse_br_money(s: str) -> float:
    s = re.sub(r'[R$\s]', '', s).strip()
    if not s or s == '-':
        return 0.0
    s = s.replace('.', '').replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return 0.0


def normalize_ws(s: str) -> str:
    return re.sub(r'\s+', ' ', s or '').strip()


def parse_long_date(line: str) -> Optional[str]:
    """'22 de janeiro de 2020' → '2020-01-22'"""
    m = DATE_LONG_RE.match(line.strip())
    if not m:
        return None
    day = int(m.group(1))
    mes_raw = m.group(2).lower().replace('ç', 'c')
    month = MESES.get(mes_raw, 0)
    year = int(m.group(3))
    if not month:
        return None
    try:
        return datetime(year, month, day).strftime('%Y-%m-%d')
    except ValueError:
        return None


def is_header_line(line: str) -> bool:
    words = set(normalize_ws(line).lower().split())
    return len(words & HEADER_WORDS) >= 2


def starts_with_tipo(line: str) -> bool:
    words = normalize_ws(line).lower().split()
    return bool(words) and words[0] in TIPO_STARTERS


def _extract_tipo(text: str) -> str:
    t_low = text.lower()
    best = ''
    for tipo in KNOWN_TIPOS:
        if t_low.startswith(tipo.lower()) and len(tipo) > len(best):
            best = tipo
    if not best:
        best = text.split()[0] if text.split() else ''
    return best


def _is_numeric_token(w: str) -> bool:
    """Aceita formatos: 100, 1.000, 1,15, 1.000,50 mas rejeita anos (1900-2099)."""
    if not re.fullmatch(r'\d[\d.,]*', w):
        return False
    # Rejeita anos isolados (4 dígitos começando com 19/20/21) — comum em nomes
    # de Tesouro ("Tesouro Selic 2025") e atrapalha extração de qty
    if re.fullmatch(r'(19|20|21)\d{2}', w):
        return False
    return True


def parse_transaction(buffer: list[str], current_date: str) -> Optional[Transaction]:
    full = ' '.join(buffer)

    money_matches = list(MONEY_RE.finditer(full))
    if not money_matches:
        return None

    valor = parse_br_money(money_matches[-1].group(1))
    preco = parse_br_money(money_matches[-2].group(1)) if len(money_matches) >= 2 else 0.0

    pre_money = full[:money_matches[0].start()].strip()
    tipo = _extract_tipo(pre_money)
    rest = pre_money[len(tipo):].strip()

    # Last numeric token in rest = quantidade
    rest_words = rest.split()
    quantidade = 0.0
    produto_end = len(rest_words)
    for i in range(len(rest_words) - 1, -1, -1):
        w = rest_words[i]
        if _is_numeric_token(w) or w == '0':
            try:
                quantidade = parse_br_money(w)
                produto_end = i
                break
            except Exception:
                pass

    produto = ' '.join(rest_words[:produto_end])

    categoria = classify(tipo)

    # FIX: override por keyword em CAIXA ALTA. Nos extratos B3, quando múltiplas
    # operações se acumulam no buffer, a palavra "COMPRA" ou "VENDA" em caixa
    # alta no meio da linha é indicador forte de tipo real da operação — mais
    # confiável do que o primeiro tipo detectado (que pode ser de outra op).
    if re.search(r'\bCOMPRA\b', full) and categoria != 'BUY':
        categoria = 'BUY'
        tipo = 'Compra'
    elif re.search(r'\bVENDA\b', full) and categoria != 'SELL':
        categoria = 'SELL'
        tipo = 'Venda'

    # FIX: se qty não foi extraído do nome mas temos val e preco, derivar
    if quantidade == 0 and preco > 0 and abs(valor) > 0:
        quantidade = round(abs(valor) / preco, 6)

    # FIX: val≈0 em BUY/SELL é portabilidade de custódia ou artefato do PDF,
    # não um fluxo de caixa real. Reclassifica como PORTABILITY para rastrear
    # o ativo sem contaminar os cálculos financeiros.
    if categoria in ('BUY', 'SELL') and abs(valor) < 1.0:
        categoria = 'PORTABILITY'

    if categoria == 'BUY':
        valor = -abs(valor)
    elif categoria in ('SELL', 'INCOME'):
        valor = abs(valor)

    if not tipo:
        return None

    return Transaction(
        date=current_date,
        tipo_b3=tipo,
        categoria=categoria,
        produto=produto,
        quantidade=quantidade,
        preco=preco,
        valor=valor,
    )


# ─── PDF extraction ───────────────────────────────────────────────────────────

def extract_transactions_from_pdf(filepath: str) -> list[Transaction]:
    try:
        with pdfplumber.open(filepath) as pdf:
            full_text = '\n'.join(page.extract_text() or '' for page in pdf.pages)
    except Exception as e:
        print(f"  ERRO ao ler {os.path.basename(filepath)}: {e}")
        return []

    return _parse_text(full_text)


def _parse_text(text: str) -> list[Transaction]:
    transactions: list[Transaction] = []
    current_date: Optional[str] = None
    buffer: list[str] = []

    def flush():
        nonlocal buffer
        if buffer:
            full = ' '.join(buffer)
            if 'R$' in full:
                txn = parse_transaction(buffer, current_date)
                if txn:
                    transactions.append(txn)
            buffer = []

    for raw_line in text.splitlines():
        line = normalize_ws(raw_line)
        if not line:
            continue

        # Date header?
        d = parse_long_date(line)
        if d:
            flush()
            current_date = d
            continue

        # Skip everything before the first date
        if current_date is None:
            continue

        # Column header line?
        if is_header_line(line):
            flush()
            continue

        # Bare number = page number, skip
        if re.match(r'^\d+$', line):
            continue

        # New transaction starting while buffer already has R$ values?
        if starts_with_tipo(line) and buffer and any('R$' in b for b in buffer):
            flush()

        buffer.append(line)

    flush()
    return transactions


# ─── Debug ────────────────────────────────────────────────────────────────────

def debug_pdf(filepath: str):
    print(f"\n{'='*60}")
    print(f"DEBUG: {os.path.basename(filepath)}")
    print(f"{'='*60}")
    with pdfplumber.open(filepath) as pdf:
        print(f"Páginas: {len(pdf.pages)}")
        for i, page in enumerate(pdf.pages[:3], 1):
            text = page.extract_text() or ''
            print(f"\n--- Página {i} (primeiros 1200 chars) ---")
            print(text[:1200])

    print(f"\n--- Transações extraídas ---")
    txns = extract_transactions_from_pdf(filepath)
    for t in txns:
        print(f"  {t.date}  {t.categoria:<8}  {t.tipo_b3:<35}  R$ {t.valor:>12,.2f}  {t.produto[:40]}")
    print(f"\nTotal: {len(txns)} transações")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Extrai ledger de transações dos Extratos B3")
    parser.add_argument('--pasta', default='.', help='Pasta com os PDFs')
    parser.add_argument('--output', default='ledger_b3.csv', help='CSV de saída')
    parser.add_argument('--verbose', action='store_true', help='Mostra cada transação')
    parser.add_argument('--debug', action='store_true', help='Diagnóstico do primeiro PDF')
    args = parser.parse_args()

    pdfs = sorted(f for f in os.listdir(args.pasta) if f.lower().endswith('.pdf'))
    if not pdfs:
        print(f"Nenhum PDF encontrado em: {args.pasta}")
        sys.exit(1)

    print(f"Encontrei {len(pdfs)} PDFs em '{args.pasta}'\n")

    if args.debug:
        debug_pdf(os.path.join(args.pasta, pdfs[0]))
        sys.exit(0)

    all_transactions: list[Transaction] = []
    stats: dict[str, int] = {}

    for filename in pdfs:
        filepath = os.path.join(args.pasta, filename)
        print(f"  Processando {filename}...")
        txns = extract_transactions_from_pdf(filepath)
        all_transactions.extend(txns)
        for t in txns:
            stats[t.categoria] = stats.get(t.categoria, 0) + 1
        print(f"    → {len(txns)} transações")

    # Deduplicar
    seen: set = set()
    unique: list[Transaction] = []
    for t in all_transactions:
        key = (t.date, t.tipo_b3.lower(), t.produto, t.quantidade, abs(t.valor))
        if key not in seen:
            seen.add(key)
            unique.append(t)

    unique.sort(key=lambda t: t.date)

    if not unique:
        print("\nNenhuma transação extraída. Use --debug para diagnosticar.")
        sys.exit(1)

    fieldnames = ['data', 'tipo_b3', 'categoria', 'produto', 'quantidade', 'preco', 'valor']
    with open(args.output, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(t.to_dict() for t in unique)

    print(f"\n{'─'*60}")
    print(f"✓ {len(unique)} transações exportadas → {args.output}")
    print(f"\nPor categoria:")
    for cat, count in sorted(stats.items()):
        if cat == 'UNKNOWN':
            marker = '?'
        elif cat in ('IGNORE', 'PORTABILITY'):
            marker = '~'
        else:
            marker = '✓'
        print(f"  {marker}  {cat:<12} {count:>4}")

    unknowns = sorted({t.tipo_b3 for t in unique if t.categoria == 'UNKNOWN'})
    if unknowns:
        print(f"\nTipos não classificados:")
        for u in unknowns:
            print(f"    • {u}")

    if args.verbose:
        print(f"\n{'DATA':<12} {'CAT':<8} {'VALOR':>14}  PRODUTO / TIPO")
        print('─' * 70)
        for t in unique:
            if t.categoria == 'IGNORE':
                continue
            print(f"{t.date:<12} {t.categoria:<8} R$ {t.valor:>12,.2f}  "
                  f"{t.produto[:30]}  [{t.tipo_b3}]")


if __name__ == '__main__':
    main()
