"""
net_positions.py -- v2 (Excel-based ledger)

Calcula posicao a partir do ledger_b3.csv gerado pelo parse_b3_excel.py.
Schema do ledger:
    data, ticker, nome, categoria, tipo_b3, entrada_saida,
    quantidade, preco, valor, instituicao, arquivo

Categorias:
    BUY, SELL, INCOME, PORTABILITY_IN, PORTABILITY_OUT,
    BONUS, SPLIT, FEE, IGNORE

Saida:
    - tabela no terminal
    - positions.csv com status por ativo

Status possiveis:
    IN              -- comprado e ainda em carteira
    IN_PORT         -- so portabilidade liquida positiva (verificar qty)
    SOLD            -- vendido completamente no periodo
    SOLD_PRE_PERIOD -- vendido mas comprado antes do extrato (pre-2019)
    ERROR           -- inconsistencia que precisa investigacao

Uso:
    python net_positions.py
    python net_positions.py --ledger ledger_b3.csv --output positions.csv --all
"""
from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass


# --- normalizacao -----------------------------------------------------------
TICKER_RE = re.compile(r'^[A-Z]{4}\d{1,2}[A-Z]?$')


def normalize_key(ticker: str, nome: str) -> tuple[str, str]:
    """Retorna (key, label) para agrupar BUY/SELL do mesmo ativo.

    O parser ja extrai ticker limpo (BOVA11, CDB320AATJ3, ou
    'Tesouro Selic 2025') no campo ticker. Confiamos nele.
    """
    t = (ticker or '').strip()
    n = (nome or '').strip()
    if t:
        return t, t if t.lower().startswith('tesouro') else (t)
    # Fallback: nome
    return n[:60] or 'UNKNOWN', n[:60] or 'UNKNOWN'


# --- acumulador de posicao --------------------------------------------------

@dataclass
class Position:
    key: str
    label: str
    nome_full: str = ''
    buy_qty: float = 0.0
    sell_qty: float = 0.0
    buy_value: float = 0.0
    sell_value: float = 0.0
    income_value: float = 0.0
    fee_value: float = 0.0
    bonus_qty: float = 0.0
    split_net_qty: float = 0.0  # split can be + or -
    port_in_qty: float = 0.0
    port_out_qty: float = 0.0
    first_buy_date: str = ''
    last_buy_date: str = ''
    first_sell_date: str = ''
    last_sell_date: str = ''
    buy_count: int = 0
    sell_count: int = 0
    income_count: int = 0

    @property
    def gross_in_qty(self) -> float:
        """All quantity coming in: BUY + BONUS + SPLIT(+).

        PORT_IN is NOT counted: in this user's data all 'Transferência'
        events are intra-broker pair (in == out same day, same qty),
        so they have no net effect on quantity.
        """
        return self.buy_qty + self.bonus_qty + max(self.split_net_qty, 0)

    @property
    def gross_out_qty(self) -> float:
        return self.sell_qty + max(-self.split_net_qty, 0)

    @property
    def port_balance(self) -> float:
        """Should be ≈0 if all portability events are paired."""
        return self.port_in_qty - self.port_out_qty

    @property
    def net_qty(self) -> float:
        # If portability is imbalanced (e.g. real broker-to-broker
        # transfer), include it. Otherwise it's just intra-broker noise.
        net = self.gross_in_qty - self.gross_out_qty
        if abs(self.port_balance) > 0.01:
            net += self.port_balance
        return net

    @property
    def avg_buy_price(self) -> float:
        return self.buy_value / self.buy_qty if self.buy_qty > 0 else 0.0

    @property
    def status(self) -> str:
        net = self.net_qty
        port_net = self.port_in_qty - self.port_out_qty
        has_buy = self.buy_qty > 0.01
        has_port = self.port_in_qty > 0.01 or self.port_out_qty > 0.01

        if net > 0.01:
            # Em carteira. Se NUNCA houve BUY real e so portabilidade,
            # marca como IN_PORT (precisa cost basis externo).
            if not has_buy and has_port:
                return 'IN_PORT'
            return 'IN'

        if net < -0.01:
            # SELL > entradas. Pode ser:
            # 1) Artefato: sell_value insignificante vs buy_value
            if self.buy_value > 100 and self.sell_value < self.buy_value * 0.05:
                return 'IN'
            # 2) Pre-period: vendido antes de comprar
            if (self.first_sell_date and self.first_buy_date
                    and self.first_sell_date < self.first_buy_date):
                return 'SOLD_PRE_PERIOD'
            # 3) Pre-period: nunca houve BUY no extrato
            if not has_buy:
                return 'SOLD_PRE_PERIOD'
            # 4) Default conservador
            return 'SOLD_PRE_PERIOD'

        # net ~ 0
        return 'SOLD'

    @property
    def display_qty(self) -> float:
        """Qty efetiva para mostrar/importar."""
        if self.status == 'IN_PORT':
            return self.port_in_qty - self.port_out_qty
        if self.status == 'IN':
            # Se SELL e artefato (valor infimo), usa gross_in
            if (self.buy_value > 100
                    and self.sell_value < self.buy_value * 0.05
                    and self.net_qty < self.buy_qty):
                return self.gross_in_qty
            return self.net_qty
        return self.net_qty


# --- leitura ----------------------------------------------------------------

def load_positions(ledger_path: str) -> dict[str, Position]:
    positions: dict[str, Position] = {}

    with open(ledger_path, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            cat = row['categoria']
            if cat in ('IGNORE', 'UNKNOWN'):
                continue

            key, label = normalize_key(row.get('ticker', ''), row.get('nome', ''))
            pos = positions.get(key)
            if pos is None:
                pos = Position(key=key, label=label, nome_full=row.get('nome', ''))
                positions[key] = pos
            elif not pos.nome_full:
                pos.nome_full = row.get('nome', '')

            try:
                qty = float(row['quantidade'] or 0)
            except ValueError:
                qty = 0.0
            try:
                val = float(row['valor'] or 0)
            except ValueError:
                val = 0.0
            d = row['data']

            if cat == 'BUY':
                pos.buy_qty += qty
                pos.buy_value += abs(val)
                pos.buy_count += 1
                if not pos.first_buy_date or d < pos.first_buy_date:
                    pos.first_buy_date = d
                if not pos.last_buy_date or d > pos.last_buy_date:
                    pos.last_buy_date = d
            elif cat == 'SELL':
                pos.sell_qty += qty
                pos.sell_value += abs(val)
                pos.sell_count += 1
                if not pos.first_sell_date or d < pos.first_sell_date:
                    pos.first_sell_date = d
                if not pos.last_sell_date or d > pos.last_sell_date:
                    pos.last_sell_date = d
            elif cat == 'INCOME':
                pos.income_value += abs(val)
                pos.income_count += 1
            elif cat == 'FEE':
                pos.fee_value += abs(val)
            elif cat == 'PORTABILITY_IN':
                pos.port_in_qty += qty
            elif cat == 'PORTABILITY_OUT':
                pos.port_out_qty += qty
            elif cat == 'BONUS':
                pos.bonus_qty += qty
            elif cat == 'SPLIT':
                # Direction by entrada_saida: C adds, D subtracts
                if row.get('entrada_saida', 'C').upper().startswith('C'):
                    pos.split_net_qty += qty
                else:
                    pos.split_net_qty -= qty

    return positions


# --- CSV de saida -----------------------------------------------------------

def _suggest_valuation(key: str, nome_full: str) -> tuple[str, str]:
    if TICKER_RE.match(key):
        return 'market_price', key + '.SA'
    if key.lower().startswith('tesouro'):
        return 'tesouro', key
    if 'cdb' in (nome_full or '').lower() or key.startswith('CDB'):
        return 'fixed_income', ''
    if any(prefix in key for prefix in ('LCI', 'LCA', 'CRI', 'CRA', 'DEB')):
        return 'fixed_income', ''
    return 'manual', ''


def write_csv(positions: dict[str, Position], output_path: str) -> None:
    header = [
        'key', 'nome', 'status',
        'qty_display', 'qty_buy', 'qty_sell',
        'qty_port_in', 'qty_port_out', 'qty_bonus', 'qty_split_net',
        'avg_buy_price_brl', 'buy_value_total_brl',
        'sell_value_total_brl', 'income_value_brl',
        'first_buy', 'last_buy', 'first_sell', 'last_sell',
        'valuation_method', 'ticker_yahoo',
        'nota',
    ]
    order = {'IN': 0, 'IN_PORT': 1, 'ERROR': 2, 'SOLD': 3, 'SOLD_PRE_PERIOD': 4}
    rows = sorted(positions.values(), key=lambda p: (order.get(p.status, 9), p.key))

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(header)
        for p in rows:
            method, ticker_yahoo = _suggest_valuation(p.key, p.nome_full)
            w.writerow([
                p.key, p.nome_full or p.label, p.status,
                f"{p.display_qty:.6f}", f"{p.buy_qty:.6f}", f"{p.sell_qty:.6f}",
                f"{p.port_in_qty:.6f}", f"{p.port_out_qty:.6f}",
                f"{p.bonus_qty:.6f}", f"{p.split_net_qty:.6f}",
                f"{p.avg_buy_price:.4f}", f"{p.buy_value:.2f}",
                f"{p.sell_value:.2f}", f"{p.income_value:.2f}",
                p.first_buy_date, p.last_buy_date,
                p.first_sell_date, p.last_sell_date,
                method, ticker_yahoo, '',
            ])


# --- main -------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--ledger', default='ledger_b3.csv')
    ap.add_argument('--output', default='positions.csv')
    ap.add_argument('--all', action='store_true')
    args = ap.parse_args()

    positions = load_positions(args.ledger)

    by_status = {'IN': [], 'IN_PORT': [], 'SOLD': [], 'SOLD_PRE_PERIOD': [], 'ERROR': []}
    for p in sorted(positions.values(), key=lambda x: x.key):
        by_status.setdefault(p.status, []).append(p)

    SEP = '=' * 90

    print()
    print(SEP)
    print(f'  EM CARTEIRA -- {len(by_status["IN"])} ativo(s)')
    print(SEP)
    print(f'  {"ATIVO":<36} {"QTY":>14}  {"PRECO MEDIO":>14}  {"INVESTIDO":>14}  PRIM. COMPRA')
    print(f'  {"-"*36} {"-"*14}  {"-"*14}  {"-"*14}')
    for p in by_status['IN']:
        avg = f'R$ {p.avg_buy_price:>10,.2f}' if p.avg_buy_price > 0 else '--'
        inv = f'R$ {p.buy_value:>11,.2f}' if p.buy_value > 0 else '--'
        print(f'  {p.label[:36]:<36} {p.display_qty:>14.4f}  {avg:<14}  {inv:<14}  {p.first_buy_date}')

    if by_status['IN_PORT']:
        print()
        print(SEP)
        print(f'  IN_PORT (so portabilidade -- precisa cost basis externo) -- '
              f'{len(by_status["IN_PORT"])} ativo(s)')
        print(SEP)
        for p in by_status['IN_PORT']:
            print(f'  {p.label[:36]:<36} qty_net={p.display_qty:>10.4f}  '
                  f'(in={p.port_in_qty:.2f} out={p.port_out_qty:.2f})')

    if by_status['SOLD_PRE_PERIOD']:
        print()
        print(SEP)
        print(f'  RESGATADOS PRE-PERIODO ({len(by_status["SOLD_PRE_PERIOD"])}) '
              f'-- comprados antes do extrato B3')
        print(SEP)
        total = 0.0
        for p in by_status['SOLD_PRE_PERIOD']:
            total += p.sell_value
            print(f'  {p.label[:50]:<50} resgatado=R$ {p.sell_value:>11,.2f}'
                  f'  ({p.sell_count} op(s))')
        print(f'  {"":50} {"-"*30}')
        print(f'  {"TOTAL":50} R$ {total:>11,.2f}')

    if by_status['ERROR']:
        print()
        print(SEP)
        print(f'  ERROS ({len(by_status["ERROR"])})')
        print(SEP)
        for p in by_status['ERROR']:
            print(f'  {p.label}: net={p.net_qty:+.4f} buy={p.buy_qty} sell={p.sell_qty}')

    if args.all:
        print()
        print(f'  VENDIDOS NORMAIS ({len(by_status["SOLD"])}):')
        for p in by_status['SOLD']:
            print(f'    {p.label}')

    write_csv(positions, args.output)
    print()
    print(SEP)
    print(f'OK {args.output} -- IN={len(by_status["IN"])}  '
          f'IN_PORT={len(by_status["IN_PORT"])}  '
          f'SOLD={len(by_status["SOLD"])}  '
          f'SOLD_PRE={len(by_status["SOLD_PRE_PERIOD"])}  '
          f'ERROR={len(by_status["ERROR"])}')
    print()


if __name__ == '__main__':
    main()
