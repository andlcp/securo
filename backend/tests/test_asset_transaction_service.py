"""Posição derivada do histórico de lançamentos.

Herdado do upstream (issue #235), reduzido e estendido no merge.

O que saiu: 12 testes do CRUD de compra/venda deles (`add_transaction`,
`update_transaction`, `delete_transaction`, `buy_into_holding`). Aquele
CRUD não entrou no fork — ele fala um histórico de dois eventos, `buy` e
`sell`, e aqui são dez. Quem serve transações neste fork é
`api/asset_transactions.py` e `/assets/{id}/transactions`.

O que ficou: os seis testes da média ponderada, que cobrem `_recompute` —
a peça mais delicada do merge, porque ela reescreve a quantidade de cotas
gravada no ativo.

O que entrou: cobertura dos tipos que só existem aqui. A versão original
do upstream, rodando sobre este histórico, zeraria posição — as 256 cotas
do desdobramento da SBSP3 e as 4 da bonificação da RENT4 entram como
DEPOSIT, que ela ignorava.
"""
import uuid
from datetime import date
from decimal import Decimal
from typing import Optional

from app.models.asset_transaction import AssetTransaction
from app.services.asset_transaction_service import _recompute


def _tx(tipo: str, qty: Optional[str], price: Optional[str], d: date,
        fees: str = "0", value: Optional[str] = None) -> AssetTransaction:
    return AssetTransaction(
        id=uuid.uuid4(), user_id=uuid.uuid4(), asset_id=uuid.uuid4(),
        workspace_id=uuid.uuid4(),
        type=tipo,
        qty=Decimal(qty) if qty is not None else None,
        price=Decimal(price) if price is not None else None,
        value=Decimal(value) if value is not None else None,
        fees=Decimal(fees),
        date=d,
    )


# ---------------------------------------------------------------------------
# Média ponderada (preço médio) — herdado do upstream
# ---------------------------------------------------------------------------
def test_recompute_weighted_average_across_buys():
    # 10 @ 100 depois 5 @ 110 -> média = (1000 + 550) / 15 = 103,3333...
    pos = _recompute([
        _tx("BUY", "10", "100", date(2026, 1, 1)),
        _tx("BUY", "5", "110", date(2026, 2, 1)),
    ])
    assert pos["units"] == Decimal("15")
    assert pos["average_price"].quantize(Decimal("0.0001")) == Decimal("103.3333")
    assert pos["cost_basis"] == Decimal("1550")
    assert pos["realized_gain"] == Decimal("0")


def test_recompute_partial_sell_keeps_average_and_realizes():
    # compra 10 @ 100, compra 5 @ 110 (média 103,3333), vende 6 @ 130
    # realizado = (130 - 103,3333) * 6 ≈ 160; a média por cota não muda
    pos = _recompute([
        _tx("BUY", "10", "100", date(2026, 1, 1)),
        _tx("BUY", "5", "110", date(2026, 1, 2)),
        _tx("SELL", "6", "130", date(2026, 3, 1)),
    ])
    assert pos["units"] == Decimal("9")
    assert pos["average_price"].quantize(Decimal("0.0001")) == Decimal("103.3333")
    assert pos["realized_gain"].quantize(Decimal("0.01")) == Decimal("160.00")


def test_recompute_sell_all_flattens_position():
    pos = _recompute([
        _tx("BUY", "10", "100", date(2026, 1, 1)),
        _tx("SELL", "10", "120", date(2026, 2, 1)),
    ])
    assert pos["units"] == Decimal("0")
    assert pos["average_price"] is None
    assert pos["cost_basis"] == Decimal("0")
    assert pos["realized_gain"].quantize(Decimal("0.01")) == Decimal("200.00")


def test_recompute_includes_fees_in_cost_basis():
    # compra 10 @ 100 com 9,90 de taxa -> custo 1009,90, média 100,99
    pos = _recompute([_tx("BUY", "10", "100", date(2026, 1, 1), fees="9.90")])
    assert pos["cost_basis"] == Decimal("1009.90")
    assert pos["average_price"].quantize(Decimal("0.0001")) == Decimal("100.9900")


def test_recompute_clamps_oversell():
    # Vender mais do que se tem não pode deixar a quantidade negativa.
    pos = _recompute([
        _tx("BUY", "5", "100", date(2026, 1, 1)),
        _tx("SELL", "10", "120", date(2026, 2, 1)),
    ])
    assert pos["units"] == Decimal("0")
    assert pos["average_price"] is None


def test_recompute_orders_by_date_not_insertion():
    # Uma compra retroativa tem de ser processada primeiro.
    pos = _recompute([
        _tx("SELL", "5", "130", date(2026, 3, 1)),
        _tx("BUY", "10", "100", date(2026, 1, 1)),
    ])
    assert pos["units"] == Decimal("5")
    assert pos["average_price"].quantize(Decimal("0.01")) == Decimal("100.00")


# ---------------------------------------------------------------------------
# Os tipos que só existem no fork
# ---------------------------------------------------------------------------
def test_recompute_deposit_adds_units_at_zero_cost():
    """Bonificação e desdobramento entram como DEPOSIT: trazem cotas sem
    custo. É o caso que a versão original do upstream perdia."""
    pos = _recompute([
        _tx("BUY", "64", "116.65", date(2025, 8, 18)),
        _tx("DEPOSIT", "256", None, date(2026, 4, 28), value="0"),
    ])
    # SBSP3: 64 compradas + 256 do desdobramento 5:1 = 320.
    assert pos["units"] == Decimal("320")
    # O custo não muda; a média por cota cai na proporção do desdobramento.
    assert pos["cost_basis"] == Decimal("7465.60")
    assert pos["average_price"].quantize(Decimal("0.0001")) == Decimal("23.3300")


def test_recompute_deposit_only_position_is_not_wiped():
    """RENT4 veio inteira de bonificação da RENT3: sem compra, só DEPOSIT.
    A versão do upstream devolveria zero cota."""
    pos = _recompute([
        _tx("DEPOSIT", "4", None, date(2026, 1, 5), value="0"),
    ])
    assert pos["units"] == Decimal("4")


def test_recompute_ignores_income_and_amortisation():
    """Proventos, juros, resgate e amortização são caixa, não cota. Quem os
    contabiliza é `_asset_to_read`, via total_returned_net."""
    base = [_tx("BUY", "100", "10", date(2026, 1, 1))]
    ruido = [
        _tx("DIVIDEND", None, None, date(2026, 2, 1), value="50"),
        _tx("JCP", None, None, date(2026, 2, 2), value="30"),
        _tx("RENDIMENTO", None, None, date(2026, 2, 3), value="20"),
        _tx("INTEREST", None, None, date(2026, 2, 4), value="15"),
        _tx("RESGATE", None, None, date(2026, 2, 5), value="10"),
        _tx("WITHDRAWAL", None, None, date(2026, 2, 6), value="500"),
        _tx("FEE", None, None, date(2026, 2, 7), value="5"),
    ]
    so_compra = _recompute(base)
    com_ruido = _recompute(base + ruido)
    assert com_ruido["units"] == so_compra["units"] == Decimal("100")
    assert com_ruido["cost_basis"] == so_compra["cost_basis"] == Decimal("1000")
    assert com_ruido["realized_gain"] == Decimal("0")


def test_recompute_buy_prefers_value_over_qty_times_price():
    """`value` é o caixa que saiu de fato, incluindo o que a corretora
    cobrou por dentro. Quando existe, manda nele."""
    pos = _recompute([
        _tx("BUY", "10", "100", date(2026, 1, 1), value="1012.34"),
    ])
    assert pos["cost_basis"] == Decimal("1012.34")


def test_recompute_dust_from_full_exit_is_zeroed():
    """BOVA11 comprou 1000,000005 e vendeu 999,999989: sobra 1,6e-5. Sem o
    corte relativo, a posição encerrada voltaria a contar como aberta e o
    reprocessamento limparia a data de venda, ressuscitando o ativo."""
    pos = _recompute([
        _tx("BUY", "1000.000005", "100", date(2023, 1, 1)),
        _tx("SELL", "999.999989", "110", date(2023, 7, 31)),
    ])
    assert pos["units"] == Decimal("0")
    assert pos["average_price"] is None
    assert pos["last_sell"] == date(2023, 7, 31)


def test_recompute_keeps_real_small_crypto_position():
    """O corte de poeira é relativo ao volume comprado, não absoluto: uma
    posição real de 0,0746 BTC não pode ser confundida com resíduo."""
    pos = _recompute([
        _tx("BUY", "0.074649", "78000", date(2025, 5, 1)),
    ])
    assert pos["units"] == Decimal("0.074649")
    assert pos["average_price"] is not None
