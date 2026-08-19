"""Cupons semestrais do Tesouro Direto — detecção de pagamentos pendentes.

Títulos "com Juros Semestrais" (IPCA+ e Prefixado) pagam cupom duas vezes
por ano. O Securo não tem como puxar esses créditos automaticamente: o
`sync_dividends` só cobre ativos com `valuation_method='market_price'`
(yfinance), e o `refresh_tesouro_assets` apenas marca o PU a mercado sem
gerar transação. Os cupons históricos vieram do import inicial do extrato
da B3 (source `tesouro_direto_juros`).

Em vez de calcular o valor do cupom — que depende do VNA corrigido pelo
IPCA com defasagem e arredondamento da B3, e erraria centavos — este
módulo apenas **detecta** as datas de cupom já vencidas que ainda não têm
um lançamento correspondente. O usuário confere o valor no extrato e
lança. São ~2 eventos por ano por título, então o custo de confirmar
manualmente é baixo e o número fica exato.

Estado é derivado, não armazenado: assim que o INTEREST é lançado na data
do cupom, o pendente some sozinho. Não há tabela de notificações nem
risco de alerta órfão.

Calendário: o Tesouro paga cupom no dia 15, no mês do vencimento e no mês
seis meses antes. Vencimento em agosto → cupons em 15/02 e 15/08;
vencimento em maio → 15/05 e 15/11.
"""
import uuid
from datetime import date
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.asset import Asset
from app.models.asset_transaction import AssetTransaction

# Só estes títulos pagam cupom semestral. Casamos pelo nome porque é como
# o resto do pipeline de RF identifica os papéis (ver rf_tasks.py).
_COUPON_NAME_MARKER = "juros semestrais"

# Nenhum título paga cupom antes de existir na carteira: o primeiro
# cupom elegível é o primeiro após a data de compra.
_COUPON_DAY = 15


def coupon_months_for(maturity: date) -> tuple[int, int]:
    """Meses de pagamento de cupom a partir do vencimento.

    O Tesouro paga no mês do vencimento e seis meses antes. Ex.: vencimento
    2030-08-15 → (2, 8); vencimento 2035-05-15 → (5, 11).
    """
    m = maturity.month
    other = m - 6 if m > 6 else m + 6
    return tuple(sorted((m, other)))  # type: ignore[return-value]


def coupon_dates_between(maturity: date, start: date, end: date) -> list[date]:
    """Todas as datas de cupom no intervalo [start, end], inclusive."""
    if start > end:
        return []
    months = coupon_months_for(maturity)
    out: list[date] = []
    for year in range(start.year, end.year + 1):
        for m in months:
            d = date(year, m, _COUPON_DAY)
            if start <= d <= end and d <= maturity:
                out.append(d)
    return sorted(out)


async def pending_coupons(
    session: AsyncSession,
    user_id: uuid.UUID,
    today: Optional[date] = None,
) -> list[dict]:
    """Cupons já vencidos e ainda não lançados, do mais antigo pro mais novo.

    Um cupom conta como lançado se existe qualquer AssetTransaction do tipo
    INTEREST naquele ativo naquela data — independente da origem, pra que um
    lançamento manual feito pela UI também zere o alerta.
    """
    today = today or date.today()

    assets = (await session.execute(
        select(Asset).where(
            Asset.user_id == user_id,
            Asset.is_archived == False,    # noqa: E712
            Asset.sell_date.is_(None),
            Asset.maturity_date.is_not(None),
        )
    )).scalars().all()

    coupon_assets = [
        a for a in assets
        if _COUPON_NAME_MARKER in (a.name or "").lower()
    ]
    if not coupon_assets:
        return []

    asset_ids = [a.id for a in coupon_assets]
    rows = (await session.execute(
        select(AssetTransaction.asset_id, AssetTransaction.date)
        .where(
            AssetTransaction.asset_id.in_(asset_ids),
            AssetTransaction.type == "INTEREST",
        )
    )).all()
    already: set[tuple[uuid.UUID, date]] = {(aid, d) for aid, d in rows}

    out: list[dict] = []
    for a in coupon_assets:
        # Cupom só existe a partir da compra. Sem purchase_date caímos na
        # primeira transação conhecida; sem nenhuma das duas, pulamos o
        # ativo em vez de inventar um histórico.
        start = a.purchase_date
        if start is None:
            first_tx = (await session.execute(
                select(AssetTransaction.date)
                .where(AssetTransaction.asset_id == a.id)
                .order_by(AssetTransaction.date)
                .limit(1)
            )).scalar_one_or_none()
            start = first_tx
        if start is None:
            continue

        for d in coupon_dates_between(a.maturity_date, start, today):
            if (a.id, d) in already:
                continue
            out.append({
                "asset_id": str(a.id),
                "asset_name": a.name,
                "currency": a.currency,
                "coupon_date": d.isoformat(),
                "days_late": (today - d).days,
            })

    out.sort(key=lambda x: x["coupon_date"])
    return out
