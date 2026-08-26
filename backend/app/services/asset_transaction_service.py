"""Asset transaction CRUD + bulk import.

Each row is a single cashflow event affecting an Asset. The investments
TWR computation aggregates these per month at request time, so adding a
transaction immediately moves the chart and the asset's holdings.
"""

import logging
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional

from sqlalchemy import delete as sa_delete
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.core.workspace_autostamp import resolve_workspace_id
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.asset import Asset
from app.models.asset_transaction import AssetTransaction
from app.schemas.asset import ASSET_TX_TYPES, AssetTransactionCreate
from app.services.portfolio_timeseries_service import invalidate_ts_cache

logger = logging.getLogger(__name__)


async def _ensure_user_owns_asset(session: AsyncSession,
                                  user_id: uuid.UUID,
                                  asset_id: uuid.UUID) -> Optional[Asset]:
    stmt = select(Asset).where(Asset.id == asset_id, Asset.user_id == user_id)
    return (await session.execute(stmt)).scalar_one_or_none()


async def list_for_asset(session: AsyncSession,
                         user_id: uuid.UUID,
                         asset_id: uuid.UUID
                         ) -> Optional[list[AssetTransaction]]:
    if not await _ensure_user_owns_asset(session, user_id, asset_id):
        return None
    stmt = (select(AssetTransaction)
            .where(AssetTransaction.asset_id == asset_id,
                   AssetTransaction.user_id == user_id)
            .order_by(AssetTransaction.date,
                      AssetTransaction.created_at))
    return list((await session.execute(stmt)).scalars().all())


async def list_for_user(
    session: AsyncSession,
    user_id: uuid.UUID,
    types: Optional[list[str]] = None,
    asset_ids: Optional[list[uuid.UUID]] = None,
    group_ids: Optional[list[uuid.UUID]] = None,
    sources: Optional[list[str]] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    q: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
) -> dict:
    """List all transactions for a user with optional filters.

    Returns a dict with `items` (joined with asset name/ticker/group) and
    `total` (filtered count). Sorted newest-first by date then created_at.
    Used by the events log page.
    """
    from app.models.asset_group import AssetGroup

    stmt = (
        select(
            AssetTransaction,
            Asset.name.label("asset_name"),
            Asset.ticker.label("asset_ticker"),
            Asset.currency.label("asset_currency"),
            Asset.group_id.label("group_id"),
            AssetGroup.name.label("group_name"),
            AssetGroup.color.label("group_color"),
        )
        .join(Asset, AssetTransaction.asset_id == Asset.id)
        .outerjoin(AssetGroup, Asset.group_id == AssetGroup.id)
        .where(AssetTransaction.user_id == user_id)
    )

    if types:
        stmt = stmt.where(AssetTransaction.type.in_(types))
    if asset_ids:
        stmt = stmt.where(AssetTransaction.asset_id.in_(asset_ids))
    if group_ids:
        stmt = stmt.where(Asset.group_id.in_(group_ids))
    if sources:
        stmt = stmt.where(AssetTransaction.source.in_(sources))
    if date_from:
        stmt = stmt.where(AssetTransaction.date >= date_from)
    if date_to:
        stmt = stmt.where(AssetTransaction.date <= date_to)
    if q:
        like = f"%{q}%"
        from sqlalchemy import or_
        stmt = stmt.where(or_(
            Asset.name.ilike(like),
            Asset.ticker.ilike(like),
            AssetTransaction.notes.ilike(like),
        ))

    # Count first (without limit/offset).
    from sqlalchemy import func as sa_func
    count_stmt = select(sa_func.count()).select_from(stmt.subquery())
    total = (await session.execute(count_stmt)).scalar_one() or 0

    stmt = (stmt
            .order_by(AssetTransaction.date.desc(),
                      AssetTransaction.created_at.desc())
            .limit(limit)
            .offset(offset))

    rows = (await session.execute(stmt)).all()
    items = []
    for r in rows:
        tx = r.AssetTransaction
        items.append({
            "id": str(tx.id),
            "asset_id": str(tx.asset_id),
            "date": tx.date.isoformat(),
            "type": tx.type,
            "qty": float(tx.qty) if tx.qty is not None else None,
            "price": float(tx.price) if tx.price is not None else None,
            "value": float(tx.value) if tx.value is not None else None,
            "fees": float(tx.fees) if tx.fees is not None else 0.0,
            "notes": tx.notes,
            "source": tx.source,
            "external_id": tx.external_id,
            "created_at": tx.created_at.isoformat() if tx.created_at else None,
            "asset": {
                "name": r.asset_name,
                "ticker": r.asset_ticker,
                "currency": r.asset_currency,
                "group_id": str(r.group_id) if r.group_id else None,
                "group_name": r.group_name,
                "group_color": r.group_color,
            },
        })
    return {"items": items, "total": total}


async def create(session: AsyncSession,
                 user_id: uuid.UUID,
                 asset_id: uuid.UUID,
                 data: AssetTransactionCreate
                 ) -> Optional[AssetTransaction]:
    if not await _ensure_user_owns_asset(session, user_id, asset_id):
        return None
    if data.type not in ASSET_TX_TYPES:
        raise ValueError(f"Invalid transaction type: {data.type}")
    row = AssetTransaction(
        user_id=user_id,
        asset_id=asset_id,
        date=data.date,
        type=data.type,
        qty=data.qty,
        price=data.price,
        value=data.value,
        fees=data.fees or Decimal("0"),
        notes=data.notes,
        source=data.source or "manual",
        external_id=data.external_id,
    )
    session.add(row)
    await session.commit()
    await session.refresh(row)
    invalidate_ts_cache(user_id)
    return row


async def delete(session: AsyncSession,
                 user_id: uuid.UUID,
                 transaction_id: uuid.UUID) -> bool:
    stmt = (sa_delete(AssetTransaction)
            .where(AssetTransaction.id == transaction_id,
                   AssetTransaction.user_id == user_id))
    result = await session.execute(stmt)
    await session.commit()
    if (result.rowcount or 0) > 0:
        invalidate_ts_cache(user_id)
    return (result.rowcount or 0) > 0


async def bulk_upsert(session: AsyncSession,
                      user_id: uuid.UUID,
                      rows: list[dict]) -> int:
    """Upsert by (user_id, asset_id, external_id). Skips rows with no
    external_id (no dedupe key)."""
    if not rows:
        return 0
    # Bulk upsert -- mapper events don't fire, so stamp the
    # workspace explicitly instead of relying on the autostamp.
    workspace_id = await resolve_workspace_id(session, user_id)
    payload = [
        {
            "user_id": user_id,
            "workspace_id": workspace_id,
            "asset_id": r["asset_id"],
            "date": r["date"],
            "type": r["type"],
            "qty": r.get("qty"),
            "price": r.get("price"),
            "value": r.get("value"),
            "fees": r.get("fees", Decimal("0")),
            "notes": r.get("notes"),
            "source": r.get("source", "csv_import"),
            "external_id": r.get("external_id"),
        }
        for r in rows
    ]
    stmt = pg_insert(AssetTransaction).values(payload)
    update_cols = {c: stmt.excluded[c] for c in
                   ("date", "type", "qty", "price", "value", "fees",
                    "notes", "source")}
    stmt = stmt.on_conflict_do_update(
        constraint="uq_asset_transactions_external",
        set_=update_cols,
        where=AssetTransaction.external_id.isnot(None),
    )
    result = await session.execute(stmt)
    await session.commit()
    invalidate_ts_cache(user_id)
    return result.rowcount or len(payload)


async def delete_all_for_user(session: AsyncSession,
                              user_id: uuid.UUID) -> int:
    """Wipe every transaction owned by user (used by --reset import)."""
    stmt = sa_delete(AssetTransaction).where(
        AssetTransaction.user_id == user_id)
    result = await session.execute(stmt)
    await session.commit()
    invalidate_ts_cache(user_id)
    return result.rowcount or 0


# ---------------------------------------------------------------------------
# Posição derivada do histórico (vindo do upstream, ensinada aos nossos tipos)
# ---------------------------------------------------------------------------
# O upstream deriva unidades, preço médio e ganho realizado relendo o
# histórico. O código deles só conhece dois eventos, `buy` e `sell`, porque
# é só isso que o histórico deles tem.
#
# O nosso tem dez. Rodar a versão original aqui zeraria posição: as 256
# cotas do desdobramento da SBSP3 e as 4 da bonificação da RENT4 entram
# como DEPOSIT, que ele ignoraria — a SBSP3 cairia de 320 para 64 e a RENT4
# para zero, sem erro nenhum na tela.

def _d(value) -> Decimal:
    return Decimal(str(value or 0))


#: Eventos que movem quantidade de cotas.
_TX_ADDS_UNITS = ("BUY", "DEPOSIT")
_TX_REMOVES_UNITS = ("SELL",)
#: Poeira de ponto flutuante, relativa ao que foi negociado. O BOVA11
#: comprou 1000,000005 cotas e vendeu 999,999989: sobra 1,6e-5, e sem
#: isto o reprocessamento trataria a posição como aberta e limparia a
#: data de venda, ressuscitando um ativo encerrado em 2023.
#:
#: Relativa, e não absoluta, por causa de cripto: um limiar fixo grande o
#: bastante para pegar 1,6e-5 de BOVA11 apagaria uma posição real de
#: Bitcoin, que aqui é de 0,0746 unidade. Um milionésimo do volume
#: comprado separa os dois casos com folga.
_DUST_RATIO = Decimal("0.000001")


def _recompute(transactions: list[AssetTransaction]) -> dict:
    """Relê o histórico em ordem de data e devolve a posição derivada.

    Preço médio pelo método da média ponderada (a convenção brasileira de
    preço médio), não PEPS: uma venda realiza `(preço - médio) × qtd` e
    reduz o custo proporcionalmente, deixando a média por cota intacta.

    Como cada tipo entra:
      BUY         cotas e custo sobem. O custo sai de `value` (caixa real
                  do lançamento) quando existe, senão de qtd × preço.
      DEPOSIT     cotas sobem, custo não: bonificação e desdobramento não
                  custam dinheiro. É o caso que a versão original perdia.
      SELL        realiza o ganho, reduz custo e cotas.
      demais      DIVIDEND, JCP, RENDIMENTO, INTEREST, RESGATE, WITHDRAWAL
                  e FEE são caixa, não cota. Ficam de fora daqui — quem os
                  contabiliza é `_asset_to_read`, via `total_returned_net`.
    """
    txs = sorted(
        transactions,
        key=lambda t: (t.date, t.created_at or datetime.min.replace(tzinfo=timezone.utc)),
    )
    qty = Decimal("0")
    cost = Decimal("0")
    realized = Decimal("0")
    bought = Decimal("0")
    first_buy: Optional[date] = None
    last_sell: Optional[date] = None

    for tx in txs:
        q = _d(tx.qty)
        p = _d(tx.price)
        fees = _d(tx.fees)
        if tx.type in _TX_ADDS_UNITS:
            if tx.type == "BUY":
                # `value` é o caixa que saiu de fato, incluindo o que a
                # corretora cobrou por dentro; qtd × preço é a aproximação.
                cost += (_d(tx.value) if tx.value is not None else q * p) + fees
                if first_buy is None:
                    first_buy = tx.date
            qty += q
            bought += q
        elif tx.type in _TX_REMOVES_UNITS:
            avg = (cost / qty) if qty > 0 else Decimal("0")
            sold = q if q <= qty else qty      # trava venda a descoberto
            realized += (p - avg) * sold - fees
            cost -= avg * sold
            qty -= sold
            last_sell = tx.date

    if bought > 0 and abs(qty) < bought * _DUST_RATIO:
        qty = Decimal("0")
        cost = Decimal("0")

    return {
        "units": qty,
        "average_price": (cost / qty) if qty > 0 else None,
        "cost_basis": cost if qty > 0 else Decimal("0"),
        "realized_gain": realized,
        "first_buy": first_buy,
        "last_sell": last_sell,
    }


async def _load_txs(session: AsyncSession, asset_id: uuid.UUID) -> list[AssetTransaction]:
    result = await session.execute(
        select(AssetTransaction).where(AssetTransaction.asset_id == asset_id)
    )
    return list(result.scalars().all())


async def recompute_and_cache(session: AsyncSession, asset: Asset) -> None:
    """Grava a posição derivada no ativo. Silencioso quando não se aplica.

    Só age sobre ativos precificados a mercado. Renda fixa e empréstimos
    guardam "unidades" que não vêm do histórico — CDB tem a quantidade de
    títulos, empréstimo tem 1 — e o histórico deles carrega só `value`, sem
    quantidade. Conferido contra os dados reais: dos 82 ativos a mercado, 81
    já batem com o que o histórico reproduz; dos 35 manuais, 7 seriam
    zerados (quatro CDBs, um Tesouro e dois empréstimos).
    """
    if asset.valuation_method != "market_price":
        return

    pos = _recompute(await _load_txs(session, asset.id))
    if pos["units"] == 0 and not pos["first_buy"]:
        # Ativo a mercado sem histórico de compra: as unidades vieram de
        # outro caminho (cadastro manual, importação antiga). Não há o que
        # derivar, e sobrescrever apagaria a posição.
        return

    asset.units = pos["units"]
    asset.average_price = pos["average_price"]
    asset.realized_gain = pos["realized_gain"].quantize(Decimal("0.01"))

    # `purchase_price` e `purchase_date` NÃO são reescritos aqui, e isso é
    # deliberado. No upstream `purchase_price` guarda o custo total das
    # cotas em carteira; aqui ele sempre significou preço POR UNIDADE, e
    # três lugares dependem disso — o "Onde Aportar"
    # (asset_allocation_service faz purchase_price × units), o fallback de
    # saldo do asset_group_service e o diálogo de edição do Patrimônio.
    # Gravar o custo total aí inflaria o valor pelo número de cotas: o
    # IVVB11, com 491 delas, apareceria 491 vezes maior na meta de
    # alocação. O equivalente por unidade já está em `average_price`.

    if pos["units"] > 0:
        asset.sell_date = None
        asset.sell_price = None
    elif pos["last_sell"] is not None:
        asset.sell_date = pos["last_sell"]


def _type_from_quote(quote_type: Optional[str]) -> str:
    """Espelha o mapeamento quoteType → tipo de ativo do frontend, para uma
    posição criada pelo histórico cair num ícone/tipo sensato."""
    mapping = {
        "EQUITY": "stock",
        "ETF": "etf",
        "CRYPTOCURRENCY": "crypto",
        "MUTUALFUND": "fund",
        "INDEX": "fund",
    }
    return mapping.get((quote_type or "").upper(), "investment")
