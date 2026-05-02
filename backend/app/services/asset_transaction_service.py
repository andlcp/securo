"""Asset transaction CRUD + bulk import.

Each row is a single cashflow event affecting an Asset. The investments
TWR computation aggregates these per month at request time, so adding a
transaction immediately moves the chart and the asset's holdings.
"""

import logging
import uuid
from datetime import date
from decimal import Decimal
from typing import Optional

from sqlalchemy import delete as sa_delete
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.asset import Asset
from app.models.asset_transaction import AssetTransaction
from app.schemas.asset import ASSET_TX_TYPES, AssetTransactionCreate

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
    return row


async def delete(session: AsyncSession,
                 user_id: uuid.UUID,
                 transaction_id: uuid.UUID) -> bool:
    stmt = (sa_delete(AssetTransaction)
            .where(AssetTransaction.id == transaction_id,
                   AssetTransaction.user_id == user_id))
    result = await session.execute(stmt)
    await session.commit()
    return (result.rowcount or 0) > 0


async def bulk_upsert(session: AsyncSession,
                      user_id: uuid.UUID,
                      rows: list[dict]) -> int:
    """Upsert by (user_id, asset_id, external_id). Skips rows with no
    external_id (no dedupe key)."""
    if not rows:
        return 0
    payload = [
        {
            "user_id": user_id,
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
    return result.rowcount or len(payload)


async def delete_all_for_user(session: AsyncSession,
                              user_id: uuid.UUID) -> int:
    """Wipe every transaction owned by user (used by --reset import)."""
    stmt = sa_delete(AssetTransaction).where(
        AssetTransaction.user_id == user_id)
    result = await session.execute(stmt)
    await session.commit()
    return result.rowcount or 0
