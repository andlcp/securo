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
