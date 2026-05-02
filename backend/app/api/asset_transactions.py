"""Asset transactions — user-wide events log endpoint.

Returns every AssetTransaction for the authenticated user with optional
filters, joined with asset and group metadata so the events log page
can render rows without follow-up requests.
"""

import uuid
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import current_active_user
from app.core.database import get_async_session
from app.models.user import User
from app.services import asset_transaction_service

router = APIRouter(prefix="/api/asset-transactions",
                   tags=["asset-transactions"])


def _parse_csv(s: Optional[str]) -> Optional[list[str]]:
    if not s:
        return None
    out = [p.strip() for p in s.split(",") if p.strip()]
    return out or None


def _parse_uuid_csv(s: Optional[str]) -> Optional[list[uuid.UUID]]:
    if not s:
        return None
    out = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(uuid.UUID(part))
        except ValueError:
            continue
    return out or None


@router.get("")
async def list_all(
    types: Optional[str] = Query(None, description="Comma-separated transaction types"),
    asset_ids: Optional[str] = Query(None, description="Comma-separated asset UUIDs"),
    group_ids: Optional[str] = Query(None, description="Comma-separated group UUIDs"),
    sources: Optional[str] = Query(None, description="Comma-separated source values"),
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    q: Optional[str] = Query(None, description="Search asset name/ticker/notes"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    return await asset_transaction_service.list_for_user(
        session, user.id,
        types=_parse_csv(types),
        asset_ids=_parse_uuid_csv(asset_ids),
        group_ids=_parse_uuid_csv(group_ids),
        sources=_parse_csv(sources),
        date_from=date_from,
        date_to=date_to,
        q=q,
        limit=limit,
        offset=offset,
    )
