"""Portfolio time series — live TWR computed from Asset/AssetValue/AssetTransaction.

This is the source the Investments dashboard reads. Adding/editing assets
or transactions in the UI changes the chart on the next request because
the data is recomputed each call (small dataset; very fast).

Endpoint:
    GET /api/portfolio/timeseries
        ?months=12
        &since_start=true
        &asset_ids=uuid,uuid
        &asset_classes=RENDA_VARIAVEL_BR,FIIS
        &group_ids=uuid,uuid

Returns a list of points:
    {month_end, month, v_end, cashflow, income, return_month, twr_cum,
     by_class: {RENDA_VARIAVEL_BR: 1234.56, ...}}

The frontend rebases each line to 0% at the start of the selected window
(see investments.tsx) — we always return the cumulative series.
"""

import logging
import uuid
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import current_active_user
from app.core.database import get_async_session
from app.models.user import User
from app.services import portfolio_timeseries_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/portfolio/timeseries",
                   tags=["portfolio-timeseries"])


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


def _parse_csv(s: Optional[str]) -> Optional[list[str]]:
    if not s:
        return None
    out = [p.strip() for p in s.split(",") if p.strip()]
    return out or None


@router.get("")
async def get_timeseries(
    months: int = Query(12, ge=1, le=240),
    since_start: bool = Query(False),
    asset_ids: Optional[str] = Query(None, description="Comma-separated asset UUIDs"),
    asset_classes: Optional[str] = Query(None, description="Comma-separated class codes"),
    group_ids: Optional[str] = Query(None, description="Comma-separated AssetGroup UUIDs"),
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    return await portfolio_timeseries_service.get_timeseries(
        session, user,
        months=months,
        since_start=since_start,
        asset_ids=_parse_uuid_csv(asset_ids),
        asset_classes=_parse_csv(asset_classes),
        group_ids=_parse_uuid_csv(group_ids),
    )
