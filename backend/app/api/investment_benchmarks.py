from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import current_active_user
from app.core.database import get_async_session
from app.models.user import User
from app.services import investment_benchmark_service

router = APIRouter(prefix="/api/investment-benchmarks", tags=["investment-benchmarks"])


@router.get("/series")
async def get_benchmark_series(
    months: int = Query(12, ge=1, le=120),
    since_start: bool = Query(False),
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    start_date = None
    if since_start:
        start_date = await investment_benchmark_service.get_portfolio_start_date(session, user.id)
    return await investment_benchmark_service.get_benchmark_series(months=months, start_date=start_date)


@router.get("/returns")
async def get_portfolio_returns(
    group_ids: Optional[str] = Query(None, description="Comma-separated group UUIDs"),
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    ids = [g.strip() for g in group_ids.split(",") if g.strip()] if group_ids else None
    return await investment_benchmark_service.get_portfolio_returns(session, user.id, ids)
