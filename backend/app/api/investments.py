import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import current_active_user
from app.core.database import get_async_session
from app.models.user import User
from app.schemas.investment import (
    InvestmentPositionCreate,
    InvestmentPositionUpdate,
    PortfolioCreate,
    PortfolioRead,
    PortfolioSummary,
    PortfolioUpdate,
    PositionRead,
)
from app.services import investment_portfolio_service as portfolio_svc
from app.services import investment_price_service as price_svc

router = APIRouter(prefix="/api/investments", tags=["investments"])


@router.get("/summary", response_model=PortfolioSummary)
async def get_summary(
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    return await portfolio_svc.get_portfolio_summary(session, user.id)


@router.get("/portfolios", response_model=list[PortfolioRead])
async def list_portfolios(
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    portfolios_db = await portfolio_svc.get_portfolios(session, user.id)
    return [
        PortfolioRead(
            id=p.id,
            user_id=p.user_id,
            name=p.name,
            color=p.color,
            description=p.description,
            created_at=p.created_at,
        )
        for p in portfolios_db
    ]


@router.post("/portfolios", response_model=PortfolioRead, status_code=status.HTTP_201_CREATED)
async def create_portfolio(
    data: PortfolioCreate,
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    try:
        pf = await portfolio_svc.create_portfolio(session, user.id, data)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    return PortfolioRead(
        id=pf.id,
        user_id=pf.user_id,
        name=pf.name,
        color=pf.color,
        description=pf.description,
        created_at=pf.created_at,
    )


@router.patch("/portfolios/{portfolio_id}", response_model=PortfolioRead)
async def update_portfolio(
    portfolio_id: uuid.UUID,
    data: PortfolioUpdate,
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    pf = await portfolio_svc.update_portfolio(session, portfolio_id, user.id, data)
    if not pf:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found")
    return PortfolioRead(
        id=pf.id,
        user_id=pf.user_id,
        name=pf.name,
        color=pf.color,
        description=pf.description,
        created_at=pf.created_at,
    )


@router.delete("/portfolios/{portfolio_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_portfolio(
    portfolio_id: uuid.UUID,
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    deleted = await portfolio_svc.delete_portfolio(session, portfolio_id, user.id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found")


@router.post(
    "/portfolios/{portfolio_id}/positions",
    response_model=PositionRead,
    status_code=status.HTTP_201_CREATED,
)
async def add_position(
    portfolio_id: uuid.UUID,
    data: InvestmentPositionCreate,
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    position = await portfolio_svc.add_position(session, portfolio_id, user.id, data)
    if not position:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found")
    usd_brl = await price_svc.get_usd_brl_rate(session)
    return await portfolio_svc._enrich_position(session, position, usd_brl)


@router.patch("/positions/{position_id}", response_model=PositionRead)
async def update_position(
    position_id: uuid.UUID,
    data: InvestmentPositionUpdate,
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    position = await portfolio_svc.update_position(session, position_id, user.id, data)
    if not position:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Position not found")
    usd_brl = await price_svc.get_usd_brl_rate(session)
    return await portfolio_svc._enrich_position(session, position, usd_brl)


@router.delete("/positions/{position_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_position(
    position_id: uuid.UUID,
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    deleted = await portfolio_svc.delete_position(session, position_id, user.id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Position not found")


@router.get("/benchmarks")
async def get_benchmarks(
    months: int = Query(12, ge=1, le=60),
    _user: User = Depends(current_active_user),
):
    return await price_svc.get_benchmark_history(months)


@router.post("/prices/refresh")
async def refresh_prices(
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    portfolios = await portfolio_svc.get_portfolios(session, user.id)
    refreshed = 0
    for pf in portfolios:
        for pos in pf.positions:
            if pos.asset_type != "cdb":
                info = await price_svc.get_price(session, pos.ticker, pos.asset_type)
                if info:
                    refreshed += 1
    return {"refreshed": refreshed}
