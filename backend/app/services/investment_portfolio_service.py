import uuid
from decimal import Decimal
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.investment_portfolio import InvestmentPortfolio
from app.models.investment_position import InvestmentPosition
from app.schemas.investment import (
    AllocationItem,
    InvestmentPositionCreate,
    InvestmentPositionUpdate,
    PortfolioCreate,
    PortfolioRead,
    PortfolioSummary,
    PortfolioUpdate,
    PositionRead,
)
from app.services import investment_price_service as price_svc


async def _enrich_position(
    session: AsyncSession, pos: InvestmentPosition, usd_brl: float
) -> PositionRead:
    units = float(pos.units)
    avg_price = float(pos.avg_price)
    total_invested = units * avg_price

    price_info = None
    if pos.asset_type != "cdb":
        price_info = await price_svc.get_price(session, pos.ticker, pos.asset_type)

    current_price = price_info["price"] if price_info else None
    current_value = units * current_price if current_price is not None else None
    gain_loss = (current_value - total_invested) if current_value is not None else None
    gain_loss_pct = (
        (gain_loss / total_invested * 100) if gain_loss is not None and total_invested > 0 else None
    )

    current_value_brl = current_value
    if current_value is not None and pos.currency == "USD":
        current_value_brl = current_value * usd_brl

    return PositionRead(
        id=pos.id,
        portfolio_id=pos.portfolio_id,
        ticker=pos.ticker,
        name=pos.name,
        asset_type=pos.asset_type,
        currency=pos.currency,
        units=units,
        avg_price=avg_price,
        broker=pos.broker,
        notes=pos.notes,
        created_at=pos.created_at,
        current_price=current_price,
        current_value=current_value,
        total_invested=total_invested,
        gain_loss=gain_loss,
        gain_loss_pct=gain_loss_pct,
        current_value_brl=current_value_brl,
        change_pct=price_info.get("change_pct") if price_info else None,
        price_updated_at=price_info.get("updated_at") if price_info else None,
    )


async def get_portfolios(
    session: AsyncSession, user_id: uuid.UUID
) -> list[InvestmentPortfolio]:
    result = await session.execute(
        select(InvestmentPortfolio)
        .where(InvestmentPortfolio.user_id == user_id)
        .options(selectinload(InvestmentPortfolio.positions))
        .order_by(InvestmentPortfolio.created_at)
    )
    return list(result.scalars().all())


async def create_portfolio(
    session: AsyncSession, user_id: uuid.UUID, data: PortfolioCreate
) -> InvestmentPortfolio:
    count = await session.scalar(
        select(func.count())
        .select_from(InvestmentPortfolio)
        .where(InvestmentPortfolio.user_id == user_id)
    )
    if (count or 0) >= 2:
        raise ValueError("Maximum 2 portfolios allowed per user")

    portfolio = InvestmentPortfolio(
        user_id=user_id,
        name=data.name,
        color=data.color,
        description=data.description,
    )
    session.add(portfolio)
    await session.commit()
    await session.refresh(portfolio)
    return portfolio


async def update_portfolio(
    session: AsyncSession,
    portfolio_id: uuid.UUID,
    user_id: uuid.UUID,
    data: PortfolioUpdate,
) -> Optional[InvestmentPortfolio]:
    result = await session.execute(
        select(InvestmentPortfolio).where(
            InvestmentPortfolio.id == portfolio_id,
            InvestmentPortfolio.user_id == user_id,
        )
    )
    portfolio = result.scalar_one_or_none()
    if not portfolio:
        return None
    for key, val in data.model_dump(exclude_unset=True).items():
        setattr(portfolio, key, val)
    await session.commit()
    await session.refresh(portfolio)
    return portfolio


async def delete_portfolio(
    session: AsyncSession, portfolio_id: uuid.UUID, user_id: uuid.UUID
) -> bool:
    result = await session.execute(
        select(InvestmentPortfolio).where(
            InvestmentPortfolio.id == portfolio_id,
            InvestmentPortfolio.user_id == user_id,
        )
    )
    portfolio = result.scalar_one_or_none()
    if not portfolio:
        return False
    await session.delete(portfolio)
    await session.commit()
    return True


async def add_position(
    session: AsyncSession,
    portfolio_id: uuid.UUID,
    user_id: uuid.UUID,
    data: InvestmentPositionCreate,
) -> Optional[InvestmentPosition]:
    owner = await session.execute(
        select(InvestmentPortfolio.id).where(
            InvestmentPortfolio.id == portfolio_id,
            InvestmentPortfolio.user_id == user_id,
        )
    )
    if not owner.scalar_one_or_none():
        return None

    position = InvestmentPosition(
        portfolio_id=portfolio_id,
        ticker=data.ticker.upper(),
        name=data.name,
        asset_type=data.asset_type,
        currency=data.currency,
        units=Decimal(str(data.units)),
        avg_price=Decimal(str(data.avg_price)),
        broker=data.broker,
        notes=data.notes,
    )
    session.add(position)
    await session.commit()
    await session.refresh(position)
    return position


async def update_position(
    session: AsyncSession,
    position_id: uuid.UUID,
    user_id: uuid.UUID,
    data: InvestmentPositionUpdate,
) -> Optional[InvestmentPosition]:
    result = await session.execute(
        select(InvestmentPosition)
        .join(InvestmentPortfolio, InvestmentPosition.portfolio_id == InvestmentPortfolio.id)
        .where(
            InvestmentPosition.id == position_id,
            InvestmentPortfolio.user_id == user_id,
        )
    )
    position = result.scalar_one_or_none()
    if not position:
        return None
    for key, val in data.model_dump(exclude_unset=True).items():
        if key == "ticker" and val:
            val = val.upper()
        setattr(position, key, val)
    await session.commit()
    await session.refresh(position)
    return position


async def delete_position(
    session: AsyncSession, position_id: uuid.UUID, user_id: uuid.UUID
) -> bool:
    result = await session.execute(
        select(InvestmentPosition)
        .join(InvestmentPortfolio, InvestmentPosition.portfolio_id == InvestmentPortfolio.id)
        .where(
            InvestmentPosition.id == position_id,
            InvestmentPortfolio.user_id == user_id,
        )
    )
    position = result.scalar_one_or_none()
    if not position:
        return False
    await session.delete(position)
    await session.commit()
    return True


async def get_portfolio_summary(
    session: AsyncSession, user_id: uuid.UUID
) -> PortfolioSummary:
    portfolios_db = await get_portfolios(session, user_id)
    usd_brl = await price_svc.get_usd_brl_rate(session)

    portfolio_reads: list[PortfolioRead] = []
    total_invested_brl = 0.0
    total_value_brl = 0.0
    allocation: dict[str, float] = {}

    for pf in portfolios_db:
        positions: list[PositionRead] = []
        pf_invested = 0.0
        pf_value = 0.0

        for pos in pf.positions:
            enriched = await _enrich_position(session, pos, usd_brl)
            positions.append(enriched)

            inv_brl = enriched.total_invested
            if pos.currency == "USD":
                inv_brl = enriched.total_invested * usd_brl

            val_brl = enriched.current_value_brl if enriched.current_value_brl is not None else inv_brl

            pf_invested += inv_brl
            pf_value += val_brl
            total_invested_brl += inv_brl
            total_value_brl += val_brl
            allocation[pos.asset_type] = allocation.get(pos.asset_type, 0.0) + val_brl

        pf_gain = pf_value - pf_invested
        pf_gain_pct = (pf_gain / pf_invested * 100) if pf_invested > 0 else 0.0

        portfolio_reads.append(
            PortfolioRead(
                id=pf.id,
                user_id=user_id,
                name=pf.name,
                color=pf.color,
                description=pf.description,
                created_at=pf.created_at,
                positions=positions,
                total_invested=round(pf_invested, 2),
                current_value=round(pf_value, 2),
                gain_loss=round(pf_gain, 2),
                gain_loss_pct=round(pf_gain_pct, 2),
            )
        )

    total_gain = total_value_brl - total_invested_brl
    total_gain_pct = (total_gain / total_invested_brl * 100) if total_invested_brl > 0 else 0.0
    total_ref = total_value_brl if total_value_brl > 0 else 1.0

    alloc_list = [
        AllocationItem(
            type=k,
            value=round(v, 2),
            pct=round(v / total_ref * 100, 1),
        )
        for k, v in sorted(allocation.items(), key=lambda x: -x[1])
    ]

    return PortfolioSummary(
        portfolios=portfolio_reads,
        total_invested_brl=round(total_invested_brl, 2),
        current_value_brl=round(total_value_brl, 2),
        gain_loss_brl=round(total_gain, 2),
        gain_loss_pct=round(total_gain_pct, 2),
        allocation=alloc_list,
        fx_usd_brl=round(usd_brl, 4),
    )
