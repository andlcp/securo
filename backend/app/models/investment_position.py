import uuid
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Optional

from sqlalchemy import DateTime, ForeignKey, Numeric, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base

if TYPE_CHECKING:
    from app.models.investment_portfolio import InvestmentPortfolio


class InvestmentPosition(Base):
    __tablename__ = "investment_positions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    portfolio_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("investment_portfolios.id")
    )
    ticker: Mapped[str] = mapped_column(String(30))
    name: Mapped[str] = mapped_column(String(200))
    # stock_br, stock_us, fii, etf_br, etf_us, tesouro, cdb, bitcoin
    asset_type: Mapped[str] = mapped_column(String(20))
    currency: Mapped[str] = mapped_column(String(3), default="BRL")
    units: Mapped[Decimal] = mapped_column(Numeric(precision=15, scale=6))
    avg_price: Mapped[Decimal] = mapped_column(Numeric(precision=15, scale=6))
    broker: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    portfolio: Mapped["InvestmentPortfolio"] = relationship(back_populates="positions")
