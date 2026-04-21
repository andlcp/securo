from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import DateTime, Numeric, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class InvestmentPriceCache(Base):
    __tablename__ = "investment_price_cache"

    ticker: Mapped[str] = mapped_column(String(30), primary_key=True)
    price: Mapped[Optional[Decimal]] = mapped_column(Numeric(precision=15, scale=6), nullable=True)
    currency: Mapped[str] = mapped_column(String(3))
    change_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(precision=8, scale=4), nullable=True)
    long_name: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
