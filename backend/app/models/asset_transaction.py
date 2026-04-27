import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import Date, DateTime, ForeignKey, Numeric, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class AssetTransaction(Base):
    """A single cashflow event affecting an Asset.

    Buys/sells move qty + cash. Dividends/JCP/Rendimentos are pure income
    (no qty change). Deposits/withdrawals (mostly for IBKR cash account)
    move external cash without touching qty.

    Modified Dietz / TWR are computed at request time by aggregating these
    rows per month, so adding a transaction immediately moves the chart.
    """

    __tablename__ = "asset_transactions"
    __table_args__ = (
        UniqueConstraint("user_id", "asset_id", "external_id",
                         name="uq_asset_transactions_external"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    asset_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("assets.id", ondelete="CASCADE"), nullable=False)

    date: Mapped[date] = mapped_column(Date, nullable=False)
    # BUY | SELL | DIVIDEND | JCP | RENDIMENTO | DEPOSIT | WITHDRAWAL
    # | INTEREST | FEE | RESGATE
    type: Mapped[str] = mapped_column(String(20), nullable=False)
    qty: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 6), nullable=True)
    price: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 6), nullable=True)
    value: Mapped[Optional[Decimal]] = mapped_column(Numeric(15, 2), nullable=True)
    fees: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=Decimal("0"))
    notes: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    source: Mapped[str] = mapped_column(String(50), default="manual")
    external_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now())
