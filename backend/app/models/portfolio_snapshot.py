import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import Date, DateTime, ForeignKey, Numeric, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class PortfolioSnapshot(Base):
    """Month-end snapshot of the user's portfolio.

    Populated by importing the CSV produced by the offline TWR pipeline
    (twr_full.csv from compute_twr_v2.py + merge_twr_benchmarks.py).
    The investments dashboard reads from this table when present, so
    historical TWR vs benchmarks renders without recomputing on the fly.
    """

    __tablename__ = "portfolio_snapshots"
    __table_args__ = (
        UniqueConstraint("user_id", "month_end",
                         name="uq_portfolio_snapshots_user_month"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    month_end: Mapped[date] = mapped_column(Date, nullable=False)

    # Per-asset-class V_end (BRL líquido)
    v_end_rv: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=Decimal("0"))
    v_end_rf: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=Decimal("0"))
    v_end_us: Mapped[Decimal] = mapped_column(Numeric(15, 2), default=Decimal("0"))
    v_end_total: Mapped[Decimal] = mapped_column(Numeric(15, 2), nullable=False)

    cashflow_month: Mapped[Decimal] = mapped_column(
        Numeric(15, 2), default=Decimal("0"))
    income_month: Mapped[Decimal] = mapped_column(
        Numeric(15, 2), default=Decimal("0"))

    return_month: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(10, 6), nullable=True)
    twr_cum: Mapped[Decimal] = mapped_column(Numeric(10, 6), default=Decimal("0"))
    twr_cum_bruto: Mapped[Decimal] = mapped_column(
        Numeric(10, 6), default=Decimal("0"))

    ibov_cum: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 6), nullable=True)
    ivvb11_cum: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 6), nullable=True)
    sp500_cum: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 6), nullable=True)
    cdi_cum: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 6), nullable=True)

    source: Mapped[str] = mapped_column(String(50), default="csv_import")
    imported_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now())
