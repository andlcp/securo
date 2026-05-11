import uuid
from datetime import date as _date, datetime

from sqlalchemy import Date, DateTime, ForeignKey, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class PortfolioDailySnapshot(Base):
    """One row per (user, day) caching a full daily timeseries entry.

    Created on-demand by `portfolio_snapshot_service.rebuild_snapshots`,
    invalidated by any mutation that affects the timeseries (see
    `invalidate_snapshots`). Read path serves these directly instead of
    recomputing.
    """

    __tablename__ = "portfolio_daily_snapshots"

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    date: Mapped[_date] = mapped_column(Date, primary_key=True)
    # Same shape as one row of get_timeseries' daily output:
    #   v_end, cashflow, income, return_month, twr_cum,
    #   by_class (asset_class -> v_end), by_group (group_id -> v_end)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
