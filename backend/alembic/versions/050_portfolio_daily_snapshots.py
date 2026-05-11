"""portfolio_daily_snapshots — pre-computed daily timeseries

Revision ID: 050
Revises: 049
Create Date: 2026-05-11

Materialized snapshot of the portfolio_timeseries_service output so the
Investments / Patrimônio pages don't have to recompute the entire daily
walk (cold path was ~14s, even after bisect+yahoo cache still ~2-3s) on
every request.

Schema:
  (user_id, date) PK — one row per user per calendar day
  payload          JSONB — full row of get_timeseries output (daily mode):
                          v_end, cashflow, income, return_month, twr_cum,
                          by_class (per asset_class breakdown),
                          by_group (per AssetGroup breakdown). The
                          read path agrégates monthly on demand and
                          filters in memory.

The maintenance contract:
  - On any mutation that affects timeseries (asset CRUD, transaction CRUD,
    AssetValue CRUD), `invalidate_snapshots(user_id, from_date)` deletes
    every row with date >= from_date for that user. The next read does an
    incremental rebuild over the deleted range.
  - A nightly Celery beat task (`refresh_portfolio_snapshots`) catches
    any missed invalidations and updates today's row with the latest
    quotes / FX rates.

We pay storage (~2.5k rows × ~1KB JSONB per user = ~2.5 MB / user) for
sub-second reads.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "050"
down_revision: Union[str, None] = "049"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "portfolio_daily_snapshots",
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "computed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("user_id", "date"),
    )
    # Index on user_id alone is implicit in the composite PK; date order
    # is also covered by the PK btree. No additional indexes needed.


def downgrade() -> None:
    op.drop_table("portfolio_daily_snapshots")
