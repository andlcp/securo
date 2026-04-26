"""add portfolio_snapshots table for monthly TWR + V_end series

Revision ID: 039
Revises: 038
Create Date: 2026-04-26

Stores month-end snapshots of the portfolio so the investments dashboard can
display historical TWR vs benchmarks. Populated via CSV import from the
offline pipeline (parse_b3_*.py + compute_twr_v2.py + merge_twr_benchmarks.py
-> twr_full.csv).

Each row is a single month-end for a user. (user_id, month_end) is unique.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "039"
down_revision: Union[str, None] = "038"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "portfolio_snapshots",
        sa.Column("id", sa.dialects.postgresql.UUID(as_uuid=True),
                  primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("user_id", sa.dialects.postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("month_end", sa.Date, nullable=False),
        # Per-asset-class V_end in BRL (líquido)
        sa.Column("v_end_rv", sa.Numeric(15, 2), nullable=False, server_default="0"),
        sa.Column("v_end_rf", sa.Numeric(15, 2), nullable=False, server_default="0"),
        sa.Column("v_end_us", sa.Numeric(15, 2), nullable=False, server_default="0"),
        sa.Column("v_end_total", sa.Numeric(15, 2), nullable=False),
        # Cashflow externo do mês (aporte líquido)
        sa.Column("cashflow_month", sa.Numeric(15, 2), nullable=False,
                  server_default="0"),
        sa.Column("income_month", sa.Numeric(15, 2), nullable=False,
                  server_default="0"),
        # Retorno mensal e TWR cumulativo
        sa.Column("return_month", sa.Numeric(10, 6), nullable=True),
        sa.Column("twr_cum", sa.Numeric(10, 6), nullable=False, server_default="0"),
        sa.Column("twr_cum_bruto", sa.Numeric(10, 6), nullable=False,
                  server_default="0"),
        # Benchmarks cumulativos no mesmo período
        sa.Column("ibov_cum", sa.Numeric(10, 6), nullable=True),
        sa.Column("ivvb11_cum", sa.Numeric(10, 6), nullable=True),
        sa.Column("sp500_cum", sa.Numeric(10, 6), nullable=True),
        sa.Column("cdi_cum", sa.Numeric(10, 6), nullable=True),
        # Metadados
        sa.Column("source", sa.String(50), nullable=False,
                  server_default="csv_import"),
        sa.Column("imported_at", sa.DateTime(timezone=True),
                  server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("user_id", "month_end",
                            name="uq_portfolio_snapshots_user_month"),
    )
    op.create_index("ix_portfolio_snapshots_user_month",
                    "portfolio_snapshots", ["user_id", "month_end"])


def downgrade() -> None:
    op.drop_index("ix_portfolio_snapshots_user_month",
                  table_name="portfolio_snapshots")
    op.drop_table("portfolio_snapshots")
