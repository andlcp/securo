"""create asset_transactions table

Revision ID: 041
Revises: 040
Create Date: 2026-04-26

A per-asset transaction log: every BUY, SELL, dividend, JCP, FII rendimento,
deposit/withdrawal that affects an asset's value or cashflow. Powers the
investments dashboard's TWR computation directly (Modified Dietz monthly).

Schema:
    id           UUID PK
    user_id      UUID FK users
    asset_id     UUID FK assets (CASCADE)
    date         DATE
    type         VARCHAR(20)  -- BUY|SELL|DIVIDEND|JCP|RENDIMENTO|DEPOSIT|WITHDRAWAL|INTEREST|FEE|RESGATE
    qty          NUMERIC(15,6)  nullable (DIVIDEND has no qty change)
    price        NUMERIC(18,6)  nullable (per unit, in asset currency)
    value        NUMERIC(15,2)  nullable (total in asset currency)
    fees         NUMERIC(15,2)  default 0
    notes        VARCHAR(500)   nullable
    source       VARCHAR(50)    default 'manual'  ('manual', 'csv_import', 'sync')
    external_id  VARCHAR(255)   nullable (de-dup key from source)
    created_at   TIMESTAMPZ     default now()
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "041"
down_revision: Union[str, None] = "040"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "asset_transactions",
        sa.Column("id", sa.dialects.postgresql.UUID(as_uuid=True),
                  primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("user_id", sa.dialects.postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("asset_id", sa.dialects.postgresql.UUID(as_uuid=True),
                  sa.ForeignKey("assets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("type", sa.String(20), nullable=False),
        sa.Column("qty", sa.Numeric(15, 6), nullable=True),
        sa.Column("price", sa.Numeric(18, 6), nullable=True),
        sa.Column("value", sa.Numeric(15, 2), nullable=True),
        sa.Column("fees", sa.Numeric(15, 2), nullable=False, server_default="0"),
        sa.Column("notes", sa.String(500), nullable=True),
        sa.Column("source", sa.String(50), nullable=False, server_default="manual"),
        sa.Column("external_id", sa.String(255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("user_id", "asset_id", "external_id",
                            name="uq_asset_transactions_external"),
    )
    op.create_index("ix_asset_transactions_user_date",
                    "asset_transactions", ["user_id", "date"])
    op.create_index("ix_asset_transactions_asset_date",
                    "asset_transactions", ["asset_id", "date"])


def downgrade() -> None:
    op.drop_index("ix_asset_transactions_asset_date",
                  table_name="asset_transactions")
    op.drop_index("ix_asset_transactions_user_date",
                  table_name="asset_transactions")
    op.drop_table("asset_transactions")
