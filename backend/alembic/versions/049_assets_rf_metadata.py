"""add RF (renda fixa) metadata columns to assets

Revision ID: 049
Revises: 048
Create Date: 2026-05-08

Adds three columns used by refresh_cdb_assets to mark CDB / LCI / LCA
positions to market with the actual contracted rate, replacing the
105 % CDI heuristic that was driving ~1.5 % drift vs the broker:

- rf_indexer (NULL / 'PRE' / 'CDI' / 'IPCA'): which index the asset
  follows. NULL keeps the legacy 105 % CDI behaviour for assets that
  haven't been migrated yet.
- rf_rate_pct (numeric): meaning depends on rf_indexer:
    PRE  -> annual fixed rate (e.g. 15.15 for 15.15 % a.a.)
    CDI  -> % of CDI (e.g. 109 for 109 % do CDI)
    IPCA -> spread above IPCA (e.g. 7.43 for IPCA + 7.43 % a.a.)
- rf_index_offset_pct: reserved for hybrid contracts ('CDI + X' style),
  not used yet but added now to avoid a second migration later.

All columns are nullable; existing CDB rows fall back to the heuristic
until the user fills them in.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "049"
down_revision: Union[str, None] = "048"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("assets",
                  sa.Column("rf_indexer", sa.String(10), nullable=True))
    op.add_column("assets",
                  sa.Column("rf_rate_pct", sa.Numeric(8, 4), nullable=True))
    op.add_column("assets",
                  sa.Column("rf_index_offset_pct", sa.Numeric(8, 4), nullable=True))


def downgrade() -> None:
    op.drop_column("assets", "rf_index_offset_pct")
    op.drop_column("assets", "rf_rate_pct")
    op.drop_column("assets", "rf_indexer")
