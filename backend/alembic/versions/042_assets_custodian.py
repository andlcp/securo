"""add custodian column to assets

Revision ID: 042
Revises: 041
Create Date: 2026-04-27

The custodian / broker that holds the asset (XP Investimentos, BTG Pactual,
Interactive Brokers, etc). Surfaced as a column on the Patrimônio list so
the user knows where each position lives.

Backfill: leaves NULL for now. The push_to_securo.py pipeline tool reads
the `instituicao` column from the parsed B3 / IBKR statements and populates
this field on the next bulk import.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "042"
down_revision: Union[str, None] = "041"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("assets",
                  sa.Column("custodian", sa.String(255), nullable=True))


def downgrade() -> None:
    op.drop_column("assets", "custodian")
