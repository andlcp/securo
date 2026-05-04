"""asset_values: deduplicate (asset_id, date) and add UNIQUE constraint

Why now: re-runs of push_to_securo created duplicate AssetValue rows
for the same (asset_id, date) over multiple imports, distorting the
portfolio_timeseries walk. The audit found 41 duplicate rows on
production. This migration cleans them up (keeping the lowest id per
pair) and adds a UNIQUE constraint so future bulk imports either
update the existing row or fail loudly instead of silently piling up
extra copies.

Revision ID: 048
Revises: 047
Create Date: 2026-05-04
"""

from alembic import op


revision = "048"
down_revision = "047"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Defensive: drop any duplicates already in the DB before adding the
    # constraint. Idempotent — safe to run on a clean schema (deletes 0).
    op.execute(
        """
        DELETE FROM asset_values av1
        WHERE EXISTS (
            SELECT 1 FROM asset_values av2
            WHERE av2.asset_id = av1.asset_id
              AND av2.date = av1.date
              AND av2.id < av1.id
        )
        """
    )
    op.create_unique_constraint(
        "uq_asset_values_asset_date",
        "asset_values",
        ["asset_id", "date"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_asset_values_asset_date",
        "asset_values",
        type_="unique",
    )
