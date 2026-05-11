"""local cache for asset logo images

Revision ID: 051
Revises: 050
Create Date: 2026-05-11

Stores the favicon/logo for each asset directly in the database (small
BYTEA, typically 1-3 KB each) so the frontend can request it from our
backend instead of hitting t0.gstatic.com/faviconV2 per asset on every
page load. That endpoint:
  - Throttles at ~6 connections per host on HTTP/1.1, serializing
    requests on a 100+ asset portfolio for tens of seconds.
  - Returns 404 for tickers Google doesn't index (BR FIIs, smaller
    B3 stocks), polluting the user's DevTools console.
  - Has external uptime risk on a page that should work offline-ish.

After backfill, the frontend hits `/api/assets/{id}/icon` which serves
the cached blob with `Cache-Control: public, max-age=31536000,
immutable` — first request downloads, subsequent loads are 304s.

Both columns are nullable. Assets we haven't cached yet keep their
external logo_url; asset cards fall back to the type icon if both are
missing.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "051"
down_revision: Union[str, None] = "050"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("assets",
                  sa.Column("logo_data", sa.LargeBinary(), nullable=True))
    op.add_column("assets",
                  sa.Column("logo_content_type", sa.String(60), nullable=True))


def downgrade() -> None:
    op.drop_column("assets", "logo_content_type")
    op.drop_column("assets", "logo_data")
