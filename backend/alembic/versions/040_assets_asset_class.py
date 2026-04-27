"""add asset_class column to assets

Revision ID: 040
Revises: 039
Create Date: 2026-04-26

Explicit asset class taxonomy on every asset, instead of heuristic detection
from the ticker. The frontend Add Asset form lets the user pick from this
list directly, and the investments dashboard groups/filters by these values.

Values used by the application (validated in Pydantic, not the DB):
    RENDA_VARIAVEL_BR  -- B3 stocks/ETFs (PETR4, IVVB11, BOVA11, ...)
    RENDA_FIXA         -- Tesouro Direto + CDBs + LCIs + LCAs
    STOCKS_US          -- IBKR / Avenue / Schwab US-listed stocks & ETFs
    FIIS               -- Fundos Imobiliários (B3 tickers ending in 11
                          that are FII, distinct from BR ETFs)
    CRIPTO             -- BTC, ETH, etc.
    OUTRO              -- catch-all (real estate, valuables, manual)

Backfill best-effort:
    - investment-typed assets with .SA ticker ending in 3/4/5/6 -> RENDA_VARIAVEL_BR
    - tickers ending in 11 not in known ETF list -> FIIS
    - tickers in known crypto set -> CRIPTO
    - tickers without .SA suffix and currency=USD -> STOCKS_US
    - assets with valuation_method='manual' AND maturity_date set
      (or name starting with 'Tesouro' / 'CDB') -> RENDA_FIXA
    - everything else -> OUTRO
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "040"
down_revision: Union[str, None] = "039"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "assets",
        sa.Column("asset_class", sa.String(32), nullable=True),
    )

    # Best-effort backfill (idempotent — re-running does nothing harmful).
    # Crypto first to avoid being caught by other rules.
    op.execute("""
        UPDATE assets
        SET asset_class = 'CRIPTO'
        WHERE upper(coalesce(ticker, '')) IN ('BTC', 'ETH', 'BTC-USD', 'ETH-USD',
                                              'SOL', 'SOL-USD', 'ADA', 'ADA-USD')
           OR upper(coalesce(ticker, '')) LIKE '%-USD'
    """)

    # FIIs: B3 ticker ending in 11 and not a known ETF
    op.execute("""
        UPDATE assets SET asset_class = 'FIIS'
        WHERE asset_class IS NULL
          AND ticker IS NOT NULL
          AND upper(ticker) ~ '^[A-Z]{4}11(\\.SA)?$'
          AND upper(replace(ticker, '.SA', '')) NOT IN (
              'IVVB11','BOVA11','SMAL11','SPXI11','HASH11','GOLD11',
              'NTNB11','IRFM11','DIVO11','FIND11','GOVE11','MATB11',
              'BOVB11','BOVS11','BOVV11','ECOO11','ISUS11','PIBB11'
          )
    """)

    # Renda Variável BR: B3 ticker (PETR4, VALE3 …) or known BR ETFs
    op.execute("""
        UPDATE assets SET asset_class = 'RENDA_VARIAVEL_BR'
        WHERE asset_class IS NULL
          AND ticker IS NOT NULL
          AND (
              upper(ticker) LIKE '%.SA'
              OR upper(replace(ticker, '.SA', '')) IN (
                  'IVVB11','BOVA11','SMAL11','SPXI11','HASH11','GOLD11',
                  'NTNB11','IRFM11','DIVO11','FIND11','GOVE11','MATB11',
                  'BOVB11','BOVS11','BOVV11','ECOO11','ISUS11','PIBB11'
              )
          )
    """)

    # Stocks US: market_price asset with non-BRL currency or ticker without .SA
    op.execute("""
        UPDATE assets SET asset_class = 'STOCKS_US'
        WHERE asset_class IS NULL
          AND valuation_method = 'market_price'
          AND ticker IS NOT NULL
          AND upper(ticker) NOT LIKE '%.SA'
          AND (currency = 'USD' OR upper(ticker_exchange) IN ('NASDAQ','NYSE','ARCA','AMEX'))
    """)

    # Renda Fixa: manual valuation + maturity OR name hints (Tesouro / CDB)
    op.execute("""
        UPDATE assets SET asset_class = 'RENDA_FIXA'
        WHERE asset_class IS NULL
          AND type = 'investment'
          AND (
              maturity_date IS NOT NULL
              OR upper(coalesce(name, '')) LIKE 'TESOURO %'
              OR upper(coalesce(name, '')) LIKE 'CDB %'
              OR upper(coalesce(name, '')) LIKE 'LCI %'
              OR upper(coalesce(name, '')) LIKE 'LCA %'
              OR upper(coalesce(name, '')) LIKE 'CRA %'
              OR upper(coalesce(name, '')) LIKE 'CRI %'
              OR upper(coalesce(name, '')) LIKE 'DEBÊNTURE%'
              OR upper(coalesce(name, '')) LIKE 'DEBENTURE%'
          )
    """)

    # Everything left in 'investment' -> OUTRO
    op.execute("""
        UPDATE assets SET asset_class = 'OUTRO'
        WHERE asset_class IS NULL AND type = 'investment'
    """)

    # Index for filter performance
    op.create_index(
        "ix_assets_user_class",
        "assets", ["user_id", "asset_class"],
    )


def downgrade() -> None:
    op.drop_index("ix_assets_user_class", table_name="assets")
    op.drop_column("assets", "asset_class")
