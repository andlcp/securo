"""create investment portfolios, positions and price cache tables

Revision ID: 032
Revises: 031
Create Date: 2026-04-20
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = '032'
down_revision = '031'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'investment_portfolios',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('name', sa.String(100), nullable=False),
        sa.Column('color', sa.String(7), nullable=False, server_default='#6366F1'),
        sa.Column('description', sa.String(500), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_investment_portfolios_user_id', 'investment_portfolios', ['user_id'])

    op.create_table(
        'investment_positions',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('portfolio_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('ticker', sa.String(30), nullable=False),
        sa.Column('name', sa.String(200), nullable=False),
        sa.Column('asset_type', sa.String(20), nullable=False),
        sa.Column('currency', sa.String(3), nullable=False, server_default='BRL'),
        sa.Column('units', sa.Numeric(precision=15, scale=6), nullable=False),
        sa.Column('avg_price', sa.Numeric(precision=15, scale=6), nullable=False),
        sa.Column('broker', sa.String(100), nullable=True),
        sa.Column('notes', sa.String(500), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['portfolio_id'], ['investment_portfolios.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_investment_positions_portfolio_id', 'investment_positions', ['portfolio_id'])

    op.create_table(
        'investment_price_cache',
        sa.Column('ticker', sa.String(30), nullable=False),
        sa.Column('price', sa.Numeric(precision=15, scale=6), nullable=True),
        sa.Column('currency', sa.String(3), nullable=False),
        sa.Column('change_pct', sa.Numeric(precision=8, scale=4), nullable=True),
        sa.Column('long_name', sa.String(200), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('ticker'),
    )


def downgrade() -> None:
    op.drop_table('investment_price_cache')
    op.drop_index('ix_investment_positions_portfolio_id', table_name='investment_positions')
    op.drop_table('investment_positions')
    op.drop_index('ix_investment_portfolios_user_id', table_name='investment_portfolios')
    op.drop_table('investment_portfolios')
