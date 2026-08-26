"""assets.average_price + assets.realized_gain

Revision ID: 084
Revises: 083
Create Date: 2026-08-26

Resgata duas colunas que se perderam na renumeração do merge.

O `059_asset_transactions.py` do upstream fazia duas coisas ao mesmo tempo:
criava a tabela `asset_transactions` deles e adicionava estas duas colunas
em `assets`. Descartei a migration inteira porque a nossa tabela de
lançamentos, com dez tipos, já vem da 041 — e levei as colunas junto sem
perceber.

O modelo declara as duas, então toda consulta a `assets` passou a pedir
`assets.average_price` ao banco e a falhar com UndefinedColumnError. Efeito
na tela: Patrimônio vazio, "Nenhum ativo ainda", e o quadro de alocação sem
conteúdo — enquanto a tabela de Resultado, que lê os snapshots e não passa
por `assets`, seguia mostrando os números certos.

Ambas anuláveis e sem backfill, de propósito. Quem as preenche é
`asset_transaction_service.recompute_and_cache`, e só para ativos
precificados a mercado com histórico de compra. Enquanto ninguém dispara um
reprocessamento elas ficam NULL, que é o mesmo estado de antes do merge —
e nenhuma tela do fork as lê.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "084"
down_revision: Union[str, None] = "083"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "assets",
        sa.Column("average_price", sa.Numeric(precision=18, scale=6), nullable=True),
    )
    op.add_column(
        "assets",
        sa.Column("realized_gain", sa.Numeric(precision=18, scale=2), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("assets", "realized_gain")
    op.drop_column("assets", "average_price")
