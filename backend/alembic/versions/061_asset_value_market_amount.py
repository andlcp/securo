"""asset_values.market_amount — valor a mercado ao lado do na curva

Revision ID: 061
Revises: 060
Create Date: 2026-08-19

Títulos marcados `rf_on_curve` passaram a gravar o valor de carrego em
`asset_values.amount` — é o número que deve dirigir patrimônio e TWR de
quem leva o papel até o vencimento. Mas o valor a mercado continua sendo
informação útil: mostra quanto o título valeria se resgatado hoje e o
tamanho da marcação.

Em vez de trocar um pelo outro, guardamos os dois na mesma linha:
`amount` segue sendo o valor oficial (na curva quando o título está
marcado assim, mercado caso contrário) e `market_amount` carrega o valor
a mercado quando ele é conhecido e diferente.

Coluna anulável de propósito: só é preenchida para Tesouro on-curve, pelo
refresh diário. Todo o resto (ações, FIIs, cripto, CDBs, RF a mercado)
deixa NULL, e o gráfico simplesmente não desenha a segunda linha.

Alternativa descartada: calcular a série de mercado sob demanda no
endpoint. O CSV do Tesouro Transparente tem ~13 MB e traz o histórico
inteiro — parsear isso por request é inviável, e o refresh diário já
baixa o arquivo de qualquer forma.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "061"
down_revision: Union[str, None] = "060"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "asset_values",
        sa.Column("market_amount", sa.Numeric(precision=15, scale=6), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("asset_values", "market_amount")
