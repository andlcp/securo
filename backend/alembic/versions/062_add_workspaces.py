"""workspaces + workspace_members + workspace_id nas tabelas financeiras

Revision ID: 062
Revises: 061
Create Date: 2026-08-24

Portado do `052_add_workspaces.py` do upstream (securo-finance/securo),
renumerado para o fim da nossa cadeia e estendido com as três tabelas que
só existem no fork: `asset_transactions`, `portfolio_snapshots` e
`portfolio_daily_snapshots`.

Estratégia
----------
1. Cria `workspaces` + `workspace_members`.
2. Para cada usuário existente, insere um workspace Pessoal e a
   associação de `owner`.
3. Adiciona `workspace_id` anulável em cada tabela financeira.
4. Preenche `workspace_id` a partir de `user_id` -> workspace Pessoal
   daquele usuário. Tabelas sem `user_id` próprio (asset_values,
   group_members, group_settlements, transaction_splits) herdam do pai.
5. Marca NOT NULL + índice + FK.

As colunas `user_id` continuam onde estão. Elas passam a significar
"created_by"/dono; a camada de consulta migra para `workspace_id` num
passo posterior. Enquanto as duas convivem, o listener em
`app/core/workspace_autostamp.py` mantém `workspace_id` preenchido em
inserts que só informam `user_id`.

Atenção: inserts em massa via `pg_insert` NÃO disparam eventos de mapper,
então os pontos que usam bulk upsert (asset_values em asset_service,
asset_transactions em asset_transaction_service e os dois serviços de
snapshot) preenchem `workspace_id` explicitamente.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "062"
down_revision: Union[str, None] = "061"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Tabelas que ganham `workspace_id` preenchido a partir do próprio
# `user_id`. As três últimas são exclusivas do fork.
TABLES_WITH_USER_ID = [
    "accounts",
    "asset_groups",
    "assets",
    "bank_connections",
    "budgets",
    "categories",
    "category_groups",
    "credit_card_bills",
    "goals",
    "groups",
    "import_logs",
    "payees",
    "recurring_transactions",
    "rules",
    "transaction_attachments",
    "transactions",
    "payee_mapping",
    "asset_transactions",
    "portfolio_snapshots",
    "portfolio_daily_snapshots",
]

# Tabelas sem `user_id`: (tabela, coluna FK, tabela pai)
PARENT_DERIVED = [
    ("asset_values", "asset_id", "assets"),
    ("group_members", "group_id", "groups"),
    ("group_settlements", "group_id", "groups"),
    ("transaction_splits", "transaction_id", "transactions"),
]


def _ws_column() -> sa.Column:
    return sa.Column(
        "workspace_id",
        postgresql.UUID(as_uuid=True),
        sa.ForeignKey("workspaces.id", ondelete="CASCADE"),
        nullable=True,
    )


def upgrade() -> None:
    # 1. workspaces
    op.create_table(
        "workspaces",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(100), nullable=False),
        sa.Column("kind", sa.String(30), nullable=False, server_default="personal"),
        sa.Column(
            "created_by_user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("is_archived", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("default_currency", sa.String(3), nullable=False, server_default="USD"),
        sa.Column("locale", sa.String(10), nullable=True),
        sa.Column("icon", sa.String(50), nullable=True),
        sa.Column("color", sa.String(7), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index("ix_workspaces_kind", "workspaces", ["kind"])

    # 2. workspace_members
    op.create_table(
        "workspace_members",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "workspace_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("workspaces.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("role", sa.String(20), nullable=False, server_default="owner"),
        sa.Column(
            "invited_by_user_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "joined_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("workspace_id", "user_id", name="uq_workspace_member"),
    )
    op.create_index("ix_workspace_members_user", "workspace_members", ["user_id"])
    op.create_index("ix_workspace_members_workspace", "workspace_members", ["workspace_id"])

    # 3. Um workspace Pessoal por usuário + associação de owner. O nome sai
    # localizado a partir de `preferences->>'language'`. gen_random_uuid()
    # exige pgcrypto; o Postgres 13+ já traz, mas deixamos explícito.
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    op.execute(
        """
        INSERT INTO workspaces (id, name, kind, created_by_user_id, default_currency, locale)
        SELECT
            gen_random_uuid(),
            CASE
                WHEN COALESCE(preferences->>'language', 'en') LIKE 'pt%' THEN 'Pessoal'
                ELSE 'Personal'
            END,
            'personal',
            id,
            COALESCE(preferences->>'currency_display', 'USD'),
            COALESCE(preferences->>'language', 'en')
        FROM users
        """
    )
    op.execute(
        """
        INSERT INTO workspace_members (id, workspace_id, user_id, role)
        SELECT gen_random_uuid(), w.id, w.created_by_user_id, 'owner'
        FROM workspaces w
        WHERE w.created_by_user_id IS NOT NULL
        """
    )

    # 4. Coluna anulável em cada tabela financeira.
    for tbl in TABLES_WITH_USER_ID:
        op.add_column(tbl, _ws_column())

    # 5. Backfill pelo user_id da própria linha (1:1 depois do passo 3).
    for tbl in TABLES_WITH_USER_ID:
        op.execute(
            f"""
            UPDATE {tbl} t
            SET workspace_id = w.id
            FROM workspaces w
            WHERE w.created_by_user_id = t.user_id
              AND w.kind = 'personal'
            """
        )

    # 6. NOT NULL + índice.
    for tbl in TABLES_WITH_USER_ID:
        op.alter_column(tbl, "workspace_id", nullable=False)
        op.create_index(f"ix_{tbl}_workspace_id", tbl, ["workspace_id"])

    # 7. Tabelas sem user_id: herdam o workspace do pai. Guardamos a coluna
    # (em vez de resolver por join a cada consulta) para que a checagem de
    # visibilidade por linha seja direta.
    for tbl, fk, parent in PARENT_DERIVED:
        op.add_column(tbl, _ws_column())
        op.execute(
            f"UPDATE {tbl} c SET workspace_id = p.workspace_id "
            f"FROM {parent} p WHERE p.id = c.{fk}"
        )
        op.alter_column(tbl, "workspace_id", nullable=False)
        op.create_index(f"ix_{tbl}_workspace_id", tbl, ["workspace_id"])


def downgrade() -> None:
    # Derruba as colunas workspace_id antes das tabelas (as FKs exigem).
    # Sem checar existencia: o upgrade adiciona a coluna nas 24 tabelas de
    # uma vez so, e o Postgres faz DDL transacional -- ou passou tudo, ou
    # nada ficou. Checar tabela a tabela so impediria gerar o SQL offline.
    for tbl in TABLES_WITH_USER_ID + [t for t, _, _ in PARENT_DERIVED]:
        op.drop_index(f"ix_{tbl}_workspace_id", table_name=tbl)
        op.drop_column(tbl, "workspace_id")

    op.drop_index("ix_workspace_members_workspace", table_name="workspace_members")
    op.drop_index("ix_workspace_members_user", table_name="workspace_members")
    op.drop_table("workspace_members")
    op.drop_index("ix_workspaces_kind", table_name="workspaces")
    op.drop_table("workspaces")
