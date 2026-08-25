"""Auto-stamp `workspace_id` on financial entities when only `user_id` is set.

Provides backwards compatibility for code that pre-dates the workspace
column. When a row is inserted without `workspace_id` but with `user_id`,
the listener resolves the user's first workspace and fills it in. Once
the query layer + every caller passes `workspace_id` explicitly, the
listeners can be removed.

Resolution caches per `(session, user_id)` to avoid re-querying for the
common case of many rows being inserted under the same user (imports,
seed data, test fixtures).
"""
from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import event, inspect, select
from sqlalchemy.orm import Mapper, Session

from app.models.account import Account
from app.models.asset import Asset
from app.models.asset_group import AssetGroup
from app.models.asset_transaction import AssetTransaction
from app.models.asset_value import AssetValue
from app.models.bank_connection import BankConnection
from app.models.budget import Budget
from app.models.category import Category
from app.models.category_group import CategoryGroup
from app.models.credit_card_bill import CreditCardBill
from app.models.goal import Goal
from app.models.group import Group, GroupMember
from app.models.group_settlement import GroupSettlement
from app.models.import_log import ImportLog
from app.models.payee import Payee, PayeeMapping
from app.models.portfolio_daily_snapshot import PortfolioDailySnapshot
from app.models.portfolio_snapshot import PortfolioSnapshot
from app.models.recurring_transaction import RecurringTransaction
from app.models.rule import Rule
from app.models.transaction import Transaction
from app.models.transaction_attachment import TransactionAttachment
from app.models.transaction_split import TransactionSplit
from app.models.workspace import Workspace, WorkspaceMember


_AUTOSTAMP_MODELS = (
    Account,
    Asset,
    AssetGroup,
    AssetTransaction,
    AssetValue,
    BankConnection,
    Budget,
    Category,
    CategoryGroup,
    CreditCardBill,
    Goal,
    Group,
    GroupMember,
    GroupSettlement,
    ImportLog,
    Payee,
    PayeeMapping,
    PortfolioDailySnapshot,
    PortfolioSnapshot,
    RecurringTransaction,
    Rule,
    Transaction,
    TransactionAttachment,
    TransactionSplit,
)


def _register_agent_models() -> tuple:
    """Optional Agent + Conversation models: register only when the agents
    feature is enabled (the modules are import-gated by AGENTS_ENABLED)."""
    try:
        from app.agents.models.agent import Agent
        from app.agents.models.conversation import Conversation

        return (Agent, Conversation)
    except Exception:
        return ()


def _cache_key(session: Session, user_id: uuid.UUID) -> str:
    return f"_ws_autostamp:{id(session)}:{user_id}"


def _resolve_workspace_for_user(session: Session, user_id: uuid.UUID) -> uuid.UUID | None:
    """Find the user's first workspace. Synchronous — listener runs on the sync mapper."""
    # The listener is invoked from a sync-binding even in async sessions
    # (SQLAlchemy translates), so use the sync-style execute.
    row = session.execute(
        select(Workspace.id)
        .join(WorkspaceMember, WorkspaceMember.workspace_id == Workspace.id)
        .where(
            WorkspaceMember.user_id == user_id,
            Workspace.is_archived.is_(False),
        )
        .order_by(Workspace.created_at.asc())
        .limit(1)
    ).first()
    return row[0] if row else None


# Parent FK columns to consult in order — when a row's workspace_id is
# missing, look up the parent and inherit its workspace_id. Order
# matters: more specific parents come first.
_PARENT_LOOKUPS: tuple[tuple[str, str], ...] = (
    ("account_id", "app.models.account:Account"),
    ("connection_id", "app.models.bank_connection:BankConnection"),
    ("asset_id", "app.models.asset:Asset"),
    ("group_id", "app.models.group:Group"),
    ("payee_id", "app.models.payee:Payee"),
    ("transaction_id", "app.models.transaction:Transaction"),
    ("agent_id", "app.agents.models.agent:Agent"),
)


def _import_model(spec: str) -> Any:
    module_path, class_name = spec.split(":")
    import importlib

    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def _resolve_from_parent(session: Session, target: Any) -> uuid.UUID | None:
    """Walk parent FKs and return the first workspace_id we find."""
    for column, spec in _PARENT_LOOKUPS:
        parent_id = getattr(target, column, None)
        if parent_id is None:
            continue
        ParentModel = _import_model(spec)
        parent = session.get(ParentModel, parent_id)
        if parent is not None:
            ws = getattr(parent, "workspace_id", None)
            if ws is not None:
                return ws
    return None


def _before_insert(mapper: Mapper, connection: Any, target: Any) -> None:
    if getattr(target, "workspace_id", None) is not None:
        return
    # Use the sync session bound to this connection. Mapper events fire
    # inside a flush, so the session is reachable via the target's
    # InstanceState.
    from sqlalchemy.orm import object_session
    session = object_session(target)
    if session is None:
        return

    # Prefer the parent's workspace — this keeps synced rows in the
    # workspace that owns the bank connection / account / asset, which
    # may not be the user's *default* workspace.
    parent_ws = _resolve_from_parent(session, target)
    if parent_ws is not None:
        target.workspace_id = parent_ws
        return

    # Fallback: route the row to the user's first workspace. Correct
    # for single-workspace users (everyone today) and a safe default
    # for orphan-shaped rows.
    user_id = getattr(target, "user_id", None)
    if user_id is None:
        return
    cache_key = _cache_key(session, user_id)
    ws_id = getattr(session, cache_key, None)
    if ws_id is None:
        ws_id = _resolve_workspace_for_user(session, user_id)
        if ws_id is None:
            return
        setattr(session, cache_key, ws_id)
    target.workspace_id = ws_id



async def resolve_workspace_id(session, user_id: uuid.UUID) -> uuid.UUID | None:
    """Async twin of `_resolve_workspace_for_user`, for bulk-insert sites.

    Mapper events don't fire for `pg_insert(...)` bulk upserts, so the
    handful of services that write that way have to stamp `workspace_id`
    themselves. They call this.

    Deliberately uncached: bulk writes are batch operations (imports,
    snapshot rebuilds), so one indexed lookup per batch is noise.

    Step 2 of the upstream merge replaces this with
    `workspace_service.get_default_workspace`, which also handles the
    managed-workspace fallback introduced in upstream's migration 053.
    """
    result = await session.execute(
        select(Workspace.id)
        .join(WorkspaceMember, WorkspaceMember.workspace_id == Workspace.id)
        .where(
            WorkspaceMember.user_id == user_id,
            Workspace.is_archived.is_(False),
        )
        .order_by(Workspace.created_at.asc())
        .limit(1)
    )
    row = result.first()
    return row[0] if row else None

def _has_workspace_column(model) -> bool:
    """True when the model actually maps a `workspace_id` column.

    The fork adopted workspaces in migration 062, which covers the
    financial tables but not the agents ones (upstream does those in
    their own 054, still unmerged). Without this guard the listener
    would set an unmapped attribute on Agent / Conversation -- silently
    doing nothing, which is worse than not registering at all.
    """
    try:
        return "workspace_id" in inspect(model).columns
    except Exception:
        return False


def install_workspace_autostamp() -> None:
    """Idempotent: register the listener on each model exactly once."""
    for model in _AUTOSTAMP_MODELS + _register_agent_models():
        if not _has_workspace_column(model):
            continue
        if not event.contains(model, "before_insert", _before_insert):
            event.listen(model, "before_insert", _before_insert)


# Install immediately on import so it's active for the FastAPI app + tests.
install_workspace_autostamp()
