"""Workspaces — criação e resolução do workspace padrão.

Versão reduzida do `workspace_service.py` do upstream, trazida junto com
a migration 062. Contém só o que o fork precisa hoje: criar o workspace
Pessoal de um usuário novo e achar o workspace padrão de alguém.

O arquivo do upstream tem ~434 linhas e cobre convites, papéis, arquivamento
e "managed workspaces" (que dependem da coluna `managed_by_user_id`, da
migration 053). Nada disso existe aqui ainda. O nome e as assinaturas são
propositalmente iguais aos do upstream: no passo 2 do merge este arquivo
é substituído pelo deles inteiro, sem precisar reescrever chamador nenhum.
"""
import uuid
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.user import User
from app.models.workspace import Workspace, WorkspaceMember


def _resolve_personal_name(lang: Optional[str]) -> str:
    """Nome padrão localizado do workspace Pessoal criado automaticamente."""
    if lang and lang.lower().startswith("pt"):
        return "Pessoal"
    return "Personal"


async def create_personal_workspace_for_user(
    session: AsyncSession,
    user: User,
    *,
    commit: bool = False,
) -> Workspace:
    """Cria o workspace Pessoal do usuário + associação de `owner`.

    Idempotente: se o usuário já tem um workspace que ele criou e do qual
    participa (por exemplo, criado pela migration), devolve o mais antigo.

    A checagem casa por identidade, não por `kind`: o workspace de
    bootstrap é o primeiro que o usuário criou e ao qual pertence. Filtrar
    por `kind == "personal"` também pegaria um segundo workspace pessoal
    criado à mão, e qual dos dois voltaria dependeria da ordem das linhas.

    Quem chama é responsável pelo commit, a menos que passe `commit=True`.
    O padrão é só dar flush porque em geral isso entra na mesma transação
    que cria o usuário.
    """
    existing = await session.execute(
        select(Workspace)
        .join(WorkspaceMember, WorkspaceMember.workspace_id == Workspace.id)
        .where(
            WorkspaceMember.user_id == user.id,
            Workspace.created_by_user_id == user.id,
        )
        .order_by(Workspace.created_at.asc())
        .limit(1)
    )
    found = existing.scalar_one_or_none()
    if found:
        return found

    prefs = user.preferences or {}
    lang = prefs.get("language")
    workspace = Workspace(
        name=_resolve_personal_name(lang),
        kind="personal",
        created_by_user_id=user.id,
        default_currency=prefs.get("currency_display", "USD"),
        locale=lang,
    )
    session.add(workspace)
    await session.flush()

    membership = WorkspaceMember(
        workspace_id=workspace.id,
        user_id=user.id,
        role="owner",
    )
    session.add(membership)
    await session.flush()
    if commit:
        await session.commit()
    return workspace


async def get_default_workspace(
    session: AsyncSession, user_id: uuid.UUID
) -> Optional[Workspace]:
    """Workspace mais antigo do usuário — o padrão quando nada é informado.

    A versão do upstream ainda cai para um workspace administrado quando o
    usuário não é membro de nenhum; aqui isso não existe (não temos
    `managed_by_user_id`), então a busca é só pelas associações.
    """
    result = await session.execute(
        select(Workspace)
        .join(WorkspaceMember, WorkspaceMember.workspace_id == Workspace.id)
        .where(
            WorkspaceMember.user_id == user_id,
            Workspace.is_archived.is_(False),
        )
        .order_by(Workspace.created_at.asc())
        .limit(1)
    )
    return result.scalar_one_or_none()
