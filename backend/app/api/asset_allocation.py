"""Asset allocation targets — endpoints powering the 'Onde Aportar'
widget on the dashboard. See asset_allocation_service for the bucket
classification and math; the API layer is just CRUD on the targets dict
stored in user.preferences."""
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import current_active_user
from app.core.database import get_async_session
from app.models.user import User
from app.services import asset_allocation_service

router = APIRouter(prefix="/api/asset-allocation", tags=["asset-allocation"])


class AssetAllocationCategory(BaseModel):
    id: str
    label: str
    total_brl: float
    current_pct: float
    target_pct: float
    delta_pp: float
    deficit_brl: float
    deficit_share_pct: float
    excluded: bool = False


class AssetAllocationResponse(BaseModel):
    primary_currency: str
    total_brl: float
    full_total_brl: float = 0.0
    categories: list[AssetAllocationCategory]
    targets_sum: float
    deficit_total_brl: float
    excluded_ids: list[str] = Field(default_factory=list)


class TargetsPayload(BaseModel):
    targets: dict[str, float] = Field(default_factory=dict)


class ExcludedPayload(BaseModel):
    excluded: list[str] = Field(default_factory=list)


class AportePlanCategory(BaseModel):
    id: str
    label: str
    current_brl: float
    current_pct: float
    target_pct: float
    aporte_brl: float
    aporte_share_pct: float
    result_brl: float
    result_pct: float
    result_delta_pp: float


class AportePlanResponse(BaseModel):
    primary_currency: str
    total_brl: float
    aporte_brl: float
    total_after_brl: float
    remaining_deficit_brl: float
    categories: list[AportePlanCategory]


@router.get("", response_model=AssetAllocationResponse)
async def get_allocation(
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
) -> dict:
    return await asset_allocation_service.compute_allocation(session, user)


@router.put("/targets", response_model=AssetAllocationResponse)
async def put_targets(
    payload: TargetsPayload,
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
) -> dict:
    user = await asset_allocation_service.save_targets(session, user, payload.targets)
    return await asset_allocation_service.compute_allocation(session, user)


@router.put("/excluded", response_model=AssetAllocationResponse)
async def put_excluded(
    payload: ExcludedPayload,
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
) -> dict:
    """Persist which buckets to leave out of the allocation math (e.g.
    'Outros' = family loans). Returns the recomputed allocation."""
    user = await asset_allocation_service.save_excluded(session, user, payload.excluded)
    return await asset_allocation_service.compute_allocation(session, user)


@router.get("/aporte-plan", response_model=AportePlanResponse)
async def get_aporte_plan(
    amount: float = Query(0.0, ge=0, description="Aporte amount in primary currency"),
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
) -> dict:
    """Given a one-shot contribution `amount`, return how to split it
    across under-target buckets and the resulting allocation (computed
    against the post-aporte total, so the % are the real outcome)."""
    return await asset_allocation_service.compute_aporte_plan(session, user, amount)
