"""Asset allocation targets — endpoints powering the 'Onde Aportar'
widget on the dashboard. See asset_allocation_service for the bucket
classification and math; the API layer is just CRUD on the targets dict
stored in user.preferences."""
from fastapi import APIRouter, Depends
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


class AssetAllocationResponse(BaseModel):
    primary_currency: str
    total_brl: float
    categories: list[AssetAllocationCategory]
    targets_sum: float
    deficit_total_brl: float


class TargetsPayload(BaseModel):
    targets: dict[str, float] = Field(default_factory=dict)


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
