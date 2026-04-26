"""Portfolio snapshots API.

Endpoints:
    GET    /api/portfolio/snapshots         -- monthly time series for the user
    POST   /api/portfolio/snapshots/import  -- bulk import twr_full.csv
    DELETE /api/portfolio/snapshots         -- wipe all snapshots (re-import)
    GET    /api/portfolio/snapshots/has-data -- light check whether the user
                                                has imported snapshots
"""

import logging

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import current_active_user
from app.core.database import get_async_session
from app.models.user import User
from app.services import portfolio_snapshot_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/portfolio/snapshots",
                   tags=["portfolio-snapshots"])


@router.get("")
async def list_snapshots(
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    return await portfolio_snapshot_service.list_snapshots(session, user.id)


@router.get("/has-data")
async def has_snapshots(
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    return {
        "has_data": await portfolio_snapshot_service.has_snapshots(
            session, user.id),
    }


@router.post("/import", status_code=status.HTTP_200_OK)
async def import_snapshots(
    file: UploadFile = File(...),
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    """Upload a `twr_full.csv` (output of merge_twr_benchmarks.py) to populate
    the portfolio_snapshots table for the authenticated user.

    Existing rows for matching (user_id, month_end) are overwritten.
    """
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Esperado um arquivo .csv (saída do merge_twr_benchmarks.py).",
        )
    content = await file.read()
    if not content:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Arquivo vazio.",
        )
    try:
        result = await portfolio_snapshot_service.import_csv(
            session, user.id, content)
    except Exception as e:
        logger.exception("import_snapshots failed")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Falha ao importar: {e}",
        )
    return result


@router.delete("", status_code=status.HTTP_200_OK)
async def delete_snapshots(
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    deleted = await portfolio_snapshot_service.delete_all(session, user.id)
    return {"deleted": deleted}
