"""Portfolio snapshots — read & bulk import.

The offline TWR pipeline (parse_b3_*.py + compute_twr_v2.py +
merge_twr_benchmarks.py) generates `twr_full.csv`. This module accepts
that CSV via upload, upserts each row into `portfolio_snapshots`, and
exposes the time series to the frontend.

Expected CSV columns (extras tolerated, missing benchmarks left null):
    month, month_end, v_end_rv, v_end_rf, v_end_us, v_end,
    cashflow_month, income_month, return_month,
    twr_cum, twr_cum_bruto,
    ibov_cum, ivvb11_cum, sp500_cum, cdi_cum
"""

import csv
import io
import logging
import uuid
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Optional

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.portfolio_snapshot import PortfolioSnapshot

logger = logging.getLogger(__name__)


def _to_decimal(s: Optional[str]) -> Optional[Decimal]:
    if s is None or s == "":
        return None
    try:
        return Decimal(str(s).strip())
    except (InvalidOperation, ValueError):
        return None


def _to_date(s: str) -> Optional[date]:
    s = (s or "").strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


async def import_csv(session: AsyncSession, user_id: uuid.UUID,
                     csv_bytes: bytes) -> dict:
    """Parse CSV bytes and upsert by (user_id, month_end)."""
    text = csv_bytes.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))

    rows_to_upsert: list[dict] = []
    errors: list[str] = []
    line_no = 1
    for row in reader:
        line_no += 1
        me_iso = row.get("month_end") or ""
        me = _to_date(me_iso)
        if not me:
            errors.append(f"linha {line_no}: month_end inválido ({me_iso!r})")
            continue
        v_end_total = _to_decimal(row.get("v_end")) \
                      or _to_decimal(row.get("v_end_total")) \
                      or Decimal("0")
        twr_cum = _to_decimal(row.get("twr_cum")) or Decimal("0")
        twr_cum_bruto = _to_decimal(row.get("twr_cum_bruto")) or twr_cum

        rows_to_upsert.append({
            "user_id": user_id,
            "month_end": me,
            "v_end_rv": _to_decimal(row.get("v_end_rv")) or Decimal("0"),
            "v_end_rf": _to_decimal(row.get("v_end_rf")) or Decimal("0"),
            "v_end_us": _to_decimal(row.get("v_end_us")) or Decimal("0"),
            "v_end_total": v_end_total,
            "cashflow_month": _to_decimal(row.get("cashflow_month"))
                              or Decimal("0"),
            "income_month": _to_decimal(row.get("income_month"))
                            or Decimal("0"),
            "return_month": _to_decimal(row.get("return_month")),
            "twr_cum": twr_cum,
            "twr_cum_bruto": twr_cum_bruto,
            "ibov_cum": _to_decimal(row.get("ibov_cum")),
            "ivvb11_cum": _to_decimal(row.get("ivvb11_cum")),
            "sp500_cum": _to_decimal(row.get("sp500_cum")),
            "cdi_cum": _to_decimal(row.get("cdi_cum")),
            "source": "csv_import",
        })

    if not rows_to_upsert:
        return {"inserted": 0, "updated": 0, "errors": errors}

    # PostgreSQL upsert ON CONFLICT (user_id, month_end)
    stmt = pg_insert(PortfolioSnapshot).values(rows_to_upsert)
    update_cols = {
        "v_end_rv": stmt.excluded.v_end_rv,
        "v_end_rf": stmt.excluded.v_end_rf,
        "v_end_us": stmt.excluded.v_end_us,
        "v_end_total": stmt.excluded.v_end_total,
        "cashflow_month": stmt.excluded.cashflow_month,
        "income_month": stmt.excluded.income_month,
        "return_month": stmt.excluded.return_month,
        "twr_cum": stmt.excluded.twr_cum,
        "twr_cum_bruto": stmt.excluded.twr_cum_bruto,
        "ibov_cum": stmt.excluded.ibov_cum,
        "ivvb11_cum": stmt.excluded.ivvb11_cum,
        "sp500_cum": stmt.excluded.sp500_cum,
        "cdi_cum": stmt.excluded.cdi_cum,
        "source": stmt.excluded.source,
        "imported_at": datetime.utcnow(),
    }
    stmt = stmt.on_conflict_do_update(
        constraint="uq_portfolio_snapshots_user_month",
        set_=update_cols,
    )
    await session.execute(stmt)
    await session.commit()

    logger.info("imported %d portfolio snapshots for user %s",
                len(rows_to_upsert), user_id)
    return {"inserted_or_updated": len(rows_to_upsert), "errors": errors}


async def list_snapshots(session: AsyncSession, user_id: uuid.UUID
                         ) -> list[dict]:
    """Return all snapshots ordered by month_end ascending."""
    stmt = (select(PortfolioSnapshot)
            .where(PortfolioSnapshot.user_id == user_id)
            .order_by(PortfolioSnapshot.month_end))
    result = await session.execute(stmt)
    rows = result.scalars().all()

    def _f(d: Optional[Decimal]) -> Optional[float]:
        return float(d) if d is not None else None

    return [{
        "month_end": r.month_end.isoformat(),
        "month": r.month_end.strftime("%Y-%m"),
        "v_end_rv": _f(r.v_end_rv),
        "v_end_rf": _f(r.v_end_rf),
        "v_end_us": _f(r.v_end_us),
        "v_end_total": _f(r.v_end_total),
        "cashflow_month": _f(r.cashflow_month),
        "income_month": _f(r.income_month),
        "return_month": _f(r.return_month),
        "twr_cum": _f(r.twr_cum),
        "twr_cum_bruto": _f(r.twr_cum_bruto),
        "ibov_cum": _f(r.ibov_cum),
        "ivvb11_cum": _f(r.ivvb11_cum),
        "sp500_cum": _f(r.sp500_cum),
        "cdi_cum": _f(r.cdi_cum),
    } for r in rows]


async def has_snapshots(session: AsyncSession, user_id: uuid.UUID) -> bool:
    stmt = (select(PortfolioSnapshot.id)
            .where(PortfolioSnapshot.user_id == user_id)
            .limit(1))
    result = await session.execute(stmt)
    return result.scalar_one_or_none() is not None


async def delete_all(session: AsyncSession, user_id: uuid.UUID) -> int:
    """Wipe all snapshots for a user (used before re-importing from scratch)."""
    from sqlalchemy import delete as sa_delete
    stmt = sa_delete(PortfolioSnapshot).where(
        PortfolioSnapshot.user_id == user_id)
    result = await session.execute(stmt)
    await session.commit()
    return result.rowcount or 0
