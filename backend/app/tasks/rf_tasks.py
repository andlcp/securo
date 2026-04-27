"""Cron tasks for Renda Fixa (Tesouro Direto + CDBs).

Yahoo Finance does not quote Brazilian fixed-income, so the generic
`refresh_market_prices` job skips RF assets (they are stored as
`valuation_method='manual'`). This module fills that gap:

* **Tesouro Direto**: pulls the daily PU (`PU Base Manhã`) from the
  public Tesouro Transparente CSV and creates a fresh `AssetValue` row
  for every Tesouro asset still alive.
* **CDB**: there's no public quote source, so we compound CDI (BCB SGS
  série 12) at a configurable percentage against the asset's
  `purchase_price` and create a daily `AssetValue` row. Default 105% CDI
  (matches the user's stated minimum acceptable rate).

The task is idempotent: it upserts the AssetValue keyed by (asset, date),
so running multiple times the same day is safe.
"""

import asyncio
import csv
import datetime as dt
import io
import json
import logging
import re
import urllib.error
import urllib.request
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

from sqlalchemy import and_, delete as sa_delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import get_settings
from app.models.asset import Asset
from app.models.asset_value import AssetValue
from app.worker import celery_app

logger = logging.getLogger(__name__)


TESOURO_CSV_URL = (
    "https://www.tesourotransparente.gov.br/ckan/dataset/"
    "df56aa42-484a-4a59-8184-7676580c81e3/resource/"
    "796d2059-14e9-44e3-80c9-2d9e30b405c1/download/"
    "PrecoTaxaTesouroDireto.csv"
)

# Default percentage of CDI for CDB MTM. Matches the offline replay default.
DEFAULT_CDB_CDI_PCT = 1.05


def _make_session_maker():
    settings = get_settings()
    engine = create_async_engine(settings.database_url)
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


# -----------------------------------------------------------------------------
# Tesouro Transparente
# -----------------------------------------------------------------------------

# Map Asset.name -> (TipoTitulo, vencimento_iso). Only Tesouro names that the
# user has follow this prefix table.
_TESOURO_TIPO_PREFIX = [
    ("Tesouro Selic ", "Tesouro Selic"),
    ("Tesouro Prefixado com Juros Semestrais ", "Tesouro Prefixado com Juros Semestrais"),
    ("Tesouro Prefixado ", "Tesouro Prefixado"),
    ("Tesouro IPCA+ com Juros Semestrais ", "Tesouro IPCA+ com Juros Semestrais"),
    ("Tesouro IPCA+ ", "Tesouro IPCA+"),
]

# Hard-coded vencimentos used by the user (extends as portfolio evolves).
# Falls back to ANY vencimento with the right tipo+year if missing.
_TESOURO_HARDCODED_VENC = {
    "Tesouro Selic 2024": "2024-09-01",
    "Tesouro Selic 2025": "2025-03-01",
    "Tesouro Selic 2026": "2026-03-01",
    "Tesouro Selic 2027": "2027-03-01",
    "Tesouro Selic 2028": "2028-03-01",
    "Tesouro Selic 2029": "2029-03-01",
    "Tesouro Selic 2030": "2030-03-01",
    "Tesouro Selic 2031": "2031-03-01",
    "Tesouro Prefixado 2026": "2026-01-01",
    "Tesouro Prefixado 2027": "2027-01-01",
    "Tesouro Prefixado 2028": "2028-01-01",
    "Tesouro IPCA+ 2026": "2026-08-15",
    "Tesouro IPCA+ 2029": "2029-05-15",
    "Tesouro IPCA+ 2035": "2035-05-15",
    "Tesouro IPCA+ 2040": "2040-08-15",
    "Tesouro IPCA+ 2045": "2045-05-15",
    "Tesouro IPCA+ com Juros Semestrais 2030": "2030-08-15",
    "Tesouro IPCA+ com Juros Semestrais 2035": "2035-05-15",
    "Tesouro IPCA+ com Juros Semestrais 2040": "2040-08-15",
    "Tesouro IPCA+ com Juros Semestrais 2045": "2045-05-15",
    "Tesouro IPCA+ com Juros Semestrais 2050": "2050-08-15",
    "Tesouro IPCA+ com Juros Semestrais 2055": "2055-05-15",
}


def _name_to_tipo_year(name: str) -> Optional[tuple[str, int]]:
    """('Tesouro Selic 2027') -> ('Tesouro Selic', 2027)."""
    for prefix, tipo in _TESOURO_TIPO_PREFIX:
        if name.startswith(prefix):
            year_str = name[len(prefix):].strip().split()[0]
            try:
                return tipo, int(year_str)
            except ValueError:
                return None
    return None


def _br_decimal(s: str) -> float:
    return float(s.strip().replace(".", "").replace(",", "."))


def _br_date(s: str) -> str:
    return dt.datetime.strptime(s.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")


def _fetch_tesouro_pu_today() -> dict[tuple[str, str], float]:
    """Fetch latest PU per (tipo, vencimento_iso) from Tesouro Transparente.

    Returns {(tipo, vencimento_iso): pu_today}. The CSV is large (~13 MB);
    we stream and keep only the most recent row per (tipo, vencimento).
    """
    req = urllib.request.Request(
        TESOURO_CSV_URL, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            raw = r.read()
    except urllib.error.URLError as e:
        logger.error("Tesouro CSV download failed: %s", e)
        return {}

    text = None
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            t = raw.decode(enc)
            if "Tipo Titulo" in t:
                text = t
                break
        except UnicodeDecodeError:
            continue
    if text is None:
        logger.error("Could not decode Tesouro CSV")
        return {}

    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    latest: dict[tuple[str, str], dict] = {}
    for row in reader:
        tipo = (row.get("Tipo Titulo") or "").strip()
        if not tipo:
            continue
        try:
            venc = _br_date(row["Data Vencimento"])
            base = _br_date(row["Data Base"])
        except Exception:
            continue
        pu_str = (row.get("PU Base Manha") or row.get("PU Base Manhã")
                  or row.get("PU Venda Manha") or row.get("PU Venda Manhã")
                  or row.get("PU Compra Manha") or row.get("PU Compra Manhã"))
        if not pu_str:
            continue
        try:
            pu = _br_decimal(pu_str)
        except Exception:
            continue
        key = (tipo, venc)
        prev = latest.get(key)
        if prev is None or base > prev["base"]:
            latest[key] = {"base": base, "pu": pu}
    return {k: v["pu"] for k, v in latest.items()}


async def _refresh_tesouro_assets() -> dict[str, int]:
    session_maker = _make_session_maker()
    today = date.today()

    pus = _fetch_tesouro_pu_today()
    if not pus:
        return {"refreshed": 0, "skipped": 0, "no_pu": 0}

    refreshed = skipped = no_pu = 0
    async with session_maker() as session:
        result = await session.execute(
            select(Asset).where(
                Asset.type == "investment",
                Asset.valuation_method == "manual",
                Asset.is_archived == False,    # noqa: E712
                Asset.sell_date.is_(None),
                Asset.name.like("Tesouro%"),
            )
        )
        assets = list(result.scalars().all())

        for asset in assets:
            # Skip if already matured
            if asset.maturity_date and asset.maturity_date < today:
                skipped += 1
                continue
            tipo_year = _name_to_tipo_year(asset.name)
            if not tipo_year:
                no_pu += 1
                continue
            tipo, _year = tipo_year
            venc = (asset.maturity_date.isoformat() if asset.maturity_date
                    else _TESOURO_HARDCODED_VENC.get(asset.name))
            if not venc:
                no_pu += 1
                continue
            pu = pus.get((tipo, venc))
            if pu is None:
                no_pu += 1
                continue
            qty = float(asset.units or 0)
            if qty <= 0:
                skipped += 1
                continue
            amount = Decimal(str(round(qty * pu, 2)))

            # Upsert by (asset, today)
            await session.execute(
                sa_delete(AssetValue).where(
                    AssetValue.asset_id == asset.id,
                    AssetValue.date == today,
                )
            )
            session.add(AssetValue(
                asset_id=asset.id, amount=amount,
                date=today, source="rule"))
            # Bonus: also write the raw last_price / at fields so the UI
            # can show "atualizado em DD/MM/YYYY"
            asset.last_price = Decimal(str(round(pu, 6)))
            asset.last_price_at = dt.datetime.now(dt.timezone.utc)
            refreshed += 1

        await session.commit()
    return {"refreshed": refreshed, "skipped": skipped, "no_pu": no_pu}


# -----------------------------------------------------------------------------
# CDB (CDI compound)
# -----------------------------------------------------------------------------

BCB_SGS_URL = (
    "https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados"
    "?formato=json&dataInicial={start}&dataFinal={end}"
)


def _fetch_cdi_factors(start: date, end: date) -> dict[str, float]:
    url = BCB_SGS_URL.format(
        start=start.strftime("%d/%m/%Y"),
        end=end.strftime("%d/%m/%Y"),
    )
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            rows = json.load(r)
    except urllib.error.URLError as e:
        logger.warning("BCB CDI fetch failed: %s", e)
        return {}
    out: dict[str, float] = {}
    for row in rows:
        d = dt.datetime.strptime(row["data"], "%d/%m/%Y").date()
        out[d.isoformat()] = 1.0 + float(row["valor"]) / 100.0
    return out


async def _refresh_cdb_assets(cdi_pct: float = DEFAULT_CDB_CDI_PCT
                              ) -> dict[str, int]:
    session_maker = _make_session_maker()
    today = date.today()
    refreshed = skipped = 0
    async with session_maker() as session:
        result = await session.execute(
            select(Asset).where(
                Asset.type == "investment",
                Asset.valuation_method == "manual",
                Asset.is_archived == False,    # noqa: E712
                Asset.sell_date.is_(None),
                Asset.name.like("CDB%"),
            )
        )
        assets = list(result.scalars().all())
        if not assets:
            return {"refreshed": 0, "skipped": 0}

        # Fetch CDI factors for the broadest range needed
        earliest = min((a.purchase_date or today for a in assets), default=today)
        cdi = _fetch_cdi_factors(earliest, today)
        if not cdi:
            logger.warning("CDB refresh: BCB CDI unavailable, skipping")
            return {"refreshed": 0, "skipped": len(assets)}

        for asset in assets:
            buy_date = asset.purchase_date
            buy_price = float(asset.purchase_price or 0)
            qty = float(asset.units or 0)
            if not buy_date or buy_price <= 0 or qty <= 0:
                skipped += 1
                continue

            # Compound cdi_pct * CDI from buy_date (exclusive) to today (inclusive)
            factor = 1.0
            for d_iso, fac in cdi.items():
                if d_iso > buy_date.isoformat() and d_iso <= today.isoformat():
                    daily_r = (fac - 1.0) * cdi_pct
                    factor *= (1.0 + daily_r)

            # Reuse purchase_price as the "PU per unit" anchor; modeling here
            # mirrors replay_renda_fixa.py.
            mtm = qty * buy_price * factor
            amount = Decimal(str(round(mtm, 2)))

            await session.execute(
                sa_delete(AssetValue).where(
                    AssetValue.asset_id == asset.id,
                    AssetValue.date == today,
                )
            )
            session.add(AssetValue(
                asset_id=asset.id, amount=amount,
                date=today, source="rule"))
            asset.last_price = Decimal(str(round(buy_price * factor, 6)))
            asset.last_price_at = dt.datetime.now(dt.timezone.utc)
            refreshed += 1

        await session.commit()
    return {"refreshed": refreshed, "skipped": skipped}


# -----------------------------------------------------------------------------
# Celery entry-points
# -----------------------------------------------------------------------------

@celery_app.task(name="app.tasks.rf_tasks.refresh_tesouro_assets")
def refresh_tesouro_assets() -> dict:
    try:
        result = asyncio.run(_refresh_tesouro_assets())
    except Exception:
        logger.exception("Tesouro refresh failed")
        return {"error": True, "refreshed": 0}
    logger.info("Tesouro refresh: %s", result)
    return result


@celery_app.task(name="app.tasks.rf_tasks.refresh_cdb_assets")
def refresh_cdb_assets() -> dict:
    try:
        result = asyncio.run(_refresh_cdb_assets())
    except Exception:
        logger.exception("CDB refresh failed")
        return {"error": True, "refreshed": 0}
    logger.info("CDB refresh: %s", result)
    return result
