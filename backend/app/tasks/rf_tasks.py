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
    "Tesouro IPCA+ 2032": "2032-08-15",
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
                # `investment` was the legacy generic type before we split
                # RF into its own `fixed_income` badge. Keep both so the
                # task still picks up assets imported before the
                # reclassification migration ran.
                Asset.type.in_(("investment", "fixed_income")),
                Asset.valuation_method == "manual",
                Asset.is_archived == False,    # noqa: E712
                Asset.sell_date.is_(None),
                Asset.name.like("Tesouro%"),
            )
        )
        assets = list(result.scalars().all())

        # Pre-fetch BCB series only if some title is marked on-curve and so
        # needs its contracted-rate accrual (IPCA / CDI) instead of market PU.
        on_curve_assets = [
            a for a in assets
            if a.rf_on_curve and a.rf_indexer and a.rf_rate_pct is not None
            and a.purchase_date and a.purchase_price
        ]
        cdi: dict[str, float] = {}
        ipca: dict[str, float] = {}
        if on_curve_assets:
            earliest = min((a.purchase_date for a in on_curve_assets),
                           default=today)
            if any((a.rf_indexer or "").upper() == "CDI" for a in on_curve_assets):
                cdi = _fetch_sgs_series(12, earliest, today)
            if any((a.rf_indexer or "").upper() == "IPCA" for a in on_curve_assets):
                ipca = _fetch_sgs_series(433, earliest, today)

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
            qty = float(asset.units or 0)
            if qty <= 0:
                skipped += 1
                continue

            # On-curve path: value by the contracted rate (carrego), not the
            # market PU. A hold-to-maturity title shouldn't swing with the
            # daily rate cycle. Falls through to market PU if the curve can't
            # be built (missing rate/index) or the BCB series was unavailable.
            pu_for_stamp: Optional[float] = None
            amount: Optional[Decimal] = None
            if asset.rf_on_curve:
                factor = _on_curve_factor(asset, today, cdi, ipca)
                if factor is not None:
                    buy_price = float(asset.purchase_price or 0)
                    amount = Decimal(str(round(qty * buy_price * factor, 2)))
                    pu_for_stamp = buy_price * factor

            if amount is None:
                # Market PU path (default).
                venc = (asset.maturity_date.isoformat() if asset.maturity_date
                        else _TESOURO_HARDCODED_VENC.get(asset.name))
                if not venc:
                    no_pu += 1
                    continue
                pu = pus.get((tipo, venc))
                if pu is None:
                    no_pu += 1
                    continue
                amount = Decimal(str(round(qty * pu, 2)))
                pu_for_stamp = pu

            # Don't overwrite a user-entered (manual) value for today.
            # Reconciliation flows paste broker-bruto values directly and
            # those should win against the daily Tesouro Transparente
            # snapshot (which trails by ~3 business days anyway).
            existing_today = await session.scalar(
                select(AssetValue).where(
                    AssetValue.asset_id == asset.id,
                    AssetValue.date == today,
                )
            )
            if existing_today is not None and existing_today.source == "manual":
                skipped += 1
                continue

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
            # can show "atualizado em DD/MM/YYYY". For on-curve titles this
            # is the carrego per-unit PU, not the market PU.
            if pu_for_stamp is not None:
                asset.last_price = Decimal(str(round(pu_for_stamp, 6)))
            asset.last_price_at = dt.datetime.now(dt.timezone.utc)
            refreshed += 1

        await session.commit()
    return {"refreshed": refreshed, "skipped": skipped, "no_pu": no_pu}


# -----------------------------------------------------------------------------
# CDB / RF privada (CDI / IPCA / PRE compound)
# -----------------------------------------------------------------------------

# BCB SGS series 12 = CDI diário (% a.d.)
# BCB SGS series 433 = IPCA mensal (% a.m.)
BCB_SGS_URL = (
    "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series}/dados"
    "?formato=json&dataInicial={start}&dataFinal={end}"
)


def _fetch_sgs_series(series: int, start: date, end: date) -> dict[str, float]:
    """Fetch a BCB SGS series; returns {YYYY-MM-DD: factor=1+pct/100}."""
    url = BCB_SGS_URL.format(
        series=series,
        start=start.strftime("%d/%m/%Y"),
        end=end.strftime("%d/%m/%Y"),
    )
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            rows = json.load(r)
    except urllib.error.URLError as e:
        logger.warning("BCB SGS %d fetch failed: %s", series, e)
        return {}
    out: dict[str, float] = {}
    for row in rows:
        d = dt.datetime.strptime(row["data"], "%d/%m/%Y").date()
        out[d.isoformat()] = 1.0 + float(row["valor"]) / 100.0
    return out


def _compound_cdi(cdi: dict[str, float], buy_iso: str, today_iso: str,
                  pct_of_cdi: float) -> float:
    """Compound `pct_of_cdi%` of daily CDI from buy (exclusive) to today
    (inclusive). pct_of_cdi=1.05 means 105 % do CDI; 1.09 means 109 %."""
    factor = 1.0
    for d_iso, fac in cdi.items():
        if d_iso > buy_iso and d_iso <= today_iso:
            daily_r = (fac - 1.0) * pct_of_cdi
            factor *= (1.0 + daily_r)
    return factor


def _compound_pre(buy_date: date, today: date, annual_rate_pct: float) -> float:
    """Compound a fixed annual rate from buy_date to today using business-day
    convention (252 business days/year, the Brazilian RF standard)."""
    # Business-day count between dates (Mon-Fri, ignores public holidays —
    # close enough for personal portfolio tracking; ~5-10 holidays/year).
    days = 0
    d = buy_date
    one = dt.timedelta(days=1)
    while d < today:
        d += one
        if d.weekday() < 5:
            days += 1
    daily_factor = (1.0 + annual_rate_pct / 100.0) ** (1.0 / 252.0)
    return daily_factor ** days


def _compound_ipca(ipca: dict[str, float], buy_date: date, today: date,
                   spread_pct: float) -> float:
    """Compound IPCA accumulated from buy_date to today + an annual spread
    above IPCA. IPCA is monthly (BCB SGS 433); we apply each month whose
    *reference* falls in (buy_month, today_month]. This is approximate but
    matches Tesouro Direto's own NTN-B accrual close enough for tracking."""
    # Walk months: collect cumulative IPCA factor for the period.
    ipca_factor = 1.0
    cur = date(buy_date.year, buy_date.month, 1)
    end = date(today.year, today.month, 1)
    while cur <= end:
        # SGS dates the monthly IPCA at the 1st of the month.
        key = cur.isoformat()
        if cur > date(buy_date.year, buy_date.month, 1) and key in ipca:
            ipca_factor *= ipca[key]
        # Advance one month
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)
    # Pro-rata partial month (from purchase day in the buy_month):
    # we approximate as full-month inclusion above; the slight over/under
    # cancels in practice.
    days = (today - buy_date).days
    years = days / 365.25
    spread_factor = (1.0 + spread_pct / 100.0) ** years
    return ipca_factor * spread_factor


def _on_curve_factor(
    asset: "Asset",
    on_date: date,
    cdi: dict[str, float],
    ipca: dict[str, float],
) -> Optional[float]:
    """Carrego factor from purchase to `on_date` using the contracted rate.

    This is the same accrual math refresh_cdb_assets uses, factored out so
    the Tesouro refresh can value an on-curve title (rf_on_curve=True) by
    its contracted rate instead of the market PU. Returns None when the
    metadata needed to build the curve is incomplete (caller then falls
    back to market PU).
    """
    if not (asset.rf_indexer and asset.rf_rate_pct is not None
            and asset.purchase_date and asset.purchase_price):
        return None
    indexer = asset.rf_indexer.upper()
    rate = float(asset.rf_rate_pct)
    buy = asset.purchase_date
    if indexer == "PRE":
        return _compound_pre(buy, on_date, rate)
    if indexer == "CDI":
        return _compound_cdi(cdi, buy.isoformat(), on_date.isoformat(),
                             rate / 100.0) if cdi else None
    if indexer == "IPCA":
        return _compound_ipca(ipca, buy, on_date, rate) if ipca else None
    return None


async def backfill_on_curve_history(session, asset: "Asset") -> int:
    """Re-value an asset's non-manual AssetValue rows na curva (contracted-
    rate accrual) from purchase to today.

    Called when rf_on_curve is switched on, so the chart shows the smooth
    carrego historically instead of a market-then-curve discontinuity on
    the toggle day (which the snapshot rebuild would otherwise read as a
    phantom jump). Manual AVs (broker-bruto reconciliation snapshots) are
    left intact. Single-purchase approximation: uses current units × avg
    purchase_price × factor — exact for the typical one-buy Tesouro
    position; a multi-tranche title's early dates are slightly overstated.

    Returns the number of AV rows rewritten.
    """
    if not (asset.rf_on_curve and asset.rf_indexer and asset.rf_rate_pct is not None
            and asset.purchase_date and asset.purchase_price):
        return 0
    today = date.today()
    indexer = asset.rf_indexer.upper()
    cdi = _fetch_sgs_series(12, asset.purchase_date, today) if indexer == "CDI" else {}
    ipca = _fetch_sgs_series(433, asset.purchase_date, today) if indexer == "IPCA" else {}
    qty = float(asset.units or 0)
    buy_price = float(asset.purchase_price or 0)
    if qty <= 0 or buy_price <= 0:
        return 0

    rows = (await session.execute(
        select(AssetValue).where(
            AssetValue.asset_id == asset.id,
            AssetValue.date >= asset.purchase_date,
            AssetValue.source != "manual",
        )
    )).scalars().all()
    n = 0
    for av in rows:
        factor = _on_curve_factor(asset, av.date, cdi, ipca)
        if factor is None:
            continue
        av.amount = Decimal(str(round(qty * buy_price * factor, 2)))
        av.source = "rule"
        n += 1
    await session.commit()
    return n


async def _refresh_cdb_assets() -> dict[str, int]:
    """Mark CDB / LCI / LCA positions to market based on each asset's
    contracted rate metadata.

    Required per-asset metadata (rf_indexer + rf_rate_pct):
      PRE  -> compound annual rate over business days
      CDI  -> compound (rf_rate_pct % of CDI) daily
      IPCA -> compound IPCA monthly + spread annually

    Assets without rf_indexer set are SKIPPED — the previous 105 % CDI
    heuristic was retired because it was overestimating MtM by ~1.5 %
    on the user's mix of IPCA+ and prefixado contracts. Skipping
    preserves the most recent AssetValue for that asset (typically a
    manual broker-bruto entry) instead of stamping a wrong value over
    it. New CDBs created via the form must specify rf_indexer + rate
    to receive automatic daily MtM updates.

    The task NEVER overwrites an AssetValue with `source='manual'` for
    today — that's the user's deliberate snapshot (e.g. broker bruto
    pasted in during reconciliation), and the daily computed value
    must lose to it. Both manual entries and rule entries coexist
    historically; the visibility of which is shown today is determined
    by whichever one we leave intact.
    """
    session_maker = _make_session_maker()
    today = date.today()
    today_iso = today.isoformat()
    refreshed = skipped_manual = skipped_no_metadata = skipped_invalid = 0
    async with session_maker() as session:
        result = await session.execute(
            select(Asset).where(
                # Mirrors the Tesouro refresh: both legacy `investment` and
                # current `fixed_income` types are accepted.
                Asset.type.in_(("investment", "fixed_income")),
                Asset.valuation_method == "manual",
                Asset.is_archived == False,    # noqa: E712
                Asset.sell_date.is_(None),
                Asset.name.like("CDB%"),
            )
        )
        assets = list(result.scalars().all())
        if not assets:
            return {"refreshed": 0, "skipped_manual": 0,
                    "skipped_no_metadata": 0, "skipped_invalid": 0}

        # Pre-filter to assets that have indexer metadata; we only fetch
        # SGS series if there's something to compute.
        with_metadata = [a for a in assets
                         if a.rf_indexer and a.rf_rate_pct is not None]
        skipped_no_metadata = len(assets) - len(with_metadata)
        if not with_metadata:
            return {"refreshed": 0, "skipped_manual": 0,
                    "skipped_no_metadata": skipped_no_metadata,
                    "skipped_invalid": 0}

        earliest = min((a.purchase_date or today for a in with_metadata),
                       default=today)

        # Fetch only the indexes we'll actually use.
        needs_cdi = any((a.rf_indexer or "").upper() == "CDI"
                        for a in with_metadata)
        needs_ipca = any((a.rf_indexer or "").upper() == "IPCA"
                         for a in with_metadata)
        cdi = _fetch_sgs_series(12, earliest, today) if needs_cdi else {}
        ipca = _fetch_sgs_series(433, earliest, today) if needs_ipca else {}

        if needs_cdi and not cdi:
            logger.warning("CDB refresh: BCB CDI unavailable, skipping CDI-indexed assets")
        if needs_ipca and not ipca:
            logger.warning("CDB refresh: BCB IPCA unavailable, skipping IPCA-indexed assets")

        for asset in with_metadata:
            buy_date = asset.purchase_date
            buy_price = float(asset.purchase_price or 0)
            qty = float(asset.units or 0)
            if not buy_date or buy_price <= 0 or qty <= 0:
                skipped_invalid += 1
                continue

            # Respect any manually-stamped value for today (broker-bruto
            # snapshots from the reconciliation flow). We only check
            # today's row — historical manual entries are untouched.
            existing_today = await session.scalar(
                select(AssetValue).where(
                    AssetValue.asset_id == asset.id,
                    AssetValue.date == today,
                )
            )
            if existing_today is not None and existing_today.source == "manual":
                skipped_manual += 1
                continue

            indexer = asset.rf_indexer.upper()
            rate = float(asset.rf_rate_pct)

            if indexer == "PRE":
                factor = _compound_pre(buy_date, today, rate)
            elif indexer == "CDI":
                if not cdi:
                    skipped_invalid += 1
                    continue
                factor = _compound_cdi(cdi, buy_date.isoformat(), today_iso,
                                       rate / 100.0)
            elif indexer == "IPCA":
                if not ipca:
                    skipped_invalid += 1
                    continue
                factor = _compound_ipca(ipca, buy_date, today, rate)
            else:
                logger.warning("Unknown rf_indexer %r on asset %s; skipping",
                               asset.rf_indexer, asset.id)
                skipped_invalid += 1
                continue

            mtm = qty * buy_price * factor
            amount = Decimal(str(round(mtm, 2)))

            # Replace today's rule-based AV (manual was already filtered out
            # above so we know there's no manual to clobber).
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
    return {"refreshed": refreshed,
            "skipped_manual": skipped_manual,
            "skipped_no_metadata": skipped_no_metadata,
            "skipped_invalid": skipped_invalid}


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
