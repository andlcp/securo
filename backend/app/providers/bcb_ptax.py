"""BCB (Banco Central do Brasil) FX provider — PTAX cotação.

Why BCB instead of OpenExchangeRates:
  - No API key required (OpenExchangeRates needs paid signup)
  - PTAX is the OFFICIAL Brazilian FX reference (used by tax/regulators)
  - We already use BCB SGS for IPCA / CDI history — same family of APIs
  - USD/BRL is the only pair we need (the rest of the user's portfolio
    is BRL-native), so the simpler PTAX endpoint is enough

Two BCB endpoints used:
  - SGS series 1 ("Taxa de câmbio - Livre - Dólar americano (compra) - diário"):
    fast bulk historical fetch via `/dados/serie/bcdata.sgs.1/dados`.
    Single value per day, sufficient for our portfolio valuation.
  - Olinda PTAX endpoint: returns the official daily PTAX with buy and
    sell quotes; used for `fetch_latest()` so the badge in the header
    matches what banks publish.

Returns rates in the same shape as OpenExchangeRatesProvider:
{currency_code: rate_vs_USD}. Since BCB only publishes USD/BRL, the
returned dict has a single entry: {'BRL': X.XXXX}.
"""

import logging
from datetime import date, timedelta
from decimal import Decimal

import httpx

from app.providers.base import FxRateProvider

logger = logging.getLogger(__name__)

# SGS series 1: USD/BRL "compra" daily (free, no key)
SGS_BASE = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados"
# Olinda PTAX (free, no key) — official PTAX with buy/sell
OLINDA_PTAX = (
    "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
    "CotacaoDolarDia(dataCotacao=@dataCotacao)"
)


class BcbPtaxProvider(FxRateProvider):
    """USD/BRL PTAX from Banco Central."""

    @property
    def name(self) -> str:
        return "bcb_ptax"

    async def fetch_latest(self) -> dict[str, Decimal]:
        """Return the last *closed* PTAX rate.

        BCB publishes intraday boletins during the trading day (Abertura,
        Intermediário 1-4) before the official PTAX comes out around
        13:00 BRT. Those intraday partials change every 30 minutes and
        aren't authoritative — using them in the badge would make the
        portfolio valuation wobble through the morning.

        Start from yesterday and walk back so the badge always reflects
        an official, closed PTAX. On weekends/holidays we keep walking
        until we hit the last business day with a published rate.
        """
        async with httpx.AsyncClient(timeout=30) as client:
            for delta in range(1, 8):
                d = date.today() - timedelta(days=delta)
                params = {
                    "@dataCotacao": f"'{d.strftime('%m-%d-%Y')}'",
                    "$top": "1",
                    "$format": "json",
                }
                try:
                    r = await client.get(OLINDA_PTAX, params=params)
                    r.raise_for_status()
                    data = r.json()
                    rows = data.get("value") or []
                    if rows:
                        # PTAX returns "cotacaoVenda" (sell) and
                        # "cotacaoCompra" (buy). For valuation we use
                        # the mid-point, matching common broker practice.
                        buy = Decimal(str(rows[0]["cotacaoCompra"]))
                        sell = Decimal(str(rows[0]["cotacaoVenda"]))
                        mid = (buy + sell) / 2
                        return {"BRL": mid}
                except Exception as exc:
                    logger.warning("BCB PTAX fetch failed for %s: %s", d, exc)
                    continue
        raise RuntimeError("No PTAX rate found in last 7 business days")

    async def fetch_historical(self, target_date: date) -> dict[str, Decimal]:
        """Return USD/BRL rate for a specific historical date.

        Falls back to the closest preceding business day if the target
        is a weekend / holiday (PTAX isn't published on those days).
        """
        async with httpx.AsyncClient(timeout=30) as client:
            for delta in range(0, 7):
                d = target_date - timedelta(days=delta)
                params = {
                    "@dataCotacao": f"'{d.strftime('%m-%d-%Y')}'",
                    "$top": "1",
                    "$format": "json",
                }
                try:
                    r = await client.get(OLINDA_PTAX, params=params)
                    r.raise_for_status()
                    data = r.json()
                    rows = data.get("value") or []
                    if rows:
                        buy = Decimal(str(rows[0]["cotacaoCompra"]))
                        sell = Decimal(str(rows[0]["cotacaoVenda"]))
                        mid = (buy + sell) / 2
                        return {"BRL": mid}
                except Exception as exc:
                    logger.warning("BCB PTAX historical fetch failed for %s: %s",
                                   d, exc)
                    continue
        return {}

    async def fetch_range(self, start: date, end: date) -> dict[date, Decimal]:
        """Bulk-fetch USD/BRL daily for a date range using SGS series 1.

        Used for historical backfill — much faster than calling the
        single-date PTAX endpoint once per day.
        """
        params = {
            "formato": "json",
            "dataInicial": start.strftime("%d/%m/%Y"),
            "dataFinal": end.strftime("%d/%m/%Y"),
        }
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.get(SGS_BASE, params=params)
            r.raise_for_status()
            data = r.json()
        out: dict[date, Decimal] = {}
        for item in data:
            try:
                d = date(*reversed([int(p) for p in item["data"].split("/")]))
                out[d] = Decimal(str(item["valor"]))
            except (KeyError, ValueError):
                continue
        return out
