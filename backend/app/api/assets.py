import logging
import uuid
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import current_active_user
from app.core.database import get_async_session
from app.models.asset import Asset
from app.models.user import User
from app.providers.market_price import (
    MarketPriceRateLimitedError,
    get_market_price_provider,
)
from app.schemas.asset import (
    AssetCreate,
    AssetRead,
    AssetTransactionCreate,
    AssetTransactionRead,
    AssetUpdate,
    AssetValueCreate,
    AssetValueRead,
    MarketSymbolMatch,
    MarketSymbolQuote,
)
from app.services import asset_service, asset_transaction_service
from app.services.fx_rate_service import convert

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/assets", tags=["assets"])


# ----------------------------------------------------------------------------
# Market price lookup (Yahoo Finance via yfinance)
# ----------------------------------------------------------------------------
#
# Lives under /api/assets/market/... rather than a top-level /market so the
# RBAC and auth middleware inherited by this router applies automatically —
# ticker lookups are gated behind an authenticated session just like other
# asset endpoints.


@router.get("/market/search", response_model=list[MarketSymbolMatch])
async def market_search(
    q: str = Query(..., min_length=1, max_length=64, description="Ticker or company name"),
    limit: int = Query(15, ge=1, le=30),
    _: User = Depends(current_active_user),
) -> list[MarketSymbolMatch]:
    """Autocomplete ticker symbols for the Add-Asset form.

    Intentionally thin — just proxies to the configured market-price
    provider. Upstream errors turn into an empty list so the UI degrades
    gracefully (a user typing a query shouldn't ever see a 500).
    """
    provider = get_market_price_provider()
    try:
        return await provider.search(q, limit=limit)
    except MarketPriceRateLimitedError:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Market data provider is currently rate-limiting. Try again in a minute.",
        )
    except Exception:
        logger.exception("Market search failed for %r", q)
        return []


@router.get("/market/quote", response_model=MarketSymbolQuote)
async def market_quote(
    symbol: str = Query(..., min_length=1, max_length=32),
    _: User = Depends(current_active_user),
) -> MarketSymbolQuote:
    """Fetch a single live quote — used to preview value before saving an asset."""
    provider = get_market_price_provider()
    try:
        quote = await provider.get_quote(symbol)
    except MarketPriceRateLimitedError:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Market data provider is currently rate-limiting. Try again in a minute.",
        )
    if quote is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No quote found for {symbol}",
        )
    return quote


@router.post("/{asset_id}/refresh-price", response_model=AssetRead)
async def refresh_asset_price(
    asset_id: uuid.UUID,
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
) -> AssetRead:
    """Trigger an immediate price refresh for a single market-priced asset.

    Mirrors what the scheduled daily task does for one asset — re-quotes
    the ticker, updates ``last_price`` + ``last_price_at``, and upserts
    today's ``AssetValue``. Returns the refreshed asset with the same
    shape as the list endpoint (including ``current_value_primary``).
    """
    from app.models.asset import Asset as AssetModel
    from sqlalchemy import select as sa_select

    result = await session.execute(
        sa_select(AssetModel).where(
            AssetModel.id == asset_id, AssetModel.user_id == user.id
        )
    )
    asset = result.scalar_one_or_none()
    if asset is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found")
    if asset.valuation_method != "market_price":
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Only market-priced assets can be refreshed via this endpoint",
        )

    try:
        ok = await asset_service.refresh_market_price_asset(session, asset)
    except MarketPriceRateLimitedError:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Market data provider is currently rate-limiting. Try again in a minute.",
        )
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Could not refresh quote for {asset.ticker}",
        )
    await session.commit()

    refreshed = await asset_service.get_asset(session, asset_id, user.id)
    if refreshed is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found")

    # Stamp the primary-currency fields so the refresh response has the same
    # shape as the list endpoint — the React Query cache update needs them
    # to keep the row rendering consistent (BRL rollup, gain/loss).
    primary_currency = user.primary_currency
    if refreshed.currency != primary_currency and refreshed.current_value is not None:
        converted, _ = await convert(
            session, Decimal(str(refreshed.current_value)), refreshed.currency, primary_currency,
        )
        refreshed.current_value_primary = float(converted)
        if refreshed.gain_loss is not None:
            gl_converted, _ = await convert(
                session, Decimal(str(refreshed.gain_loss)), refreshed.currency, primary_currency,
            )
            refreshed.gain_loss_primary = float(gl_converted)
    return refreshed


@router.get("", response_model=list[AssetRead])
async def list_assets(
    include_archived: bool = False,
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    assets = await asset_service.get_assets(session, user.id, include_archived=include_archived)
    primary_currency = user.primary_currency

    # Pre-load today's FX rates for every non-primary currency in the
    # portfolio. The previous version called `convert()` per asset × per
    # field, and each convert() ran 2 _get_exact_date_rate queries — for
    # a 145-asset portfolio with a USD slice that meant ~580 serialized
    # round-trips to PostgreSQL and pushed /api/assets to 50+ seconds
    # under pool contention. Now we issue at most N+1 queries where N is
    # the number of foreign currencies (typically 1: USD).
    foreign_ccys = {a.currency for a in assets
                    if a.currency != primary_currency
                    and a.current_value is not None}
    rate_cache: dict[str, Decimal] = {}
    if foreign_ccys:
        from app.services.fx_rate_service import get_rate
        for ccy in foreign_ccys:
            rate_cache[ccy] = await get_rate(session, ccy, primary_currency)

    for asset in assets:
        if asset.currency == primary_currency or asset.current_value is None:
            continue
        rate = rate_cache.get(asset.currency)
        if rate is None:
            continue
        asset.current_value_primary = float(
            (Decimal(str(asset.current_value)) * rate).quantize(Decimal("0.01"))
        )
        if asset.gain_loss is not None:
            asset.gain_loss_primary = float(
                (Decimal(str(asset.gain_loss)) * rate).quantize(Decimal("0.01"))
            )
    return assets


@router.get("/{asset_id}/icon")
async def get_asset_icon(
    asset_id: uuid.UUID,
    session: AsyncSession = Depends(get_async_session),
):
    """Serve the cached logo bytes for an asset.

    Public route by design — favicons are not sensitive, and adding auth
    would force the frontend to send credentials with every <img src=...>
    which slows the page down for zero security benefit. The route only
    returns bytes we previously fetched ourselves, never user data.

    Browser caches aggressively (1 year, immutable) so subsequent page
    loads don't even hit the network.
    """
    row = (await session.execute(
        select(Asset.logo_data, Asset.logo_content_type)
        .where(Asset.id == asset_id)
    )).first()
    if row is None or row.logo_data is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    return Response(
        content=row.logo_data,
        media_type=row.logo_content_type or "image/png",
        headers={"Cache-Control": "public, max-age=31536000, immutable"},
    )


@router.get("/pending-coupons")
async def get_pending_coupons(
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
) -> list[dict]:
    """Cupons semestrais do Tesouro já vencidos e ainda não lançados.

    Estado derivado dos dados (não há tabela de notificações): assim que o
    INTEREST é registrado na data do cupom, o item some da lista. Ver
    tesouro_coupon_service para o calendário e o porquê de não calcularmos
    o valor automaticamente.

    Registrada antes de `/{asset_id}` porque o FastAPI casa as rotas na
    ordem de declaração — invertido, "pending-coupons" seria lido como um
    UUID de ativo e devolveria 422.
    """
    from app.services import tesouro_coupon_service
    return await tesouro_coupon_service.pending_coupons(session, user.id)


@router.get("/portfolio-trend")
async def portfolio_trend(
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    return await asset_service.get_portfolio_trend(session, user.id)


@router.get("/custodian-summary")
async def custodian_summary(
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    """Live portfolio totals grouped by (custodian, wallet) — the broker
    reconciliation view. Registered before /{asset_id} on purpose."""
    return await asset_service.get_custodian_summary(session, user.id)


@router.get("/{asset_id}", response_model=AssetRead)
async def get_asset(
    asset_id: uuid.UUID,
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    asset = await asset_service.get_asset(session, asset_id, user.id)
    if not asset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found")
    return asset


@router.post("", response_model=AssetRead, status_code=status.HTTP_201_CREATED)
async def create_asset(
    data: AssetCreate,
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    return await asset_service.create_asset(session, user.id, data)


@router.patch("/{asset_id}", response_model=AssetRead)
async def update_asset(
    asset_id: uuid.UUID,
    data: AssetUpdate,
    regenerate_growth: bool = Query(False),
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    asset = await asset_service.update_asset(session, asset_id, user.id, data, regenerate_growth=regenerate_growth)
    if not asset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found")
    return asset


@router.delete("/{asset_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_asset(
    asset_id: uuid.UUID,
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    deleted = await asset_service.delete_asset(session, asset_id, user.id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found")


@router.get("/{asset_id}/values", response_model=list[AssetValueRead])
async def list_asset_values(
    asset_id: uuid.UUID,
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    values = await asset_service.get_asset_values(session, asset_id, user.id)
    if values is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found")
    return values


@router.get("/{asset_id}/value-trend")
async def get_asset_value_trend(
    asset_id: uuid.UUID,
    months: int = Query(12, ge=1, le=120),
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    trend = await asset_service.get_asset_value_trend(session, asset_id, user.id, months=months)
    if trend is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found")
    return trend


@router.post("/{asset_id}/values", response_model=AssetValueRead, status_code=status.HTTP_201_CREATED)
async def add_asset_value(
    asset_id: uuid.UUID,
    data: AssetValueCreate,
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    value = await asset_service.add_asset_value(session, asset_id, user.id, data)
    if value is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found")
    return value


@router.delete("/values/{value_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_asset_value(
    value_id: uuid.UUID,
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    deleted = await asset_service.delete_asset_value(session, value_id, user.id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Value not found")


# --------- Asset transactions (BUY/SELL/DIVIDEND/...) ---------

@router.get("/{asset_id}/transactions", response_model=list[AssetTransactionRead])
async def list_asset_transactions(
    asset_id: uuid.UUID,
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    txs = await asset_transaction_service.list_for_asset(
        session, user.id, asset_id)
    if txs is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Asset not found")
    return txs


@router.post("/{asset_id}/transactions",
             response_model=AssetTransactionRead,
             status_code=status.HTTP_201_CREATED)
async def add_asset_transaction(
    asset_id: uuid.UUID,
    data: AssetTransactionCreate,
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    try:
        tx = await asset_transaction_service.create(
            session, user.id, asset_id, data)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=str(e))
    if tx is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Asset not found")
    return tx


@router.delete("/transactions/{transaction_id}",
               status_code=status.HTTP_204_NO_CONTENT)
async def delete_asset_transaction(
    transaction_id: uuid.UUID,
    session: AsyncSession = Depends(get_async_session),
    user: User = Depends(current_active_user),
):
    ok = await asset_transaction_service.delete(
        session, user.id, transaction_id)
    if not ok:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Transaction not found")
