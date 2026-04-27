import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, ConfigDict


class AssetCreate(BaseModel):
    name: str
    type: str
    currency: str = "USD"
    units: Optional[Decimal] = None
    valuation_method: str = "manual"
    purchase_date: Optional[date] = None
    purchase_price: Optional[Decimal] = None
    sell_date: Optional[date] = None
    sell_price: Optional[Decimal] = None
    current_value: Optional[Decimal] = None  # convenience: creates initial AssetValue
    growth_type: Optional[str] = None
    growth_rate: Optional[Decimal] = None
    growth_frequency: Optional[str] = None
    growth_start_date: Optional[date] = None
    is_archived: bool = False
    position: int = 0
    group_id: Optional[uuid.UUID] = None
    # Market-priced assets: ticker is enough to create one. The service
    # fetches the live quote on create and seeds the first AssetValue.
    ticker: Optional[str] = None
    ticker_exchange: Optional[str] = None
    # Explicit class — one of RENDA_VARIAVEL_BR, RENDA_FIXA, STOCKS_US,
    # FIIS, CRIPTO, OUTRO. If omitted the backend tries to infer from
    # ticker/name; the frontend Add Asset form sends it explicitly.
    asset_class: Optional[str] = None
    maturity_date: Optional[date] = None


class AssetUpdate(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None
    currency: Optional[str] = None
    units: Optional[Decimal] = None
    valuation_method: Optional[str] = None
    purchase_date: Optional[date] = None
    purchase_price: Optional[Decimal] = None
    sell_date: Optional[date] = None
    sell_price: Optional[Decimal] = None
    growth_type: Optional[str] = None
    growth_rate: Optional[Decimal] = None
    growth_frequency: Optional[str] = None
    growth_start_date: Optional[date] = None
    is_archived: Optional[bool] = None
    position: Optional[int] = None
    # Use a sentinel to differentiate "don't change group" (field omitted)
    # from "remove from group" (explicit null). Pydantic's exclude_unset
    # already handles this via model_dump.
    group_id: Optional[uuid.UUID] = None
    ticker: Optional[str] = None
    ticker_exchange: Optional[str] = None
    asset_class: Optional[str] = None
    maturity_date: Optional[date] = None


class AssetRead(BaseModel):
    id: uuid.UUID
    user_id: uuid.UUID
    name: str
    type: str
    currency: str
    units: Optional[float] = None
    valuation_method: str
    purchase_date: Optional[date] = None
    purchase_price: Optional[float] = None
    sell_date: Optional[date] = None
    sell_price: Optional[float] = None
    growth_type: Optional[str] = None
    growth_rate: Optional[float] = None
    growth_frequency: Optional[str] = None
    growth_start_date: Optional[date] = None
    is_archived: bool
    position: int
    current_value: Optional[float] = None
    current_value_primary: Optional[float] = None
    gain_loss: Optional[float] = None
    gain_loss_primary: Optional[float] = None
    value_count: int = 0
    source: str = "manual"
    connection_id: Optional[uuid.UUID] = None
    isin: Optional[str] = None
    maturity_date: Optional[date] = None
    group_id: Optional[uuid.UUID] = None
    ticker: Optional[str] = None
    ticker_exchange: Optional[str] = None
    last_price: Optional[float] = None
    last_price_at: Optional[datetime] = None
    logo_url: Optional[str] = None
    asset_class: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


# ---------- AssetTransaction schemas ----------

ASSET_TX_TYPES = {
    "BUY", "SELL", "DIVIDEND", "JCP", "RENDIMENTO",
    "DEPOSIT", "WITHDRAWAL", "INTEREST", "FEE", "RESGATE",
}


class AssetTransactionCreate(BaseModel):
    date: date
    type: str  # one of ASSET_TX_TYPES
    qty: Optional[Decimal] = None
    price: Optional[Decimal] = None
    value: Optional[Decimal] = None
    fees: Decimal = Decimal("0")
    notes: Optional[str] = None
    source: str = "manual"
    external_id: Optional[str] = None


class AssetTransactionRead(BaseModel):
    id: uuid.UUID
    asset_id: uuid.UUID
    date: date
    type: str
    qty: Optional[float] = None
    price: Optional[float] = None
    value: Optional[float] = None
    fees: float = 0.0
    notes: Optional[str] = None
    source: str = "manual"
    external_id: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class MarketSymbolQuote(BaseModel):
    """Live quote for a ticker, used by the add-asset form to preview value."""

    symbol: str
    name: Optional[str] = None
    exchange: Optional[str] = None
    currency: str
    price: float
    quote_type: Optional[str] = None  # EQUITY, ETF, CRYPTOCURRENCY, MUTUALFUND, ...
    # Fully-formed logo URL if the provider can derive one. Caller stores
    # this verbatim on the asset; no further processing required.
    logo_url: Optional[str] = None


class MarketSymbolMatch(BaseModel):
    """A single search result returned by /assets/market/search."""

    symbol: str
    name: Optional[str] = None
    exchange: Optional[str] = None
    quote_type: Optional[str] = None


class AssetValueCreate(BaseModel):
    amount: Decimal
    date: date


class AssetValueRead(BaseModel):
    id: uuid.UUID
    asset_id: uuid.UUID
    amount: float
    date: date
    source: str

    model_config = ConfigDict(from_attributes=True)
