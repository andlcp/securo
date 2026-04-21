import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class PortfolioCreate(BaseModel):
    name: str
    color: str = "#6366F1"
    description: Optional[str] = None


class PortfolioUpdate(BaseModel):
    name: Optional[str] = None
    color: Optional[str] = None
    description: Optional[str] = None


class InvestmentPositionCreate(BaseModel):
    ticker: str
    name: str
    asset_type: str
    currency: str = "BRL"
    units: float
    avg_price: float
    broker: Optional[str] = None
    notes: Optional[str] = None


class InvestmentPositionUpdate(BaseModel):
    ticker: Optional[str] = None
    name: Optional[str] = None
    asset_type: Optional[str] = None
    currency: Optional[str] = None
    units: Optional[float] = None
    avg_price: Optional[float] = None
    broker: Optional[str] = None
    notes: Optional[str] = None


class PositionRead(BaseModel):
    id: uuid.UUID
    portfolio_id: uuid.UUID
    ticker: str
    name: str
    asset_type: str
    currency: str
    units: float
    avg_price: float
    broker: Optional[str] = None
    notes: Optional[str] = None
    created_at: datetime
    current_price: Optional[float] = None
    current_value: Optional[float] = None
    total_invested: float = 0.0
    gain_loss: Optional[float] = None
    gain_loss_pct: Optional[float] = None
    current_value_brl: Optional[float] = None
    change_pct: Optional[float] = None
    price_updated_at: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class PortfolioRead(BaseModel):
    id: uuid.UUID
    user_id: uuid.UUID
    name: str
    color: str
    description: Optional[str] = None
    created_at: datetime
    positions: list[PositionRead] = []
    total_invested: float = 0.0
    current_value: float = 0.0
    gain_loss: float = 0.0
    gain_loss_pct: float = 0.0

    model_config = ConfigDict(from_attributes=True)


class AllocationItem(BaseModel):
    type: str
    value: float
    pct: float


class PortfolioSummary(BaseModel):
    portfolios: list[PortfolioRead]
    total_invested_brl: float
    current_value_brl: float
    gain_loss_brl: float
    gain_loss_pct: float
    allocation: list[AllocationItem]
    fx_usd_brl: Optional[float] = None
