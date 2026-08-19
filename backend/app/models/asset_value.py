import uuid
from datetime import date
from decimal import Decimal
from typing import Optional

from sqlalchemy import Date, ForeignKey, Numeric, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base
from app.models.asset import Asset


class AssetValue(Base):
    __tablename__ = "asset_values"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    asset_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("assets.id", ondelete="CASCADE"))
    amount: Mapped[Decimal] = mapped_column(Numeric(precision=15, scale=6))
    date: Mapped[date] = mapped_column(Date)
    source: Mapped[str] = mapped_column(String(20), default="manual")  # manual, rule, sync
    # Valor a mercado, preenchido só para Tesouro marcado na curva. Nesses
    # títulos `amount` carrega o carrego (valor oficial para patrimônio e
    # TWR) e esta coluna guarda quanto valeria resgatando hoje, para a
    # segunda linha do gráfico de evolução. NULL em todo o resto — sem
    # linha secundária a desenhar.
    market_amount: Mapped[Optional[Decimal]] = mapped_column(
        Numeric(precision=15, scale=6), nullable=True
    )

    asset: Mapped["Asset"] = relationship(back_populates="values")
