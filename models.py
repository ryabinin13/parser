from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped, mapped_column
from datetime import datetime, date

class Base(DeclarativeBase): pass

class SpimexTradingResults(Base):
    __tablename__ = "spimex_trading_results"
 
    id: Mapped[int] = mapped_column(primary_key=True)
    exchange_product_id: Mapped[str]
    exchange_product_name: Mapped[str]
    oil_id: Mapped[str]
    delivery_basis_id: Mapped[str]
    delivery_basis_name: Mapped[str]
    delivery_type_id: Mapped[str]
    volume: Mapped[str]
    total: Mapped[str]
    count: Mapped[str]
    created_on: Mapped[date]
    updated_on: Mapped[date]
    date: Mapped[date]
    

