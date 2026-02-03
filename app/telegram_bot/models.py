from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    String,
    Date,
    Float,
    Boolean,
    ForeignKey,
    Index,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_id = Column(BigInteger, unique=True, nullable=False, index=True)
    phone_number = Column(String(20), nullable=False)
    registered_at = Column(TIMESTAMP(timezone=True), nullable=False, default=datetime.utcnow)
    is_active = Column(Boolean, nullable=False, default=True)

    purchases = relationship("Purchase", back_populates="user", cascade="all, delete-orphan")
    sales = relationship("Sale", back_populates="user", cascade="all, delete-orphan")

    __table_args__ = (
        Index("idx_users_telegram_id", "telegram_id"),
    )


class Purchase(Base):
    __tablename__ = "purchases"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    ticker = Column(String(20), nullable=False)
    purchase_price = Column(Float, nullable=False)
    quantity = Column(Integer, nullable=False)
    purchase_date = Column(Date, nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, default=datetime.utcnow)

    user = relationship("User", back_populates="purchases")

    __table_args__ = (
        Index("idx_purchases_user_ticker", "user_id", "ticker"),
        Index("idx_purchases_user_date", "user_id", "purchase_date"),
    )


class Sale(Base):
    __tablename__ = "sales"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    ticker = Column(String(20), nullable=False)
    sale_price = Column(Float, nullable=False)
    quantity = Column(Integer, nullable=False)
    sale_date = Column(Date, nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, default=datetime.utcnow)

    user = relationship("User", back_populates="sales")

    __table_args__ = (
        Index("idx_sales_user_ticker", "user_id", "ticker"),
        Index("idx_sales_user_date", "user_id", "sale_date"),
    )
