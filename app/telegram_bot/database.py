import logging
from datetime import datetime, date, timezone
from typing import Optional, List, Dict
from collections import defaultdict

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import select, func
from sqlalchemy.exc import IntegrityError

from .config import PortfolioDatabaseConfig
from .models import Base, User, Purchase, Sale

logger = logging.getLogger(__name__)


class PortfolioDatabase:
    def __init__(self, config: PortfolioDatabaseConfig):
        self.config = config
        self.engine = None
        self.async_session_maker = None

    async def connect(self):
        try:
            database_url = (
                f"postgresql+asyncpg://{self.config.user}:{self.config.password}"
                f"@{self.config.host}:{self.config.port}/{self.config.database}"
            )

            self.engine = create_async_engine(
                database_url, echo=False, pool_size=10, max_overflow=20
            )

            self.async_session_maker = async_sessionmaker(
                self.engine, class_=AsyncSession, expire_on_commit=False
            )

            logger.info("Portfolio database connection established")
        except Exception as e:
            logger.error(f"Portfolio database connection error: {e}")
            raise

    async def close(self):
        if self.engine:
            await self.engine.dispose()
            logger.info("Portfolio database connection closed")

    async def create_tables(self, drop_existing=False):
        async with self.engine.begin() as conn:
            if drop_existing:
                await conn.run_sync(Base.metadata.drop_all)
                logger.info("Existing portfolio tables dropped")
            await conn.run_sync(Base.metadata.create_all)
            logger.info("Portfolio database tables created/verified")

    async def create_user(self, telegram_id: int, phone_number: str) -> User:
        async with self.async_session_maker() as session:
            try:
                user = User(
                    telegram_id=telegram_id,
                    phone_number=phone_number,
                    registered_at=datetime.now(timezone.utc),
                    is_active=True,
                )
                session.add(user)
                await session.commit()
                await session.refresh(user)
                logger.info(f"Created user with telegram_id {telegram_id}")
                return user
            except IntegrityError:
                await session.rollback()
                raise ValueError(f"User with telegram_id {telegram_id} already exists")

    async def get_user_by_telegram_id(self, telegram_id: int) -> Optional[User]:
        async with self.async_session_maker() as session:
            result = await session.scalar(
                select(User).where(User.telegram_id == telegram_id)
            )
            return result

    async def get_user_by_phone(self, phone_number: str) -> Optional[User]:
        async with self.async_session_maker() as session:
            result = await session.scalar(
                select(User).where(User.phone_number == phone_number)
            )
            return result

    async def add_purchase(
        self,
        user_id: int,
        ticker: str,
        purchase_price: float,
        quantity: int,
        purchase_date: date,
    ) -> Purchase:
        async with self.async_session_maker() as session:
            purchase = Purchase(
                user_id=user_id,
                ticker=ticker.upper(),
                purchase_price=purchase_price,
                quantity=quantity,
                purchase_date=purchase_date,
                created_at=datetime.now(timezone.utc),
            )
            session.add(purchase)
            await session.commit()
            await session.refresh(purchase)
            logger.info(
                f"Added purchase: user_id={user_id}, ticker={ticker}, "
                f"price={purchase_price}, quantity={quantity}"
            )
            return purchase

    async def get_user_purchases(self, user_id: int) -> List[Purchase]:
        async with self.async_session_maker() as session:
            result = await session.scalars(
                select(Purchase)
                .where(Purchase.user_id == user_id)
                .order_by(Purchase.purchase_date.desc(), Purchase.created_at.desc())
            )
            return list(result.all())

    async def get_user_portfolio(self, user_id: int) -> Dict[str, Dict]:
        purchases = await self.get_user_purchases(user_id)
        sales = await self.get_user_sales(user_id)
        
        portfolio = defaultdict(lambda: {
            "purchases": [],
            "total_purchased_quantity": 0,
            "total_sold_quantity": 0,
            "total_cost": 0.0,
        })
        
        for purchase in purchases:
            ticker = purchase.ticker
            portfolio[ticker]["purchases"].append({
                "id": purchase.id,
                "price": purchase.purchase_price,
                "quantity": purchase.quantity,
                "date": purchase.purchase_date,
            })
            portfolio[ticker]["total_purchased_quantity"] += purchase.quantity
            portfolio[ticker]["total_cost"] += purchase.purchase_price * purchase.quantity
        
        for sale in sales:
            ticker = sale.ticker
            portfolio[ticker]["total_sold_quantity"] += sale.quantity
        
        result = {}
        for ticker, data in portfolio.items():
            total_quantity = data["total_purchased_quantity"] - data["total_sold_quantity"]
            
            if total_quantity <= 0:
                continue
            
            avg_price = (
                data["total_cost"] / data["total_purchased_quantity"]
                if data["total_purchased_quantity"] > 0
                else 0.0
            )
            
            remaining_cost = avg_price * total_quantity
            
            result[ticker] = {
                "purchases": data["purchases"],
                "total_quantity": total_quantity,
                "total_purchased_quantity": data["total_purchased_quantity"],
                "total_sold_quantity": data["total_sold_quantity"],
                "average_price": avg_price,
                "total_cost": remaining_cost,
            }
        
        return result

    async def add_sale(
        self,
        user_id: int,
        ticker: str,
        sale_price: float,
        quantity: int,
        sale_date: date,
    ) -> Sale:
        async with self.async_session_maker() as session:
            sale = Sale(
                user_id=user_id,
                ticker=ticker.upper(),
                sale_price=sale_price,
                quantity=quantity,
                sale_date=sale_date,
                created_at=datetime.now(timezone.utc),
            )
            session.add(sale)
            await session.commit()
            await session.refresh(sale)
            logger.info(
                f"Added sale: user_id={user_id}, ticker={ticker}, "
                f"price={sale_price}, quantity={quantity}"
            )
            return sale

    async def get_user_sales(self, user_id: int) -> List[Sale]:
        async with self.async_session_maker() as session:
            result = await session.scalars(
                select(Sale)
                .where(Sale.user_id == user_id)
                .order_by(Sale.sale_date.desc(), Sale.created_at.desc())
            )
            return list(result.all())

    async def get_available_quantity(self, user_id: int, ticker: str) -> int:
        purchases = await self.get_user_purchases(user_id)
        sales = await self.get_user_sales(user_id)
        
        total_purchased = sum(
            p.quantity for p in purchases if p.ticker.upper() == ticker.upper()
        )
        total_sold = sum(
            s.quantity for s in sales if s.ticker.upper() == ticker.upper()
        )
        
        return total_purchased - total_sold
