import logging
from datetime import datetime, timezone
from typing import Optional, List
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import select, exists
from sqlalchemy.exc import IntegrityError

from .config import DatabaseConfig
from .models import Base, News, PollingState

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, config: DatabaseConfig):
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

            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise

    async def close(self):
        if self.engine:
            await self.engine.dispose()
            logger.info("Database connection closed")

    async def create_tables(self, drop_existing=True):
        async with self.engine.begin() as conn:
            if drop_existing:
                await conn.run_sync(Base.metadata.drop_all)
                logger.info("Existing tables dropped")
            await conn.run_sync(Base.metadata.create_all)
            logger.info("Database tables created/verified")

    async def message_exists(self, msg_id: int, channel: str) -> bool:
        async with self.async_session_maker() as session:
            result = await session.scalar(
                select(exists().where(News.msg_id == msg_id, News.channel == channel))
            )
            return result

    async def save_news(
        self, msg_id: int, channel: str, date: datetime, tags: List[str], text: str
    ) -> bool:
        if await self.message_exists(msg_id, channel):
            logger.debug(
                f"Message {msg_id} from channel {channel} already exists, skipping"
            )
            return False

        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)

        async with self.async_session_maker() as session:
            try:
                news = News(
                    msg_id=msg_id, channel=channel, date=date, tags=tags, text=text
                )
                session.add(news)
                await session.commit()
                logger.info(
                    f"Saved news {msg_id} from channel {channel} with tags: {tags}"
                )
                return True
            except IntegrityError:
                await session.rollback()
                logger.debug(
                    f"Message {msg_id} from channel {channel} already exists (integrity error)"
                )
                return False

    async def get_last_poll_state(self, channel: str) -> Optional[datetime]:
        async with self.async_session_maker() as session:
            result = await session.scalar(
                select(PollingState).where(PollingState.channel == channel)
            )

            if result:
                return result.last_poll_time
            return None

    async def update_poll_state(self, channel: str, last_poll_time: datetime):
        if last_poll_time.tzinfo is None:
            last_poll_time = last_poll_time.replace(tzinfo=timezone.utc)

        async with self.async_session_maker() as session:
            result = await session.scalar(
                select(PollingState).where(PollingState.channel == channel)
            )

            if result:
                result.last_poll_time = last_poll_time
            else:
                result = PollingState(channel=channel, last_poll_time=last_poll_time)
                session.add(result)

            await session.commit()
            logger.debug(f"Updated poll state for channel {channel}")
