from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    String,
    Text,
    Index,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import ARRAY, TIMESTAMP

Base = declarative_base()


class News(Base):
    __tablename__ = "news"

    id = Column(Integer, primary_key=True, autoincrement=True)
    msg_id = Column(BigInteger, nullable=False)
    channel = Column(String(255), nullable=False)
    date = Column(TIMESTAMP(timezone=True), nullable=False)
    tags = Column(ARRAY(Text), nullable=False, server_default="{}")
    text = Column(Text, nullable=False)

    __table_args__ = (
        UniqueConstraint("msg_id", "channel", name="uq_news_msg_channel"),
        Index("idx_news_msg_channel", "msg_id", "channel"),
    )


class PollingState(Base):
    __tablename__ = "polling_state"

    channel = Column(String(255), primary_key=True)
    last_poll_time = Column(TIMESTAMP(timezone=True), nullable=False)
