import logging
from datetime import datetime
from typing import List, Optional

import aiohttp
from aiochclient import ChClient

from .config import ClickHouseConfig

logger = logging.getLogger(__name__)


class ClickHouseDatabase:
    def __init__(self, config: ClickHouseConfig):
        self.config = config
        self.client: Optional[ChClient] = None
        self.session: Optional[aiohttp.ClientSession] = None

    async def connect(self):
        try:
            self.session = aiohttp.ClientSession()
            url = f"http://{self.config.host}:{self.config.port}/"
            self.client = ChClient(
                self.session,
                url=url,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database,
            )
            await self.client.execute("SELECT 1")
            logger.info("ClickHouse connection established")
        except Exception as e:
            logger.error(f"ClickHouse connection error: {e}")
            raise

    async def close(self):
        if self.client:
            await self.client.close()
        if self.session:
            await self.session.close()
        self.client = None
        self.session = None
        logger.info("ClickHouse connection closed")

    async def create_tables(self):
        if self.client is None:
            raise RuntimeError("Database not connected. Call connect() first.")

        ticker_table_sql = """
        CREATE TABLE IF NOT EXISTS ticker_candles (
            ticker String,
            begin DateTime,
            end DateTime,
            open Float64,
            close Float64,
            high Float64,
            low Float64,
            value Float64,
            volume UInt64,
            trade_date Date DEFAULT toDate(begin)
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(trade_date)
        ORDER BY (ticker, begin)
        """

        index_table_sql = """
        CREATE TABLE IF NOT EXISTS index_values (
            secid String,
            boardid String,
            lastvalue Nullable(Float64),
            openvalue Nullable(Float64),
            currentvalue Nullable(Float64),
            lastchange Nullable(Float64),
            lastchangetoopenprc Nullable(Float64),
            lastchangetoopen Nullable(Float64),
            updatetime Nullable(String),
            lastchangeprc Nullable(Float64),
            valtoday Nullable(Float64),
            monthchangeprc Nullable(Float64),
            yearchangeprc Nullable(Float64),
            seqnum Nullable(UInt64),
            systime Nullable(DateTime),
            time Nullable(String),
            valtoday_usd Nullable(Float64),
            lastchangebp Nullable(Int64),
            monthchangebp Nullable(Int64),
            yearchangebp Nullable(Int64),
            capitalization Nullable(Float64),
            capitalization_usd Nullable(Float64),
            high Nullable(Float64),
            low Nullable(Float64),
            tradedate Nullable(Date),
            tradingsession Nullable(String),
            voltoday Nullable(Float64),
            trade_session_date Nullable(Date),
            insert_time DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(insert_time)
        ORDER BY (secid, insert_time)
        """

        try:
            await self.client.execute(ticker_table_sql)
            logger.info("Table ticker_candles created/verified")
            await self.client.execute(index_table_sql)
            logger.info("Table index_values created/verified")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise

    async def insert_ticker_candles_batch(self, ticker: str, candles: List[dict]):
        if self.client is None:
            raise RuntimeError("Database not connected. Call connect() first.")

        if not candles:
            return

        insert_sql = """
        INSERT INTO ticker_candles 
        (ticker, begin, end, open, close, high, low, value, volume, trade_date)
        VALUES
        """
        
        values_list = []
        for data in candles:
            begin_str = data.get("begin")
            if not begin_str:
                continue
            
            try:
                begin_dt = datetime.fromisoformat(begin_str.replace(" ", "T"))
            except (ValueError, AttributeError):
                logger.warning(f"Invalid begin time '{begin_str}' for {ticker}, skipping")
                continue
            
            end_str = data.get("end")
            end_dt = datetime.fromisoformat(end_str.replace(" ", "T")) if end_str else None
            trade_date = begin_dt.date()
            
            values_list.append((
                ticker,
                begin_dt,
                end_dt,
                data.get("open"),
                data.get("close"),
                data.get("high"),
                data.get("low"),
                data.get("value"),
                data.get("volume"),
                trade_date,
            ))

        if not values_list:
            return

        try:
            await self.client.execute(insert_sql, *values_list)
            logger.debug(f"Inserted {len(values_list)} candles for ticker {ticker}")
        except Exception as e:
            logger.error(f"Error inserting ticker candles batch for {ticker}: {e}")
            raise

    async def insert_index_value(self, index_data: dict):
        if self.client is None:
            raise RuntimeError("Database not connected. Call connect() first.")

        insert_sql = """
        INSERT INTO index_values 
        (secid, boardid, lastvalue, openvalue, currentvalue, lastchange, lastchangetoopenprc,
         lastchangetoopen, updatetime, lastchangeprc, valtoday, monthchangeprc, yearchangeprc,
         seqnum, systime, time, valtoday_usd, lastchangebp, monthchangebp, yearchangebp,
         capitalization, capitalization_usd, high, low, tradedate, tradingsession, voltoday,
         trade_session_date)
        VALUES
        """

        def parse_datetime(dt_str):
            if not dt_str:
                return None
            try:
                return datetime.fromisoformat(dt_str.replace(" ", "T"))
            except:
                return None

        def parse_date(date_str):
            if not date_str:
                return None
            try:
                return datetime.strptime(date_str, "%Y-%m-%d").date()
            except:
                return None

        values = (
            index_data.get("SECID"),
            index_data.get("BOARDID"),
            index_data.get("LASTVALUE"),
            index_data.get("OPENVALUE"),
            index_data.get("CURRENTVALUE"),
            index_data.get("LASTCHANGE"),
            index_data.get("LASTCHANGETOOPENPRC"),
            index_data.get("LASTCHANGETOOPEN"),
            index_data.get("UPDATETIME"),
            index_data.get("LASTCHANGEPRC"),
            index_data.get("VALTODAY"),
            index_data.get("MONTHCHANGEPRC"),
            index_data.get("YEARCHANGEPRC"),
            index_data.get("SEQNUM"),
            parse_datetime(index_data.get("SYSTIME")),
            index_data.get("TIME"),
            index_data.get("VALTODAY_USD"),
            index_data.get("LASTCHANGEBP"),
            index_data.get("MONTHCHANGEBP"),
            index_data.get("YEARCHANGEBP"),
            index_data.get("CAPITALIZATION"),
            index_data.get("CAPITALIZATION_USD"),
            index_data.get("HIGH"),
            index_data.get("LOW"),
            parse_date(index_data.get("TRADEDATE")),
            index_data.get("TRADINGSESSION"),
            index_data.get("VOLTODAY"),
            parse_date(index_data.get("TRADE_SESSION_DATE")),
        )

        try:
            await self.client.execute(insert_sql, values)
            logger.debug(f"Inserted index data for {index_data.get('SECID')}")
        except Exception as e:
            logger.error(f"Error inserting index value: {e}")
            raise
