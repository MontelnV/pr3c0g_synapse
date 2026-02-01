import asyncio
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from app.moex_market.client import MoexMarketClient
from app.moex_market.database import ClickHouseDatabase

logger = logging.getLogger(__name__)


class MarketMonitor:
    def __init__(self, moex_client: MoexMarketClient, db: ClickHouseDatabase, filter_hours: int = 1):
        self.moex_client = moex_client
        self.db = db
        self.filter_hours = filter_hours
        self.cache: dict[str, set] = {}
        self.index_cache: set = set()

    def get_tickers_list(self) -> List[str]:
        return ["SBER"]

    async def fetch_ticker_data(self, ticker: str) -> List[dict]:
        today = datetime.now().strftime("%Y-%m-%d")
        
        try:
            data = await self.moex_client.get_ticker_data(
                ticker=ticker,
                start_date=today,
                end_date=None,
                interval=1
            )
            
            if not data:
                return []
            
            now = datetime.now()
            time_threshold = now - timedelta(hours=self.filter_hours)
            
            filtered_data = []
            for candle in data:
                begin_str = candle.get("begin", "")
                if not begin_str:
                    continue
                
                try:
                    begin_dt = datetime.fromisoformat(begin_str.replace(" ", "T"))
                    if begin_dt >= time_threshold:
                        filtered_data.append(candle)
                except (ValueError, AttributeError) as e:
                    logger.debug(f"Error parsing begin time '{begin_str}' for {ticker}: {e}")
                    continue
            
            return filtered_data
        except Exception as e:
            logger.error(f"Error fetching data for ticker {ticker}: {e}")
            return []

    def filter_new_candles(self, ticker: str, candles: List[dict]) -> List[dict]:
        if ticker not in self.cache:
            self.cache[ticker] = set()
        
        new_candles = []
        for candle in candles:
            begin_str = candle.get("begin", "")
            if not begin_str:
                continue
            
            if begin_str not in self.cache[ticker]:
                new_candles.append(candle)
                self.cache[ticker].add(begin_str)
        
        return new_candles

    async def fetch_index_data(self) -> dict:
        try:
            index_data = await self.moex_client.get_moex_index("IMOEX")
            return index_data or {}
        except Exception as e:
            logger.error(f"Error fetching index data: {e}")
            return {}

    async def save_ticker_data(self, ticker: str, data: List[dict], new_data: List[dict] = None):
        if not data:
            logger.debug(f"No data to save for ticker {ticker}")
            return

        if new_data is None:
            new_data = self.filter_new_candles(ticker, data)
        
        if not new_data:
            logger.debug(f"No new candles for ticker {ticker} (all in cache)")
            return

        try:
            await self.db.insert_ticker_candles_batch(ticker, new_data)
            logger.debug(f"Saved {len(new_data)} new candles for {ticker} (filtered from {len(data)})")
        except Exception as e:
            logger.error(f"Error saving candle data for {ticker}: {e}")
            for candle in new_data:
                begin_str = candle.get("begin", "")
                if begin_str in self.cache.get(ticker, set()):
                    self.cache[ticker].discard(begin_str)

    async def save_index_data(self, index_data: dict):
        if not index_data:
            logger.debug("No index data to save")
            return

        systime_str = index_data.get("SYSTIME", "")
        if not systime_str:
            logger.warning("Index data missing SYSTIME, skipping")
            return

        if systime_str in self.index_cache:
            logger.debug(f"Index data with systime {systime_str} already in cache, skipping")
            return

        try:
            await self.db.insert_index_value(index_data)
            self.index_cache.add(systime_str)
            logger.debug(f"Saved index data with systime {systime_str}")
        except Exception as e:
            logger.error(f"Error saving index data: {e}")
            if systime_str in self.index_cache:
                self.index_cache.discard(systime_str)

    async def monitor_cycle(self):
        logger.info("Starting monitoring cycle")
        
        tickers = self.get_tickers_list()
        
        for ticker in tickers:
            try:
                data = await self.fetch_ticker_data(ticker)
                if data:
                    new_data = self.filter_new_candles(ticker, data)
                    await self.save_ticker_data(ticker, data, new_data)
                    logger.info(
                        f"Processed {len(data)} candles (last {self.filter_hours}h) for {ticker}, "
                        f"{len(new_data)} new"
                    )
                else:
                    logger.debug(f"No candles in last {self.filter_hours}h for ticker {ticker}")
            except Exception as e:
                logger.error(f"Error processing ticker {ticker}: {e}")
        
        try:
            index_data = await self.fetch_index_data()
            await self.save_index_data(index_data)
            if index_data:
                logger.info(f"Processed index data for {index_data.get('SECID')}")
        except Exception as e:
            logger.error(f"Error processing index data: {e}")
        
        logger.info("Monitoring cycle completed")

    async def run(self):
        logger.info("Starting market monitor")
        
        while True:
            try:
                await self.monitor_cycle()
                
                wait_seconds = 60 - datetime.now().second
                if wait_seconds > 0:
                    logger.debug(f"Waiting {wait_seconds} seconds until next minute")
                    await asyncio.sleep(wait_seconds)
            except KeyboardInterrupt:
                logger.info("Monitor stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in monitor loop: {e}", exc_info=True)
                await asyncio.sleep(60)


async def main():
    from app.moex_market.config import load_clickhouse_config
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    config = load_clickhouse_config()
    db = ClickHouseDatabase(config)
    
    try:
        await db.connect()
        await db.create_tables()
        
        async with MoexMarketClient() as moex_client:
            monitor = MarketMonitor(moex_client, db, filter_hours=config.filter_hours)
            await monitor.run()
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
