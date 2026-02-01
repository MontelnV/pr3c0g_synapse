import asyncio
import logging
import re
from typing import Optional

import aiohttp
from aiohttp import ClientConnectorError, ClientError, ServerTimeoutError
from aiomoex import candles, history
from aiomoex.client import ISSMoexError

logger = logging.getLogger(__name__)

VALID_INTERVALS = {1, 10, 60, 24, 7, 31, 4}
DATE_FORMAT = r"^\d{4}-\d{2}-\d{2}$"


def _validate_date(date_str: str, param_name: str) -> None:
    if not re.match(DATE_FORMAT, date_str):
        raise ValueError(
            f"{param_name} must be in format YYYY-MM-DD (e.g., 2024-01-01), "
            f"got: {date_str}"
        )


def _validate_interval(interval: int) -> None:
    if interval not in VALID_INTERVALS:
        valid_values = ", ".join(map(str, sorted(VALID_INTERVALS)))
        raise ValueError(
            f"interval must be one of: {valid_values}, "
            f"got: {interval}"
        )


class MoexMarketClient:
    def __init__(self, timeout: aiohttp.ClientTimeout = None):
        if timeout is None:
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
        self._timeout = timeout
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "MoexMarketClient":
        self._session = aiohttp.ClientSession(timeout=self._timeout)
        logger.debug("HTTP session created")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()
            logger.debug("HTTP session closed")
        self._session = None

    def _ensure_session(self):
        if self._session is None:
            raise RuntimeError(
                "MoexMarketClient must be used as a context manager: "
                "async with MoexMarketClient() as client: ..."
            )

    async def get_ticker_data(
        self,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        interval: int = 24,
        market: str = "shares",
        engine: str = "stock",
    ) -> list[dict]:
        self._ensure_session()
        
        if start_date:
            _validate_date(start_date, "start_date")
        if end_date:
            _validate_date(end_date, "end_date")
        _validate_interval(interval)

        logger.info(
            f"Requesting data for ticker {ticker}, "
            f"period: {start_date or 'start'} - {end_date or 'end'}, "
            f"interval: {interval}"
        )

        try:
            data = await candles.get_market_candles(
                session=self._session,
                security=ticker,
                interval=interval,
                start=start_date,
                end=end_date,
                market=market,
                engine=engine,
            )
            logger.info(f"Retrieved {len(data)} records for ticker {ticker}")
            return data
        except (ClientConnectorError, ServerTimeoutError, asyncio.TimeoutError) as e:
            logger.error(f"Connection timeout or error when requesting data for {ticker}: {e}")
            raise
        except ISSMoexError as e:
            logger.error(f"MOEX ISS API error when requesting data for {ticker}: {e}")
            raise
        except ClientError as e:
            logger.error(f"HTTP client error when requesting data for {ticker}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error when requesting data for {ticker}: {e}", exc_info=True)
            raise

    async def get_current_quotes(
        self,
        ticker: Optional[str] = None,
        market: str = "shares",
        engine: str = "stock",
    ) -> list[dict]:
        self._ensure_session()
        
        logger.info(f"Requesting current quotes" + (f" for ticker {ticker}" if ticker else ""))
        
        try:
            data = await history.get_board_securities(
                session=self._session,
                table="marketdata",
                columns=None,
                market=market,
                engine=engine,
            )
            
            if ticker:
                ticker_upper = ticker.upper()
                filtered_data = [item for item in data if item.get("SECID") == ticker_upper]
                logger.info(f"Retrieved {len(filtered_data)} quote(s) for ticker {ticker}")
                return filtered_data
            else:
                logger.info(f"Retrieved {len(data)} quotes")
                return data

        except (ClientConnectorError, ServerTimeoutError, asyncio.TimeoutError) as e:
            logger.error(f"Connection timeout or error when requesting quotes: {e}")
            raise
        except ISSMoexError as e:
            logger.error(f"MOEX ISS API error when requesting quotes: {e}")
            raise
        except ClientError as e:
            logger.error(f"HTTP client error when requesting quotes: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error when requesting quotes: {e}", exc_info=True)
            raise

    async def get_moex_index(
        self,
        index_name: str = "IMOEX",
    ) -> Optional[dict]:
        self._ensure_session()
        
        logger.info(f"Requesting current value for index {index_name}")
        
        try:
            data = await history.get_board_securities(
                session=self._session,
                table="marketdata",
                columns=None,
                board="SNDX",
                market="index",
                engine="stock",
            )
            
            index_data = next((item for item in data if item.get("SECID") == index_name), None)
            
            if index_data:
                logger.info(f"Retrieved data for index {index_name}")
            else:
                logger.warning(f"Index {index_name} not found in market data")
            
            return index_data

        except (ClientConnectorError, ServerTimeoutError, asyncio.TimeoutError) as e:
            logger.error(f"Connection timeout or error when requesting index {index_name}: {e}")
            raise
        except ISSMoexError as e:
            logger.error(f"MOEX ISS API error when requesting index {index_name}: {e}")
            raise
        except ClientError as e:
            logger.error(f"HTTP client error when requesting index {index_name}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error when requesting index {index_name}: {e}", exc_info=True)
            raise


async def main():
    async with MoexMarketClient() as client:
        print("=== Example 1: Get ticker data ===")
        data = await client.get_ticker_data(
            ticker="SBER",
            start_date="2024-01-01",
            end_date="2024-01-31",
            interval=1
        )
        print(f"Retrieved {len(data)} records")
        if data:
            print(f"First record: {data[0]}")
        
        print("\n=== Example 2: Get MOEX index ===")
        index_data = await client.get_moex_index("IMOEX")
        if index_data:
            print(f"Index data: {index_data}")
            print(f"Current value: {index_data.get('CURRENTVALUE')}")


if __name__ == "__main__":
    import asyncio
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    asyncio.run(main())
