import logging
from typing import Optional, Dict, List

import aiohttp
from aiohttp import ClientError, ClientConnectorError, ServerTimeoutError
import asyncio

logger = logging.getLogger(__name__)


class PriceService:
    def __init__(self, api_url: str, timeout: float = 10.0):
        self.api_url = api_url.rstrip("/")
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None

    async def _ensure_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self.timeout)

    async def close(self):
        if self._session:
            await self._session.close()
            self._session = None

    async def get_current_price(self, ticker: str) -> Optional[float]:
        await self._ensure_session()
        
        url = f"{self.api_url}/api/v1/prices/{ticker}"
        
        try:
            async with self._session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("price")
                elif response.status == 404:
                    logger.warning(f"Ticker {ticker} not found")
                    return None
                else:
                    logger.error(f"API returned status {response.status} for {ticker}")
                    return None
        except (ClientConnectorError, ServerTimeoutError, asyncio.TimeoutError) as e:
            logger.error(f"Connection error when requesting price for {ticker}: {e}")
            return None
        except ClientError as e:
            logger.error(f"HTTP client error when requesting price for {ticker}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error when requesting price for {ticker}: {e}", exc_info=True)
            return None

    async def get_current_prices(self, tickers: List[str]) -> Dict[str, Optional[float]]:
        await self._ensure_session()
        
        url = f"{self.api_url}/api/v1/prices/batch"
        payload = {"tickers": tickers}
        
        try:
            async with self._session.post(url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("prices", {})
                else:
                    logger.error(f"API returned status {response.status} for batch prices")
                    return {ticker: None for ticker in tickers}
        except (ClientConnectorError, ServerTimeoutError, asyncio.TimeoutError) as e:
            logger.error(f"Connection error when requesting batch prices: {e}")
            return {ticker: None for ticker in tickers}
        except ClientError as e:
            logger.error(f"HTTP client error when requesting batch prices: {e}")
            return {ticker: None for ticker in tickers}
        except Exception as e:
            logger.error(f"Unexpected error when requesting batch prices: {e}", exc_info=True)
            return {ticker: None for ticker in tickers}
