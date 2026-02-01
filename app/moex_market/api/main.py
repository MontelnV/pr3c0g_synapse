import logging
import sys
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.responses import JSONResponse

from app.moex_market.client import MoexMarketClient
from app.moex_market.database import ClickHouseDatabase
from app.moex_market.config import load_clickhouse_config
from .models import (
    PriceResponse,
    BatchPriceRequest,
    BatchPriceResponse,
    TickerDataResponse,
    QuoteResponse,
    IndexResponse,
    ErrorResponse,
)

logger = logging.getLogger(__name__)

moex_client: Optional[MoexMarketClient] = None
clickhouse_db: Optional[ClickHouseDatabase] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global moex_client, clickhouse_db
    
    logger.info("Starting MOEX API service...")
    
    moex_client = MoexMarketClient()
    await moex_client.__aenter__()
    
    clickhouse_config = load_clickhouse_config()
    clickhouse_db = ClickHouseDatabase(clickhouse_config)
    await clickhouse_db.connect()
    
    logger.info("MOEX API service started")
    
    yield
    
    logger.info("Shutting down MOEX API service...")
    
    if moex_client:
        await moex_client.__aexit__(None, None, None)
    if clickhouse_db:
        await clickhouse_db.close()
    
    logger.info("MOEX API service stopped")


app = FastAPI(
    title="MOEX Market API",
    description="API for accessing MOEX market data and prices",
    version="1.0.0",
    lifespan=lifespan,
)


def get_moex_client() -> MoexMarketClient:
    if moex_client is None:
        raise HTTPException(status_code=503, detail="MOEX client not initialized")
    return moex_client


def get_clickhouse_db() -> ClickHouseDatabase:
    if clickhouse_db is None:
        raise HTTPException(status_code=503, detail="ClickHouse database not initialized")
    return clickhouse_db


async def get_current_price_from_db(ticker: str, db: ClickHouseDatabase) -> Optional[float]:
    if db.client is None:
        return None
    
    today = datetime.now().strftime("%Y-%m-%d")
    ticker_upper = ticker.upper()
    
    query = """
    SELECT close
    FROM ticker_candles
    WHERE ticker = %s
      AND trade_date = %s
    ORDER BY begin DESC
    LIMIT 1
    """
    
    try:
        rows = await db.client.fetch(query, ticker_upper, today)
        if rows:
            return float(rows[0][0])
    except Exception as e:
        logger.debug(f"ClickHouse query error for {ticker}: {e}")
    
    return None


async def get_current_price_from_moex(ticker: str, client: MoexMarketClient) -> Optional[float]:
    try:
        quotes = await client.get_current_quotes(ticker=ticker.upper())
        if quotes and len(quotes) > 0:
            quote = quotes[0]
            price = quote.get("LAST") or quote.get("CLOSE")
            if price is not None:
                return float(price)
    except Exception as e:
        logger.debug(f"MOEX API error for {ticker}: {e}")
    
    return None


@app.get("/api/v1/prices/{ticker}", response_model=PriceResponse)
async def get_price(
    ticker: str,
    moex: MoexMarketClient = Depends(get_moex_client),
    db: ClickHouseDatabase = Depends(get_clickhouse_db),
):
    try:
        price = await get_current_price_from_db(ticker, db)
        if price is not None:
            return PriceResponse(ticker=ticker.upper(), price=price)
        
        price = await get_current_price_from_moex(ticker, moex)
        return PriceResponse(ticker=ticker.upper(), price=price)
    except Exception as e:
        logger.error(f"Error getting price for {ticker}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get price for {ticker}")


@app.post("/api/v1/prices/batch", response_model=BatchPriceResponse)
async def get_prices_batch(
    request: BatchPriceRequest,
    moex: MoexMarketClient = Depends(get_moex_client),
    db: ClickHouseDatabase = Depends(get_clickhouse_db),
):
    try:
        prices = {}
        
        for ticker in request.tickers:
            price = await get_current_price_from_db(ticker, db)
            if price is None:
                price = await get_current_price_from_moex(ticker, moex)
            prices[ticker.upper()] = price
        
        return BatchPriceResponse(prices=prices)
    except Exception as e:
        logger.error(f"Error getting batch prices: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get batch prices")


@app.get("/api/v1/ticker/{ticker}/data", response_model=TickerDataResponse)
async def get_ticker_data(
    ticker: str,
    start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
    end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format"),
    interval: int = Query(24, description="Interval in minutes"),
    moex: MoexMarketClient = Depends(get_moex_client),
):
    try:
        data = await moex.get_ticker_data(
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
            interval=interval,
        )
        return TickerDataResponse(ticker=ticker.upper(), data=data, count=len(data))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting ticker data for {ticker}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get ticker data for {ticker}")


@app.get("/api/v1/quotes/{ticker}", response_model=QuoteResponse)
async def get_quotes(
    ticker: str,
    moex: MoexMarketClient = Depends(get_moex_client),
):
    try:
        quotes = await moex.get_current_quotes(ticker=ticker)
        return QuoteResponse(ticker=ticker.upper(), quotes=quotes)
    except Exception as e:
        logger.error(f"Error getting quotes for {ticker}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get quotes for {ticker}")


@app.get("/api/v1/quotes", response_model=QuoteResponse)
async def get_all_quotes(
    moex: MoexMarketClient = Depends(get_moex_client),
):
    try:
        quotes = await moex.get_current_quotes()
        return QuoteResponse(quotes=quotes)
    except Exception as e:
        logger.error(f"Error getting all quotes: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get quotes")


@app.get("/api/v1/index/{index_name}", response_model=IndexResponse)
async def get_index(
    index_name: str = "IMOEX",
    moex: MoexMarketClient = Depends(get_moex_client),
):
    try:
        index_data = await moex.get_moex_index(index_name=index_name)
        if index_data is None:
            raise HTTPException(status_code=404, detail=f"Index {index_name} not found")
        return IndexResponse(index_name=index_name, data=index_data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting index {index_name}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get index {index_name}")


@app.get("/health")
async def health_check():
    return {"status": "ok"}


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(error="Internal server error", detail=str(exc)).dict(),
    )
