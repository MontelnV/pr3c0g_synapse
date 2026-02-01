from typing import List, Dict, Optional, Any
from pydantic import BaseModel


class PriceResponse(BaseModel):
    ticker: str
    price: Optional[float]


class BatchPriceRequest(BaseModel):
    tickers: List[str]


class BatchPriceResponse(BaseModel):
    prices: Dict[str, Optional[float]]


class TickerDataResponse(BaseModel):
    ticker: str
    data: List[Dict[str, Any]]
    count: int


class QuoteResponse(BaseModel):
    ticker: Optional[str] = None
    quotes: List[Dict[str, Any]]


class IndexResponse(BaseModel):
    index_name: str
    data: Optional[Dict[str, Any]]


class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
