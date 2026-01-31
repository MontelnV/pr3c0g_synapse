from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel


class NewsItem(BaseModel):
    id: int
    msg_id: int
    channel: str
    date: datetime
    tags: List[str]
    text: str

    class Config:
        from_attributes = True


class NewsListResponse(BaseModel):
    news: List[NewsItem]
    total: int
    limit: int
    offset: int


class NewsResponse(BaseModel):
    news: NewsItem


class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
