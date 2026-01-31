import logging
import sys
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional, List

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.responses import JSONResponse
from sqlalchemy import select, func, or_, and_

from app.news_feeder.database import Database
from app.news_feeder.config import load_config
from app.news_feeder.models import News
from .models import (
    NewsListResponse,
    NewsResponse,
    NewsItem,
    ErrorResponse,
)

logger = logging.getLogger(__name__)

news_db: Optional[Database] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global news_db
    
    logger.info("Starting News API service...")
    
    config = load_config()
    news_db = Database(config.database)
    await news_db.connect()
    
    logger.info("News API service started")
    
    yield
    
    logger.info("Shutting down News API service...")
    
    if news_db:
        await news_db.close()
    
    logger.info("News API service stopped")


app = FastAPI(
    title="News Feeder API",
    description="API for accessing news feed data",
    version="1.0.0",
    lifespan=lifespan,
)


def get_database() -> Database:
    if news_db is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    return news_db


@app.get("/api/v1/news", response_model=NewsListResponse)
async def get_news(
    limit: int = Query(50, ge=1, le=1000, description="Number of news items to return"),
    offset: int = Query(0, ge=0, description="Number of news items to skip"),
    channel: Optional[str] = Query(None, description="Filter by channel name"),
    start_date: Optional[str] = Query(None, description="Start date in ISO format (YYYY-MM-DDTHH:MM:SS)"),
    end_date: Optional[str] = Query(None, description="End date in ISO format (YYYY-MM-DDTHH:MM:SS)"),
    tags: Optional[str] = Query(None, description="Filter by tags (comma-separated)"),
    search: Optional[str] = Query(None, description="Search in news text"),
    db: Database = Depends(get_database),
):
    try:
        session_maker = db.async_session_maker
        if session_maker is None:
            raise HTTPException(status_code=503, detail="Database session not available")
        
        async with session_maker() as session:
            query = select(News)
            count_query = select(func.count(News.id))
            
            conditions = []
            
            if channel:
                conditions.append(News.channel == channel)
            
            if start_date:
                try:
                    start_dt = datetime.fromisoformat(start_date.replace("Z", "+00:00"))
                    conditions.append(News.date >= start_dt)
                except ValueError:
                    raise HTTPException(status_code=400, detail=f"Invalid start_date format: {start_date}")
            
            if end_date:
                try:
                    end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                    conditions.append(News.date <= end_dt)
                except ValueError:
                    raise HTTPException(status_code=400, detail=f"Invalid end_date format: {end_date}")
            
            if tags:
                tag_list = [tag.strip().lower() for tag in tags.split(",") if tag.strip()]
                if tag_list:
                    tag_conditions = [News.tags.contains([tag]) for tag in tag_list]
                    conditions.append(or_(*tag_conditions))
            
            if search:
                conditions.append(News.text.ilike(f"%{search}%"))
            
            if conditions:
                query = query.where(and_(*conditions))
                count_query = count_query.where(and_(*conditions))
            
            total_result = await session.scalar(count_query)
            total = total_result if total_result is not None else 0
            
            query = query.order_by(News.date.desc()).limit(limit).offset(offset)
            
            result = await session.scalars(query)
            news_list = result.all()
            
            news_items = [
                NewsItem(
                    id=news.id,
                    msg_id=news.msg_id,
                    channel=news.channel,
                    date=news.date,
                    tags=news.tags or [],
                    text=news.text,
                )
                for news in news_list
            ]
            
            return NewsListResponse(
                news=news_items,
                total=total,
                limit=limit,
                offset=offset,
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting news: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get news")


@app.get("/api/v1/news/{news_id}", response_model=NewsResponse)
async def get_news_by_id(
    news_id: int,
    db: Database = Depends(get_database),
):
    try:
        session_maker = db.async_session_maker
        if session_maker is None:
            raise HTTPException(status_code=503, detail="Database session not available")
        
        async with session_maker() as session:
            query = select(News).where(News.id == news_id)
            result = await session.scalar(query)
            
            if result is None:
                raise HTTPException(status_code=404, detail=f"News with id {news_id} not found")
            
            news_item = NewsItem(
                id=result.id,
                msg_id=result.msg_id,
                channel=result.channel,
                date=result.date,
                tags=result.tags or [],
                text=result.text,
            )
            
            return NewsResponse(news=news_item)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting news by id {news_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get news by id {news_id}")


@app.get("/api/v1/news/channel/{channel}", response_model=NewsListResponse)
async def get_news_by_channel(
    channel: str,
    limit: int = Query(50, ge=1, le=1000, description="Number of news items to return"),
    offset: int = Query(0, ge=0, description="Number of news items to skip"),
    db: Database = Depends(get_database),
):
    try:
        session_maker = db.async_session_maker
        if session_maker is None:
            raise HTTPException(status_code=503, detail="Database session not available")
        
        async with session_maker() as session:
            query = select(News).where(News.channel == channel)
            count_query = select(func.count(News.id)).where(News.channel == channel)
            
            total_result = await session.scalar(count_query)
            total = total_result if total_result is not None else 0
            
            query = query.order_by(News.date.desc()).limit(limit).offset(offset)
            
            result = await session.scalars(query)
            news_list = result.all()
            
            news_items = [
                NewsItem(
                    id=news.id,
                    msg_id=news.msg_id,
                    channel=news.channel,
                    date=news.date,
                    tags=news.tags or [],
                    text=news.text,
                )
                for news in news_list
            ]
            
            return NewsListResponse(
                news=news_items,
                total=total,
                limit=limit,
                offset=offset,
            )
    except Exception as e:
        logger.error(f"Error getting news for channel {channel}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get news for channel {channel}")


@app.get("/api/v1/channels", response_model=List[str])
async def get_channels(
    db: Database = Depends(get_database),
):
    try:
        session_maker = db.async_session_maker
        if session_maker is None:
            raise HTTPException(status_code=503, detail="Database session not available")
        
        async with session_maker() as session:
            query = select(News.channel).distinct()
            result = await session.scalars(query)
            channels = [channel for channel in result.all()]
            
            return sorted(channels)
    except Exception as e:
        logger.error(f"Error getting channels: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get channels")


@app.get("/api/v1/tags", response_model=List[str])
async def get_tags(
    db: Database = Depends(get_database),
):
    try:
        session_maker = db.async_session_maker
        if session_maker is None:
            raise HTTPException(status_code=503, detail="Database session not available")
        
        async with session_maker() as session:
            query = select(News.tags)
            result = await session.scalars(query)
            
            all_tags = set()
            for tags_list in result.all():
                if tags_list:
                    all_tags.update(tag.lower() for tag in tags_list)
            
            return sorted(list(all_tags))
    except Exception as e:
        logger.error(f"Error getting tags: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get tags")


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
