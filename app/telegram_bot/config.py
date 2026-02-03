import os
from dataclasses import dataclass
from typing import List, Optional
from dotenv import load_dotenv

load_dotenv()


@dataclass
class BotConfig:
    token: str
    admin_ids: List[int]


@dataclass
class PortfolioDatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass
class MoexAPIConfig:
    url: str


def load_bot_config() -> BotConfig:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise ValueError("TELEGRAM_BOT_TOKEN must be set in .env file")
    
    admin_ids_str = os.getenv("TELEGRAM_ADMIN_IDS", "")
    admin_ids = []
    if admin_ids_str:
        admin_ids = [int(id.strip()) for id in admin_ids_str.split(",") if id.strip()]
    
    return BotConfig(token=token, admin_ids=admin_ids)


def load_portfolio_db_config() -> PortfolioDatabaseConfig:
    host = os.getenv("PORTFOLIO_DB_HOST", "localhost")
    port = int(os.getenv("PORTFOLIO_DB_PORT", "5432"))
    database = os.getenv("PORTFOLIO_DB_NAME", "portfolio")
    user = os.getenv("PORTFOLIO_DB_USER", "postgres")
    password = os.getenv("PORTFOLIO_DB_PASSWORD", "MontelnS2005")
    
    if not password:
        raise ValueError("PORTFOLIO_DB_PASSWORD must be set in .env file")
    
    return PortfolioDatabaseConfig(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )


def load_moex_api_config() -> MoexAPIConfig:
    url = os.getenv("MOEX_API_URL", "http://localhost:8000")
    
    if not url:
        raise ValueError("MOEX_API_URL must be set in .env file")
    
    return MoexAPIConfig(url=url)
