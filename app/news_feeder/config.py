import os
from dataclasses import dataclass
from typing import List
from dotenv import load_dotenv

load_dotenv()

@dataclass
class TelegramConfig:
    api_id: int
    api_hash: str
    session_file: str


@dataclass
class DatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass
class Config:
    telegram: TelegramConfig
    database: DatabaseConfig
    channels: List[str]


def load_config() -> Config:
    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    session_file = os.getenv("TELEGRAM_SESSION_FILE")

    if not api_id or not api_hash:
        raise ValueError(
            "TELEGRAM_API_ID and TELEGRAM_API_HASH must be set in .env file"
        )

    telegram_config = TelegramConfig(
        api_id=int(api_id), api_hash=api_hash, session_file=session_file
    )

    db_host = os.getenv("DB_HOST")
    db_port = int(os.getenv("DB_PORT"))
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")

    if not db_password:
        raise ValueError("DB_PASSWORD must be set in .env file")

    database_config = DatabaseConfig(
        host=db_host, port=db_port, database=db_name, user=db_user, password=db_password
    )

    channels_str = os.getenv("TELEGRAM_CHANNELS", "")
    if not channels_str:
        raise ValueError("TELEGRAM_CHANNELS must be set in .env file")
    channels = [ch.strip() for ch in channels_str.split(",") if ch.strip()]

    return Config(telegram=telegram_config, database=database_config, channels=channels)
