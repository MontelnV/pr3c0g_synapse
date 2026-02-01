import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class ClickHouseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    filter_hours: int = 1


def load_clickhouse_config() -> ClickHouseConfig:
    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    database = os.getenv("CLICKHOUSE_DATABASE", "default")
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "123")
    filter_hours = int(os.getenv("MOEX_FILTER_HOURS", 1))

    return ClickHouseConfig(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        filter_hours=filter_hours,
    )
