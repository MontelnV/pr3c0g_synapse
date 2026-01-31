import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class APIConfig:
    host: str
    port: int


def load_api_config() -> APIConfig:
    host = os.getenv("NEWS_API_HOST")
    port = int(os.getenv("NEWS_API_PORT"))
    
    return APIConfig(host=host, port=port)
