import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class APIConfig:
    host: str
    port: int


def load_api_config() -> APIConfig:
    host = os.getenv("MOEX_API_HOST", "0.0.0.0")
    port = int(os.getenv("MOEX_API_PORT", "8000"))
    
    return APIConfig(host=host, port=port)
