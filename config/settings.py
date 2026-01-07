"""
Application configuration for api-market-data.

Centralizes environment variables using python-dotenv.

Note:
- The runtime ingestion configuration is stored in MongoDB.
- The .env contains only Mongo connection + optional bootstrap defaults.
"""

import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    """
    Configuration settings for the api-market-data service.
    """

    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
    APP_NAME: str = os.getenv("APP_NAME", "api-market-data")

    # Mongo
    MONGODB_URL: str = os.getenv("MONGODB_URL", "mongodb://mongo-market-data:27017")
    MONGODB_DB_NAME: str = os.getenv("MONGODB_DB_NAME", "api_market_data")

    # Fallback only (if Mongo system_config not defined yet)
    SIGNALS_BASE_URL: str = os.getenv("SIGNALS_BASE_URL", "http://host.docker.internal:8080")

    # Bootstrap defaults (optional; used only if Mongo has no ingestion_streams yet)
    BOOTSTRAP_BINANCE_WS_BASE_URL: str = os.getenv("BOOTSTRAP_BINANCE_WS_BASE_URL", "wss://stream.binance.com:9443")
    BOOTSTRAP_BINANCE_REST_BASE_URL: str = os.getenv("BOOTSTRAP_BINANCE_REST_BASE_URL", "https://api.binance.com")
    BOOTSTRAP_BINANCE_STREAM_INTERVAL: str = os.getenv("BOOTSTRAP_BINANCE_STREAM_INTERVAL", "1m")
    BOOTSTRAP_BINANCE_STREAM_SYMBOLS: str = os.getenv("BOOTSTRAP_BINANCE_STREAM_SYMBOLS", "btcusdt")
    BOOTSTRAP_ENABLE_BACKFILL_ON_START: bool = os.getenv("BOOTSTRAP_ENABLE_BACKFILL_ON_START", "true").lower() == "true"


settings = Settings()
