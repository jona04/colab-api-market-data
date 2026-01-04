"""
Application configuration for api-market-data.

Centralizes environment variables using python-dotenv.
"""

import os
from dotenv import load_dotenv

# Load variables from .env (if present)
load_dotenv()


class Settings:
    """
    Configuration settings for the api-market-data service.
    """

    # Log / app
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
    APP_NAME: str = os.getenv("APP_NAME", "api-market-data")

    # Mongo
    MONGODB_URL: str = os.getenv("MONGODB_URL", "mongodb://mongo-market-data:27017")
    MONGODB_DB_NAME: str = os.getenv("MONGODB_DB_NAME", "api_market_data")

    # Ingestion source name (future-proof)
    INGESTION_SOURCE: str = os.getenv("INGESTION_SOURCE", "binance").lower()

    # Binance
    BINANCE_REST_BASE_URL: str = os.getenv("BINANCE_REST_BASE_URL", "https://api.binance.com")
    BINANCE_WS_BASE_URL: str = os.getenv("BINANCE_WS_BASE_URL", "wss://stream.binance.com:9443")
    BINANCE_STREAM_INTERVAL: str = os.getenv("BINANCE_STREAM_INTERVAL", "1m")

    # Comma-separated symbols in env -> List parsing is done where used
    BINANCE_STREAM_SYMBOLS: str = os.getenv("BINANCE_STREAM_SYMBOLS", "btcusdt")

    # Backfill
    ENABLE_BACKFILL_ON_START: bool = os.getenv("ENABLE_BACKFILL_ON_START", "true").lower() == "true"

    # External APIs
    SIGNALS_BASE_URL: str = os.getenv("SIGNALS_BASE_URL", "http://host.docker.internal:8080")


settings = Settings()
