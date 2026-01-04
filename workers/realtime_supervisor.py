# workers/realtime_supervisor.py
from __future__ import annotations

import asyncio
import contextlib
import logging

from motor.motor_asyncio import AsyncIOMotorClient

from adapters.external.signals.signals_http_client import SignalsHttpClient
from config.settings import settings

from adapters.external.binance.binance_rest_client import BinanceRestClient
from adapters.external.binance.binance_websocket_client import BinanceWebsocketClient
from adapters.external.database.candle_repository_mongodb import CandleRepositoryMongoDB
from adapters.external.database.indicator_repository_mongodb import IndicatorRepositoryMongoDB
from adapters.external.database.indicator_set_repository_mongodb import IndicatorSetRepositoryMongoDB
from adapters.external.database.mongodb_client import get_mongo_client
from adapters.external.database.processing_offset_repository_mongodb import ProcessingOffsetRepositoryMongoDB
from core.services.indicator_calculation_service import IndicatorCalculationService
from core.usecases.backfill_candles_use_case import BackfillCandlesUseCase
from core.usecases.compute_indicators_use_case import ComputeIndicatorsUseCase
from core.usecases.start_realtime_ingestion_use_case import StartRealtimeIngestionUseCase


class RealtimeSupervisor:
    """
   api-market-data supervisor.

    Responsibilities:
    - Connect to MongoDB and ensure indexes.
    - Start realtime ingestion (Binance WS -> candles).
    - Optional backfill on start (resume from offsets).
    - Compute indicators for each ACTIVE indicator set per stream_key.
    """

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._mongo_client: AsyncIOMotorClient | None = None
        self._db = None

        self._ws_clients: list[BinanceWebsocketClient] = []
        self._ingestions: list[StartRealtimeIngestionUseCase] = []

    @property
    def db(self):
        """
        Return the current MongoDB database handle (if initialized).
        """
        return self._db

    async def start(self) -> None:
        """
        Initialize repositories/services and start ingestion pipelines.
        """
        self._mongo_client = get_mongo_client()
        self._db = self._mongo_client[settings.MONGODB_DB_NAME]

        candle_repo = CandleRepositoryMongoDB(self._db)
        offset_repo = ProcessingOffsetRepositoryMongoDB(self._db)
        indicator_repo = IndicatorRepositoryMongoDB(self._db)
        indicator_set_repo = IndicatorSetRepositoryMongoDB(self._db)

        await candle_repo.ensure_indexes()
        await offset_repo.ensure_indexes()
        await indicator_repo.ensure_indexes()
        await indicator_set_repo.ensure_indexes()

        indicator_svc = IndicatorCalculationService()
        compute_indicators_uc = ComputeIndicatorsUseCase(
            candle_repository=candle_repo,
            indicator_repository=indicator_repo,
            indicator_service=indicator_svc,
        )

        binance_rest = BinanceRestClient(base_url=settings.BINANCE_REST_BASE_URL)
        backfill_uc = BackfillCandlesUseCase(
            binance_client=binance_rest,
            candle_repository=candle_repo,
            processing_offset_repository=offset_repo,
        )

        source = (settings.INGESTION_SOURCE or "binance").lower()
        interval = settings.BINANCE_STREAM_INTERVAL
        symbols = [s.strip() for s in (settings.BINANCE_STREAM_SYMBOLS or "").split(",") if s.strip()]

        if not symbols:
            self._logger.error("No BINANCE_STREAM_SYMBOLS configured. Aborting start().")
            return

        self._logger.info("Starting ingestion source=%s symbols=%s interval=%s", source, symbols, interval)

        for symbol in symbols:
            if settings.ENABLE_BACKFILL_ON_START:
                try:
                    await backfill_uc.execute_for_symbol(source=source, symbol=symbol, interval=interval)
                except Exception as exc:
                    self._logger.exception("Backfill error for %s@%s: %s", symbol, interval, exc)

            ws_client = BinanceWebsocketClient(base_ws_url=settings.BINANCE_WS_BASE_URL)
            signals_client = SignalsHttpClient(base_url=settings.SIGNALS_BASE_URL)
            
            ingestion_uc = StartRealtimeIngestionUseCase(
                source=source,
                symbol=symbol,
                interval=interval,
                websocket_client=ws_client,
                candle_repository=candle_repo,
                processing_offset_repository=offset_repo,
                compute_indicators_use_case=compute_indicators_uc,
                indicator_set_repo=indicator_set_repo,
                logger=None,
                signals_client=signals_client
            )

            self._ws_clients.append(ws_client)
            self._ingestions.append(ingestion_uc)

        for ingestion_uc in self._ingestions:
            await ingestion_uc.execute()

        self._logger.info("Realtime ingestion started.")

    async def stop(self) -> None:
        """
        Stop websocket clients and close MongoDB connection.
        """
        for ws in self._ws_clients:
            with contextlib.suppress(Exception):
                await ws.close()

        if self._mongo_client:
            self._mongo_client.close()
