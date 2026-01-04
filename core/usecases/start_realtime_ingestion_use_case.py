# core/usecases/start_realtime_ingestion_use_case.py
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from adapters.external.binance.binance_websocket_client import BinanceWebsocketClient  # type: ignore
from adapters.external.signals.signals_http_client import SignalsHttpClient
from core.domain.entities.candle_entity import CandleEntity
from core.repositories.candle_repository import CandleRepository
from core.repositories.indicator_set_repository import IndicatorSetRepository
from core.repositories.processing_offset_repository import ProcessingOffsetRepository
from core.usecases.compute_indicators_use_case import ComputeIndicatorsUseCase


class StartRealtimeIngestionUseCase:
    """
   Starts realtime ingestion from an exchange websocket and stores closed candles.
    """

    def __init__(
        self,
        *,
        source: str,
        symbol: str,
        interval: str,
        websocket_client: BinanceWebsocketClient,
        candle_repository: CandleRepository,
        processing_offset_repository: ProcessingOffsetRepository,
        compute_indicators_use_case: Optional[ComputeIndicatorsUseCase] = None,
        indicator_set_repo: Optional[IndicatorSetRepository] = None,
        logger: logging.Logger | None = None,
        signals_client: Optional[SignalsHttpClient] = None,
    ):
        self._source = str(source).lower()
        self._symbol = symbol.upper()
        self._interval = str(interval)
        self._ws = websocket_client
        self._candle_repo = candle_repository
        self._offset_repo = processing_offset_repository
        self._compute_indicators = compute_indicators_use_case
        self._indicator_set_repo = indicator_set_repo
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self._stream_key = self._build_stream_key(source=self._source, symbol=self._symbol, interval=self._interval)
        self._signals_client = signals_client
        
    async def execute(self) -> None:
        """
        Subscribe to the websocket stream and handle closed klines.
        """
        self._logger.info(
            "Starting realtime ingestion for source=%s symbol=%s interval=%s",
            self._source,
            self._symbol,
            self._interval,
        )
        await self._ws.subscribe_kline_1m(self._symbol, self._on_kline_closed)

    async def _on_kline_closed(self, event: Dict[str, Any]) -> None:
        """
        Handle a closed kline websocket event and persist candle + offsets.
        """
        try:
            k = event["k"]
            candle = CandleEntity(
                stream_key=self._stream_key,
                source=self._source,
                symbol=str(event["s"]).upper(),
                interval=str(k["i"]),
                open_time=int(k["t"]),
                close_time=int(k["T"]),
                open=float(k["o"]),
                high=float(k["h"]),
                low=float(k["l"]),
                close=float(k["c"]),
                volume=float(k["v"]),
                trades=int(k["n"]),
                is_closed=True,
            )

            await self._candle_repo.upsert_closed_candle(candle)
            await self._offset_repo.set_last_closed_open_time(self._stream_key, candle.open_time)

            if self._compute_indicators is not None and self._indicator_set_repo is not None:
                active_sets = await self._indicator_set_repo.get_active_by_stream(self._stream_key)
                for indset in active_sets:
                    indicator_snapshot = await self._compute_indicators.execute_for_indicator_set(
                        stream_key=self._stream_key,
                        ema_fast=int(indset.ema_fast),
                        ema_slow=int(indset.ema_slow),
                        atr_window=int(indset.atr_window),
                        indicator_set_id=indset.cfg_hash,
                        cfg_hash=indset.cfg_hash,
                        ts=candle.close_time,
                    )

                    if self._signals_client is not None and indicator_snapshot is not None:
                        await self._signals_client.candle_closed(
                            indicator_set_id=indset.cfg_hash,
                            ts=candle.close_time,
                            indicator_set=indset.to_dict(),
                            indicator_snapshot=indicator_snapshot.to_dict(),
                        )
                        
        except Exception as exc:
            self._logger.exception("Failed to process closed kline: %s", exc)

    @staticmethod
    def _build_stream_key(*, source: str, symbol: str, interval: str) -> str:
        """
        Build a canonical stream key for multi-source ingestion.
        """
        return f"{source.lower()}:{symbol.lower()}:{interval}"
