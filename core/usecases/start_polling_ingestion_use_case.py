from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from adapters.external.signals.signals_http_client import SignalsHttpClient
from core.domain.entities.candle_entity import CandleEntity
from core.repositories.candle_repository import CandleRepository
from core.repositories.indicator_set_repository import IndicatorSetRepository
from core.usecases.compute_indicators_use_case import ComputeIndicatorsUseCase


class StartPollingIngestionUseCase:
    """
    Starts a polling-based ingestion loop (e.g. The Graph, REST snapshots, etc).

    Each tick produces a synthetic 1m candle for the configured stream_key:
      - open_time/close_time aligned to minute boundaries (UTC)
      - close price derived from the polled spot price
      - if only one sample is available per minute, OHLC defaults to close (and previous close for open if possible)

    This is designed to run concurrently with other sources for the same market/pair.
    """

    def __init__(
        self,
        *,
        stream_key: str,
        source: str,
        symbol: str,
        interval: str,
        poll_every_s: float,
        candle_repository: CandleRepository,
        processing_offset_repository: Any,  # optional; can be a real offset repo later
        fetch_fn,
        compute_indicators_use_case: Optional[ComputeIndicatorsUseCase] = None,
        indicator_set_repo: Optional[IndicatorSetRepository] = None,
        signals_client: Optional[SignalsHttpClient] = None,
        logger: logging.Logger | None = None,
        static_candle_fields: Optional[Dict[str, Any]] = None,
    ):
        self._stream_key = str(stream_key)
        self._source = str(source)
        self._symbol = str(symbol)
        self._interval = str(interval)
        self._poll_every_s = float(poll_every_s)
        self._candle_repo = candle_repository
        self._offset_repo = processing_offset_repository
        self._fetch_fn = fetch_fn
        self._compute_indicators = compute_indicators_use_case
        self._indicator_set_repo = indicator_set_repo
        self._signals_client = signals_client
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self._static_fields = static_candle_fields or {}

        self._task: asyncio.Task | None = None
        self._last_close: Optional[float] = None

    async def start(self) -> None:
        """
        Start the polling loop as an asyncio task.
        """
        if self._task and not self._task.done():
            return

        async def _loop() -> None:
            # self._logger.info("Starting polling ingestion stream_key=%s poll_every_s=%s", self._stream_key, self._poll_every_s)
            while True:
                try:
                    await self._tick_once()
                except Exception as exc:
                    self._logger.exception("Polling ingestion tick failed stream_key=%s: %s", self._stream_key, exc)
                await asyncio.sleep(self._poll_every_s)

        self._task = asyncio.create_task(_loop())

    async def stop(self) -> None:
        """
        Stop the polling loop task.
        """
        if self._task:
            self._task.cancel()
            self._task = None

    async def _tick_once(self) -> None:
        """
        Fetch current state/price and build a synthetic 1m candle.
        """
        fetched: Dict[str, Any] = await self._fetch_fn()
        price = float(fetched["price"])

        now = datetime.now(tz=timezone.utc)
        close_time = int(now.replace(second=0, microsecond=0).timestamp() * 1000)
        open_time = close_time - 60_000

        o = float(self._last_close) if self._last_close is not None else price
        h = max(o, price)
        l = min(o, price)

        candle = CandleEntity(
            stream_key=self._stream_key,
            source=self._source,
            symbol=self._symbol,
            interval=self._interval,
            open_time=open_time,
            close_time=close_time,
            open=o,
            high=h,
            low=l,
            close=price,
            volume=float(fetched.get("volume", 0.0) or 0.0),
            trades=int(fetched.get("trades", 0) or 0),
            is_closed=True,
            raw_event_id=str(fetched.get("raw_event_id") or ""),
        )

        # Apply extra metadata fields
        for k, v in (fetched.get("candle_fields") or {}).items():
            setattr(candle, k, v)

        for k, v in self._static_fields.items():
            setattr(candle, k, v)

        await self._candle_repo.upsert_closed_candle(candle)
        self._last_close = price

        # Compute indicators + push triggers
        if self._compute_indicators is not None and self._indicator_set_repo is not None:
            active_sets = await self._indicator_set_repo.get_active_by_stream(self._stream_key)
            for indset in active_sets:
                snapshot = await self._compute_indicators.execute_for_indicator_set(
                    stream_key=self._stream_key,
                    ema_fast=int(indset.ema_fast),
                    ema_slow=int(indset.ema_slow),
                    atr_window=int(indset.atr_window),
                    indicator_set_id=indset.cfg_hash,
                    cfg_hash=indset.cfg_hash,
                    ts=close_time,
                )
                if self._signals_client is not None and snapshot is not None:
                    # fire-and-forget to avoid blocking polling
                    asyncio.create_task(
                        self._signals_client.candle_closed(
                            indicator_set_id=indset.cfg_hash,
                            ts=close_time,
                            indicator_set=indset.to_dict(),
                            indicator_snapshot=snapshot.to_dict(),
                        )
                    )
