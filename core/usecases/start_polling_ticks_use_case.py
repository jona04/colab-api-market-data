# core/usecases/start_polling_ticks_use_case.py
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable, Dict, Optional

from adapters.external.signals.signals_http_client import SignalsHttpClient
from core.repositories.indicator_set_repository import IndicatorSetRepository
from core.usecases.compute_indicators_use_case import ComputeIndicatorsUseCase

from core.domain.entities.price_tick_entity import PriceTickEntity
from core.usecases.build_candle_from_ticks_use_case import BuildCandleFromTicksUseCase
from core.repositories.price_tick_repository import PriceTickRepository


class StartPollingTicksUseCase:
    """
    Polls a data source frequently (e.g. every 5s) and stores ticks.
    When a minute closes, it builds the 1m candle from ticks and can optionally:
      - compute indicators for active indicator sets
      - fire a candle-closed trigger to api-signals (non-blocking)
    """

    def __init__(
        self,
        *,
        stream_key: str,
        source: str,
        symbol: str,
        interval: str,  # the candle interval, typically "1m"
        poll_every_s: float,
        tick_repository: PriceTickRepository,
        build_candle_uc: BuildCandleFromTicksUseCase,
        fetch_fn: Callable[[], Awaitable[Dict[str, Any]]],
        static_tick_fields: Optional[Dict[str, Any]] = None,
        static_candle_fields: Optional[Dict[str, Any]] = None,
        compute_indicators_use_case: Optional[ComputeIndicatorsUseCase] = None,
        indicator_set_repo: Optional[IndicatorSetRepository] = None,
        signals_client: Optional[SignalsHttpClient] = None,
        logger: logging.Logger | None = None,
    ):
        self._stream_key = stream_key
        self._source = source
        self._symbol = symbol
        self._interval = interval
        self._poll_every_s = float(poll_every_s)
        self._ticks = tick_repository
        self._build_candle_uc = build_candle_uc
        self._fetch_fn = fetch_fn
        self._static_tick_fields = static_tick_fields or {}
        self._static_candle_fields = static_candle_fields or {}
        self._compute_indicators = compute_indicators_use_case
        self._indicator_set_repo = indicator_set_repo
        self._signals_client = signals_client
        self._logger = logger or logging.getLogger(self.__class__.__name__)

        self._last_flushed_minute_open_time: int | None = None

        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    def start(self) -> None:
        """Start the polling loop in background."""
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Stop the polling loop gracefully."""
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                now_ms = int(time.time() * 1000)
                minute_open = (now_ms // 60_000) * 60_000

                data = await self._fetch_fn()
                price = float(data["price"])

                tick = PriceTickEntity(
                    stream_key=self._stream_key,
                    source=self._source,
                    symbol=self._symbol,
                    interval=self._interval,
                    ts=now_ms,
                    minute_open_time=int(minute_open),
                    price=price,
                    volume=float(data.get("volume") or 0.0),
                    trades=int(data.get("trades") or 0),
                    raw_event_id=data.get("raw_event_id"),
                    extras=data.get("candle_fields") or {},
                )

                for k, v in self._static_tick_fields.items():
                    tick.extras = tick.extras or {}
                    tick.extras[k] = v

                await self._ticks.insert_tick(tick)

                prev_minute_open = minute_open - 60_000
                if self._last_flushed_minute_open_time is None:
                    self._last_flushed_minute_open_time = prev_minute_open - 60_000

                if prev_minute_open > self._last_flushed_minute_open_time:
                    built = await self._build_candle_uc.execute(
                        stream_key=self._stream_key,
                        source=self._source,
                        symbol=self._symbol,
                        interval=self._interval,
                        minute_open_time=int(prev_minute_open),
                        static_fields=self._static_candle_fields,
                    )
                    if built is not None:
                        self._last_flushed_minute_open_time = int(prev_minute_open)

                        await self._after_candle_closed(
                            close_time=int(built.close_time),
                        )

            except Exception as exc:
                self._logger.exception(
                    "Tick poll loop error stream_key=%s: %s",
                    self._stream_key,
                    exc,
                )

            await asyncio.sleep(self._poll_every_s)

    async def _after_candle_closed(self, *, close_time: int) -> None:
        """
        After a candle is built, compute indicators for active indicator sets
        and optionally notify api-signals (fire-and-forget).
        """
        if self._compute_indicators is None or self._indicator_set_repo is None:
            return

        active_sets = await self._indicator_set_repo.get_active_by_stream(self._stream_key)
        for indset in active_sets:
            snapshot = await self._compute_indicators.execute_for_indicator_set(
                stream_key=self._stream_key,
                ema_fast=int(indset.ema_fast),
                ema_slow=int(indset.ema_slow),
                atr_window=int(indset.atr_window),
                indicator_set_id=indset.cfg_hash,
                cfg_hash=indset.cfg_hash,
                ts=int(close_time),
            )

            if self._signals_client is not None and snapshot is not None:
                asyncio.create_task(
                    self._signals_client.candle_closed(
                        indicator_set_id=indset.cfg_hash,
                        ts=int(close_time),
                        indicator_set=indset.to_dict(),
                        indicator_snapshot=snapshot.to_dict(),
                    )
                )
