from __future__ import annotations

import logging
from typing import Optional

from core.domain.entities.candle_entity import CandleEntity
from core.repositories.candle_repository import CandleRepository
from core.repositories.price_tick_repository import PriceTickRepository


class BuildCandleFromTicksUseCase:
    """
    Builds and persists a 1-minute OHLC candle from tick data.

    Behavior:
      - If no ticks exist for the minute, returns None.
      - Candle OHLC is derived from tick prices ordered by ts.
      - Writes candle to CandleRepository (candles_1m).
      - Optionally deletes ticks for that minute after successful write.
    """

    def __init__(
        self,
        *,
        tick_repository: PriceTickRepository,
        candle_repository: CandleRepository,
        logger: logging.Logger | None = None,
        delete_ticks_after_build: bool = True,
    ):
        self._ticks = tick_repository
        self._candles = candle_repository
        self._logger = logger or logging.getLogger(self.__class__.__name__)
        self._delete_after = bool(delete_ticks_after_build)

    async def execute(
        self,
        *,
        stream_key: str,
        source: str,
        symbol: str,
        interval: str,
        minute_open_time: int,
        static_fields: dict | None = None,
    ) -> Optional[CandleEntity]:
        ticks = await self._ticks.list_ticks_for_minute(stream_key, int(minute_open_time))
        if not ticks:
            return None
                
        open_time = int(minute_open_time)
        close_time = int(minute_open_time) + 60_000 - 1

        prices = [float(t.price) for t in ticks]
        o = float(prices[0])
        h = float(max(prices))
        l = float(min(prices))
        c = float(prices[-1])

        volume = float(sum(float(t.volume or 0.0) for t in ticks))
        trades = int(sum(int(t.trades or 0) for t in ticks))

        candle = CandleEntity(
            stream_key=stream_key,
            source=source,
            symbol=symbol,
            interval=interval,
            open_time=open_time,
            close_time=close_time,
            open=o,
            high=h,
            low=l,
            close=c,
            volume=volume,
            trades=trades,
            is_closed=True,
        )

        last_tick = ticks[-1]
        extras = last_tick.extras or {}

        # Apply pool metadata fields into candle (keeps candle "complete")
        for k, v in extras.items():
            setattr(candle, k, v)

        # Enrich candle with any extra fields you already support in CandleEntity.to_mongo()
        # If CandleEntity doesn't allow extras today, you can extend it with optional fields.
        if static_fields:
            for k, v in static_fields.items():
                setattr(candle, k, v)

        await self._candles.upsert_closed_candle(candle)

        if self._delete_after:
            await self._ticks.delete_ticks_for_minute(stream_key, int(minute_open_time))

        return candle
