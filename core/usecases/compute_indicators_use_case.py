# core/usecases/compute_indicators_use_case.py
from __future__ import annotations

import logging
from typing import Optional

from core.domain.entities.indicator_entity import IndicatorSnapshotEntity
from core.repositories.candle_repository import CandleRepository
from core.repositories.indicator_repository import IndicatorRepository
from core.services.indicator_calculation_service import IndicatorCalculationService


class ComputeIndicatorsUseCase:
    """
   Computes and persists indicator snapshots for a given indicator set and stream.
    """

    def __init__(
        self,
        candle_repository: CandleRepository,
        indicator_repository: IndicatorRepository,
        indicator_service: IndicatorCalculationService,
        logger: logging.Logger | None = None,
    ):
        self._candle_repo = candle_repository
        self._indicator_repo = indicator_repository
        self._svc = indicator_service
        self._logger = logger or logging.getLogger(self.__class__.__name__)

    @staticmethod
    def required_bars_for(ema_slow: int, atr_window: int) -> int:
        """
        Return the minimum candles required to compute the indicators.

        Notes:
            - EMA(period) requires at least `period` values.
            - ATR(period) requires at least `period + 1` closes because TR uses prev_close.
        """
        ema_need = int(ema_slow)
        atr_need = int(atr_window) + 1
        return max(ema_need, atr_need)

    async def execute_for_indicator_set(
        self,
        *,
        stream_key: str,
        ema_fast: int,
        ema_slow: int,
        atr_window: int,
        indicator_set_id: str,
        cfg_hash: str,
        ts: int | None = None,
    ) -> Optional[IndicatorSnapshotEntity]:
        """
        Compute and upsert indicator snapshot using the last closed candle for the stream.
        """
        need = self.required_bars_for(ema_slow, atr_window)
        candles = await self._candle_repo.get_last_n_closed(stream_key, need)

        snapshot = self._svc.compute_snapshot_for_last(
            candles,
            ema_fast=int(ema_fast),
            ema_slow=int(ema_slow),
            atr_window=int(atr_window),
            indicator_set_id=indicator_set_id,
            cfg_hash=cfg_hash,
        )
        if snapshot is None:
            self._logger.debug("Not enough candles for indicators: have=%s need=%s", len(candles), need)
            return None

        if ts is not None:
            snapshot.ts = int(ts)
        
        await self._indicator_repo.upsert_snapshot(snapshot)
        return snapshot
