from __future__ import annotations

import contextlib
import logging
from typing import Any, Dict, List, Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from adapters.external.database.candle_repository_mongodb import CandleRepositoryMongoDB
from adapters.external.database.indicator_repository_mongodb import IndicatorRepositoryMongoDB
from adapters.external.database.indicator_set_repository_mongodb import IndicatorSetRepositoryMongoDB
from adapters.external.database.price_tick_repository_mongodb import PriceTickRepositoryMongoDB
from adapters.external.database.processing_offset_repository_mongodb import ProcessingOffsetRepositoryMongoDB
from adapters.external.database.mongodb_client import get_mongo_client

from adapters.external.database.ingestion_stream_repository_mongodb import IngestionStreamRepositoryMongoDB
from adapters.external.database.system_config_repository_mongodb import SystemConfigRepositoryMongoDB

from adapters.external.binance.binance_rest_client import BinanceRestClient
from adapters.external.binance.binance_websocket_client import BinanceWebsocketClient
from adapters.external.signals.signals_http_client import SignalsHttpClient

from adapters.external.thegraph.pancakeswap_v3_base_pool_client import PancakeSwapV3BasePoolClient

from config.settings import settings
from core.domain.entities.ingestion_stream_entity import IngestionStreamEntity
from core.domain.entities.system_config_entity import SystemConfigEntity
from core.services.indicator_calculation_service import IndicatorCalculationService
from core.services.stream_key_service import StreamKeyService
from core.usecases.backfill_candles_use_case import BackfillCandlesUseCase
from core.usecases.build_candle_from_ticks_use_case import BuildCandleFromTicksUseCase
from core.usecases.compute_indicators_use_case import ComputeIndicatorsUseCase
from core.usecases.start_polling_ticks_use_case import StartPollingTicksUseCase
from core.usecases.start_realtime_ingestion_use_case import StartRealtimeIngestionUseCase
from core.usecases.start_polling_ingestion_use_case import StartPollingIngestionUseCase


class IngestionSupervisor:
    """
    High-level supervisor for api-market-data.

    Responsibilities:
    - Connect to MongoDB and ensure indexes.
    - Load system_config and ingestion_streams from MongoDB.
    - Start multiple ingestion pipelines concurrently (Binance WS, TheGraph poll, etc.).
    - Keep backward compatibility by bootstrapping Binance config from .env if DB is empty.
    """

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._mongo_client: AsyncIOMotorClient | None = None
        self._db: AsyncIOMotorDatabase | None = None

        self._signals_client: SignalsHttpClient | None = None

        self._ws_clients: List[BinanceWebsocketClient] = []
        self._ws_ingestions: List[StartRealtimeIngestionUseCase] = []
        self._tick_pollers: List[StartPollingTicksUseCase] = []

        self._thegraph_clients: List[PancakeSwapV3BasePoolClient] = []

    @property
    def db(self) -> AsyncIOMotorDatabase | None:
        """
        Expose the database handle after start().
        """
        return self._db

    async def start(self) -> None:
        """
        Initialize DB, ensure indexes, load configs from Mongo, and start ingestion.
        """
        self._mongo_client = get_mongo_client()
        self._db = self._mongo_client[settings.MONGODB_DB_NAME]

        # Core repositories
        candle_repo = CandleRepositoryMongoDB(self._db)
        offset_repo = ProcessingOffsetRepositoryMongoDB(self._db)
        indicator_repo = IndicatorRepositoryMongoDB(self._db)
        indicator_set_repo = IndicatorSetRepositoryMongoDB(self._db)
        tick_repo = PriceTickRepositoryMongoDB(self._db)
        
        await tick_repo.ensure_indexes()
        await candle_repo.ensure_indexes()
        await offset_repo.ensure_indexes()
        await indicator_repo.ensure_indexes()
        await indicator_set_repo.ensure_indexes()

        # Config repositories
        system_repo = SystemConfigRepositoryMongoDB(self._db)
        streams_repo = IngestionStreamRepositoryMongoDB(self._db)
        await streams_repo.ensure_indexes()

        # Ensure runtime config exists (or create fallback)
        runtime_cfg = await system_repo.get_runtime()
        if runtime_cfg is None:
            runtime_cfg = SystemConfigEntity(
                key="runtime",
                signals_base_url=settings.SIGNALS_BASE_URL,
                thegraph_api_key=None,
                extras={},
            )
            await system_repo.upsert_runtime(runtime_cfg)

        self._signals_client = SignalsHttpClient(base_url=runtime_cfg.signals_base_url, timeout_s=30.0)

        # Bootstrap ingestion streams if empty
        total_streams = await streams_repo.count_all()
        if total_streams == 0:
            await self._bootstrap_from_env(streams_repo=streams_repo)
            total_streams = await streams_repo.count_all()
            self._logger.info("Bootstrapped ingestion_streams from .env. total=%s", total_streams)

        # Load enabled streams
        streams = await streams_repo.list_enabled()
        if not streams:
            self._logger.error("No enabled ingestion streams found in MongoDB.")
            return

        # Indicator computation
        indicator_svc = IndicatorCalculationService()
        compute_indicators_uc = ComputeIndicatorsUseCase(
            candle_repository=candle_repo,
            indicator_repository=indicator_repo,
            indicator_service=indicator_svc,
        )

        # Start streams
        for stream in streams:
            await self._start_stream(
                stream=stream,
                candle_repo=candle_repo,
                offset_repo=offset_repo,
                indicator_set_repo=indicator_set_repo,
                compute_indicators_uc=compute_indicators_uc,
                runtime_cfg=runtime_cfg,
            )

        # Start websocket subscriptions (non-blocking, the subscribe method holds its own loop)
        for uc in self._ws_ingestions:
            await uc.execute()

        # Start poll loops
        # for puc in self._poll_ingestions:
        #     await puc.start()
        for t in self._tick_pollers:
            t.start()
    
        self._logger.info("All ingestion streams started. ws=%s poll=%s", len(self._ws_ingestions), len(self._tick_pollers))

    async def stop(self) -> None:
        """
        Stop pollers, websocket clients, and close external clients.
        """
        # for p in self._poll_ingestions:
        #     with contextlib.suppress(Exception):
        #         await p.stop()
        for t in self._tick_pollers:
            with contextlib.suppress(Exception):
                await t.stop()
                
        for ws in self._ws_clients:
            with contextlib.suppress(Exception):
                await ws.close()

        for tg in self._thegraph_clients:
            with contextlib.suppress(Exception):
                await tg.aclose()

        if self._signals_client is not None:
            with contextlib.suppress(Exception):
                await self._signals_client.aclose()
            self._signals_client = None

        if self._mongo_client:
            self._mongo_client.close()

    async def _bootstrap_from_env(self, *, streams_repo: IngestionStreamRepositoryMongoDB) -> None:
        """
        Create default Binance streams from .env only if ingestion_streams is empty.

        This allows the service to come up on first deploy without manual Mongo inserts.
        After bootstrap, future changes should be done by editing Mongo documents.
        """
        interval = settings.BOOTSTRAP_BINANCE_STREAM_INTERVAL
        symbols = [s.strip() for s in (settings.BOOTSTRAP_BINANCE_STREAM_SYMBOLS or "").split(",") if s.strip()]
        if not symbols:
            symbols = ["BTCUSDT"]

        for sym in symbols:
            stream = IngestionStreamEntity(
                enabled=True,
                source_type="binance_ws",
                source_name="binance",
                symbol=sym.upper(),
                interval=interval,
                enable_backfill_on_start=settings.BOOTSTRAP_ENABLE_BACKFILL_ON_START,
                push_signals=True,
                config={
                    "ws_base_url": settings.BOOTSTRAP_BINANCE_WS_BASE_URL,
                    "rest_base_url": settings.BOOTSTRAP_BINANCE_REST_BASE_URL,
                },
            )
            await streams_repo.upsert(stream)

    async def _start_stream(
        self,
        *,
        stream: IngestionStreamEntity,
        candle_repo: CandleRepositoryMongoDB,
        offset_repo: ProcessingOffsetRepositoryMongoDB,
        indicator_set_repo: IndicatorSetRepositoryMongoDB,
        compute_indicators_uc: ComputeIndicatorsUseCase,
        runtime_cfg: SystemConfigEntity,
    ) -> None:
        """
        Start a single stream based on its source_type.
        """
        stype = (stream.source_type or "").lower().strip()

        if stype == "binance_ws":
            await self._start_binance_ws_stream(
                stream=stream,
                candle_repo=candle_repo,
                offset_repo=offset_repo,
                indicator_set_repo=indicator_set_repo,
                compute_indicators_uc=compute_indicators_uc,
            )
            return

        if stype == "thegraph_pancake_v3_base":
            await self._start_thegraph_pancake_v3_base_stream(
                stream=stream,
                candle_repo=candle_repo,
                indicator_set_repo=indicator_set_repo,
                compute_indicators_uc=compute_indicators_uc,
                runtime_cfg=runtime_cfg,
            )
            return

        self._logger.warning("Unknown stream source_type=%s (skipping). stream=%s", stype, stream.to_dict())

    async def _start_binance_ws_stream(
        self,
        *,
        stream: IngestionStreamEntity,
        candle_repo: CandleRepositoryMongoDB,
        offset_repo: ProcessingOffsetRepositoryMongoDB,
        indicator_set_repo: IndicatorSetRepositoryMongoDB,
        compute_indicators_uc: ComputeIndicatorsUseCase,
    ) -> None:
        """
        Start a Binance websocket ingestion stream.
        """
        cfg = stream.config or {}
        ws_base_url = str(cfg.get("ws_base_url") or settings.BOOTSTRAP_BINANCE_WS_BASE_URL)
        rest_base_url = str(cfg.get("rest_base_url") or settings.BOOTSTRAP_BINANCE_REST_BASE_URL)

        # Backfill
        if stream.enable_backfill_on_start:
            try:
                binance_rest = BinanceRestClient(base_url=rest_base_url)
                backfill_uc = BackfillCandlesUseCase(
                    binance_client=binance_rest,
                    candle_repository=candle_repo,
                    processing_offset_repository=offset_repo,
                )
                await backfill_uc.execute_for_symbol(
                    source="binance",
                    symbol=stream.symbol,
                    interval=stream.interval,
                )
            except Exception as exc:
                self._logger.exception("Backfill error for %s: %s", stream.symbol, exc)

        ws_client = BinanceWebsocketClient(base_ws_url=ws_base_url)

        stream_key = StreamKeyService.build(
            source=stream.source_name,
            symbol=stream.symbol,
            interval=stream.interval,
        )
                
        uc = StartRealtimeIngestionUseCase(
            stream_key=stream_key,
            source=stream.source_name,
            symbol=stream.symbol,
            interval=stream.interval,
            websocket_client=ws_client,
            candle_repository=candle_repo,
            processing_offset_repository=offset_repo,
            compute_indicators_use_case=compute_indicators_uc,
            indicator_set_repo=indicator_set_repo,
            signals_client=self._signals_client if stream.push_signals else None,
        )

        self._ws_clients.append(ws_client)
        self._ws_ingestions.append(uc)

        self._logger.info("Binance WS stream started: %s %s %s", stream.source_name, stream.symbol, stream.interval)

    async def _start_thegraph_pancake_v3_base_stream(
        self,
        *,
        stream: IngestionStreamEntity,
        candle_repo: CandleRepositoryMongoDB,
        indicator_set_repo: IndicatorSetRepositoryMongoDB,
        compute_indicators_uc: ComputeIndicatorsUseCase,
        runtime_cfg: SystemConfigEntity,
    ) -> None:
        """
        Start a The Graph polling ingestion stream for PancakeSwap V3 Base pool.

        Pricing convention used here:
        - We want the spot price of **WETH per 1 USDC** (WETH/USDC),
            i.e. how much WETH you get for 1 USDC.
        - In V3 subgraphs:
            token0Price = token1 per token0
            token1Price = token0 per token1
            Therefore, if token0=WETH and token1=USDC:
            token1Price = WETH per USDC  ✅ (this is what we want)
            token0Price = USDC per WETH
        """
        api_key = (runtime_cfg.thegraph_api_key or "").strip()
        if not api_key:
            self._logger.error(
                "thegraph_api_key is missing in system_config.runtime. Cannot start %s",
                stream.symbol,
            )
            return

        pool = (stream.pool_address or "").strip()
        if not pool:
            self._logger.error(
                "pool_address is required for thegraph_pancake_v3_base stream. symbol=%s",
                stream.symbol,
            )
            return

        tg = PancakeSwapV3BasePoolClient(api_key=api_key, timeout_s=20.0)
        self._thegraph_clients.append(tg)

        # stream_key includes pool to avoid collisions
        stream_key = StreamKeyService.build(
            source=stream.source_name,
            symbol=stream.symbol,
            interval=stream.interval,
            pool_address=pool,
        )

        async def fetch_fn() -> Dict[str, Any]:
            """
            Fetch pool state and return normalized price + metadata for candle enrichment.

            Returns:
            price: float -> WETH per 1 USDC
            """
            data = await tg.get_pool(pool_address=pool)

            token0 = (data.get("token0") or {}) if isinstance(data.get("token0"), dict) else {}
            token1 = (data.get("token1") or {}) if isinstance(data.get("token1"), dict) else {}

            token0_symbol = str(token0.get("symbol") or "")
            token1_symbol = str(token1.get("symbol") or "")

            token0_price = data.get("token0Price")  # token1 per token0
            token1_price = data.get("token1Price")  # token0 per token1

            price: float | None = None

            # We want: WETH per 1 USDC
            if token0_symbol.upper() == "WETH" and token1_symbol.upper() == "USDC":
                # token1Price = token0 per token1 = WETH per USDC ✅
                if token1_price is None:
                    raise ValueError("token1Price is missing from pool response (expected for WETH/USDC)")
                price = float(token1_price)

            elif token0_symbol.upper() == "USDC" and token1_symbol.upper() == "WETH":
                # token0Price = token1 per token0 = WETH per USDC ✅
                if token0_price is None:
                    raise ValueError("token0Price is missing from pool response (expected for USDC/WETH)")
                price = float(token0_price)

            else:
                # Generic fallback:
                # Prefer whichever side gives "WETH per USDC" if either token is WETH and the other is USDC,
                # otherwise fallback to token1Price when present.
                if token0_symbol.upper() == "WETH" and token0_price is not None:
                    # token0Price = token1 per token0 => (other per WETH). We need WETH per other => invert.
                    price = 1.0 / float(token0_price)
                elif token1_symbol.upper() == "WETH" and token1_price is not None:
                    # token1Price = token0 per token1 => (other per WETH). We need WETH per other => invert.
                    price = 1.0 / float(token1_price)
                elif token1_price is not None:
                    price = float(token1_price)
                elif token0_price is not None:
                    price = float(token0_price)
                else:
                    raise ValueError("Both token0Price and token1Price are missing from pool response")

            candle_fields = {
                "chain": stream.chain or "base",
                "dex": stream.dex or "pancakeswap_v3",
                "pool_address": pool.lower(),
                "token0_symbol": token0_symbol,
                "token1_symbol": token1_symbol,
                "token0_price": float(token0_price) if token0_price is not None else None,
                "token1_price": float(token1_price) if token1_price is not None else None,
                "fee_tier": int(data.get("feeTier")) if data.get("feeTier") is not None else None,
                "liquidity": str(data.get("liquidity")) if data.get("liquidity") is not None else None,
                "sqrt_price": str(data.get("sqrtPrice")) if data.get("sqrtPrice") is not None else None,
                "tick": int(data.get("tick")) if data.get("tick") is not None else None,
                "tvl_usd": float(data.get("totalValueLockedUSD")) if data.get("totalValueLockedUSD") is not None else None,
                "volume_usd": float(data.get("volumeUSD")) if data.get("volumeUSD") is not None else None,
                "extras": {"raw_pool": data},
            }

            return {
                "price": price,
                "volume": 0.0,
                "trades": 0,
                "raw_event_id": data.get("id"),
                "candle_fields": candle_fields,
            }

        # how often to sample the pool price
        poll_every_s = float((stream.config or {}).get("poll_every_s") or 5.0)
        tick_repo = PriceTickRepositoryMongoDB(self._db)
        
        build_candle_uc = BuildCandleFromTicksUseCase(
            tick_repository=tick_repo,
            candle_repository=candle_repo,
            logger=self._logger,
            delete_ticks_after_build=False,
        )
                
        tick_poller = StartPollingTicksUseCase(
            stream_key=stream_key,
            source=stream.source_name,
            symbol=stream.symbol,
            interval=stream.interval,  # "1m"
            poll_every_s=poll_every_s,
            tick_repository=tick_repo,
            build_candle_uc=build_candle_uc,
            compute_indicators_use_case=compute_indicators_uc,
            indicator_set_repo=indicator_set_repo,
            signals_client=self._signals_client,
            fetch_fn=fetch_fn,
            static_tick_fields={
                "chain": stream.chain or "base",
                "dex": stream.dex or "pancakeswap_v3",
                "pool_address": pool.lower(),
            },
            static_candle_fields={
                "chain": stream.chain or "base",
                "dex": stream.dex or "pancakeswap_v3",
                "pool_address": pool.lower(),
            },
            logger=self._logger,
        )

        self._tick_pollers.append(tick_poller)

        self._logger.info(
            "TheGraph tick poller registered: %s pool=%s interval=%s poll_every_s=%s",
            stream.symbol,
            pool.lower(),
            stream.interval,
            poll_every_s,
        )
