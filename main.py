import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from adapters.entry.http.admin_router import router as admin_router
from adapters.entry.http.admin_config_router import router as admin_config_router
from config.settings import settings
from workers.ingestion_supervisor import IngestionSupervisor


def _setup_logging() -> None:
    logging.basicConfig(
        level=settings.LOG_LEVEL,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


supervisor = IngestionSupervisor()


@asynccontextmanager
async def lifespan(app: FastAPI):
    _setup_logging()
    logging.getLogger(__name__).info("Starting api-market-data (lifespan startup)...")

    await supervisor.start()
    app.state.db = supervisor.db

    app.include_router(admin_router)
    app.include_router(admin_config_router)
    
    try:
        yield
    finally:
        logging.getLogger(__name__).info("Shutting down api-market-data (lifespan shutdown)...")
        await supervisor.stop()


app = FastAPI(title="api-market-data", version="0.1.0", lifespan=lifespan)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
