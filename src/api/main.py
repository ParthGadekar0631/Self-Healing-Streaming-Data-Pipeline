"""FastAPI application entry point."""

from __future__ import annotations

import uvicorn
from fastapi import FastAPI

from src.api.routes import router
from src.api.services import PipelineService
from src.config import Settings, get_settings
from src.utils.logger import configure_logging


def create_app(settings: Settings | None = None) -> FastAPI:
    active_settings = settings or get_settings()
    app = FastAPI(
        title="Self-Healing Streaming Pipeline API",
        version="1.0.0",
        description="Operational status, incident summaries, replay and recovery controls.",
    )
    app.state.pipeline_service = PipelineService(active_settings)
    app.include_router(router)
    return app


app = create_app()


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    uvicorn.run(
        "src.api.main:app",
        host=settings.incident_api_host,
        port=settings.incident_api_port,
        reload=False,
    )


if __name__ == "__main__":
    main()
