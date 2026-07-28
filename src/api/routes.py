"""FastAPI routes for status, incidents, replay, and recovery."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from src.api.schemas import IncidentInput, ReplayRequest, RetryRequest
from src.api.services import PipelineService
from src.utils.time_utils import utc_now_iso

router = APIRouter()


def service(request: Request) -> PipelineService:
    return request.app.state.pipeline_service


@router.get("/health")
def health(svc: PipelineService = Depends(service)):
    return {
        "status": "ok",
        "timestamp": utc_now_iso(),
        "pipeline_status": svc.status().get("status", "unknown"),
    }


@router.get("/pipeline/status")
def pipeline_status(svc: PipelineService = Depends(service)):
    return svc.status()


@router.get("/pipeline/metrics")
def pipeline_metrics(svc: PipelineService = Depends(service)):
    return svc.metrics()


@router.get("/incidents")
def incidents(svc: PipelineService = Depends(service)):
    return svc.incidents()


@router.get("/incidents/latest")
def latest_incident(svc: PipelineService = Depends(service)):
    latest = svc.collector.latest()
    if latest is None:
        raise HTTPException(status_code=404, detail="No incidents recorded")
    return latest


@router.post("/incidents/summarize")
def summarize_incident(payload: IncidentInput, svc: PipelineService = Depends(service)):
    return svc.summarize(payload.model_dump())


@router.post("/replay/quarantine")
def replay_quarantine(payload: ReplayRequest, svc: PipelineService = Depends(service)):
    try:
        return svc.replay(payload.limit, payload.dry_run)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Replay failed: {exc}") from exc


@router.post("/recovery/retry")
def recovery_retry(payload: RetryRequest, svc: PipelineService = Depends(service)):
    if payload.operation != "dependency_health_check":
        raise HTTPException(status_code=400, detail="Only dependency_health_check is supported")
    try:
        return svc.retry_health_check(payload.max_attempts)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Recovery retry exhausted: {exc}") from exc


@router.get("/quarantine/records")
def quarantine_records(
    limit: int = Query(default=100, ge=1, le=10_000),
    svc: PipelineService = Depends(service),
):
    return svc.quarantine_records(limit)


@router.get("/quarantine/stats")
def quarantine_stats(svc: PipelineService = Depends(service)):
    return svc.quarantine_stats()
