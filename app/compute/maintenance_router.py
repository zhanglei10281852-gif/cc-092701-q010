from __future__ import annotations

from fastapi import APIRouter, Query

from app.compute.maintenance import MaintenanceWindowService
from app.compute.maintenance_schemas import MaintenanceRevoke, MaintenanceWindowCreate

router = APIRouter(prefix="/api/compute/maintenance-windows", tags=["评分组件维护窗口"])


def service() -> MaintenanceWindowService:
    return MaintenanceWindowService()


@router.post("", status_code=201)
def create_window(payload: MaintenanceWindowCreate, actor: str = Query(..., min_length=1)):
    return service().create_window(payload.model_dump(), actor)


@router.get("")
def list_windows(include_closed: bool = False, limit: int = Query(default=100, ge=1, le=500)):
    return service().list_windows(include_closed=include_closed, limit=limit)


@router.get("/{window_id}")
def get_window(window_id: int):
    return service().get_window(window_id)


@router.get("/{window_id}/progress")
def window_progress(window_id: int):
    return service().progress(window_id)


@router.post("/{window_id}/advance")
def advance_window(window_id: int, actor: str = Query(..., min_length=1), action: str | None = Query(default=None, pattern="^(drain|enforce|recover)$")):
    return service().advance(window_id, actor, action)


@router.post("/{window_id}/revoke")
def revoke_window(window_id: int, payload: MaintenanceRevoke):
    return service().revoke(window_id, payload.actor, payload.reason)
