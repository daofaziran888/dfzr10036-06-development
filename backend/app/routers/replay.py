"""重放数据包路由：溢出报告、分段导出、断点续取、恢复核验与回填追溯。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel, Field

from app.services import replay_pack as service

router = APIRouter(prefix="/api/replay", tags=["重放数据包"])


class CreatePackPayload(BaseModel):
    report_id: int
    lower: int | None = None
    upper: int | None = None


class DownloadPayload(BaseModel):
    stop_after_offset: int | None = Field(default=None, description="模拟传输中断：仅交付到该偏移为止")
    delivery_no: str | None = None


class OrphanPackPayload(BaseModel):
    report_id: int
    stream: str
    file_name: str
    lower: int
    upper: int
    event_keys: list[str] = Field(default_factory=list)


class TracePayload(BaseModel):
    offsets: list[int]


class LateEventPayload(BaseModel):
    report_id: int
    offset: int


@router.post("/reports/{report_id}/late-event")
def ingest_late_event(report_id: int, payload: LateEventPayload) -> dict[str, Any]:
    if payload.report_id != report_id:
        raise HTTPException(status_code=400, detail="路径与报文体中的报告ID不一致")
    event, message = service.ingest_late_event(report_id, payload.offset)
    if event is None:
        raise HTTPException(status_code=409, detail=message)
    return {"ok": True, "message": message, "event": event}


@router.get("/reports")
def get_reports() -> dict[str, Any]:
    return {"items": service.list_reports()}


@router.get("/source/{file_name}/events")
def get_source_events(file_name: str, lower: int = Query(...), upper: int = Query(...)) -> dict[str, Any]:
    events = service.list_source_events(file_name, lower, upper)
    if not events and service._source_file(file_name) is None:
        raise HTTPException(status_code=404, detail=f"源日志文件 {file_name} 不存在")
    return {"file": file_name, "lower": lower, "upper": upper, "events": events}


@router.post("/packs")
def create_pack(payload: CreatePackPayload) -> dict[str, Any]:
    pack, message = service.create_pack(payload.report_id, lower=payload.lower, upper=payload.upper)
    if pack is None:
        raise HTTPException(status_code=400, detail=message)
    return {"ok": True, "message": message, "pack": pack}


@router.get("/packs")
def get_packs(report_id: int | None = None) -> dict[str, Any]:
    return {"items": service.list_packs(report_id)}


@router.get("/packs/{pack_id}")
def get_pack(pack_id: int) -> dict[str, Any]:
    pack = service.get_pack(pack_id)
    if pack is None:
        raise HTTPException(status_code=404, detail="重放数据包不存在")
    return pack


@router.post("/segments/{segment_id}/download")
def download_segment(segment_id: int, payload: DownloadPayload) -> dict[str, Any]:
    result, message = service.download_segment(
        segment_id,
        stop_after_offset=payload.stop_after_offset,
        delivery_no=payload.delivery_no,
    )
    if result is None:
        raise HTTPException(status_code=409, detail=message)
    return {"ok": True, "message": message, "delivery": result}


@router.get("/segments/{segment_id}/event-file")
def get_segment_event_file(segment_id: int) -> Response:
    result = service.segment_event_file(segment_id)
    if result is None:
        raise HTTPException(status_code=404, detail="分段不存在")
    file_name, content, media_type = result
    return Response(
        content=content.encode("utf-8"),
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{file_name}"'},
    )


@router.get("/segments/{segment_id}/index-file")
def get_segment_index_file(segment_id: int) -> Response:
    result = service.segment_index_file(segment_id)
    if result is None:
        raise HTTPException(status_code=404, detail="分段不存在")
    file_name, content, media_type = result
    return Response(
        content=content.encode("utf-8"),
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{file_name}"'},
    )


@router.post("/packs/{pack_id}/delivery-trace")
def delivery_trace(pack_id: int, payload: TracePayload) -> dict[str, Any]:
    result = service.delivery_trace(pack_id, payload.offsets)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@router.post("/packs/{pack_id}/recover")
def recover_with_pack(pack_id: int) -> dict[str, Any]:
    result = service.recover_with_pack(pack_id)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.post("/packs/{pack_id}/redispatch")
def redispatch(pack_id: int) -> dict[str, Any]:
    result = service.redispatch(pack_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@router.post("/orphan-packs")
def register_orphan_pack(payload: OrphanPackPayload) -> dict[str, Any]:
    pack, message = service.register_orphan_pack(
        payload.report_id,
        stream=payload.stream,
        file_name=payload.file_name,
        lower=payload.lower,
        upper=payload.upper,
        event_keys=payload.event_keys,
    )
    if pack is None:
        raise HTTPException(status_code=400, detail=message)
    return {"ok": True, "message": message, "pack": pack}


@router.get("/backfill-ledger")
def get_backfill_ledger() -> dict[str, Any]:
    return {"items": service.list_ledger()}


@router.get("/backfill-trace/{event_key:path}")
def trace_backfill(event_key: str) -> dict[str, Any]:
    result = service.trace_backfill(event_key)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result["message"])
    return result
