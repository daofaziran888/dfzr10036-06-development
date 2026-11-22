"""重放数据包服务：乱序溢出区间的分段导出、断点续取与恢复核验。

领域对象：
- replay_source_log 源日志文件（带逐事件的字节偏移样本，模拟可核对的原始日志）
- overflow_report  乱序超窗口溢出报告（值班员待处理的问题清单）
- replay_pack      重放数据包：针对溢出报告区间做分段导出，含可下载事件文件与源位置索引
- pack_segment     数据包分段：每段携带数据流标识、源日志文件和偏移上下界
- backfill_ledger  增量回填台账：记录已验收写入的事件，恢复时据此去重

关键约束（对应需求）：
1. 分段每段必须携带数据流标识、源日志文件、偏移上下界。
2. 下载按已完成区间续取，并校验来源（数据流/文件/边界/事件校验和）。
3. 同一包可查询实际包含的事件及其原始偏移；补包不重复派发已回填事件，
   也不因区间重叠而漏掉新到事件（按事件唯一键=源文件+偏移判定）。
4. 恢复时用收到的包核验溢出报告、源文件与实际事件；有据可核且尚未回填的
   事件登记为增量回填；缺来源的包仍可定位到原报告，标记待核。
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

from app.store import store


# ---------------------------------------------------------------------------
# 演示用源数据构造：源日志事件 + 溢出报告
# ---------------------------------------------------------------------------

def _event(offset: int, event_type: str, order_id: str, amount: int) -> dict[str, Any]:
    payload = {
        "order_id": order_id,
        "event_type": event_type,
        "amount": amount,
        "event_time": f"2026-11-18 10:{30 + offset // 60:02d}:{offset % 60:02d}",
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return {
        "offset": offset,
        "payload": payload,
        "raw_line": raw,
        "checksum": hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16],
    }


# 初始样本：240~320 区间内除 275 外的事件。275 是导出读到文件末尾后才补来的迟到记录。
_INITIAL_OFFSETS = [240, 248, 255, 260, 268, 284, 291, 299, 306, 312, 320]
_LATE_EVENT = (275, "UPDATE", "ORD-10245", 312)
_EVENT_SPECS = {
    240: ("INSERT", "ORD-10240", 120),
    248: ("UPDATE", "ORD-10241", 260),
    255: ("INSERT", "ORD-10242", 88),
    260: ("UPDATE", "ORD-10243", 540),
    268: ("DELETE", "ORD-10244", 0),
    284: ("INSERT", "ORD-10246", 45),
    291: ("UPDATE", "ORD-10247", 720),
    299: ("INSERT", "ORD-10248", 99),
    306: ("UPDATE", "ORD-10249", 180),
    312: ("DELETE", "ORD-10250", 0),
    320: ("INSERT", "ORD-10251", 610),
}


def _seed_domain() -> None:
    """初始化源日志、溢出报告和回填台账（仅内存态，启动一次）。"""
    if store.rows("replay_source_log"):
        return

    main_events = [_event(offset, *_EVENT_SPECS[offset]) for offset in _INITIAL_OFFSETS]
    other_events = [_event(240, "INSERT", "ORD-90240", 11)]

    for idx, (file_name, stream, events) in enumerate([
        ("binlog.001234", "order_events_cdc", main_events),
        ("binlog.001235", "order_events_cdc", other_events),
    ], start=1):
        store.rows("replay_source_log").append({
            "id": idx,
            "文件": file_name,
            "数据流": stream,
            "事件": events,
        })

    store.rows("overflow_report").append({
        "id": 1,
        "status": "待恢复",
        "pending": True,
        "abnormal": True,
        "报告编号": "OVF-0001",
        "数据流": "order_events_cdc",
        "源日志文件": "binlog.001234",
        "偏移下界": 240,
        "偏移上界": 320,
        "乱序策略": "OOO-0002",
        "清单偏移": list(_INITIAL_OFFSETS),
        "缺失偏移": [275],
        "产生时间": "2026-11-18 10:42:00",
        "说明": "乱序超窗口丢弃区间，导出读至文件末尾后又收到同报告偏移275",
    })

    store.rows("backfill_ledger")
    store.rows("replay_pack")
    store.rows("pack_segment")


# ---------------------------------------------------------------------------
# 源文件与事件查询
# ---------------------------------------------------------------------------

def _source_file(file_name: str) -> dict[str, Any] | None:
    for row in store.rows("replay_source_log"):
        if row["文件"] == file_name:
            return row
    return None


def _find_event(file_name: str, offset: int) -> dict[str, Any] | None:
    row = _source_file(file_name)
    if row is None:
        return None
    for event in row["事件"]:
        if event["offset"] == offset:
            return event
    return None


def _event_key(file_name: str, offset: int) -> str:
    return f"{file_name}@{offset}"


def list_source_events(file_name: str, lower: int, upper: int) -> list[dict[str, Any]]:
    """返回源文件中偏移落在闭区间 [lower, upper] 的事件（按偏移升序）。"""
    row = _source_file(file_name)
    if row is None:
        return []
    return [
        event for event in sorted(row["事件"], key=lambda item: item["offset"])
        if lower <= event["offset"] <= upper
    ]


def ingest_late_event(report_id: int, offset: int) -> tuple[dict[str, Any] | None, str]:
    """模拟导出读到源文件末尾后，同属一个溢出报告的迟到记录补达源文件。"""
    _seed_domain()
    report = store.find("overflow_report", report_id)
    if report is None:
        return None, "溢出报告不存在"
    source = _source_file(report["源日志文件"])
    if source is None:
        return None, "源日志文件不可读"
    if any(event["offset"] == offset for event in source["事件"]):
        return None, f"偏移 {offset} 的事件已存在于源文件"
    if not (report["偏移下界"] <= offset <= report["偏移上界"]):
        return None, f"偏移 {offset} 不在溢出报告区间内"

    spec = _EVENT_SPECS.get(offset)
    if offset == _LATE_EVENT[0]:
        spec = _LATE_EVENT[1:]
    if spec is None:
        return None, f"缺少偏移 {offset} 的迟到事件样本"
    event = _event(offset, *spec)
    source["事件"] = sorted(source["事件"] + [event], key=lambda item: item["offset"])
    if offset in report["缺失偏移"]:
        report["缺失偏移"].remove(offset)
    if offset not in report["清单偏移"]:
        report["清单偏移"] = sorted(report["清单偏移"] + [offset])
    return event, f"迟到事件（偏移 {offset}）已补达源文件 {source['文件']}"


# ---------------------------------------------------------------------------
# 重放数据包：分段导出
# ---------------------------------------------------------------------------

SEGMENT_SIZE = 40  # 每段偏移跨度，演示 240~320 切成 [240,279]/[280,319]/[320,320]


def _next_id(module: str) -> int:
    return max((int(row.get("id", 0)) for row in store.rows(module)), default=0) + 1


def create_pack(report_id: int, *, lower: int | None = None,
                upper: int | None = None) -> tuple[dict[str, Any] | None, str]:
    """针对溢出报告创建重放数据包并做分段。

    分段只固化数据流标识、源日志文件和偏移上下界；段内事件在下载时从源文件
    实时发现并写入源位置索引，这样读到文件末尾后补达的迟到事件也能被续取带走。
    """
    _seed_domain()
    report = store.find("overflow_report", report_id)
    if report is None:
        return None, "溢出报告不存在或已归档"

    lo = report["偏移下界"] if lower is None else lower
    hi = report["偏移上界"] if upper is None else upper
    if lo < report["偏移下界"] or hi > report["偏移上界"] or lo > hi:
        return None, "请求区间超出溢出报告覆盖范围"

    file_name = report["源日志文件"]
    stream = report["数据流"]
    if _source_file(file_name) is None:
        return None, f"源日志文件 {file_name} 当前不可读，无法导出"

    pack_id = _next_id("replay_pack")
    pack_no = f"RPK-{pack_id:04d}"
    pack: dict[str, Any] = {
        "id": pack_id,
        "status": "导出中",
        "pending": True,
        "abnormal": False,
        "数据包编号": pack_no,
        "报告编号": report["报告编号"],
        "报告ID": report_id,
        "数据流": stream,
        "源日志文件": file_name,
        "偏移下界": lo,
        "偏移上界": hi,
        "分段大小": SEGMENT_SIZE,
        "创建时间": _now(),
        "来源缺失": False,
    }
    store.rows("replay_pack").append(pack)

    cursor = lo
    seg_index = 0
    while cursor <= hi:
        seg_index += 1
        seg_lo = cursor
        seg_hi = min(cursor + SEGMENT_SIZE - 1, hi)
        segment = _build_segment(pack, seg_index, seg_lo, seg_hi)
        store.rows("pack_segment").append(segment)
        cursor = seg_hi + 1

    return get_pack(pack_id), "重放数据包已分段创建"


def _build_segment(pack: dict[str, Any], seq: int, lower: int, upper: int) -> dict[str, Any]:
    """构造分段：携带数据流标识、源文件、偏移上下界；索引随下载累积。"""
    segment_no = f"{pack['数据包编号']}-S{seq:02d}"
    return {
        "id": _next_id("pack_segment"),
        "status": "待下载",
        "分段编号": segment_no,
        "数据包ID": pack["id"],
        "数据包编号": pack["数据包编号"],
        # 每段必须携带的三要素
        "数据流": pack["数据流"],
        "源日志文件": pack["源日志文件"],
        "偏移下界": lower,
        "偏移上界": upper,
        "事件偏移": [],
        "源位置索引": [],
        "索引校验和": None,
        "下载交付批次": [],
        "最后续取偏移": lower,
    }


def list_packs(report_id: int | None = None) -> list[dict[str, Any]]:
    _seed_domain()
    packs = store.rows("replay_pack")
    if report_id is not None:
        packs = [p for p in packs if p["报告ID"] == report_id]
    return [_attach_progress(dict(p)) for p in packs]


def get_pack(pack_id: int) -> dict[str, Any] | None:
    _seed_domain()
    pack = store.find("replay_pack", pack_id)
    if pack is None:
        return None
    detail = _attach_progress(dict(pack))
    detail["分段"] = [_segment_view(seg) for seg in _segments_of(pack_id)]
    detail["事件清单"] = _pack_event_view(pack)
    return detail


def _segments_of(pack_id: int) -> list[dict[str, Any]]:
    return [s for s in store.rows("pack_segment") if s["数据包ID"] == pack_id]


def _attach_progress(pack: dict[str, Any]) -> dict[str, Any]:
    segments = _segments_of(pack["id"])
    delivered = sorted({offset for seg in segments for offset in seg["事件偏移"]})
    pack["已下载事件偏移"] = delivered
    pack["已完成区间"] = _coverage_spans(
        pack["源日志文件"],
        [span for seg in segments for span in _batch_spans(seg)],
    )
    if segments and all(seg["status"] == "已下载" for seg in segments):
        pack["status"] = "已下载"
    return pack

def _pack_event_view(pack: dict[str, Any]) -> list[dict[str, Any]]:
    """同一包可查到实际包含的事件、原始偏移、来源分段与交付批次。"""
    result = []
    for seg in _segments_of(pack["id"]):
        for row in seg["源位置索引"]:
            offset = row["offset"]
            event = _find_event(pack["源日志文件"], offset)
            result.append({
                "event_key": row["event_key"],
                "offset": offset,
                "源日志文件": pack["源日志文件"],
                "数据流": pack["数据流"],
                "所属分段": seg["分段编号"],
                "checksum": row["checksum"],
                "payload": event["payload"] if event else None,
                "已交付批次": [b["交付批次"] for b in seg["下载交付批次"]
                          if offset in b["事件偏移"]],
            })
    return sorted(result, key=lambda item: item["offset"])


def _segment_view(seg: dict[str, Any]) -> dict[str, Any]:
    view = {key: value for key, value in seg.items() if key != "源位置索引"}
    view["源位置索引条数"] = len(seg["源位置索引"])
    return view


# ---------------------------------------------------------------------------
# 分段下载 / 断点续取
# ---------------------------------------------------------------------------

def _verify_segment_origin(seg: dict[str, Any], pack: dict[str, Any]) -> str | None:
    """续取前校验来源：数据流、源文件、偏移边界与源文件归属。"""
    if seg["数据流"] != pack["数据流"]:
        return f"分段 {seg['分段编号']} 数据流标识与数据包不一致"
    if seg["源日志文件"] != pack["源日志文件"]:
        return f"分段 {seg['分段编号']} 源日志文件与数据包不一致"
    if seg["偏移下界"] > seg["偏移上界"]:
        return f"分段 {seg['分段编号']} 偏移上下界非法"
    source = _source_file(seg["源日志文件"])
    if source is None:
        return f"源日志文件 {seg['源日志文件']} 不可读，无法校验来源"
    if source["数据流"] != seg["数据流"]:
        return f"分段 {seg['分段编号']} 数据流与源文件实际归属不符"
    return None


def download_segment(segment_id: int, *, stop_after_offset: int | None = None,
                     delivery_no: str | None = None) -> tuple[dict[str, Any] | None, str]:
    """下载一个分段；可在 stop_after_offset 处模拟中断，下次按已完成区间续取。

    段内事件每次从源文件实时发现：已在索引中的事件不重复交付，新补达的事件
    即使落在已下载区间之后（甚至之间）也不会被漏掉。
    """
    _seed_domain()
    seg = store.find("pack_segment", segment_id)
    if seg is None:
        return None, "分段不存在或已归档"
    pack = store.find("replay_pack", seg["数据包ID"])
    error = _verify_segment_origin(seg, pack)
    if error:
        seg["status"] = "来源异常"
        pack["abnormal"] = True
        return None, error

    known_offsets = {row["offset"] for row in seg["源位置索引"]}
    source_events = list_source_events(seg["源日志文件"], seg["偏移下界"], seg["偏移上界"])
    candidates = [event for event in source_events if event["offset"] not in known_offsets]
    if stop_after_offset is not None:
        candidates = [event for event in candidates if event["offset"] <= stop_after_offset]

    delivered_now: list[dict[str, Any]] = []
    new_index_rows: list[dict[str, Any]] = []
    for event in candidates:
        offset = event["offset"]
        index_row = {
            "offset": offset,
            "event_key": _event_key(seg["源日志文件"], offset),
            "checksum": event["checksum"],
            "order_id": event["payload"]["order_id"],
        }
        new_index_rows.append(index_row)
        delivered_now.append({
            "offset": offset,
            "event_key": index_row["event_key"],
            "checksum": event["checksum"],
            "payload": event["payload"],
        })

    batch_no = delivery_no or f"D{len(seg['下载交付批次']) + 1}"
    if delivered_now:
        seg["下载交付批次"].append({
            "交付批次": batch_no,
            "事件偏移": [item["offset"] for item in delivered_now],
            "交付时间": _now(),
        })
        seg["源位置索引"] = sorted(seg["源位置索引"] + new_index_rows,
                                  key=lambda row: row["offset"])
        seg["事件偏移"] = [row["offset"] for row in seg["源位置索引"]]
        index_payload = json.dumps(seg["源位置索引"], ensure_ascii=False, sort_keys=True)
        seg["索引校验和"] = hashlib.sha256(index_payload.encode("utf-8")).hexdigest()
        seg["最后续取偏移"] = max(seg["事件偏移"]) + 1

    source_now = {e["offset"] for e in source_events}
    pending = [offset for offset in source_now if offset not in set(seg["事件偏移"])]
    if not pending:
        seg["status"] = "已下载"
    else:
        seg["status"] = "部分下载"

    return {
        "分段编号": seg["分段编号"],
        "数据流": seg["数据流"],
        "源日志文件": seg["源日志文件"],
        "偏移下界": seg["偏移下界"],
        "偏移上界": seg["偏移上界"],
        "本次交付批次": batch_no if delivered_now else None,
        "本次事件": delivered_now,
        "已完成区间": _coverage_spans(seg["源日志文件"], _batch_spans(seg)),
        "续取起始偏移": seg["最后续取偏移"],
        "待交付偏移": sorted(pending),
        "分段状态": seg["status"],
    }, "分段下载成功"


def _batch_spans(seg: dict[str, Any]) -> list[list[int]]:
    """每个交付批次形成一个已完成覆盖跨度（批次首偏移→末偏移，含边界）。"""
    return [
        [min(batch["事件偏移"]), max(batch["事件偏移"])]
        for batch in seg["下载交付批次"] if batch["事件偏移"]
    ]


def _coverage_spans(file_name: str, spans: list[list[int]]) -> list[list[int]]:
    """按源文件实际事件合并覆盖跨度：间隙内不存在源事件时才视为同一已完成区间。

    事件偏移是稀疏的，单纯按字节相邻会把 [240,260] 与 [268,275] 错误拆开；
    而 261~267 之间本就没有事件，所以二者应合并成已完成区间 240~275。
    """
    source = _source_file(file_name)
    if source is None:
        # 来源缺失的包无源文件可查，直接保留原始覆盖跨度，恢复时再挂起待核
        return [list(span) for span in sorted(spans)]
    source_offsets = {event["offset"] for event in source["事件"]}
    merged: list[list[int]] = []
    for start, end in sorted(spans):
        if merged:
            gap_events = [off for off in source_offsets if merged[-1][1] < off < start]
            if not gap_events:
                merged[-1][1] = max(merged[-1][1], end)
                continue
        merged.append([start, end])
    return merged


def segment_event_file(segment_id: int) -> tuple[str, str, str] | None:
    """生成可下载事件文件（JSONL），内容来自分段累积的源位置索引并回查源文件。"""
    _seed_domain()
    seg = store.find("pack_segment", segment_id)
    if seg is None:
        return None
    lines = []
    for row in seg["源位置索引"]:
        event = _find_event(seg["源日志文件"], row["offset"])
        lines.append(json.dumps({
            "数据流": seg["数据流"],
            "源日志文件": seg["源日志文件"],
            "offset": row["offset"],
            "checksum": row["checksum"],
            "event": event["payload"] if event else None,
        }, ensure_ascii=False))
    file_name = f"{seg['分段编号']}_events.jsonl"
    return file_name, "\n".join(lines) + ("\n" if lines else ""), "application/x-ndjson; charset=utf-8"


def segment_index_file(segment_id: int) -> tuple[str, str, str] | None:
    """生成源位置索引文件（JSON）。"""
    _seed_domain()
    seg = store.find("pack_segment", segment_id)
    if seg is None:
        return None
    body = {
        "分段编号": seg["分段编号"],
        "数据包编号": seg["数据包编号"],
        "数据流": seg["数据流"],
        "源日志文件": seg["源日志文件"],
        "偏移下界": seg["偏移下界"],
        "偏移上界": seg["偏移上界"],
        "索引校验和": seg["索引校验和"],
        "源位置索引": seg["源位置索引"],
    }
    return (f"{seg['分段编号']}_index.json",
            json.dumps(body, ensure_ascii=False, indent=2),
            "application/json; charset=utf-8")


def delivery_trace(pack_id: int, offsets: list[int]) -> dict[str, Any]:
    """按包索引核对指定偏移分别来自哪次交付。"""
    _seed_domain()
    pack = store.find("replay_pack", pack_id)
    if pack is None:
        return {"ok": False, "message": "数据包不存在"}
    trace = []
    for offset in offsets:
        hit = None
        for seg in _segments_of(pack_id):
            for batch in seg["下载交付批次"]:
                if offset in batch["事件偏移"]:
                    hit = {
                        "offset": offset,
                        "分段": seg["分段编号"],
                        "交付批次": batch["交付批次"],
                        "源日志文件": seg["源日志文件"],
                        "数据流": seg["数据流"],
                    }
        trace.append(hit or {
            "offset": offset,
            "分段": None,
            "交付批次": None,
            "说明": "该偏移尚未随任何批次交付",
        })
    return {"ok": True, "数据包编号": pack["数据包编号"], "交付核对": trace}


# ---------------------------------------------------------------------------
# 恢复核验与增量回填登记
# ---------------------------------------------------------------------------

def _accepted_keys() -> set[str]:
    return {row["event_key"] for row in store.rows("backfill_ledger") if row.get("status") == "已验收"}


def recover_with_pack(pack_id: int) -> dict[str, Any]:
    """用收到的重放数据包发起恢复：核验报告/源文件/实际事件并登记增量回填。

    - 有据可核（报告区间内、源文件存在、校验和一致）且未回填 → 登记增量回填
    - 已验收写入的事件 → 不再派发，仅标注
    - 来源缺失的包 → 不登记事件，但定位到原报告并标记待核
    """
    _seed_domain()
    pack = store.find("replay_pack", pack_id)
    if pack is None:
        return {"ok": False, "message": "重放数据包不存在"}
    report = store.find("overflow_report", pack["报告ID"])
    if report is None:
        return {"ok": False, "message": "关联的溢出报告不存在"}

    if pack.get("来源缺失"):
        report["status"] = "待核"
        report["pending"] = True
        return {
            "ok": True,
            "结论": "来源缺失-挂起待核",
            "报告编号": report["报告编号"],
            "数据包编号": pack["数据包编号"],
            "待核事件键": pack.get("事件键", []),
            "增量回填": [],
            "说明": "包声明的源文件无法核对，已挂到原溢出报告待人工核验",
        }

    verified_new: list[dict[str, Any]] = []
    already_written: list[dict[str, Any]] = []
    unverifiable: list[dict[str, Any]] = []
    accepted = _accepted_keys()

    for seg in _segments_of(pack_id):
        for row in seg["源位置索引"]:
            offset = row["offset"]
            key = row["event_key"]
            event = _find_event(pack["源日志文件"], offset)
            in_report = (report["源日志文件"] == pack["源日志文件"]
                         and report["偏移下界"] <= offset <= report["偏移上界"])
            if not in_report:
                unverifiable.append({"event_key": key, "offset": offset, "原因": "不在溢出报告区间内"})
                continue
            if event is None:
                unverifiable.append({"event_key": key, "offset": offset, "原因": "源文件中找不到该偏移事件"})
                continue
            if event["checksum"] != row["checksum"]:
                unverifiable.append({"event_key": key, "offset": offset, "原因": "事件校验和与源文件不符"})
                continue
            item = {
                "event_key": key,
                "offset": offset,
                "源日志文件": pack["源日志文件"],
                "数据流": pack["数据流"],
                "分段编号": seg["分段编号"],
                "order_id": event["payload"]["order_id"],
            }
            if key in accepted:
                already_written.append(item)
            else:
                verified_new.append(item)

    backfill_records = _register_backfill(report, pack, verified_new)
    if verified_new:
        report["status"] = "恢复中"
    elif not unverifiable:
        report["status"] = "已恢复"
        report["pending"] = False

    return {
        "ok": True,
        "结论": "核验通过-已登记增量回填" if verified_new else "无新增可回填事件",
        "报告编号": report["报告编号"],
        "数据包编号": pack["数据包编号"],
        "新增回填事件": verified_new,
        "已验收不重复派发": already_written,
        "待核事件": unverifiable,
        "增量回填": backfill_records,
    }


def _register_backfill(report: dict[str, Any], pack: dict[str, Any],
                       events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """把核验通过且尚未回填的事件登记到增量回填台账。"""
    records = []
    ledger = store.rows("backfill_ledger")
    for item in events:
        bf_id = _next_id("backfill_ledger")
        record = {
            "id": bf_id,
            "status": "已验收",
            "回填编号": f"IBF-{bf_id:04d}",
            "报告编号": report["报告编号"],
            "数据包编号": pack["数据包编号"],
            "分段编号": item["分段编号"],
            "event_key": item["event_key"],
            "数据流": item["数据流"],
            "源日志文件": item["源日志文件"],
            "原始偏移": item["offset"],
            "order_id": item["order_id"],
            "登记时间": _now(),
        }
        ledger.append(record)
        records.append(record)
    return records


def redispatch(pack_id: int) -> dict[str, Any]:
    """使用同一数据包发起恢复并重送：已写入的原事件不再派发，只派发新增量。"""
    _seed_domain()
    pack = store.find("replay_pack", pack_id)
    if pack is None:
        return {"ok": False, "message": "重放数据包不存在"}
    accepted = _accepted_keys()
    to_send, skipped = [], []
    for seg in _segments_of(pack_id):
        for row in seg["源位置索引"]:
            offset = row["offset"]
            key = row["event_key"]
            event = _find_event(pack["源日志文件"], offset)
            entry = {
                "event_key": key,
                "offset": offset,
                "分段编号": seg["分段编号"],
                "checksum": event["checksum"] if event else None,
                "payload": event["payload"] if event else None,
            }
            (skipped if key in accepted else to_send).append(entry)
    return {
        "ok": True,
        "数据包编号": pack["数据包编号"],
        "本次重送事件": to_send,
        "已写入跳过事件": skipped,
        "重送数量": len(to_send),
        "跳过数量": len(skipped),
    }


def trace_backfill(event_key: str) -> dict[str, Any]:
    """查询某个事件（如迟到的 275）实际进入了哪次增量回填。"""
    _seed_domain()
    for row in store.rows("backfill_ledger"):
        if row["event_key"] == event_key:
            return {"ok": True, "event_key": event_key, "回填记录": row}
    return {"ok": False, "event_key": event_key, "message": "该事件尚未进入任何回填"}


def register_orphan_pack(report_id: int, *, stream: str, file_name: str,
                         lower: int, upper: int,
                         event_keys: list[str]) -> tuple[dict[str, Any] | None, str]:
    """登记一个来源缺失的补包：源文件在平台不可读，仍挂到原报告待核。"""
    _seed_domain()
    report = store.find("overflow_report", report_id)
    if report is None:
        return None, "溢出报告不存在，无法挂靠待核包"
    pack_id = _next_id("replay_pack")
    pack = {
        "id": pack_id,
        "status": "来源待核",
        "pending": True,
        "abnormal": True,
        "数据包编号": f"RPK-{pack_id:04d}",
        "报告编号": report["报告编号"],
        "报告ID": report_id,
        "数据流": stream,
        "源日志文件": file_name,
        "偏移下界": lower,
        "偏移上界": upper,
        "分段大小": SEGMENT_SIZE,
        "创建时间": _now(),
        "事件键": event_keys,
        "来源缺失": True,
    }
    store.rows("replay_pack").append(pack)
    report["status"] = "待核"
    return get_pack(pack_id), "来源缺失的补包已挂到原报告，状态待核"


def list_reports() -> list[dict[str, Any]]:
    _seed_domain()
    return [dict(row) for row in store.rows("overflow_report")]


def list_ledger() -> list[dict[str, Any]]:
    _seed_domain()
    return [dict(row) for row in store.rows("backfill_ledger")]


def _now() -> str:
    return datetime(2026, 11, 18, 10, 45, 0).strftime("%Y-%m-%d %H:%M:%S")
