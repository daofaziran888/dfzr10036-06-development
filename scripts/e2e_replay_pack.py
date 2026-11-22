"""重放数据包端到端验收脚本（仅标准库，使用 python -I 运行）。

覆盖需求叙述的完整剧情：
  取同一文件偏移 240~320（含两端）制作样本 → 下载至 260 中断 →
  迟到的 275 补达源文件 → 续取带走 275 → 按包索引核对 240/260/275/320
  各自的交付批次 → 用包发起恢复并重送 → 已写入事件不重复派发 →
  275 可追溯到实际进入的增量回填；另验缺来源补包挂起待核与补包幂等。
"""
from __future__ import annotations

import hashlib
import json
import sys
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:8000/api/replay"
PASS = 0
FAIL = 0


def call(method: str, path: str, payload: dict | None = None, raw: bool = False):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read()
            if raw:
                return resp.status, body, dict(resp.headers)
            return resp.status, json.loads(body.decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8")
        raise AssertionError(f"{method} {path} -> {exc.code}: {detail}") from None


def check(name: str, condition: bool, detail: object = "") -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name} :: {detail}")


def header(headers: dict, name: str) -> str:
    lowered = {key.lower(): value for key, value in headers.items()}
    return lowered.get(name.lower(), "")


def step(title: str) -> None:
    print(f"\n=== {title} ===")


# 0. 健康检查（走通用健康端点）
with urllib.request.urlopen("http://127.0.0.1:8000/api/health", timeout=10) as resp:
    health = json.loads(resp.read().decode("utf-8"))
check("后端服务在线", health.get("ok") is True, health)

# 1. 溢出报告：区间 240~320（含两端），初始清单缺 275
step("1. 溢出报告与源文件初始状态")
_, reports = call("GET", "/reports")
report = reports["items"][0]
check("报告编号 OVF-0001", report["报告编号"] == "OVF-0001", report)
check("报告偏移区间为 240~320（含两端）",
      report["偏移下界"] == 240 and report["偏移上界"] == 320, report)
check("初始清单缺少偏移 275", report["缺失偏移"] == [275], report["缺失偏移"])
check("275 不在初始清单偏移中", 275 not in report["清单偏移"], report["清单偏移"])

_, src = call("GET", "/source/binlog.001234/events?lower=240&upper=320")
initial_offsets = [e["offset"] for e in src["events"]]
check("源文件初始样本为清单内 11 条事件", len(initial_offsets) == 11, initial_offsets)
check("源文件初始读不到迟到偏移 275", 275 not in initial_offsets, initial_offsets)

try:
    call("GET", "/source/ghost-file/events?lower=240&upper=320")
    check("不存在的源文件返回 404", False)
except AssertionError as exc:
    check("不存在的源文件返回 404", "404" in str(exc), str(exc))

# 2. 针对报告制作重放数据包（默认取报告全区间 240~320）
step("2. 分段导出：每段携带数据流标识、源日志文件和偏移上下界")
_, created = call("POST", "/packs", {"report_id": 1})
pack = created["pack"]
pack_id = pack["id"]
check("数据包编号生成", pack["数据包编号"].startswith("RPK-"), pack["数据包编号"])
check("数据包绑定报告 OVF-0001", pack["报告编号"] == "OVF-0001")
check("数据包区间 240~320", pack["偏移下界"] == 240 and pack["偏移上界"] == 320)
segments = pack["分段"]
check("按 40 偏移跨度切成 3 段", len(segments) == 3, [s["分段编号"] for s in segments])
expected_bounds = [(240, 279), (280, 319), (320, 320)]
for seg, (lo, hi) in zip(segments, expected_bounds):
    check(f"{seg['分段编号']} 三要素齐全且边界为 {lo}~{hi}",
          seg["数据流"] == "order_events_cdc"
          and seg["源日志文件"] == "binlog.001234"
          and seg["偏移下界"] == lo and seg["偏移上界"] == hi, seg)
seg1, seg2, seg3 = segments[0], segments[1], segments[2]

# 3. 第一段下载至 260 后中断
step("3. 传输中断：第一段下载至 260")
_, dl1 = call("POST", f"/segments/{seg1['id']}/download",
              {"stop_after_offset": 260, "delivery_no": "D1"})
delivery1 = dl1["delivery"]
offsets_d1 = [e["offset"] for e in delivery1["本次事件"]]
check("D1 交付 240/248/255/260 四条事件",
      offsets_d1 == [240, 248, 255, 260], offsets_d1)
check("已完成区间覆盖 240~260", delivery1["已完成区间"] == [[240, 260]],
      delivery1["已完成区间"])
check("续取起始偏移为 261", delivery1["续取起始偏移"] == 261, delivery1)
check("分段状态为部分下载", delivery1["分段状态"] == "部分下载", delivery1)
check("待交付偏移包含 268", 268 in delivery1["待交付偏移"], delivery1["待交付偏移"])

# 4. 可下载的事件文件与源位置索引（含来源校验信息）
step("4. 事件文件与源位置索引下载、客户端校验来源")
_, event_file_raw, headers1 = call("GET", f"/segments/{seg1['id']}/event-file", raw=True)
_, index_file_raw, headers2 = call("GET", f"/segments/{seg1['id']}/index-file", raw=True)
check("事件文件为 JSONL 附件下载",
      "attachment" in header(headers1, "Content-Disposition")
      and header(headers1, "Content-Type").startswith("application/x-ndjson"))
check("索引文件为 JSON 附件下载",
      "attachment" in header(headers2, "Content-Disposition")
      and header(headers2, "Content-Type").startswith("application/json"))
index_doc = json.loads(index_file_raw.decode("utf-8"))
check("索引携带数据流/源文件/偏移上下界",
      index_doc["数据流"] == "order_events_cdc"
      and index_doc["源日志文件"] == "binlog.001234"
      and (index_doc["偏移下界"], index_doc["偏移上界"]) == (240, 279))
recomputed = hashlib.sha256(
    json.dumps(index_doc["源位置索引"], ensure_ascii=False, sort_keys=True).encode("utf-8")
).hexdigest()
check("索引校验和可独立复算（来源可核）",
      recomputed == index_doc["索引校验和"], index_doc["索引校验和"])
for line in event_file_raw.decode("utf-8").strip().splitlines():
    row = json.loads(line)
    check(f"事件文件行携带源位置 offset={row['offset']} 与 checksum",
          row["源日志文件"] == "binlog.001234" and row["数据流"] == "order_events_cdc"
          and isinstance(row["checksum"], str) and len(row["checksum"]) == 16)

# 5. 读至文件末尾后，同属溢出报告的迟到记录 275 补达
step("5. 迟到记录 275 补达源文件（同属 OVF-0001）")
_, late = call("POST", "/reports/1/late-event", {"report_id": 1, "offset": 275})
check("迟到事件 275 已写入源文件", late["event"]["offset"] == 275, late)
_, src2 = call("GET", "/source/binlog.001234/events?lower=240&upper=320")
check("源文件现在能读到 275", 275 in [e["offset"] for e in src2["events"]])
_, reports2 = call("GET", "/reports")
check("报告缺失偏移清单已核销 275",
      reports2["items"][0]["缺失偏移"] == [] and 275 in reports2["items"][0]["清单偏移"],
      reports2["items"][0])

# 6. 断点续取：按已完成区间续取，275 随本次恢复受理，先前片段不混入别的交付
step("6. 断点续取：275 随同一数据包的新批次交付")
_, dl2 = call("POST", f"/segments/{seg1['id']}/download", {"delivery_no": "D2"})
delivery2 = dl2["delivery"]
offsets_d2 = [e["offset"] for e in delivery2["本次事件"]]
check("D2 只交付新增的 268 与迟到的 275",
      offsets_d2 == [268, 275], offsets_d2)
check("已下载的 240/248/255/260 未在 D2 重复交付",
      not (set(offsets_d1) & set(offsets_d2)))
check("续取后第一段已完成", delivery2["分段状态"] == "已下载", delivery2)
check("已完成区间合并为 240~275 覆盖跨度",
      delivery2["已完成区间"] == [[240, 275]], delivery2["已完成区间"])

# 第二、三段正常下载
_, dl3 = call("POST", f"/segments/{seg2['id']}/download", {"delivery_no": "D3"})
_, dl4 = call("POST", f"/segments/{seg3['id']}/download", {"delivery_no": "D4"})
check("第二段交付 284~312 五条",
      [e["offset"] for e in dl3["delivery"]["本次事件"]] == [284, 291, 299, 306, 312])
check("第三段交付 320（含右端点）",
      [e["offset"] for e in dl4["delivery"]["本次事件"]] == [320])

# 7. 按包索引核对 240、260、275、320 分别来自哪次交付
step("7. 按包索引核对四个偏移的交付来源")
_, trace = call("POST", f"/packs/{pack_id}/delivery-trace",
                {"offsets": [240, 260, 275, 320]})
trace_map = {item["offset"]: item for item in trace["交付核对"]}
check("240 来自第一段 D1",
      trace_map[240]["分段"] == seg1["分段编号"] and trace_map[240]["交付批次"] == "D1",
      trace_map[240])
check("260 来自第一段 D1",
      trace_map[260]["交付批次"] == "D1", trace_map[260])
check("275 来自第一段 D2（迟到记录的续取批次）",
      trace_map[275]["分段"] == seg1["分段编号"] and trace_map[275]["交付批次"] == "D2",
      trace_map[275])
check("320 来自第三段 D4",
      trace_map[320]["分段"] == seg3["分段编号"] and trace_map[320]["交付批次"] == "D4",
      trace_map[320])

# 8. 同一包可查询实际包含的事件和原始偏移
step("8. 数据包实际事件与原始偏移查询")
_, detail = call("GET", f"/packs/{pack_id}")
event_list = detail["事件清单"]
all_offsets = [e["offset"] for e in event_list]
check("包含区间内全部 12 条事件（含 275）",
      all_offsets == sorted([240, 248, 255, 260, 268, 275, 284, 291, 299, 306, 312, 320]),
      all_offsets)
check("每条事件带 event_key（源文件@偏移）与所属分段",
      all(e["event_key"] == f"binlog.001234@{e['offset']}" and e["所属分段"] for e in event_list))
check("275 的交付批次标记为 D2",
      next(e for e in event_list if e["offset"] == 275)["已交付批次"] == ["D2"])
check("数据包整体状态为已下载", detail["status"] == "已下载", detail["status"])

# 9. 使用包发起恢复：核验报告/源文件/实际事件，登记增量回填
step("9. 用包发起恢复，有据可核且未回填的事件登记为增量回填")
_, recover1 = call("POST", f"/packs/{pack_id}/recover")
check("恢复结论为核验通过并登记回填",
      recover1["结论"] == "核验通过-已登记增量回填", recover1["结论"])
new_offsets = sorted(e["offset"] for e in recover1["新增回填事件"])
check("12 条事件全部登记增量回填（含 275）",
      new_offsets == all_offsets and 275 in new_offsets, new_offsets)
check("回填记录带原始偏移与来源包/分段",
      all(r["源日志文件"] == "binlog.001234" and r["数据包编号"] == pack["数据包编号"]
          and r["event_key"].startswith("binlog.001234@") for r in recover1["增量回填"]))

_, ledger1 = call("GET", "/backfill-ledger")
check("回填台账落账 12 条且状态已验收",
      len(ledger1["items"]) == 12 and all(i["status"] == "已验收" for i in ledger1["items"]))

# 10. 重送：已写入的原事件不再派发
step("10. 同包重送：已验收写入事件不再次派发")
_, redispatch1 = call("POST", f"/packs/{pack_id}/redispatch")
check("重送数量为 0", redispatch1["重送数量"] == 0, redispatch1)
check("12 条已写入事件全部跳过",
      redispatch1["跳过数量"] == 12
      and sorted(e["offset"] for e in redispatch1["已写入跳过事件"]) == all_offsets)

# 再次恢复：补包不能使已验收事件再次进入回填
_, recover2 = call("POST", f"/packs/{pack_id}/recover")
check("二次恢复无新增可回填事件",
      recover2["结论"] == "无新增可回填事件" and recover2["增量回填"] == [], recover2)
check("12 条事件被标注为已验收不重复派发",
      len(recover2["已验收不重复派发"]) == 12)
_, ledger2 = call("GET", "/backfill-ledger")
check("台账仍为 12 条（无重复回填）", len(ledger2["items"]) == 12, len(ledger2["items"]))

# 11. 275 能找到它实际进入的回填
step("11. 迟到事件 275 的回填追溯")
_, trace275 = call("GET", "/backfill-trace/binlog.001234@275")
record = trace275["回填记录"]
check("275 追溯到具体回填编号",
      record["回填编号"].startswith("IBF-") and record["原始偏移"] == 275, record)
check("275 回填来源为本次数据包与第一段",
      record["数据包编号"] == pack["数据包编号"] and record["分段编号"] == seg1["分段编号"],
      record)

# 12. 重叠区间补包：相同区间不遗漏（275 在新包内），也不重复回填
step("12. 重叠区间补包：不漏新事件、不重复回填")
_, created2 = call("POST", "/packs", {"report_id": 1})
pack2 = created2["pack"]
for seg in pack2["分段"]:
    call("POST", f"/segments/{seg['id']}/download", {})
_, detail2 = call("GET", f"/packs/{pack2['id']}")
offsets_pack2 = [e["offset"] for e in detail2["事件清单"]]
check("补包同样包含迟到的 275（未因区间重叠而漏事件）",
      275 in offsets_pack2 and len(offsets_pack2) == 12, offsets_pack2)
_, recover3 = call("POST", f"/packs/{pack2['id']}/recover")
check("补包恢复不产生新增回填",
      recover3["增量回填"] == [] and len(recover3["已验收不重复派发"]) == 12, recover3)
_, ledger3 = call("GET", "/backfill-ledger")
check("台账依旧 12 条", len(ledger3["items"]) == 12)

# 13. 缺来源的包：仍定位到原报告待核，不产生回填
step("13. 来源缺失的补包挂起待核")
_, orphan = call("POST", "/orphan-packs", {
    "report_id": 1,
    "stream": "order_events_cdc",
    "file_name": "binlog.009999",
    "lower": 240,
    "upper": 320,
    "event_keys": ["binlog.009999@300"],
})
orphan_pack = orphan["pack"]
check("缺来源包状态为来源待核并挂靠 OVF-0001",
      orphan_pack["status"] == "来源待核" and orphan_pack["报告编号"] == "OVF-0001")
_, recover4 = call("POST", f"/packs/{orphan_pack['id']}/recover")
check("缺来源包恢复结论为挂起待核",
      recover4["结论"] == "来源缺失-挂起待核" and recover4["增量回填"] == [], recover4)
_, reports3 = call("GET", "/reports")
check("原报告被置为待核状态", reports3["items"][0]["status"] == "待核", reports3["items"][0])
_, ledger4 = call("GET", "/backfill-ledger")
check("待核包未写入任何回填", len(ledger4["items"]) == 12)

# 14. 数据包列表可按报告查询，含进度区间
step("14. 列表与进度查询")
_, pack_list = call("GET", "/packs?report_id=1")
check("报告下共 3 个数据包（正式包/补包/缺来源包）",
      len(pack_list["items"]) == 3, len(pack_list["items"]))
first_pack = next(p for p in pack_list["items"] if p["id"] == pack_id)
check("列表带已下载事件偏移与已完成区间",
      first_pack["已下载事件偏移"] == all_offsets and first_pack["已完成区间"], first_pack)

print(f"\n验收结果：通过 {PASS} 项，失败 {FAIL} 项")
sys.exit(1 if FAIL else 0)
