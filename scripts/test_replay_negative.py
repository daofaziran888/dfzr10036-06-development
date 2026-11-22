"""重放数据包服务层负向路径测试（仅标准库，python -I 运行）。

HTTP 端到端脚本覆盖主剧情；这里直接调用服务层验证来源校验的失败分支：
- 索引校验和被篡改 → 恢复时事件进待核，不写入回填
- 源文件不可读 → 续取被拒并标记来源异常
- 分段边界/数据流被改 → 续取被拒
- 重复迟到事件、区间越界等参数校验
"""
from __future__ import annotations

import sys

sys.path.insert(0, "/workspace/backend")

from app.services import replay_pack as svc  # noqa: E402
from app.store import store  # noqa: E402

PASS = 0
FAIL = 0


def check(name: str, condition: bool, detail: object = "") -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name} :: {detail}")


# 每个用例前重置内存仓库
def reset() -> None:
    for name in ("replay_source_log", "overflow_report", "replay_pack",
                 "pack_segment", "backfill_ledger"):
        store.rows(name).clear()
    svc._seed_domain()


print("=== N1. 索引事件校验和被篡改：恢复进待核，不回填 ===")
reset()
pack, _ = svc.create_pack(1)
segments = store.rows("pack_segment")
dl, msg = svc.download_segment(segments[0]["id"], stop_after_offset=260)
check("首次下载成功", dl is not None, msg)
# 篡改已入索引事件的校验和（模拟运输/落盘损坏）
segments[0]["源位置索引"][0]["checksum"] = "deadbeefdeadbeef"
result = svc.recover_with_pack(pack["id"])
bad = [item for item in result["待核事件"] if item["offset"] == 240]
check("240 因校验和不符进入待核", bool(bad) and "校验和" in bad[0]["原因"], bad)
check("被篡改事件未登记回填",
      all(r["原始偏移"] != 240 for r in result["增量回填"]))
ledger_keys = {r["event_key"] for r in store.rows("backfill_ledger")}
check("台账中无 binlog.001234@240", "binlog.001234@240" not in ledger_keys, ledger_keys)

print("\n=== N2. 源文件不可读：续取拒绝并标记来源异常 ===")
reset()
pack, _ = svc.create_pack(1)
seg = store.rows("pack_segment")[0]
svc.download_segment(seg["id"], stop_after_offset=260)
# 模拟源文件下线
source_rows = store.rows("replay_source_log")
removed = source_rows.pop(0)
dl, msg = svc.download_segment(seg["id"])
check("源文件缺失时续取被拒", dl is None and "不可读" in msg, msg)
check("分段被标记来源异常", store.find("pack_segment", seg["id"])["status"] == "来源异常")
check("数据包被标记异常", store.find("replay_pack", pack["id"])["abnormal"] is True)
# 源文件恢复后可正常续取
source_rows.append(removed)
dl, msg = svc.download_segment(seg["id"])
check("源文件恢复后续取成功", dl is not None and 268 in [e["offset"] for e in dl["本次事件"]], msg)

print("\n=== N3. 分段三要素被破坏：续取拒绝 ===")
reset()
pack, _ = svc.create_pack(1)
seg = store.rows("pack_segment")[0]
svc.download_segment(seg["id"], stop_after_offset=260)
seg["数据流"] = "payment_log_cdc"
dl, msg = svc.download_segment(seg["id"])
check("数据流标识不一致被拒", dl is None and "数据流标识" in msg, msg)
seg["数据流"] = pack["数据流"]
seg["偏移上界"] = seg["偏移下界"] - 1
dl, msg = svc.download_segment(seg["id"])
check("非法偏移上下界被拒", dl is None and "上下界非法" in msg, msg)

print("\n=== N4. 建包区间越界与重复迟到事件 ===")
reset()
pack, msg = svc.create_pack(1, lower=200, upper=320)
check("下界越界被拒", pack is None and "超出溢出报告" in msg, msg)
pack, msg = svc.create_pack(1, lower=300, upper=299)
check("上下界颠倒被拒", pack is None, msg)
event, msg = svc.ingest_late_event(1, 275)
check("275 首次补达成功", event is not None and event["offset"] == 275, msg)
event, msg = svc.ingest_late_event(1, 275)
check("275 重复补达被拒", event is None and "已存在" in msg, msg)
event, msg = svc.ingest_late_event(1, 999)
check("报告区间外的迟到事件被拒", event is None and "不在溢出报告区间" in msg, msg)

print("\n=== N5. 恢复幂等：多轮恢复台账不重复、不遗漏 ===")
reset()
pack, _ = svc.create_pack(1)
for seg in store.rows("pack_segment"):
    svc.download_segment(seg["id"])
svc.ingest_late_event(1, 275)
# 275 落在第一段已下载区间之后，再续取一次即可带走
svc.download_segment(store.rows("pack_segment")[0]["id"])
r1 = svc.recover_with_pack(pack["id"])
check("首轮登记 12 条回填", len(r1["增量回填"]) == 12, len(r1["增量回填"]))
r2 = svc.recover_with_pack(pack["id"])
r3 = svc.recover_with_pack(pack["id"])
check("后续两轮零新增", r2["增量回填"] == [] and r3["增量回填"] == [])
check("台账始终 12 条", len(store.rows("backfill_ledger")) == 12)
check("两轮均识别为已验收不重复派发",
      len(r2["已验收不重复派发"]) == 12 and len(r3["已验收不重复派发"]) == 12)

print("\n=== N6. 缺来源包挂靠后仍可定位原报告，且不参与下载/回填 ===")
reset()
orphan, msg = svc.register_orphan_pack(
    1, stream="order_events_cdc", file_name="binlog.009999",
    lower=240, upper=320, event_keys=["binlog.009999@300"])
check("缺来源包创建成功并待核", orphan["status"] == "来源待核", msg)
result = svc.recover_with_pack(orphan["id"])
check("恢复结论挂起待核", result["结论"] == "来源缺失-挂起待核")
check("挂起包零回填", result["增量回填"] == [] and store.rows("backfill_ledger") == [])
report = store.find("overflow_report", 1)
check("原报告状态为待核", report["status"] == "待核", report["status"])
check("挂起包仍可查到待核事件键", result["待核事件键"] == ["binlog.009999@300"])

print(f"\n负向路径验收：通过 {PASS} 项，失败 {FAIL} 项")
sys.exit(1 if FAIL else 0)
