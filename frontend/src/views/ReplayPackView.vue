<template>
  <section class="page replay-page" data-testid="replay-page">
    <header class="page-head">
      <div>
        <h2>重放数据包 · 溢出区间分段导出与断点恢复</h2>
        <p class="page-desc">
          为乱序超窗口事件流制作分段重放包：每段携带数据流标识、源日志文件与偏移上下界，
          支持按已完成区间续取、来源校验、增量回填登记与回填追溯。
        </p>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="replay-error">{{ errorMessage }}</p>

    <!-- 溢出报告 -->
    <article class="panel" data-testid="report-panel">
      <h3>溢出报告</h3>
      <div v-for="report in reports" :key="report.id" class="report-card">
        <div class="report-grid">
          <span><b>报告编号</b>{{ report.报告编号 }}</span>
          <span><b>状态</b>{{ report.status }}</span>
          <span><b>数据流</b>{{ report.数据流 }}</span>
          <span><b>源日志文件</b>{{ report.源日志文件 }}</span>
          <span><b>偏移区间</b>{{ report.偏移下界 }} ~ {{ report.偏移上界 }}（含两端）</span>
          <span><b>清单缺失偏移</b>{{ report.缺失偏移.length ? report.缺失偏移.join(', ') : '无' }}</span>
        </div>
        <p class="muted">{{ report.说明 }}</p>
        <div class="page-actions">
          <button class="btn primary" data-testid="create-pack" type="button" @click="onCreatePack(report.id)">
            制作重放数据包（{{ report.偏移下界 }}~{{ report.偏移上界 }}）
          </button>
          <button class="btn" data-testid="ingest-late" type="button" @click="onIngestLate(report.id, 275)">
            模拟迟到记录 275 补达源文件
          </button>
          <button class="btn ghost" data-testid="register-orphan" type="button" @click="onRegisterOrphan(report.id)">
            登记缺来源补包（挂起待核）
          </button>
        </div>
      </div>
    </article>

    <!-- 数据包列表 -->
    <article class="panel" data-testid="pack-panel">
      <h3>重放数据包（{{ packs.length }}）</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>数据包编号</th>
            <th>关联报告</th>
            <th>数据流</th>
            <th>源日志文件</th>
            <th>偏移上下界</th>
            <th>已完成区间</th>
            <th>状态</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="pack in packs"
            :key="pack.id"
            :class="{ selected: selectedPackId === pack.id }"
            :data-testid="`pack-row-${pack.id}`"
          >
            <td>{{ pack.数据包编号 }}</td>
            <td>{{ pack.报告编号 }}</td>
            <td>{{ pack.数据流 }}</td>
            <td>{{ pack.源日志文件 }}{{ pack.来源缺失 ? '（不可读）' : '' }}</td>
            <td>{{ pack.偏移下界 }} ~ {{ pack.偏移上界 }}</td>
            <td>{{ formatRanges(pack.已完成区间) || '—' }}</td>
            <td>{{ pack.status }}</td>
            <td class="row-actions">
              <button class="link" type="button" @click="onSelectPack(pack.id)">查看/续取</button>
            </td>
          </tr>
          <tr v-if="!packs.length">
            <td colspan="8" class="empty-state">暂无重放数据包</td>
          </tr>
        </tbody>
      </table>
    </article>

    <template v-if="selectedPack">
      <!-- 分段与下载 -->
      <article class="panel" data-testid="segment-panel">
        <h3>{{ selectedPack.数据包编号 }} · 分段（每段含数据流标识、源文件、偏移上下界）</h3>
        <table class="data-table">
          <thead>
            <tr>
              <th>分段编号</th>
              <th>数据流</th>
              <th>源日志文件</th>
              <th>偏移下界</th>
              <th>偏移上界</th>
              <th>已下载事件</th>
              <th>状态</th>
              <th>下载/续取</th>
              <th>文件</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="seg in selectedPack.分段" :key="seg.id" :data-testid="`segment-row-${seg.id}`">
              <td>{{ seg.分段编号 }}</td>
              <td>{{ seg.数据流 }}</td>
              <td>{{ seg.源日志文件 }}</td>
              <td>{{ seg.偏移下界 }}</td>
              <td>{{ seg.偏移上界 }}</td>
              <td>{{ seg.事件偏移.join(', ') || '—' }}</td>
              <td>{{ seg.status }}</td>
              <td class="row-actions">
                <button class="link" type="button" @click="onDownload(seg.id, 260)">下载至260中断</button>
                <button class="link" type="button" @click="onDownload(seg.id, null)">续取本段</button>
              </td>
              <td class="row-actions">
                <button class="link" type="button" @click="onDownloadFile(seg.id, 'event')">事件文件</button>
                <button class="link" type="button" @click="onDownloadFile(seg.id, 'index')">位置索引</button>
              </td>
            </tr>
          </tbody>
        </table>
      </article>

      <!-- 包内实际事件 -->
      <article class="panel" data-testid="event-panel">
        <h3>包内实际事件与原始偏移（{{ selectedPack.事件清单?.length ?? 0 }}）</h3>
        <table class="data-table compact">
          <thead>
            <tr>
              <th>原始偏移</th>
              <th>事件键（源文件@偏移）</th>
              <th>所属分段</th>
              <th>order_id</th>
              <th>checksum</th>
              <th>交付批次</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="event in selectedPack.事件清单" :key="event.event_key">
              <td>{{ event.offset }}</td>
              <td>{{ event.event_key }}</td>
              <td>{{ event.所属分段 }}</td>
              <td>{{ event.payload?.order_id ?? '—' }}</td>
              <td class="mono">{{ event.checksum ?? '—' }}</td>
              <td>{{ event.已交付批次.join(', ') || '未交付' }}</td>
            </tr>
          </tbody>
        </table>
      </article>

      <!-- 交付核对 -->
      <article class="panel" data-testid="trace-panel">
        <h3>按包索引核对交付来源</h3>
        <div class="page-actions">
          <button class="btn primary" type="button" @click="onTraceDelivery([240, 260, 275, 320])">
            核对 240 / 260 / 275 / 320 的交付批次
          </button>
        </div>
        <table v-if="deliveryTrace.length" class="data-table compact">
          <thead>
            <tr><th>偏移</th><th>分段</th><th>交付批次</th><th>说明</th></tr>
          </thead>
          <tbody>
            <tr v-for="row in deliveryTrace" :key="row.offset" :data-testid="`trace-row-${row.offset}`">
              <td>{{ row.offset }}</td>
              <td>{{ row.分段 ?? '—' }}</td>
              <td>{{ row.交付批次 ?? '—' }}</td>
              <td>{{ row.说明 ?? '' }}</td>
            </tr>
          </tbody>
        </table>
      </article>

      <!-- 恢复与重送 -->
      <article class="panel" data-testid="recover-panel">
        <h3>断点恢复与增量回填</h3>
        <div class="page-actions">
          <button class="btn primary" data-testid="recover-pack" type="button" @click="onRecover">
            使用本包发起恢复
          </button>
          <button class="btn" data-testid="redispatch-pack" type="button" @click="onRedispatch">
            重送（已写入事件不再派发）
          </button>
          <button class="btn ghost" type="button" @click="onTraceBackfill('binlog.001234@275')">
            追溯迟到事件 275 的回填
          </button>
        </div>
        <pre v-if="actionResult" class="result-box" data-testid="action-result">{{ actionResult }}</pre>
      </article>
    </template>

    <!-- 回填台账 -->
    <article class="panel" data-testid="ledger-panel">
      <h3>增量回填台账（{{ ledger.length }}）</h3>
      <table class="data-table compact">
        <thead>
          <tr>
            <th>回填编号</th><th>报告</th><th>数据包</th><th>分段</th>
            <th>事件键</th><th>原始偏移</th><th>order_id</th><th>状态</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in ledger" :key="row.id" :data-testid="`ledger-row-${row.event_key}`">
            <td>{{ row.回填编号 }}</td>
            <td>{{ row.报告编号 }}</td>
            <td>{{ row.数据包编号 }}</td>
            <td>{{ row.分段编号 }}</td>
            <td>{{ row.event_key }}</td>
            <td>{{ row.原始偏移 }}</td>
            <td>{{ row.order_id }}</td>
            <td>{{ row.status }}</td>
          </tr>
          <tr v-if="!ledger.length">
            <td colspan="8" class="empty-state">暂无已验收回填</td>
          </tr>
        </tbody>
      </table>
    </article>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  createPack,
  downloadSegment,
  downloadSegmentEventFile,
  downloadSegmentIndexFile,
  ingestLateEvent,
  getPack,
  listLedger,
  listPacks,
  listReports,
  recoverPack,
  redispatchPack,
  registerOrphanPack,
  traceBackfill,
  traceDelivery,
  type BackfillRecord,
  type DeliveryTraceRow,
  type ReplayPack,
  type ReplayReport,
} from '@/api/replay'

const reports = ref<ReplayReport[]>([])
const packs = ref<ReplayPack[]>([])
const ledger = ref<BackfillRecord[]>([])
const selectedPackId = ref<number | null>(null)
const selectedPack = ref<ReplayPack | null>(null)
const deliveryTrace = ref<DeliveryTraceRow[]>([])
const actionResult = ref('')
const errorMessage = ref('')

function formatRanges(ranges: number[][]): string {
  return ranges.map(([lo, hi]) => (lo === hi ? String(lo) : `${lo}~${hi}`)).join('，')
}

function fail(error: unknown): void {
  errorMessage.value = error instanceof Error ? error.message : '操作失败'
}

async function reload(keepSelection = true): Promise<void> {
  errorMessage.value = ''
  try {
    const [reportPayload, packPayload, ledgerPayload] = await Promise.all([
      listReports(),
      listPacks(),
      listLedger(),
    ])
    reports.value = reportPayload.items
    packs.value = packPayload.items
    ledger.value = ledgerPayload.items
    if (keepSelection && selectedPackId.value !== null) {
      selectedPack.value = await getPack(selectedPackId.value)
    }
  } catch (error) {
    fail(error)
  }
}

async function onSelectPack(packId: number): Promise<void> {
  selectedPackId.value = packId
  deliveryTrace.value = []
  actionResult.value = ''
  try {
    selectedPack.value = await getPack(packId)
  } catch (error) {
    fail(error)
  }
}

async function onCreatePack(reportId: number): Promise<void> {
  try {
    const result = await createPack(reportId)
    actionResult.value = result.message
    selectedPackId.value = result.pack.id
    await reload()
  } catch (error) {
    fail(error)
  }
}

async function onIngestLate(reportId: number, offset: number): Promise<void> {
  try {
    const result = await ingestLateEvent(reportId, offset)
    actionResult.value = result.message
    await reload()
  } catch (error) {
    fail(error)
  }
}

async function onRegisterOrphan(reportId: number): Promise<void> {
  try {
    const result = await registerOrphanPack({
      reportId,
      stream: 'order_events_cdc',
      fileName: 'binlog.009999',
      lower: 240,
      upper: 320,
      eventKeys: ['binlog.009999@300'],
    })
    actionResult.value = result.message
    await reload()
  } catch (error) {
    fail(error)
  }
}

async function onDownload(segmentId: number, stopAfterOffset: number | null): Promise<void> {
  try {
    const result = await downloadSegment(segmentId, stopAfterOffset)
    actionResult.value = JSON.stringify(
      {
        分段: result.delivery.分段编号,
        本次批次: result.delivery.本次交付批次,
        本次事件偏移: result.delivery.本次事件.map((event) => event.offset),
        已完成区间: result.delivery.已完成区间,
        续取起始偏移: result.delivery.续取起始偏移,
      },
      null,
      2,
    )
    await reload()
  } catch (error) {
    fail(error)
  }
}

async function onDownloadFile(segmentId: number, kind: 'event' | 'index'): Promise<void> {
  try {
    if (kind === 'event') {
      await downloadSegmentEventFile(segmentId)
    } else {
      await downloadSegmentIndexFile(segmentId)
    }
  } catch (error) {
    fail(error)
  }
}

async function onTraceDelivery(offsets: number[]): Promise<void> {
  if (selectedPackId.value === null) return
  try {
    const result = await traceDelivery(selectedPackId.value, offsets)
    deliveryTrace.value = result.交付核对
  } catch (error) {
    fail(error)
  }
}

async function onRecover(): Promise<void> {
  if (selectedPackId.value === null) return
  try {
    actionResult.value = JSON.stringify(await recoverPack(selectedPackId.value), null, 2)
    await reload()
  } catch (error) {
    fail(error)
  }
}

async function onRedispatch(): Promise<void> {
  if (selectedPackId.value === null) return
  try {
    actionResult.value = JSON.stringify(await redispatchPack(selectedPackId.value), null, 2)
    await reload()
  } catch (error) {
    fail(error)
  }
}

async function onTraceBackfill(eventKey: string): Promise<void> {
  try {
    actionResult.value = JSON.stringify(await traceBackfill(eventKey), null, 2)
  } catch (error) {
    fail(error)
  }
}

onMounted(() => {
  void reload(false)
})
</script>

<style scoped>
.replay-page {
  display: flex;
  flex-direction: column;
  gap: 14px;
}
.panel {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 12px 14px;
}
.panel h3 {
  margin: 0 0 10px;
  font-size: 15px;
}
.report-card {
  border: 1px solid #eef1f5;
  border-radius: 8px;
  padding: 10px 12px;
}
.report-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 8px 16px;
  font-size: 13px;
}
.report-grid b {
  display: inline-block;
  min-width: 96px;
  color: var(--muted);
  font-weight: normal;
}
.muted {
  color: var(--muted);
  font-size: 12px;
  margin: 8px 0;
}
tr.selected {
  background: #eff6ff;
}
.data-table.compact th,
.data-table.compact td {
  padding: 6px 8px;
  font-size: 12px;
}
.mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 11px;
}
.result-box {
  margin-top: 10px;
  background: #0f172a;
  color: #dbeafe;
  border-radius: 8px;
  padding: 12px;
  font-size: 12px;
  max-height: 320px;
  overflow: auto;
}
</style>
