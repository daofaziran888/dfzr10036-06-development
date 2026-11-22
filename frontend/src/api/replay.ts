/** 重放数据包接口封装：溢出报告、分段导出、断点续取、恢复核验与回填追溯。 */
import { request } from './client'

export interface ReplayReport {
  id: number
  status: string
  报告编号: string
  数据流: string
  源日志文件: string
  偏移下界: number
  偏移上界: number
  清单偏移: number[]
  缺失偏移: number[]
  产生时间: string
  说明: string
}

export interface DeliveryBatch {
  交付批次: string
  事件偏移: number[]
  交付时间: string
}

export interface ReplaySegment {
  id: number
  status: string
  分段编号: string
  数据包ID: number
  数据包编号: string
  数据流: string
  源日志文件: string
  偏移下界: number
  偏移上界: number
  事件数: number
  事件偏移: number[]
  源位置索引条数: number
  下载交付批次: DeliveryBatch[]
  最后续取偏移: number
}

export interface PackEvent {
  event_key: string
  offset: number
  源日志文件: string
  数据流: string
  所属分段: string
  checksum: string | null
  payload: Record<string, unknown> | null
  已交付批次: string[]
}

export interface ReplayPack {
  id: number
  status: string
  数据包编号: string
  报告编号: string
  报告ID: number
  数据流: string
  源日志文件: string
  偏移下界: number
  偏移上界: number
  已下载事件偏移: number[]
  已完成区间: number[][]
  来源缺失: boolean
  事件键?: string[]
  分段?: ReplaySegment[]
  事件清单?: PackEvent[]
}

export interface DownloadResult {
  分段编号: string
  数据流: string
  源日志文件: string
  偏移下界: number
  偏移上界: number
  本次交付批次: string | null
  本次事件: { offset: number; event_key: string; checksum: string; payload: Record<string, unknown> }[]
  已完成区间: number[][]
  续取起始偏移: number
  待交付偏移: number[]
  分段状态: string
}

export interface BackfillRecord {
  id: number
  status: string
  回填编号: string
  报告编号: string
  数据包编号: string
  分段编号: string
  event_key: string
  数据流: string
  源日志文件: string
  原始偏移: number
  order_id: string
  登记时间: string
}

export interface DeliveryTraceRow {
  offset: number
  分段: string | null
  交付批次: string | null
  源日志文件?: string
  数据流?: string
  说明?: string
}

async function postJson<T>(path: string, body: unknown = {}): Promise<T> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
  if (!response.ok) {
    const detail = await response.json().catch(() => ({ detail: '请求失败' }))
    throw new Error(detail.detail ?? '请求失败')
  }
  return (await response.json()) as T
}

async function getJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) throw new Error('重放数据读取失败')
  return (await response.json()) as T
}

export function listReports(): Promise<{ items: ReplayReport[] }> {
  return getJson('/api/replay/reports')
}

export function listPacks(reportId?: number): Promise<{ items: ReplayPack[] }> {
  const query = reportId === undefined ? '' : `?report_id=${reportId}`
  return getJson(`/api/replay/packs${query}`)
}

export function getPack(packId: number): Promise<ReplayPack> {
  return getJson(`/api/replay/packs/${packId}`)
}

export function createPack(reportId: number): Promise<{ ok: boolean; message: string; pack: ReplayPack }> {
  return postJson('/api/replay/packs', { report_id: reportId })
}

export function ingestLateEvent(reportId: number, offset: number): Promise<{ ok: boolean; message: string }> {
  return postJson(`/api/replay/reports/${reportId}/late-event`, { report_id: reportId, offset })
}

export function downloadSegment(
  segmentId: number,
  stopAfterOffset: number | null = null,
  deliveryNo: string | null = null,
): Promise<{ ok: boolean; message: string; delivery: DownloadResult }> {
  return postJson(`/api/replay/segments/${segmentId}/download`, {
    stop_after_offset: stopAfterOffset,
    delivery_no: deliveryNo,
  })
}

export function recoverPack(packId: number): Promise<Record<string, unknown>> {
  return postJson(`/api/replay/packs/${packId}/recover`)
}

export function redispatchPack(packId: number): Promise<Record<string, unknown>> {
  return postJson(`/api/replay/packs/${packId}/redispatch`)
}

export function traceDelivery(
  packId: number,
  offsets: number[],
): Promise<{ ok: boolean; 数据包编号: string; 交付核对: DeliveryTraceRow[] }> {
  return postJson(`/api/replay/packs/${packId}/delivery-trace`, { offsets })
}

export function registerOrphanPack(input: {
  reportId: number
  stream: string
  fileName: string
  lower: number
  upper: number
  eventKeys: string[]
}): Promise<{ ok: boolean; message: string; pack: ReplayPack }> {
  return postJson('/api/replay/orphan-packs', {
    report_id: input.reportId,
    stream: input.stream,
    file_name: input.fileName,
    lower: input.lower,
    upper: input.upper,
    event_keys: input.eventKeys,
  })
}

export function listLedger(): Promise<{ items: BackfillRecord[] }> {
  return getJson('/api/replay/backfill-ledger')
}

export function traceBackfill(eventKey: string): Promise<{ ok: boolean; event_key: string; 回填记录: BackfillRecord }> {
  return getJson(`/api/replay/backfill-trace/${encodeURIComponent(eventKey)}`)
}

async function downloadFile(path: string, fallbackName: string): Promise<void> {
  const response = await request(path)
  if (!response.ok) throw new Error('文件下载失败')
  const disposition = response.headers.get('Content-Disposition') ?? ''
  const matched = disposition.match(/filename="?([^";]+)"?/)
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = matched?.[1] ?? fallbackName
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export function downloadSegmentEventFile(segmentId: number): Promise<void> {
  return downloadFile(`/api/replay/segments/${segmentId}/event-file`, `segment-${segmentId}-events.jsonl`)
}

export function downloadSegmentIndexFile(segmentId: number): Promise<void> {
  return downloadFile(`/api/replay/segments/${segmentId}/index-file`, `segment-${segmentId}-index.json`)
}
