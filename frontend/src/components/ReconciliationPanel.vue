<template>
  <section class="recon-panel" data-testid="reconciliation-panel">
    <header class="recon-head">
      <div>
        <h3>当日分区凭据、补算幂等与报告范围核对</h3>
        <p>
          区分「确实为零」和「尚未拿到源数据」；未成功读取的地区不生成数值零，
          发布时也不计入当日总量。
        </p>
      </div>
      <button class="btn ghost" data-testid="recon-refresh" type="button" @click="loadState">
        刷新记录
      </button>
    </header>

    <div class="recon-actions">
      <button class="btn" data-testid="prepare-scenario" type="button" @click="prepareScenario">
        准备华东30、华南确认空、华北超时
      </button>
      <button class="btn" data-testid="validate-partitions" type="button" @click="validatePartitions">
        校验预聚合输入与覆盖区间
      </button>
      <button class="btn primary" data-testid="first-query" type="button" @click="queryFirst">
        查询当期数据 / 申请当日汇报
      </button>
      <button class="btn" data-testid="submit-backfill" type="button" @click="submitDuplicateBackfills">
        提交华北补算（重复3次）
      </button>
      <button class="btn primary" data-testid="complete-backfill" type="button" @click="completeBackfill">
        华北补到 15
      </button>
      <button class="btn primary" data-testid="second-query" type="button" @click="querySecond">
        补算后重新查询
      </button>
      <button class="btn" data-testid="compare-reports" type="button" @click="compareReports">
        核对两份报告实际范围
      </button>
      <button class="btn" data-testid="publish-first-national" type="button" @click="publishFirstNational">
        第一份按全国总量发布（应拦截）
      </button>
      <button class="btn" data-testid="publish-first-partial" type="button" @click="publishFirstPartial">
        发布第一份已确认范围
      </button>
      <button class="btn primary" data-testid="publish-second-national" type="button" @click="publishSecondNational">
        发布第二份完整当日总量
      </button>
      <button class="btn ghost" data-testid="run-full-scenario" type="button" @click="runFullScenario">
        一键演练并生成对照
      </button>
    </div>

    <p v-if="message" class="recon-message" data-testid="recon-message">{{ message }}</p>
    <p v-if="errorMessage" class="recon-error" data-testid="recon-error">{{ errorMessage }}</p>

    <div v-if="validation" class="recon-summary" data-testid="validation-summary">
      <span>统计期：{{ validation.period }}</span>
      <span>已确认：{{ validation.confirmed_regions?.join('、') || '无' }}</span>
      <span>确认零行：{{ validation.true_zero_regions?.join('、') || '无' }}</span>
      <span>待源数据：{{ validation.source_unavailable_regions?.join('、') || '无' }}</span>
      <span :class="validation.coverage_complete ? 'ok' : 'warn'">
        覆盖完整：{{ validation.coverage_complete ? '是' : '否' }}
      </span>
    </div>

    <table v-if="records.length" class="recon-table" data-testid="partition-table">
      <thead>
        <tr>
          <th>地区</th>
          <th>来源</th>
          <th>分类</th>
          <th>读取状态</th>
          <th>读取行数</th>
          <th>指标值</th>
          <th>分区读取凭据</th>
          <th>错误</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in records" :key="row.record_id" :data-testid="`partition-row-${row.region}`">
          <td>{{ row.region }}</td>
          <td>{{ row.source_name }}</td>
          <td>{{ row.classification_label }}</td>
          <td>{{ row.status }}</td>
          <td>{{ row.rows_read ?? '未取得' }}</td>
          <td :data-testid="`partition-value-${row.region}`">{{ row.metric_value ?? '未确认（不是0）' }}</td>
          <td class="credential">{{ row.read_credential || '无（读取未成功）' }}</td>
          <td>{{ row.error || '—' }}</td>
        </tr>
      </tbody>
    </table>

    <div v-if="submissions.length" class="recon-card" data-testid="backfill-result">
      <h4>补算领取幂等</h4>
      <p data-testid="same-claim">
        三次提交的领取号是否同一个：<strong>{{ sameClaim ? '是' : '否' }}</strong>
        <span v-if="claimId">；领取号 {{ claimId }}</span>
      </p>
      <ul>
        <li v-for="(item, index) in submissions" :key="index">
          第{{ index + 1 }}次：{{ item.backfill_id || '未生成补算单' }} /
          {{ item.claim_id || '无领取号' }} / {{ item.message || item.status }}
        </li>
      </ul>
      <p v-if="emptySubmission" data-testid="empty-submission">
        华南补单：{{ emptySubmission.status }}；实际执行新任务：
        {{ emptySubmission.execution_created ? '是' : '否' }}（{{ emptySubmission.message }}）
      </p>
      <p v-if="completion" data-testid="completion-result">
        完成结果：{{ completion.backfill?.status_label }}，华北值
        {{ completion.backfill?.result_value }}，凭据
        {{ completion.record?.read_credential }}
      </p>
    </div>

    <div class="report-grid">
      <article v-if="firstReport" class="recon-card" data-testid="first-report">
        <h4>第一次报告 {{ firstReport.report_id }}</h4>
        <p><strong>已确认总量：{{ firstReport.confirmed_total }}</strong></p>
        <p>请求范围：{{ firstReport.requested_regions?.join('、') }}</p>
        <p>实际确认范围：<span data-testid="first-actual-scope">{{ firstReport.actual_regions?.join('、') }}</span></p>
        <p>未确认/排除：{{ firstReport.unconfirmed_regions?.join('、') || '无' }}</p>
        <SourceList :sources="firstReport.sources" />
      </article>

      <article v-if="secondReport" class="recon-card" data-testid="second-report">
        <h4>重新查询报告 {{ secondReport.report_id }}</h4>
        <p><strong>已确认总量：{{ secondReport.confirmed_total }}</strong></p>
        <p>请求范围：{{ secondReport.requested_regions?.join('、') }}</p>
        <p>实际确认范围：<span data-testid="second-actual-scope">{{ secondReport.actual_regions?.join('、') }}</span></p>
        <p>未确认/排除：{{ secondReport.unconfirmed_regions?.join('、') || '无' }}</p>
        <SourceList :sources="secondReport.sources" />
      </article>
    </div>

    <article v-if="comparison" class="recon-card comparison" data-testid="report-comparison">
      <h4>两份报告实际范围对照</h4>
      <p data-testid="comparison-conclusion">{{ comparison.conclusion }}</p>
      <p>总量差：<strong>{{ comparison.total_delta }}</strong>；范围新增：{{ comparison.scope_added?.join('、') || '无' }}</p>
    </article>

    <div v-if="publishResults.length" class="recon-card" data-testid="publish-results">
      <h4>发布核对</h4>
      <ul>
        <li v-for="(item, index) in publishResults" :key="index" :data-testid="`publish-result-${index}`">
          {{ item.ok ? '允许发布' : '已拦截' }}：{{ item.message || item.error }}
          <template v-if="item.published_total !== undefined">；发布总量 {{ item.published_total }}</template>
          <template v-if="item.excluded_regions?.length">；排除 {{ item.excluded_regions.join('、') }}</template>
        </li>
      </ul>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, h, onMounted, ref } from 'vue'

import { request } from '@/api/client'

defineProps<{ moduleKey?: string }>()

const state = ref<any>(null)
const validation = ref<any>(null)
const records = ref<any[]>([])
const firstReport = ref<any>(null)
const secondReport = ref<any>(null)
const submissions = ref<any[]>([])
const emptySubmission = ref<any>(null)
const completion = ref<any>(null)
const comparison = ref<any>(null)
const publishResults = ref<any[]>([])
const message = ref('')
const errorMessage = ref('')

const claimId = computed(() => submissions.value.find((item) => item.claim_id)?.claim_id || '')
const sameClaim = computed(() => {
  const ids = submissions.value.map((item) => item.claim_id).filter(Boolean)
  return ids.length > 0 && new Set(ids).size === 1
})

async function getJson(path: string): Promise<any> {
  const response = await request(path)
  const data = await response.json()
  if (!response.ok) {
    const error = new Error(data?.error || `请求失败：${response.status}`)
    ;(error as any).data = data
    throw error
  }
  return data
}

async function postJson(path: string, body: Record<string, unknown> = {}): Promise<any> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
  const data = await response.json()
  if (!response.ok || data.ok === false && data.http_status >= 400) {
    // 409 发布拦截是预期演示结果，由调用方保留 data；这里仅在 HTTP 层抛错。
    if (!response.ok) {
      const error = new Error(data?.error || `请求失败：${response.status}`)
      ;(error as any).data = data
      throw error
    }
  }
  return data
}

function showError(error: unknown) {
  errorMessage.value = error instanceof Error ? error.message : '操作失败'
}

async function loadState() {
  try {
    state.value = await getJson('/api/reconciliation/state')
    records.value = state.value.partition_execution_records || []
    validation.value = state.value.validation || null
  } catch (error) {
    showError(error)
  }
}

async function prepareScenario() {
  errorMessage.value = ''
  message.value = '正在准备三来源场景…'
  try {
    await postJson('/api/reconciliation/scenario/prepare', {})
    firstReport.value = null
    secondReport.value = null
    submissions.value = []
    emptySubmission.value = null
    completion.value = null
    comparison.value = null
    publishResults.value = []
    message.value = '已准备：华东 30、华南确认零行、华北数据源超时。'
    await loadState()
  } catch (error) {
    showError(error)
  }
}

async function validatePartitions() {
  try {
    validation.value = await getJson('/api/domain/aggregate_job/1/validate-partitions')
    records.value = validation.value.records || []
    message.value = validation.value.coverage_complete
      ? '覆盖区间完整，三个地区均已取得来源凭据。'
      : '校验完成：华南是确认零行，华北是来源失败/未拿到源数据，不能写成 0。'
  } catch (error) {
    showError(error)
  }
}

async function queryFirst() {
  try {
    const data = await getJson('/api/domain/dim_query/1/execute')
    firstReport.value = data.daily_report || data
    message.value = `第一次查询完成：报告 ${firstReport.value.report_id}，已确认总量 ${firstReport.value.confirmed_total}。`
    await loadState()
  } catch (error) {
    showError(error)
  }
}

async function submitDuplicateBackfills() {
  submissions.value = []
  publishResults.value = []
  try {
    for (let index = 0; index < 3; index += 1) {
      const result = await postJson('/api/reconciliation/backfills', {
        region: '华北',
        reason: index === 0 ? '华北数据源超时，值班员接续' : '同一缺项重复提交',
        owner: '值班员',
      })
      submissions.value.push(result)
    }
    emptySubmission.value = await postJson('/api/reconciliation/backfills', {
      region: '华南',
      reason: '已确认空地区的重复补单',
    })
    message.value = sameClaim.value
      ? '华北三次提交保留同一个领取号；华南确认空，不产生新执行。'
      : '补算提交完成。'
    await loadState()
  } catch (error) {
    showError(error)
  }
}

async function ensureBackfillId(): Promise<string> {
  const existing = submissions.value.find((item) => item.backfill_id)
  if (existing?.backfill_id) return existing.backfill_id
  const result = await postJson('/api/reconciliation/backfills', { region: '华北' })
  submissions.value.push(result)
  return result.backfill_id
}

async function completeBackfill() {
  try {
    const backfillId = await ensureBackfillId()
    completion.value = await postJson(`/api/reconciliation/backfills/${backfillId}/complete`, {
      value: 15,
      claim_id: claimId.value,
    })
    message.value = '华北已补到 15，并留下新的实际读取凭据。'
    await loadState()
  } catch (error) {
    showError(error)
  }
}

async function querySecond() {
  try {
    const data = await getJson('/api/domain/dim_query/1/execute')
    secondReport.value = data.daily_report || data
    message.value = `重新查询完成：报告 ${secondReport.value.report_id}，总量 ${secondReport.value.confirmed_total}。`
    await loadState()
  } catch (error) {
    showError(error)
  }
}

async function compareReports() {
  try {
    const first = firstReport.value?.report_id
    const second = secondReport.value?.report_id
    const path = first && second
      ? `/api/reconciliation/reports/${first}/compare/${second}`
      : '/api/reconciliation/reports/compare'
    comparison.value = await getJson(path)
    message.value = comparison.value.conclusion
  } catch (error) {
    showError(error)
  }
}

async function publish(report: any, asNational: boolean) {
  if (!report?.report_id) throw new Error('请先查询生成报告')
  const response = await request(`/api/reconciliation/reports/${report.report_id}/publish`, {
    method: 'POST',
    body: JSON.stringify(asNational ? { as_national: true } : { mode: 'confirmed' }),
  })
  const data = await response.json()
  publishResults.value.push(data)
  return data
}

async function publishFirstNational() {
  try {
    const result = await publish(firstReport.value, true)
    if (result.ok === false) {
      message.value = `发布已拦截：${result.error}`
    }
  } catch (error: any) {
    const data = error?.data
    if (data) publishResults.value.push(data)
    message.value = data?.error || error.message
    errorMessage.value = ''
  }
}

async function publishFirstPartial() {
  try {
    const result = await publish(firstReport.value, false)
    message.value = result.message || '第一份报告已按已确认范围发布。'
  } catch (error) {
    showError(error)
  }
}

async function publishSecondNational() {
  try {
    const result = await publish(secondReport.value, true)
    message.value = result.message || '第二份报告已按完整范围发布。'
  } catch (error) {
    showError(error)
  }
}

async function runFullScenario() {
  try {
    message.value = '正在执行完整场景…'
    const result = await postJson('/api/reconciliation/scenario/run', {})
    firstReport.value = result.first_report
    secondReport.value = result.second_report
    submissions.value = result.backfill_submissions
    emptySubmission.value = result.empty_region_submission
    completion.value = result.backfill_completion
    comparison.value = result.comparison
    publishResults.value = [
      result.publish_first_national_blocked,
      result.publish_first_confirmed_scope,
      result.publish_second_national,
    ]
    validation.value = result.validation_before
    records.value = [
      ...result.validation_before.records.filter((row: any) => row.region !== '华北'),
      result.backfill_completion.record,
    ]
    message.value = result.comparison.conclusion
    await loadState()
  } catch (error) {
    showError(error)
  }
}

const SourceList = {
  props: { sources: { type: Array as () => any[], default: () => [] } },
  setup(sourceProps: { sources: any[] }) {
    return () =>
      h('ul', { class: 'source-list', 'data-testid': 'source-list' },
        sourceProps.sources.map((source) =>
          h('li', { 'data-testid': `source-${source.region}` }, [
            h('strong', `${source.region}：${source.source_name}`),
            h('span', `；用途：${source.usage}`),
            h('span', `；凭据：${source.credential || '无'}`),
          ]),
        ),
      )
  },
}

onMounted(loadState)
</script>

<style scoped>
.recon-panel {
  margin: 16px 0;
  padding: 18px;
  border: 1px solid #d9e2ef;
  border-radius: 14px;
  background: #f8fbff;
}
.recon-head { display: flex; justify-content: space-between; gap: 16px; align-items: flex-start; }
.recon-head h3 { margin: 0 0 6px; font-size: 18px; }
.recon-head p { margin: 0; color: #5b6b82; line-height: 1.6; }
.recon-actions { display: flex; flex-wrap: wrap; gap: 8px; margin: 14px 0; }
.recon-message { padding: 10px 12px; border-radius: 8px; background: #e8f3ff; color: #185fa5; }
.recon-error { padding: 10px 12px; border-radius: 8px; background: #fff0f0; color: #b42318; }
.recon-summary { display: flex; flex-wrap: wrap; gap: 12px; margin: 12px 0; font-size: 14px; }
.ok { color: #067647; font-weight: 700; }
.warn { color: #b54708; font-weight: 700; }
.recon-table { width: 100%; border-collapse: collapse; background: #fff; font-size: 13px; }
.recon-table th, .recon-table td { border: 1px solid #e1e8f2; padding: 8px; text-align: left; vertical-align: top; }
.recon-table th { background: #eef4fb; }
.credential { max-width: 220px; overflow-wrap: anywhere; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }
.report-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 12px; margin-top: 12px; }
.recon-card { background: #fff; border: 1px solid #dce5f0; border-radius: 12px; padding: 12px 14px; margin-top: 12px; }
.recon-card h4 { margin: 0 0 8px; }
.comparison { border-color: #9cc4ff; background: #f4f9ff; }
.source-list { margin: 8px 0 0; padding-left: 18px; font-size: 13px; line-height: 1.7; }
@media (max-width: 720px) { .recon-head { flex-direction: column; } }
</style>
