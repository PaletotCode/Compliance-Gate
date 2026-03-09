/* eslint-disable no-console */

import { mkdirSync, writeFileSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

type CallOptions = {
  method: 'GET' | 'POST' | 'PUT'
  body?: Record<string, unknown>
  attachCsrf?: boolean
}

type LoginResponse =
  | {
      mfa_required: true
      challenge_id: string
    }
  | {
      expires_in: number
      user: { id: string; tenant_id: string; username: string; role: string }
    }

type PreviewRawResponse = {
  status: 'ok' | 'error'
  detected_headers: string[]
}

type ProfileResponse = { id: string }

type IngestResponse = {
  status: 'success' | 'error'
  dataset_version_id: string
  total_records: number
}

type MaterializeResponseEnvelope = {
  data: {
    dataset_version_id: string
    row_count: number
    checksum?: string | null
  }
}

type PaginatedEnvelope = {
  data: {
    items: Array<Record<string, unknown>>
    meta: {
      total: number
      page: number
      size: number
      has_next: boolean
      has_previous: boolean
    }
  }
}

const DEFAULT_BASE_URL = 'http://localhost:8000'
const DEFAULT_USERNAME = 'admin'
const DEFAULT_PASSWORD = 'Admin1234'

const AUTH_COOKIE_NAME = process.env.AUTH_COOKIE_NAME ?? 'cg_access'
const CSRF_COOKIE_NAME = process.env.CSRF_COOKIE_NAME ?? 'cg_csrf'
const CSRF_HEADER_NAME = process.env.CSRF_HEADER_NAME ?? 'X-CSRF-Token'

async function main() {
  const __filename = fileURLToPath(import.meta.url)
  const __dirname = path.dirname(__filename)
  const repoRoot = path.resolve(__dirname, '..', '..')

  const baseUrl = normalizeBaseUrl(process.env.MAIN_VIEW_CHECK_BASE_URL ?? DEFAULT_BASE_URL)
  const username = process.env.MAIN_VIEW_CHECK_USERNAME ?? process.env.AUTH_BOOTSTRAP_ADMIN_USERNAME ?? DEFAULT_USERNAME
  const password = process.env.MAIN_VIEW_CHECK_PASSWORD ?? process.env.AUTH_BOOTSTRAP_ADMIN_PASSWORD ?? DEFAULT_PASSWORD
  const dataDir = process.env.MAIN_VIEW_CHECK_DATA_DIR?.trim()
  const uploadSessionId = process.env.MAIN_VIEW_CHECK_UPLOAD_SESSION_ID?.trim()

  await waitForBackend(baseUrl, 60_000)
  console.log('PASSO 1: backend /health OK')

  const jar = new CookieJar()
  const login = (await callJson(baseUrl, '/api/v1/auth/login', jar, {
    method: 'POST',
    body: { username, password },
    attachCsrf: false,
  })) as LoginResponse

  if ('mfa_required' in login) {
    throw new Error('PASSO 2 falhou: login exigiu MFA. Defina usuário sem MFA para o fluxo headless.')
  }

  assert(jar.get(AUTH_COOKIE_NAME), `PASSO 2 falhou: cookie ${AUTH_COOKIE_NAME} ausente`)
  assert(jar.get(CSRF_COOKIE_NAME), `PASSO 2 falhou: cookie ${CSRF_COOKIE_NAME} ausente`)
  console.log('PASSO 2: login cookie-only + csrf OK')

  const sources = (await callJson(baseUrl, '/api/v1/csv-tabs/sources', jar, {
    method: 'GET',
  })) as string[]

  const expectedSources = ['AD', 'UEM', 'EDR', 'ASSET']
  const availableSources = expectedSources.filter((source) => sources.includes(source))
  assert(availableSources.length === expectedSources.length, 'PASSO 3 falhou: fontes incompletas')
  console.log(`PASSO 3: fontes ${availableSources.join(', ')}`)

  const profileIds: Record<string, string> = {}

  for (const source of availableSources) {
    const raw = (await callJson(baseUrl, '/api/v1/csv-tabs/preview/raw', jar, {
      method: 'POST',
      body: {
        source,
        ...(dataDir ? { data_dir: dataDir } : {}),
        ...(uploadSessionId ? { upload_session_id: uploadSessionId } : {}),
      },
    })) as PreviewRawResponse

    assert(raw.status === 'ok', `PASSO 4.${source} falhou: preview/raw status=${raw.status}`)
    assert(raw.detected_headers.length > 0, `PASSO 4.${source} falhou: sem headers detectados`)

    const profile = (await callJson(baseUrl, '/api/v1/csv-tabs/profiles', jar, {
      method: 'POST',
      body: {
        source,
        scope: 'PRIVATE',
        name: `TI FullFlow ${source} ${Date.now()}`,
        payload: {
          header_row: 0,
          sic_column: raw.detected_headers[0],
          selected_columns: raw.detected_headers.slice(0, Math.min(5, raw.detected_headers.length)),
        },
      },
    })) as ProfileResponse

    profileIds[source] = profile.id
    console.log(`PASSO 4.${source}: profile=${profile.id}`)
  }

  const previewDataset = (await callJson(baseUrl, '/api/v1/datasets/machines/preview', jar, {
    method: 'POST',
    body: {
      profile_ids: profileIds,
      ...(dataDir ? { data_dir: dataDir } : {}),
      ...(uploadSessionId ? { upload_session_id: uploadSessionId } : {}),
    },
  })) as Record<string, unknown>

  assert(String(previewDataset.status) === 'ok', 'PASSO 5 falhou: datasets preview sem status ok')
  console.log('PASSO 5: datasets preview OK')

  const ingest = (await callJson(baseUrl, '/api/v1/datasets/machines/ingest', jar, {
    method: 'POST',
    body: {
      source: 'path',
      profile_ids: profileIds,
      ...(dataDir ? { data_dir: dataDir } : {}),
      ...(uploadSessionId ? { upload_session_id: uploadSessionId } : {}),
    },
  })) as IngestResponse

  assert(ingest.status === 'success', 'PASSO 6 falhou: ingest sem success')
  assert(Boolean(ingest.dataset_version_id), 'PASSO 6 falhou: dataset_version_id ausente')
  console.log(`PASSO 6: ingest OK dataset_version_id=${ingest.dataset_version_id}`)

  const materialize = (await callJson(
    baseUrl,
    `/api/v1/engine/materialize/machines?dataset_version_id=${encodeURIComponent(ingest.dataset_version_id)}`,
    jar,
    {
      method: 'POST',
    },
  )) as MaterializeResponseEnvelope

  assert(materialize.data.row_count >= 0, 'PASSO 7 falhou: row_count inválido')
  console.log(`PASSO 7: materialize OK rows=${materialize.data.row_count}`)

  const table = (await callJson(
    baseUrl,
    `/api/v1/engine/tables/machines?dataset_version_id=${encodeURIComponent(ingest.dataset_version_id)}&page=1&size=200`,
    jar,
    { method: 'GET' },
  )) as PaginatedEnvelope

  assert(table.data.items.length > 0, 'PASSO 8 falhou: tabela sem itens')
  console.log(`PASSO 8: table OK rows_page=${table.data.items.length}`)

  const summary = (await callJson(
    baseUrl,
    `/api/v1/machines/summary?dataset_version_id=${encodeURIComponent(ingest.dataset_version_id)}`,
    jar,
    { method: 'GET' },
  )) as { data: { total: number } }

  const filters = (await callJson(baseUrl, '/api/v1/machines/filters', jar, {
    method: 'GET',
  })) as { data: Array<Record<string, unknown>> }

  assert(summary.data.total >= table.data.items.length, 'PASSO 9 falhou: summary.total incoerente')
  assert(filters.data.length > 0, 'PASSO 9 falhou: filtros vazios')
  console.log(`PASSO 9: summary/filters OK total=${summary.data.total}`)

  const report = (await callJson(
    baseUrl,
    `/api/v1/engine/reports/run?dataset_version_id=${encodeURIComponent(ingest.dataset_version_id)}`,
    jar,
    {
      method: 'POST',
      body: {
        template_name: 'machines_status_summary',
        limit: 5000,
      },
    },
  )) as { data: { row_count: number } }

  console.log(`PASSO 10: report/run OK row_count=${report.data.row_count}`)

  const output = {
    generated_at: new Date().toISOString(),
    dataset_version_id: ingest.dataset_version_id,
    ingest_total_records: ingest.total_records,
    materialize_row_count: materialize.data.row_count,
    materialize_checksum: materialize.data.checksum ?? null,
    table_page_rows: table.data.items.length,
    table_total: table.data.meta.total,
    summary_total: summary.data.total,
    filters_count: filters.data.length,
    report_row_count: report.data.row_count,
    profile_ids: profileIds,
  }

  const outputDir = path.resolve(repoRoot, 'retests', 'output')
  mkdirSync(outputDir, { recursive: true })
  const outputPath = path.resolve(outputDir, 'main_view_full_flow_check.json')
  writeFileSync(outputPath, JSON.stringify(output, null, 2), 'utf-8')

  console.log(`PASSO 11: output salvo em ${outputPath}`)
  console.log('PASSO 12: validação full flow concluída -> EXIT 0')
}

async function callJson(baseUrl: string, pathName: string, jar: CookieJar, options: CallOptions): Promise<unknown> {
  const headers: Record<string, string> = {
    Accept: 'application/json',
  }

  if (options.body) {
    headers['Content-Type'] = 'application/json'
  }

  const cookieHeader = jar.toHeader()
  if (cookieHeader) {
    headers.Cookie = cookieHeader
  }

  const shouldAttachCsrf = options.attachCsrf ?? (options.method === 'POST' || options.method === 'PUT')
  if (shouldAttachCsrf) {
    const csrfToken = jar.get(CSRF_COOKIE_NAME)
    if (csrfToken) {
      headers[CSRF_HEADER_NAME] = csrfToken
    }
  }

  let response: Response
  try {
    response = await fetch(`${baseUrl}${pathName}`, {
      method: options.method,
      headers,
      body: options.body ? JSON.stringify(options.body) : undefined,
    })
  } catch (error) {
    throw new Error(`${options.method} ${pathName} -> request failed: ${extractMessage(error)}`)
  }

  jar.captureFromResponse(response)

  const rawText = await response.text()
  const payload = tryParseJson(rawText)

  if (!response.ok) {
    const message =
      typeof payload === 'object' && payload
        ? String((payload as Record<string, unknown>).detail ?? (payload as Record<string, unknown>).message ?? rawText)
        : rawText || response.statusText
    throw new Error(`${options.method} ${pathName} -> ${response.status}: ${message}`)
  }

  return payload
}

class CookieJar {
  private readonly store = new Map<string, string>()

  get(name: string): string | undefined {
    return this.store.get(name)
  }

  toHeader(): string {
    if (this.store.size === 0) return ''
    return Array.from(this.store.entries())
      .map(([name, value]) => `${name}=${value}`)
      .join('; ')
  }

  captureFromResponse(response: Response): void {
    const getSetCookie = (response.headers as unknown as { getSetCookie?: () => string[] }).getSetCookie
    const setCookieHeaders = getSetCookie ? getSetCookie.call(response.headers) : []

    if (setCookieHeaders.length === 0) {
      const single = response.headers.get('set-cookie')
      if (single) this.captureSetCookieHeader(single)
      return
    }

    for (const header of setCookieHeaders) {
      this.captureSetCookieHeader(header)
    }
  }

  private captureSetCookieHeader(rawHeader: string): void {
    const [nameValue] = rawHeader.split(';', 1)
    const eqIndex = nameValue.indexOf('=')
    if (eqIndex <= 0) return

    const name = nameValue.slice(0, eqIndex).trim()
    const value = nameValue.slice(eqIndex + 1).trim()

    if (!name) return
    if (!value) {
      this.store.delete(name)
      return
    }

    this.store.set(name, value)
  }
}

async function waitForBackend(baseUrl: string, timeoutMs: number): Promise<void> {
  const startedAt = Date.now()

  while (Date.now() - startedAt < timeoutMs) {
    try {
      const response = await fetch(`${baseUrl}/health`, { method: 'GET' })
      if (response.ok) return
    } catch {
      // retry
    }

    await new Promise((resolve) => setTimeout(resolve, 1200))
  }

  throw new Error('Backend não respondeu /health dentro do timeout')
}

function tryParseJson(rawText: string): unknown {
  if (!rawText) return {}
  try {
    return JSON.parse(rawText)
  } catch {
    return rawText
  }
}

function normalizeBaseUrl(value: string): string {
  return value.endsWith('/') ? value.slice(0, -1) : value
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) {
    throw new Error(message)
  }
}

function extractMessage(error: unknown): string {
  if (error instanceof Error) return error.message
  return String(error)
}

main().catch((error) => {
  console.error(`[main_view_full_flow_check] ${extractMessage(error)}`)
  process.exit(1)
})
