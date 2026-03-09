/* eslint-disable no-console */

import { spawnSync } from 'node:child_process'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

type LoginChallengeResponse = {
  mfa_required: true
  challenge_id: string
}

type LoginSuccessResponse = {
  expires_in: number
  user: {
    id: string
    tenant_id: string
    username: string
    role: string
  }
}

type LoginResponse = LoginChallengeResponse | LoginSuccessResponse

type CallOptions = {
  method: 'GET' | 'POST' | 'PUT'
  body?: Record<string, unknown>
  attachCsrf?: boolean
}

type PreviewRawResponse = {
  status: 'ok' | 'error'
  detected_headers: string[]
  sample_rows: Array<Record<string, unknown>>
  error?: string
}

type CreateProfileResponse = {
  id: string
}

type PreviewParsedResponse = {
  status: 'ok' | 'error'
  sample_rows: Array<Record<string, unknown>>
  error?: string
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
  const challengeTotpFromEnv = process.env.MAIN_VIEW_CHECK_TOTP_CODE?.trim()
  const shouldStartDocker = String(process.env.MAIN_VIEW_CHECK_START_DOCKER ?? '').trim() === '1'

  if (shouldStartDocker) {
    console.log('PASSO 1: docker compose up -d')
    const result = spawnSync('docker', ['compose', 'up', '-d'], {
      cwd: repoRoot,
      stdio: 'inherit',
    })

    if (result.status !== 0) {
      throw new Error(`PASSO 1 falhou: docker compose retornou código ${result.status ?? 1}`)
    }
    console.log('PASSO 1: docker backend -> ok')
  } else {
    console.log('PASSO 1: backend assumido em execução (MAIN_VIEW_CHECK_START_DOCKER!=1)')
  }

  await waitForBackend(baseUrl, 60_000)
  console.log('PASSO 1.1: backend respondeu /health')

  const cookieJar = new CookieJar()

  let loginResponse = (await callJson(baseUrl, '/api/v1/auth/login', cookieJar, {
    method: 'POST',
    body: { username, password },
    attachCsrf: false,
  })) as LoginResponse

  if (isLoginChallenge(loginResponse)) {
    if (!challengeTotpFromEnv) {
      throw new Error(
        'PASSO 2 falhou: login exigiu MFA e MAIN_VIEW_CHECK_TOTP_CODE não foi informado.',
      )
    }

    loginResponse = (await callJson(baseUrl, '/api/v1/auth/login', cookieJar, {
      method: 'POST',
      body: {
        username,
        password,
        challenge_id: loginResponse.challenge_id,
        totp_code: challengeTotpFromEnv,
      },
      attachCsrf: false,
    })) as LoginResponse

    if (isLoginChallenge(loginResponse)) {
      throw new Error('PASSO 2 falhou: MFA challenge permaneceu ativo após envio do código.')
    }
  }

  assert(cookieJar.get(AUTH_COOKIE_NAME), `PASSO 2 falhou: cookie ${AUTH_COOKIE_NAME} ausente`)
  assert(cookieJar.get(CSRF_COOKIE_NAME), `PASSO 2 falhou: cookie ${CSRF_COOKIE_NAME} ausente`)
  console.log('PASSO 2: login cookie-only + csrf -> ok')

  const sources = (await callJson(baseUrl, '/api/v1/csv-tabs/sources', cookieJar, {
    method: 'GET',
  })) as string[]

  const sourceList = ['AD', 'UEM', 'EDR', 'ASSET'].filter((source) => sources.includes(source))
  assert(sourceList.length === 4, 'PASSO 3 falhou: backend não retornou todas as fontes AD/UEM/EDR/ASSET')
  console.log(`PASSO 3: fontes disponíveis -> ${sourceList.join(', ')}`)

  const dataDir = process.env.MAIN_VIEW_CHECK_DATA_DIR?.trim()
  const uploadSessionId = process.env.MAIN_VIEW_CHECK_UPLOAD_SESSION_ID?.trim()

  for (const source of sourceList) {
    console.log(`PASSO 4.${source}: preview/raw`)
    const rawPreview = (await callJson(baseUrl, '/api/v1/csv-tabs/preview/raw', cookieJar, {
      method: 'POST',
      body: {
        source,
        ...(dataDir ? { data_dir: dataDir } : {}),
        ...(uploadSessionId ? { upload_session_id: uploadSessionId } : {}),
        header_row_override: 0,
      },
    })) as PreviewRawResponse

    assert(rawPreview.status === 'ok', `preview/raw ${source} retornou status erro: ${rawPreview.error ?? 'sem detalhe'}`)
    assert(rawPreview.detected_headers.length > 0, `preview/raw ${source} sem headers detectados`)
    assert(Array.isArray(rawPreview.sample_rows), `preview/raw ${source} sem sample_rows`)

    const sicColumn = rawPreview.detected_headers[0]
    const selectedColumns = rawPreview.detected_headers.slice(0, Math.min(4, rawPreview.detected_headers.length))

    console.log(`PASSO 5.${source}: create profile`)
    const createdProfile = (await callJson(baseUrl, '/api/v1/csv-tabs/profiles', cookieJar, {
      method: 'POST',
      body: {
        source,
        scope: 'PRIVATE',
        name: `MainView Check ${source} ${Date.now()}`,
        payload: {
          header_row: 0,
          sic_column: sicColumn,
          selected_columns: selectedColumns,
        },
      },
    })) as CreateProfileResponse

    assert(Boolean(createdProfile.id), `create profile ${source} não retornou id`)

    console.log(`PASSO 6.${source}: update profile`) 
    await callJson(baseUrl, `/api/v1/csv-tabs/profiles/${createdProfile.id}`, cookieJar, {
      method: 'PUT',
      body: {
        payload: {
          header_row: 0,
          sic_column: sicColumn,
          selected_columns: selectedColumns,
        },
        change_note: 'main_view_profile_check',
      },
    })

    console.log(`PASSO 7.${source}: preview/parsed`)
    const parsedPreview = (await callJson(baseUrl, '/api/v1/csv-tabs/preview/parsed', cookieJar, {
      method: 'POST',
      body: {
        source,
        profile_id: createdProfile.id,
        ...(dataDir ? { data_dir: dataDir } : {}),
        ...(uploadSessionId ? { upload_session_id: uploadSessionId } : {}),
      },
    })) as PreviewParsedResponse

    assert(
      parsedPreview.status === 'ok',
      `preview/parsed ${source} retornou status erro: ${parsedPreview.error ?? 'sem detalhe'}`,
    )
    assert(Array.isArray(parsedPreview.sample_rows), `preview/parsed ${source} sem sample_rows`)

    console.log(`PASSO 8.${source}: fluxo profile -> ok`)
  }

  console.log('PASSO 9: validação completa -> EXIT 0')
}

function isLoginChallenge(response: LoginResponse): response is LoginChallengeResponse {
  return 'mfa_required' in response
}

async function callJson(baseUrl: string, pathName: string, cookieJar: CookieJar, options: CallOptions): Promise<unknown> {
  const headers: Record<string, string> = {
    Accept: 'application/json',
  }

  if (options.body) {
    headers['Content-Type'] = 'application/json'
  }

  const cookieHeader = cookieJar.toHeader()
  if (cookieHeader) {
    headers.Cookie = cookieHeader
  }

  const shouldAttachCsrf = options.attachCsrf ?? (options.method === 'POST' || options.method === 'PUT')
  if (shouldAttachCsrf) {
    const csrfToken = cookieJar.get(CSRF_COOKIE_NAME)
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

  cookieJar.captureFromResponse(response)

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
      const singleSetCookie = response.headers.get('set-cookie')
      if (singleSetCookie) {
        this.captureSetCookieHeader(singleSetCookie)
      }
      return
    }

    for (const setCookie of setCookieHeaders) {
      this.captureSetCookieHeader(setCookie)
    }
  }

  private captureSetCookieHeader(rawHeader: string): void {
    const [nameValue] = rawHeader.split(';', 1)
    const equalsIndex = nameValue.indexOf('=')
    if (equalsIndex <= 0) return

    const name = nameValue.slice(0, equalsIndex).trim()
    const value = nameValue.slice(equalsIndex + 1).trim()

    if (!name) return
    if (!value) {
      this.store.delete(name)
      return
    }

    this.store.set(name, value)
  }
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

async function waitForBackend(baseUrl: string, timeoutMs: number): Promise<void> {
  const startedAt = Date.now()

  while (Date.now() - startedAt < timeoutMs) {
    try {
      const response = await fetch(`${baseUrl}/health`, { method: 'GET' })
      if (response.ok) {
        return
      }
    } catch {
      // keep retrying until timeout
    }

    await new Promise((resolve) => setTimeout(resolve, 1500))
  }

  throw new Error('Backend não respondeu /health dentro do timeout')
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) {
    throw new Error(message)
  }
}

function extractMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message
  }
  return String(error)
}

main().catch((error) => {
  console.error(`[main_view_profile_check] ${extractMessage(error)}`)
  process.exit(1)
})
