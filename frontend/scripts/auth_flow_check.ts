/* eslint-disable no-console */

import { createHmac } from 'node:crypto'

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
    is_active: boolean
    mfa_enabled: boolean
    require_password_change: boolean
  }
}

type LoginResponse = LoginChallengeResponse | LoginSuccessResponse

type MfaSetupResponse = {
  otpauth_url: string
  qr_code_base64_png: string
  instructions: string
}

type MfaConfirmResponse = {
  recovery_codes: string[]
}

type MeResponse = {
  id: string
  tenant_id: string
  username: string
  role: string
  is_active: boolean
  mfa_enabled: boolean
  require_password_change: boolean
}

type CallOptions = {
  method: 'GET' | 'POST'
  body?: Record<string, unknown>
  attachCsrf?: boolean
}

const DEFAULT_BASE_URL = 'http://localhost:8000'
const DEFAULT_USERNAME = 'admin'
const DEFAULT_PASSWORD = 'Admin1234'

const AUTH_COOKIE_NAME = process.env.AUTH_COOKIE_NAME ?? 'cg_access'
const CSRF_COOKIE_NAME = process.env.CSRF_COOKIE_NAME ?? 'cg_csrf'
const CSRF_HEADER_NAME = process.env.CSRF_HEADER_NAME ?? 'X-CSRF-Token'

async function main() {
  const baseUrl = normalizeBaseUrl(process.env.AUTH_CHECK_BASE_URL ?? DEFAULT_BASE_URL)
  const username = process.env.AUTH_CHECK_USERNAME ?? process.env.AUTH_BOOTSTRAP_ADMIN_USERNAME ?? DEFAULT_USERNAME
  const password = process.env.AUTH_CHECK_PASSWORD ?? process.env.AUTH_BOOTSTRAP_ADMIN_PASSWORD ?? DEFAULT_PASSWORD
  const challengeTotpFromEnv = process.env.AUTH_CHECK_TOTP_CODE?.trim()

  const cookieJar = new CookieJar()

  let loginResponse = (await callJson(baseUrl, '/api/v1/auth/login', cookieJar, {
    method: 'POST',
    body: { username, password },
    attachCsrf: false,
  })) as LoginResponse

  if (isLoginChallenge(loginResponse)) {
    console.log('PASSO 1: login -> challenge')

    if (!challengeTotpFromEnv) {
      throw new Error(
        'Login exigiu MFA no PASSO 1. Defina AUTH_CHECK_TOTP_CODE para resolver o challenge_id e continuar.',
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
      throw new Error('PASSO 1 falhou: MFA challenge permaneceu ativo após envio de totp_code.')
    }

    console.log('PASSO 1: login challenge -> ok')
  } else {
    console.log('PASSO 1: login -> ok')
  }

  assert(cookieJar.get(AUTH_COOKIE_NAME), `PASSO 1 falhou: cookie ${AUTH_COOKIE_NAME} não foi definido`)
  assert(cookieJar.get(CSRF_COOKIE_NAME), `PASSO 1 falhou: cookie ${CSRF_COOKIE_NAME} não foi definido`)

  const setupResponse = (await callJson(baseUrl, '/api/v1/auth/mfa/setup', cookieJar, {
    method: 'POST',
  })) as MfaSetupResponse

  assert(setupResponse.otpauth_url, 'PASSO 2 falhou: otpauth_url ausente')
  assert(setupResponse.qr_code_base64_png, 'PASSO 2 falhou: qr_code_base64_png ausente')
  console.log('PASSO 2: mfa setup -> ok')

  const totpCode = generateTotpFromOtpauth(setupResponse.otpauth_url)

  const confirmResponse = (await callJson(baseUrl, '/api/v1/auth/mfa/confirm', cookieJar, {
    method: 'POST',
    body: { totp_code: totpCode },
  })) as MfaConfirmResponse

  assert(Array.isArray(confirmResponse.recovery_codes), 'PASSO 3 falhou: recovery_codes ausente')
  assert(confirmResponse.recovery_codes.length > 0, 'PASSO 3 falhou: recovery_codes vazio')
  console.log('PASSO 3: mfa confirm -> ok')

  const meResponse = (await callJson(baseUrl, '/api/v1/auth/me', cookieJar, {
    method: 'GET',
  })) as MeResponse

  assert(Boolean(meResponse.id), 'PASSO 4 falhou: /me sem id')
  assert(Boolean(meResponse.username), 'PASSO 4 falhou: /me sem username')
  console.log('PASSO 4: /me -> ok')
}

function isLoginChallenge(response: LoginResponse): response is LoginChallengeResponse {
  return 'mfa_required' in response
}

async function callJson(baseUrl: string, path: string, cookieJar: CookieJar, options: CallOptions): Promise<unknown> {
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

  const shouldAttachCsrf = options.attachCsrf ?? options.method === 'POST'
  if (shouldAttachCsrf) {
    const csrfToken = cookieJar.get(CSRF_COOKIE_NAME)
    if (csrfToken) {
      headers[CSRF_HEADER_NAME] = csrfToken
    }
  }

  let response: Response
  try {
    response = await fetch(`${baseUrl}${path}`, {
      method: options.method,
      headers,
      body: options.body ? JSON.stringify(options.body) : undefined,
    })
  } catch (error) {
    throw new Error(`${options.method} ${path} -> request failed: ${extractMessage(error)}`)
  }

  cookieJar.captureFromResponse(response)

  const rawText = await response.text()
  const payload = tryParseJson(rawText)

  if (!response.ok) {
    const message =
      typeof payload === 'object' && payload
        ? String((payload as Record<string, unknown>).detail ?? (payload as Record<string, unknown>).message ?? rawText)
        : rawText || response.statusText

    throw new Error(`${options.method} ${path} -> ${response.status}: ${message}`)
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

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) {
    throw new Error(message)
  }
}

function generateTotpFromOtpauth(otpauthUrl: string): string {
  let secret = ''
  try {
    const parsed = new URL(otpauthUrl)
    secret = parsed.searchParams.get('secret') ?? ''
  } catch {
    throw new Error('PASSO 3 falhou: otpauth_url inválida para gerar TOTP')
  }

  if (!secret) {
    throw new Error('PASSO 3 falhou: secret ausente na otpauth_url')
  }

  return generateTotp(secret)
}

function generateTotp(base32Secret: string, digits = 6, stepSeconds = 30): string {
  const key = decodeBase32(base32Secret)
  const counter = Math.floor(Date.now() / 1000 / stepSeconds)

  const counterBuffer = Buffer.alloc(8)
  counterBuffer.writeBigUInt64BE(BigInt(counter))

  const hash = createHmac('sha1', key).update(counterBuffer).digest()
  const offset = hash[hash.length - 1] & 0x0f

  const binary =
    ((hash[offset] & 0x7f) << 24) |
    ((hash[offset + 1] & 0xff) << 16) |
    ((hash[offset + 2] & 0xff) << 8) |
    (hash[offset + 3] & 0xff)

  const otp = binary % 10 ** digits
  return otp.toString().padStart(digits, '0')
}

function decodeBase32(input: string): Buffer {
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567'
  const normalized = input.toUpperCase().replace(/=+$/g, '').replace(/[^A-Z2-7]/g, '')

  let bits = ''
  for (const char of normalized) {
    const value = alphabet.indexOf(char)
    if (value < 0) {
      throw new Error('Base32 inválido em secret TOTP')
    }
    bits += value.toString(2).padStart(5, '0')
  }

  const bytes: number[] = []
  for (let i = 0; i + 8 <= bits.length; i += 8) {
    bytes.push(Number.parseInt(bits.slice(i, i + 8), 2))
  }

  return Buffer.from(bytes)
}

main().catch((error) => {
  const message = extractMessage(error)
  console.error(`[auth_flow_check] ${message}`)
  process.exit(1)
})

function extractMessage(error: unknown): string {
  if (error instanceof Error) {
    const cause = (error as Error & { cause?: unknown }).cause
    if (cause instanceof Error) {
      return `${error.message} (${cause.message})`
    }
    return error.message
  }
  return String(error)
}
