/* eslint-disable no-console */
import { spawnSync } from 'node:child_process'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { expect, test } from '@playwright/test'

const API_BASE_URL = process.env.E2E_API_BASE_URL ?? 'http://localhost:8000'
const E2E_USERNAME = process.env.E2E_USERNAME ?? process.env.AUTH_BOOTSTRAP_ADMIN_USERNAME ?? 'admin'
const E2E_PASSWORD = process.env.E2E_PASSWORD ?? process.env.AUTH_BOOTSTRAP_ADMIN_PASSWORD ?? 'Admin1234'
const CSRF_COOKIE_NAME = process.env.CSRF_COOKIE_NAME ?? 'cg_csrf'
const CSRF_HEADER_NAME = process.env.CSRF_HEADER_NAME ?? 'X-CSRF-Token'

const SOURCES = ['AD', 'UEM', 'EDR', 'ASSET']

test.beforeAll(async ({ request }) => {
  if (process.env.E2E_START_DOCKER === '1') {
    const __filename = fileURLToPath(import.meta.url)
    const __dirname = path.dirname(__filename)
    const repoRoot = path.resolve(__dirname, '..', '..')
    console.log('PASSO E2E-0: docker compose up -d')
    const result = spawnSync('docker', ['compose', 'up', '-d'], {
      cwd: repoRoot,
      stdio: 'inherit',
    })
    if (result.status !== 0) {
      throw new Error(`docker compose up failed with code ${result.status ?? 1}`)
    }
  }

  const startedAt = Date.now()
  while (Date.now() - startedAt < 60_000) {
    const response = await request.get(`${API_BASE_URL}/health`)
    if (response.ok()) {
      return
    }
    await new Promise((resolve) => setTimeout(resolve, 1000))
  }

  throw new Error('Backend not ready for E2E (/health timeout)')
})

test('Main View TI full flow with virtualized table', async ({ page }) => {
  console.log('PASSO 1: login via API no contexto do browser (cookie-only)')
  await page.goto('/auth')
  const loginResult = await loginInBrowserContext(page)

  expect(loginResult.ok).toBeTruthy()
  if (isMfaChallenge(loginResult.payload)) {
    throw new Error('E2E login requires MFA. Use a non-MFA bootstrap user for this test.')
  }

  const csrfToken = await readCsrfToken(page)
  expect(csrfToken).toBeTruthy()

  console.log('PASSO 2: garantir profiles para AD/UEM/EDR/ASSET')
  const profileIds = await ensureProfiles(page, csrfToken)

  console.log('PASSO 3: abrir /app com guard que hidrata sessão por cookie')
  await page.goto('/app')
  await expect(page).toHaveURL(/\/app/)

  console.log('PASSO 4: importar fontes e preparar estado pronto')
  await page.getByTestId('import-csv-bases').click()

  for (const sourceLabel of [
    'Active Directory',
    'Workspace ONE (UEM)',
    'CrowdStrike (EDR)',
    'GLPI (Ativos)',
  ]) {
    await page.getByText(sourceLabel, { exact: true }).first().click()
    await page.getByRole('button', { name: 'Início' }).click()
  }

  const pipelineButton = page.getByRole('button', { name: /EXECUTAR PIPELINE/i })
  await expect(pipelineButton).toBeEnabled()

  console.log('PASSO 5: executar ingest + materialize pela UI')
  await pipelineButton.click()
  await expect(page.getByTestId('machines-virtual-grid')).toBeVisible({ timeout: 180_000 })

  await expect
    .poll(async () => readLoadedRows(page), { timeout: 180_000 })
    .toBeGreaterThan(0)
  const initialLoaded = await readLoadedRows(page)
  expect(initialLoaded).toBeGreaterThan(0)

  console.log(`PASSO 6: virtual grid carregou ${initialLoaded} linhas`)
  const gridScroll = page.getByTestId('machines-grid-scroll')
  await gridScroll.evaluate((el) => {
    el.scrollTop = el.scrollHeight
  })

  await page.waitForTimeout(1500)
  const afterScroll = await readLoadedRows(page)
  expect(afterScroll).toBeGreaterThanOrEqual(initialLoaded)
  console.log(`PASSO 7: scroll infinito ativo (${afterScroll} linhas carregadas)`)

  const firstStatusFilter = page.locator('[data-testid^="status-filter-"]').first()
  if ((await firstStatusFilter.count()) > 0) {
    await firstStatusFilter.click()
    await page.waitForTimeout(1500)
    const afterFilter = await readLoadedRows(page)
    expect(afterFilter).toBeLessThanOrEqual(afterScroll)
    console.log(`PASSO 8: filtro alterou resultados (${afterFilter} linhas carregadas)`)
  }

  console.log(`PASSO 9: profiles usados ${JSON.stringify(profileIds)}`)
  console.log('PASSO 10: E2E TI v1 concluído')
})

async function ensureProfiles(page: import('@playwright/test').Page, csrfToken: string) {
  const profileIds: Record<string, string> = {}

  for (const source of SOURCES) {
    const rawPreviewResponse = await page.context().request.post(`${API_BASE_URL}/api/v1/csv-tabs/preview/raw`, {
      data: { source },
      headers: {
        [CSRF_HEADER_NAME]: csrfToken,
      },
    })

    expect(rawPreviewResponse.ok()).toBeTruthy()
    const rawPreview = (await rawPreviewResponse.json()) as {
      status: 'ok' | 'error'
      detected_headers: string[]
    }

    expect(rawPreview.status).toBe('ok')
    expect(rawPreview.detected_headers.length).toBeGreaterThan(0)

    const createProfileResponse = await page.context().request.post(`${API_BASE_URL}/api/v1/csv-tabs/profiles`, {
      headers: {
        [CSRF_HEADER_NAME]: csrfToken,
      },
      data: {
        source,
        scope: 'PRIVATE',
        name: `E2E ${source} ${Date.now()}`,
        payload: {
          header_row: 0,
          sic_column: rawPreview.detected_headers[0],
          selected_columns: rawPreview.detected_headers.slice(0, Math.min(5, rawPreview.detected_headers.length)),
        },
      },
    })

    expect(createProfileResponse.ok()).toBeTruthy()
    const createdProfile = (await createProfileResponse.json()) as { id: string }
    profileIds[source] = createdProfile.id

    const promoteResponse = await page
      .context()
      .request.post(`${API_BASE_URL}/api/v1/csv-tabs/profiles/${createdProfile.id}/promote-default`, {
        headers: {
          [CSRF_HEADER_NAME]: csrfToken,
        },
      })

    expect(promoteResponse.ok()).toBeTruthy()
  }

  return profileIds
}

async function readCsrfToken(page: import('@playwright/test').Page): Promise<string> {
  const cookies = await page.context().cookies(API_BASE_URL)
  const csrf = cookies.find((cookie) => cookie.name === CSRF_COOKIE_NAME)
  return csrf?.value ?? ''
}

async function readLoadedRows(page: import('@playwright/test').Page): Promise<number> {
  const text = (await page.getByTestId('machines-grid-counter').textContent()) ?? ''
  const match = text.match(/(\d+)\s*\//)
  return match ? Number.parseInt(match[1], 10) : 0
}

async function loginInBrowserContext(page: import('@playwright/test').Page): Promise<{
  ok: boolean
  status: number
  payload: unknown
}> {
  return page.evaluate(
    async ({ apiBaseUrl, username, password }) => {
      const response = await fetch(`${apiBaseUrl}/api/v1/auth/login`, {
        method: 'POST',
        credentials: 'include',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ username, password }),
      })

      let payload: unknown = null
      try {
        payload = await response.json()
      } catch {
        payload = null
      }

      return { ok: response.ok, status: response.status, payload }
    },
    {
      apiBaseUrl: API_BASE_URL,
      username: E2E_USERNAME,
      password: E2E_PASSWORD,
    },
  )
}

function isMfaChallenge(payload: unknown): payload is { mfa_required: boolean } {
  return Boolean(payload && typeof payload === 'object' && 'mfa_required' in payload)
}
