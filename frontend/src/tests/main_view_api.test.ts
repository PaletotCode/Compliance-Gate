import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import MockAdapter from 'axios-mock-adapter'
import { api } from '@/api/client'
import {
  createProfile,
  previewParsed,
  previewRaw,
  updateProfile,
} from '@/main_view/api/csvTabsApi'

let mock: MockAdapter

beforeEach(() => {
  mock = new MockAdapter(api)
  document.cookie = 'cg_csrf=test-csrf-token'
})

afterEach(() => {
  mock.restore()
  document.cookie = 'cg_csrf=; Max-Age=0; path=/'
})

describe('main_view csvTabsApi', () => {
  it('creates and updates profile', async () => {
    mock.onPost('/api/v1/csv-tabs/profiles').reply(201, {
      id: 'profile-ad-1',
      tenant_id: 'tenant-1',
      source: 'AD',
      scope: 'PRIVATE',
      owner_user_id: 'user-1',
      name: 'AD Perfil',
      active_version: 1,
      is_default_for_source: false,
      payload: {
        header_row: 0,
        sic_column: 'Computer Name',
        selected_columns: ['Computer Name', 'DNS Name'],
      },
    })

    const created = await createProfile({
      source: 'AD',
      name: 'AD Perfil',
      payload: {
        header_row: 0,
        sic_column: 'Computer Name',
        selected_columns: ['Computer Name', 'DNS Name'],
      },
    })

    expect(created.id).toBe('profile-ad-1')

    mock.onPut('/api/v1/csv-tabs/profiles/profile-ad-1').reply(200, {
      status: 'ok',
      message: 'Appended new version',
    })

    const updated = await updateProfile('profile-ad-1', {
      payload: {
        header_row: 0,
        sic_column: 'Computer Name',
        selected_columns: ['Computer Name', 'DNS Name', 'Operating System'],
      },
      change_note: 'ajuste colunas',
    })

    expect(updated.status).toBe('ok')
    expect(updated.message).toContain('Appended')
  })

  it('fetches raw and parsed previews', async () => {
    mock.onPost('/api/v1/csv-tabs/preview/raw').reply(200, {
      status: 'ok',
      source: 'AD',
      exists: true,
      detected_encoding: 'utf-8',
      detected_delimiter: ',',
      header_row_index: 0,
      detected_headers: ['Computer Name', 'DNS Name'],
      original_headers: ['Computer Name', 'DNS Name'],
      rows_total_read: 15,
      sample_rows: [{ 'Computer Name': 'BR-LT-001', 'DNS Name': 'br-lt-001.sicoob.local' }],
      warnings: [],
      elapsed_ms: 12.5,
    })

    const raw = await previewRaw({ source: 'AD', header_row_override: 0 })

    expect(raw.status).toBe('ok')
    expect(raw.detected_headers).toEqual(['Computer Name', 'DNS Name'])

    mock.onPost('/api/v1/csv-tabs/preview/parsed').reply(200, {
      status: 'ok',
      source: 'AD',
      config_applied: {
        header_row: 0,
        sic_column: 'Computer Name',
        selected_columns: ['Computer Name', 'DNS Name'],
      },
      sample_rows: [{ 'Computer Name': 'BR-LT-001', 'DNS Name': 'br-lt-001.sicoob.local' }],
      warnings: [],
      elapsed_ms: 9.4,
    })

    const parsed = await previewParsed({ source: 'AD', profile_id: 'profile-ad-1' })

    expect(parsed.status).toBe('ok')
    expect(parsed.sample_rows).toHaveLength(1)
  })
})
