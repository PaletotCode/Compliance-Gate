import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import MockAdapter from 'axios-mock-adapter'
import { api } from '@/api/client'
import {
  listViews,
  runView,
  validateRuleSetPayload,
} from '@/engine_studio/api'

let mock: MockAdapter

beforeEach(() => {
  mock = new MockAdapter(api)
  document.cookie = 'cg_csrf=test-csrf-token'
})

afterEach(() => {
  mock.restore()
  document.cookie = 'cg_csrf=; Max-Age=0; path=/'
})

describe('engine_studio api contracts', () => {
  it('loads views and runs declarative view endpoint', async () => {
    mock.onGet('/api/v1/engine/views').reply(200, {
      data: [
        {
          id: 'view-1',
          tenant_id: 'default',
          name: 'Main View',
          description: null,
          created_by: 'u-1',
          created_at: '2026-03-10T00:00:00Z',
          active_version: 1,
          payload: {
            schema_version: 1,
            dataset_scope: { mode: 'dataset_version', dataset_version_id: 'dataset-1' },
            columns: [{ kind: 'base', column_name: 'hostname' }],
            filters: { segment_ids: [], ad_hoc_expression: null },
            sort: { column_name: 'hostname', direction: 'asc' },
            row_limit: 1000,
          },
        },
      ],
    })

    const views = await listViews()
    expect(views).toHaveLength(1)
    expect(views[0]?.name).toBe('Main View')

    mock.onPost('/api/v1/engine/views/run').reply(200, {
      data: {
        total_rows: 2,
        page: 1,
        size: 120,
        has_next: false,
        has_previous: false,
        columns: ['hostname', 'primary_status'],
        items: [
          { hostname: 'HOST-01', primary_status: 'COMPLIANT' },
          { hostname: 'HOST-02', primary_status: 'ROGUE' },
        ],
        warnings: [],
      },
    })

    const result = await runView({
      dataset_version_id: 'dataset-1',
      view_id: 'view-1',
      page: 1,
      size: 120,
    })
    expect(result.total_rows).toBe(2)
    expect(result.items[0]?.hostname).toBe('HOST-01')
  })

  it('calls validate-ruleset and maps staged payload response', async () => {
    mock.onPost('/api/v1/engine/validate-ruleset').reply(200, {
      data: {
        is_valid: false,
        stages: [
          { stage: 'syntax', ok: true, issues: [], warnings: [] },
          { stage: 'semantics', ok: false, issues: [], warnings: [] },
          { stage: 'viability', ok: false, issues: [], warnings: [] },
        ],
        issues: [
          {
            code: 'UnknownColumn',
            message: 'Coluna inválida.',
            details: { column: 'hostnme', suggestions: ['hostname'] },
            hint: 'Use hostname.',
            node_path: 'root.left',
            stage: 'semantics',
            severity: 'error',
          },
        ],
        warnings: [],
        summary: { error_count: 1, warning_count: 0 },
      },
    })

    const validation = await validateRuleSetPayload({
      payload: {
        schema_version: 2,
        blocks: [],
      },
    })

    expect(validation.is_valid).toBe(false)
    expect(validation.issues[0]?.code).toBe('UnknownColumn')
    expect(validation.issues[0]?.node_path).toBe('root.left')
  })
})
