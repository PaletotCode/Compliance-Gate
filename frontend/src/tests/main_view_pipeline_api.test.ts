import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import MockAdapter from 'axios-mock-adapter'
import { api } from '@/api/client'
import {
  fetchMachinesFilters,
  fetchMachinesSummary,
  fetchMachinesTable,
  ingestDatasetMachines,
  materializeMachines,
  previewDatasetMachines,
  runMachinesReport,
} from '@/main_view/api/pipelineApi'

let mock: MockAdapter

beforeEach(() => {
  mock = new MockAdapter(api)
  document.cookie = 'cg_csrf=test-csrf-token'
})

afterEach(() => {
  mock.restore()
  vi.restoreAllMocks()
  document.cookie = 'cg_csrf=; Max-Age=0; path=/'
})

describe('main_view pipelineApi', () => {
  it('executes preview -> ingest -> materialize contracts', async () => {
    mock.onPost('/api/v1/datasets/machines/preview').reply(200, {
      status: 'ok',
      layouts: [],
      summary: { row_count: 15, warning_count: 0 },
    })

    const preview = await previewDatasetMachines({ profile_ids: { AD: 'p-ad' } })
    expect(preview.status).toBe('ok')

    mock.onPost('/api/v1/datasets/machines/ingest').reply(200, {
      status: 'success',
      dataset_version_id: 'dataset-1',
      total_records: 150,
      metrics: {},
      file_checksums: { AD: 'abc' },
      warnings: [],
    })

    const ingest = await ingestDatasetMachines({ profile_ids: { AD: 'p-ad' } })
    expect(ingest.dataset_version_id).toBe('dataset-1')

    mock
      .onPost('/api/v1/engine/materialize/machines')
      .reply(200, { data: { row_count: 150, checksum: 'ck1', dataset_version_id: 'dataset-1' } })

    const materialize = await materializeMachines('dataset-1')
    expect(materialize.row_count).toBe(150)
    expect(materialize.checksum).toBe('ck1')
  })

  it('loads table with fallback to /machines/table and fetches summary/filters/report', async () => {
    mock.onGet('/api/v1/engine/tables/machines').reply(404, { detail: 'not found' })

    mock.onGet('/api/v1/machines/table').reply(200, {
      data: {
        items: [
          {
            id: 'h1',
            hostname: 'HOST-01',
            pa_code: 'PA01',
            primary_status: 'COMPLIANT',
            primary_status_label: 'Compliant',
            flags: [],
            has_ad: true,
            has_uem: true,
            has_edr: true,
            has_asset: true,
          },
        ],
        meta: {
          total: 120,
          page: 1,
          size: 100,
          has_next: true,
          has_previous: false,
        },
      },
    })

    const table = await fetchMachinesTable({
      dataset_version_id: 'dataset-1',
      page: 1,
      size: 100,
    })

    expect(table.items[0]?.hostname).toBe('HOST-01')
    expect(table.meta.has_next).toBe(true)

    mock.onGet('/api/v1/machines/summary').reply(200, {
      data: {
        total: 120,
        by_status: { COMPLIANT: 100 },
        by_flag: { OFFLINE: 20 },
      },
    })

    const summary = await fetchMachinesSummary({ dataset_version_id: 'dataset-1' })
    expect(summary.total).toBe(120)

    mock.onGet('/api/v1/machines/filters').reply(200, {
      data: [
        { key: 'COMPLIANT', label: 'Compliant', severity: 'low', description: 'ok', is_flag: false },
      ],
    })

    const filters = await fetchMachinesFilters()
    expect(filters).toHaveLength(1)

    mock.onPost('/api/v1/engine/reports/run').reply(200, {
      data: {
        template_name: 'machines_status_summary',
        query: 'select 1',
        row_count: 1,
        data: [{ status: 'COMPLIANT', total: 100 }],
      },
    })

    const report = await runMachinesReport('dataset-1', {
      template_name: 'machines_status_summary',
      limit: 100,
    })

    expect(report.row_count).toBe(1)
  })
})
