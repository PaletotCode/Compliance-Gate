import { beforeEach, describe, expect, it, vi } from 'vitest'

vi.mock('@/shared/notifications/notificationStore', () => ({
  pushNotification: vi.fn(),
}))

vi.mock('@/engine_studio/api', () => ({
  archiveRuleSet: vi.fn(),
  createRuleSet: vi.fn(),
  createRuleSetVersion: vi.fn(),
  createSegment: vi.fn(),
  createSegmentFromTemplate: vi.fn(),
  createTransformation: vi.fn(),
  createView: vi.fn(),
  dryRunRuleSet: vi.fn(),
  explainRuleSetRow: vi.fn(),
  explainRuleSetSample: vi.fn(),
  getClassificationMode: vi.fn(),
  getEngineCatalog: vi.fn(),
  getRuleSet: vi.fn(),
  listClassificationDivergences: vi.fn(),
  listClassificationMetrics: vi.fn(),
  listRuleSets: vi.fn(),
  listSegments: vi.fn(),
  listSegmentTemplates: vi.fn(),
  listTransformations: vi.fn(),
  listViews: vi.fn(),
  previewSegment: vi.fn(),
  previewView: vi.fn(),
  publishRuleSetVersion: vi.fn(),
  rollbackRuleSet: vi.fn(),
  runView: vi.fn(),
  setClassificationMode: vi.fn(),
  updateRuleSet: vi.fn(),
  updateRuleSetVersion: vi.fn(),
  updateSegment: vi.fn(),
  updateTransformation: vi.fn(),
  updateView: vi.fn(),
  validateRuleSetPayload: vi.fn(),
  validateRuleSetVersion: vi.fn(),
}))

import * as engineApi from '@/engine_studio/api'
import { engineStudioStore } from '@/engine_studio/state'

describe('engineStudioStore view normalization', () => {
  beforeEach(() => {
    vi.resetAllMocks()
    engineStudioStore.setState({
      is_open: true,
      active_panel: 'catalog',
      dataset_version_id: null,
      is_bootstrapping: false,
      catalog: null,
      transformations: [],
      segments: [],
      segment_templates: [],
      views: [],
      rulesets: [],
      selected_view_id: null,
      selected_ruleset_id: null,
      selected_ruleset_detail: null,
      table: {
        items: [],
        columns: [],
        total_rows: 0,
        page: 1,
        size: 120,
        has_next: false,
        has_previous: false,
        is_loading_initial: false,
        is_loading_more: false,
        warnings: [],
      },
      segment_preview: null,
      view_preview: null,
      mode_state: null,
      divergences: [],
      run_metrics: [],
      last_validation_payload: null,
      last_explain_row: null,
      last_explain_sample: null,
      last_dry_run: null,
      highlighted_node_path: null,
      last_error: null,
    })
  })

  it('normalizes stale views with invalid columns and oversized row_limit', async () => {
    const catalog = {
      tenant_id: 'default',
      dataset_version_id: 'dataset-1',
      row_count: 247,
      columns: [
        {
          name: 'hostname',
          data_type: 'string',
          sample_values: ['HOST-01'],
          null_rate: 0,
          approx_cardinality: 247,
        },
        {
          name: 'pa_code',
          data_type: 'string',
          sample_values: ['0001'],
          null_rate: 0,
          approx_cardinality: 247,
        },
      ],
    }

    const staleView = {
      id: 'view-1',
      tenant_id: 'default',
      name: 'Admin Studio Table',
      description: null,
      created_by: 'u-1',
      created_at: '2026-03-11T00:00:00Z',
      active_version: 1,
      payload: {
        schema_version: 1,
        dataset_scope: { mode: 'dataset_version', dataset_version_id: 'old-dataset' },
        columns: [{ kind: 'base', column_name: 'UEM.Last Seen' }],
        filters: { segment_ids: ['segment-missing'], ad_hoc_expression: null },
        sort: { column_name: 'UEM.Last Seen', direction: 'desc' },
        row_limit: 999_999,
      },
    }

    vi.mocked(engineApi.getEngineCatalog).mockResolvedValue(catalog as never)
    vi.mocked(engineApi.listTransformations).mockResolvedValue([])
    vi.mocked(engineApi.listSegmentTemplates).mockResolvedValue([])
    vi.mocked(engineApi.listSegments).mockResolvedValue([])
    vi.mocked(engineApi.listRuleSets).mockResolvedValue([])
    vi.mocked(engineApi.getClassificationMode).mockResolvedValue({
      mode: 'legacy',
      updated_at: null,
      updated_by: null,
    } as never)
    vi.mocked(engineApi.listClassificationDivergences).mockResolvedValue([])
    vi.mocked(engineApi.listClassificationMetrics).mockResolvedValue([])
    vi.mocked(engineApi.listViews).mockResolvedValue([staleView as never])
    vi.mocked(engineApi.updateView).mockImplementation(async (_viewId, input) => ({
      ...staleView,
      payload: input.payload ?? staleView.payload,
    }) as never)
    vi.mocked(engineApi.runView).mockResolvedValue({
      total_rows: 1,
      page: 1,
      size: 120,
      has_next: false,
      has_previous: false,
      columns: ['hostname', 'pa_code'],
      items: [{ hostname: 'HOST-01', pa_code: '0001' }],
      warnings: [],
    } as never)

    await engineStudioStore.getState().bootstrap('dataset-1')

    expect(engineApi.updateView).toHaveBeenCalledTimes(1)
    const normalizedPayload = vi.mocked(engineApi.updateView).mock.calls[0]?.[1]?.payload as {
      dataset_scope: { dataset_version_id: string }
      row_limit: number
      columns: Array<{ kind: string; column_name?: string }>
      filters: { segment_ids: string[] }
      sort: { column_name: string; direction: string } | null
    }

    expect(normalizedPayload.dataset_scope.dataset_version_id).toBe('dataset-1')
    expect(normalizedPayload.row_limit).toBe(10_000)
    expect(normalizedPayload.columns).toEqual([
      { kind: 'base', column_name: 'hostname' },
      { kind: 'base', column_name: 'pa_code' },
    ])
    expect(normalizedPayload.filters.segment_ids).toEqual([])
    expect(normalizedPayload.sort).toEqual({
      column_name: 'hostname',
      direction: 'asc',
    })

    expect(engineApi.runView).toHaveBeenCalledTimes(1)
    expect(engineStudioStore.getState().table.total_rows).toBe(1)
  })
})
