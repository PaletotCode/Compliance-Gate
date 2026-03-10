import { beforeEach, describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { EngineStudioDock } from '@/engine_studio/components'
import { engineStudioStore } from '@/engine_studio/state'

describe('EngineStudioDock smoke', () => {
  beforeEach(() => {
    engineStudioStore.setState({
      is_open: true,
      active_panel: 'catalog',
      dataset_version_id: 'dataset-1',
      is_bootstrapping: false,
      catalog: {
        tenant_id: 'default',
        dataset_version_id: 'dataset-1',
        row_count: 2,
        columns: [
          {
            name: 'hostname',
            data_type: 'string',
            sample_values: ['HOST-01'],
            null_rate: 0,
            approx_cardinality: 2,
          },
        ],
      },
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

  it('renders dock shell and catalog panel', () => {
    render(<EngineStudioDock />)
    expect(screen.getByText(/Engine Studio/i)).toBeInTheDocument()
    expect(screen.getByText(/Catálogo Materializado/i)).toBeInTheDocument()
  })
})
