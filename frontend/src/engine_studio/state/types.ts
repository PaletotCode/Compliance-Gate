import type {
  ClassificationDivergenceRecord,
  ClassificationModeState,
  ClassificationRunMetricRecord,
  ClassificationRuntimeMode,
  DryRunResult,
  EngineCatalogSnapshot,
  EngineErrorPayload,
  ExplainRowResult,
  ExplainSampleResult,
  RuleSetDetailRecord,
  RuleSetPayloadV2,
  RuleSetRecord,
  RuleSetValidationPayloadResult,
  RuleSetVersionRecord,
  SegmentPreviewResult,
  SegmentRecord,
  SegmentTemplate,
  TransformationRecord,
  ViewPreviewResult,
  ViewRecord,
} from '@/engine_studio/types'

export type EngineStudioPanelKey =
  | 'catalog'
  | 'transformations'
  | 'segments'
  | 'views'
  | 'rulesets'
  | 'diagnostics'

export type EngineStudioTableState = {
  items: Array<Record<string, unknown>>
  columns: string[]
  total_rows: number
  page: number
  size: number
  has_next: boolean
  has_previous: boolean
  is_loading_initial: boolean
  is_loading_more: boolean
  warnings: string[]
}

export type EngineStudioState = {
  is_open: boolean
  active_panel: EngineStudioPanelKey
  dataset_version_id: string | null
  is_bootstrapping: boolean
  catalog: EngineCatalogSnapshot | null
  transformations: TransformationRecord[]
  segments: SegmentRecord[]
  segment_templates: SegmentTemplate[]
  views: ViewRecord[]
  rulesets: RuleSetRecord[]
  selected_view_id: string | null
  selected_ruleset_id: string | null
  selected_ruleset_detail: RuleSetDetailRecord | null
  table: EngineStudioTableState
  segment_preview: SegmentPreviewResult | null
  view_preview: ViewPreviewResult | null
  mode_state: ClassificationModeState | null
  divergences: ClassificationDivergenceRecord[]
  run_metrics: ClassificationRunMetricRecord[]
  last_validation_payload: RuleSetValidationPayloadResult | null
  last_explain_row: ExplainRowResult | null
  last_explain_sample: ExplainSampleResult | null
  last_dry_run: DryRunResult | null
  highlighted_node_path: string | null
  last_error: EngineErrorPayload | null
}

export type EngineStudioActions = {
  setOpen: (isOpen: boolean) => void
  toggleOpen: () => void
  setActivePanel: (panel: EngineStudioPanelKey) => void
  setHighlightedNodePath: (nodePath: string | null) => void
  clearError: () => void
  bootstrap: (datasetVersionId: string) => Promise<void>
  refreshCatalog: (datasetVersionId?: string) => Promise<void>
  refreshTransformations: () => Promise<void>
  createTransformation: (input: {
    name: string
    description?: string | null
    output_column_name: string
    expression: Record<string, unknown>
    output_type: 'string' | 'int' | 'bool' | 'date'
  }) => Promise<void>
  updateTransformation: (
    transformationId: string,
    input: {
      name?: string
      description?: string | null
      output_column_name?: string
      expression?: Record<string, unknown>
      output_type?: 'string' | 'int' | 'bool' | 'date'
    },
  ) => Promise<void>
  refreshSegments: () => Promise<void>
  createSegment: (input: {
    name: string
    description?: string | null
    filter_expression: Record<string, unknown>
  }) => Promise<void>
  createSegmentFromTemplate: (input: {
    template_key: string
    name: string
    description?: string | null
  }) => Promise<void>
  updateSegment: (
    segmentId: string,
    input: {
      name?: string
      description?: string | null
      filter_expression?: Record<string, unknown>
    },
  ) => Promise<void>
  previewSegment: (input: {
    segment_id?: string
    expression?: Record<string, unknown>
    limit?: number
  }) => Promise<void>
  refreshViews: () => Promise<void>
  createView: (input: {
    name: string
    description?: string | null
    payload: Record<string, unknown>
  }) => Promise<void>
  updateView: (
    viewId: string,
    input: {
      name?: string
      description?: string | null
      payload?: Record<string, unknown>
    },
  ) => Promise<void>
  previewView: (input: {
    view_id?: string
    inline_view_payload?: Record<string, unknown>
    limit?: number
  }) => Promise<void>
  selectView: (viewId: string | null) => Promise<void>
  reloadTable: () => Promise<void>
  fetchNextTablePage: () => Promise<void>
  refreshRuleSets: () => Promise<void>
  createRuleSet: (input: {
    name: string
    description?: string | null
    payload: RuleSetPayloadV2
  }) => Promise<void>
  updateRuleSet: (
    rulesetId: string,
    input: {
      name?: string
      description?: string | null
    },
  ) => Promise<void>
  archiveRuleSet: (rulesetId: string) => Promise<void>
  loadRuleSetDetail: (rulesetId: string | null) => Promise<void>
  createRuleSetVersion: (input: {
    ruleset_id: string
    source_version?: number
    payload?: RuleSetPayloadV2
  }) => Promise<void>
  updateRuleSetVersion: (input: {
    ruleset_id: string
    version: number
    payload: RuleSetPayloadV2
  }) => Promise<void>
  validateRuleSetVersion: (input: {
    ruleset_id: string
    version: number
    column_types: Record<string, string>
  }) => Promise<RuleSetVersionRecord | null>
  publishRuleSetVersion: (input: { ruleset_id: string; version: number }) => Promise<void>
  rollbackRuleSet: (input: { ruleset_id: string; target_version?: number }) => Promise<void>
  validateRuleSetPayload: (input: {
    payload: RuleSetPayloadV2
    column_types?: Record<string, string>
  }) => Promise<RuleSetValidationPayloadResult | null>
  explainRuleSetRow: (input: {
    payload: RuleSetPayloadV2
    row: Record<string, unknown>
    ruleset_name?: string
    version?: number
  }) => Promise<ExplainRowResult | null>
  explainRuleSetSample: (input: {
    payload: RuleSetPayloadV2
    rows: Array<Record<string, unknown>>
    limit?: number
    ruleset_name?: string
    version?: number
  }) => Promise<ExplainSampleResult | null>
  dryRunRuleSet: (input: {
    payload: RuleSetPayloadV2
    rows: Array<Record<string, unknown>>
    mode: ClassificationRuntimeMode
    explain_sample_limit?: number
    ruleset_name?: string
    version?: number
  }) => Promise<DryRunResult | null>
  refreshDiagnostics: () => Promise<void>
  setRuntimeMode: (input: {
    mode: ClassificationRuntimeMode
    ruleset_name?: string | null
  }) => Promise<void>
}

export type EngineStudioStore = EngineStudioState & EngineStudioActions
