export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue }

export type JsonRecord = Record<string, JsonValue>

export type EngineErrorPayload = {
  code: string
  message: string
  details: Record<string, unknown>
  hint: string
  node_path: string | null
  suggestions: string[]
}

export type EngineCatalogColumn = {
  name: string
  data_type: string
  sample_values: unknown[]
  null_rate: number
  approx_cardinality: number
}

export type EngineCatalogSnapshot = {
  tenant_id: string
  dataset_version_id: string
  row_count: number
  columns: EngineCatalogColumn[]
}

export type TransformationOutputType = 'string' | 'int' | 'bool' | 'date'

export type TransformationPayload = {
  schema_version?: 1
  output_column_name: string
  expression: JsonRecord
  output_type: TransformationOutputType
}

export type TransformationRecord = {
  id: string
  tenant_id: string
  name: string
  description: string | null
  created_by: string | null
  created_at: string
  active_version: number
  payload: TransformationPayload
}

export type SegmentPayload = {
  schema_version?: 1
  filter_expression: JsonRecord
}

export type SegmentRecord = {
  id: string
  tenant_id: string
  name: string
  description: string | null
  created_by: string | null
  created_at: string
  active_version: number
  payload: SegmentPayload
}

export type SegmentTemplate = {
  key: string
  name: string
  description: string
}

export type SegmentPreviewResult = {
  total_rows: number
  matched_rows: number
  match_rate: number
  sample_rows: Array<Record<string, unknown>>
  warnings: string[]
}

export type ViewSortDirection = 'asc' | 'desc'

export type ViewDatasetScope = {
  mode: 'dataset_version'
  dataset_version_id: string
}

export type ViewBaseColumn = {
  kind: 'base'
  column_name: string
}

export type ViewDerivedColumn = {
  kind: 'derived'
  transformation_id: string
  alias?: string | null
}

export type ViewColumnSpec = ViewBaseColumn | ViewDerivedColumn

export type ViewFilterSpec = {
  segment_ids: string[]
  ad_hoc_expression?: JsonRecord | null
}

export type ViewSortSpec = {
  column_name: string
  direction: ViewSortDirection
}

export type ViewPayload = {
  schema_version?: 1
  dataset_scope: ViewDatasetScope
  columns: ViewColumnSpec[]
  filters?: ViewFilterSpec
  sort?: ViewSortSpec | null
  row_limit?: number
}

export type ViewRecord = {
  id: string
  tenant_id: string
  name: string
  description: string | null
  created_by: string | null
  created_at: string
  active_version: number
  payload: ViewPayload
}

export type ViewPreviewResult = {
  total_rows: number
  returned_rows: number
  sample_rows: Array<Record<string, unknown>>
  warnings: string[]
}

export type ViewRunResult = {
  total_rows: number
  page: number
  size: number
  has_next: boolean
  has_previous: boolean
  columns: string[]
  items: Array<Record<string, unknown>>
  warnings: string[]
}

export type RuleBlockKind = 'special' | 'primary' | 'flags'
export type RuleSetVersionStatus = 'draft' | 'validated' | 'published' | 'archived'
export type ClassificationRuntimeMode = 'legacy' | 'shadow' | 'declarative'

export type RuleEntryPayload = {
  rule_key?: string | null
  description?: string | null
  priority: number
  condition: JsonRecord
  output: Record<string, unknown>
}

export type RuleBlockPayload = {
  kind: RuleBlockKind
  entries: RuleEntryPayload[]
}

export type RuleSetPayloadV2 = {
  schema_version?: 2
  blocks: RuleBlockPayload[]
}

export type RuleSetValidationIssue = {
  code: string
  message: string
  details: Record<string, unknown>
  hint?: string | null
  node_path?: string | null
}

export type RuleSetValidationResult = {
  is_valid: boolean
  issues: RuleSetValidationIssue[]
}

export type RuleSetVersionRecord = {
  version: number
  status: RuleSetVersionStatus
  created_at: string
  created_by: string | null
  validated_at: string | null
  validated_by: string | null
  published_at: string | null
  published_by: string | null
  payload: RuleSetPayloadV2
}

export type RuleSetRecord = {
  id: string
  tenant_id: string
  name: string
  description: string | null
  created_by: string | null
  created_at: string
  updated_at: string | null
  active_version: number
  active_status: RuleSetVersionStatus
  published_version: number | null
  is_archived: boolean
  active_payload: RuleSetPayloadV2
}

export type RuleSetDetailRecord = RuleSetRecord & {
  versions: RuleSetVersionRecord[]
}

export type RuleSetValidationPayloadIssue = {
  code: string
  message: string
  details: Record<string, unknown>
  hint?: string | null
  node_path?: string | null
  stage: string
  severity: 'error' | 'warning' | 'info'
}

export type RuleSetValidationPayloadStage = {
  stage: string
  ok: boolean
  issues: RuleSetValidationPayloadIssue[]
  warnings: RuleSetValidationPayloadIssue[]
}

export type RuleSetValidationPayloadResult = {
  is_valid: boolean
  stages: RuleSetValidationPayloadStage[]
  issues: RuleSetValidationPayloadIssue[]
  warnings: RuleSetValidationPayloadIssue[]
  summary: {
    error_count: number
    warning_count: number
  }
}

export type ExplainFailedCondition = {
  node_path: string
  message: string
  details: Record<string, unknown>
}

export type ExplainConditionTraceNode = {
  node_path: string
  node_type: string
  operator?: string
  value: unknown
}

export type ExplainRuleTrace = {
  rule_key: string
  block_kind: RuleBlockKind
  priority: number
  evaluation_order: number | null
  evaluated: boolean
  matched: boolean
  condition_result: boolean | null
  failed_conditions: ExplainFailedCondition[]
  condition_trace: ExplainConditionTraceNode[]
  skip_reason: string | null
  output_preview: Record<string, unknown>
  entry_index: number | null
}

export type ExplainRowResult = {
  machine_id: string
  hostname: string
  final_output: {
    primary_status: string
    primary_status_label: string
    flags: string[]
  }
  matched_rules: string[]
  evaluation_order: string[]
  rules: ExplainRuleTrace[]
  decision_reason: string
  special_evaluated: number
}

export type ExplainSampleResult = {
  total_rows: number
  explained_rows: number
  rows: ExplainRowResult[]
}

export type DryRunResult = {
  mode: ClassificationRuntimeMode
  rows_scanned: number
  rows_classified: number
  elapsed_ms: number
  rule_hits: Record<string, number>
  divergences: number
  status_counts: Record<string, number>
  flag_counts: Record<string, number>
  warnings: RuleSetValidationPayloadIssue[]
  sample_explain: ExplainSampleResult
}

export type ClassificationModeState = {
  mode: ClassificationRuntimeMode
  ruleset_name: string | null
  source: string
  updated_at: string | null
  updated_by: string | null
}

export type ClassificationDivergenceRecord = {
  id: string
  tenant_id: string
  dataset_version_id: string | null
  run_id: string | null
  ruleset_name: string | null
  machine_id: string | null
  hostname: string | null
  legacy_primary_status: string | null
  legacy_primary_status_label: string | null
  legacy_flags: string[]
  declarative_primary_status: string | null
  declarative_primary_status_label: string | null
  declarative_flags: string[]
  diff: Record<string, unknown>
  created_at: string
}

export type ClassificationRunMetricRecord = {
  run_id: string
  tenant_id: string
  dataset_version_id: string | null
  status: string
  started_at: string | null
  ended_at: string | null
  mode: ClassificationRuntimeMode | null
  ruleset_name: string | null
  rows_scanned: number | null
  rows_classified: number | null
  elapsed_ms: number | null
  rule_hits: Record<string, number>
  divergences: number | null
  error_truncated: string | null
}
