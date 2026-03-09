export type SourceId = 'AD' | 'UEM' | 'EDR' | 'ASSET'

export type MainViewMode = 'home-empty' | 'home-filled' | 'viewer' | 'materialized'

export type ProfileStatus = 'pendente' | 'pronto'

export type SourceWorkflowStatus = 'not_configured' | 'configured' | 'default' | 'ready'

export type SourceItem = {
  id: SourceId
  name: string
  type: 'CSV'
  createdAt: string
}

export type SourceConfig = {
  status: ProfileStatus
  headerRow: number
  sicColumn: string
  selectedCols: string[]
}

export type SourceCell = string | number | boolean | null

export type SourceRecord = Record<string, SourceCell>

export type SourceMockData = Record<SourceId, SourceRecord[]>

export type CsvProfilePayloadState = {
  header_row: number
  sic_column: string
  selected_columns: string[]
}

export type RawPreviewState = {
  headers: string[]
  sample_rows: SourceRecord[]
  warnings: string[]
}

export type ParsedPreviewState = {
  sample_rows: SourceRecord[]
  warnings: string[]
}

export type SourceRuntimeState = {
  profile_id: string | null
  payload: CsvProfilePayloadState
  status: SourceWorkflowStatus
  is_default_for_source: boolean
  raw_preview: RawPreviewState | null
  parsed_preview: ParsedPreviewState | null
  is_loading_raw: boolean
  is_saving_profile: boolean
}

export type SourceRuntimeMap = Record<SourceId, SourceRuntimeState>

export type ExcelFiltersState = {
  AD: Record<string, string[]>
  UEM: Record<string, string[]>
  EDR: Record<string, string[]>
  ASSET: Record<string, string[]>
  MATERIALIZED: Record<string, string[]>
}

export type PipelineStepStatus = 'idle' | 'running' | 'success' | 'error'

export type PipelineState = {
  dataset_version_id: string | null
  ingest_status: PipelineStepStatus
  materialize_status: PipelineStepStatus
  ingest_message: string | null
  materialize_message: string | null
  ingest_total_records: number | null
  materialize_row_count: number | null
  materialize_checksum: string | null
}

export type MachineTableRow = {
  id: string
  hostname: string
  pa_code: string
  primary_status: string
  primary_status_label: string
  flags: string[]
  has_ad: boolean
  has_uem: boolean
  has_edr: boolean
  has_asset: boolean
  model?: string | null
  ip?: string | null
  tags?: string | null
  selected_data?: Record<string, unknown> | null
  main_user?: string | null
  ad_os?: string | null
  us_ad?: string | null
  us_uem?: string | null
  us_edr?: string | null
  uem_extra_user_logado?: string | null
  edr_os?: string | null
  status_check_win11?: string | null
  uem_serial?: string | null
  edr_serial?: string | null
  chassis?: string | null
}

export type MachineSummaryState = {
  total: number
  by_status: Record<string, number>
  by_flag: Record<string, number>
}

export type MachineFilterDefinitionState = {
  key: string
  label: string
  severity: string
  description: string
  is_flag: boolean
}

export type MachinesGridState = {
  rows: MachineTableRow[]
  page: number
  size: number
  total: number
  has_next: boolean
  is_loading_initial: boolean
  is_loading_more: boolean
  summary: MachineSummaryState | null
  filter_definitions: MachineFilterDefinitionState[]
  selected_statuses: string[]
  selected_flags: string[]
  search_input: string
  search_query: string
  is_exporting: boolean
}
