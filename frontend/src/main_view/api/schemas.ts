import type { SourceId } from '@/main_view/state/types'

export type CsvTabPayload = {
  header_row: number
  delimiter?: string | null
  encoding?: string | null
  sic_column: string
  selected_columns: string[]
  alias_map?: Record<string, string>
  normalize_key_strategy?: string
}

export type CsvTabProfileSchema = {
  id: string
  tenant_id: string
  source: SourceId
  scope: string
  owner_user_id: string | null
  name: string
  active_version: number
  is_default_for_source: boolean
  payload?: CsvTabPayload | null
}

export type PreviewRawRequest = {
  source: SourceId
  data_dir?: string
  upload_session_id?: string
  header_row_override?: number
}

export type PreviewRawResponse = {
  status: 'ok' | 'error'
  source: SourceId
  exists: boolean
  detected_encoding: string
  detected_delimiter: string
  header_row_index: number
  detected_headers: string[]
  original_headers: string[]
  rows_total_read: number
  sample_rows: Array<Record<string, string | number | boolean | null>>
  warnings: string[]
  error?: string | null
  elapsed_ms: number
}

export type PreviewParsedRequest = {
  source: SourceId
  profile_id: string
  data_dir?: string
  upload_session_id?: string
}

export type PreviewParsedResponse = {
  status: 'ok' | 'error'
  source: SourceId
  config_applied: CsvTabPayload
  sample_rows: Array<Record<string, string | number | boolean | null>>
  warnings: string[]
  error?: string | null
  elapsed_ms: number
}

export type CreateProfileRequest = {
  source: SourceId
  scope?: 'PRIVATE' | 'TEAM' | 'TENANT' | 'GLOBAL'
  name: string
  payload: CsvTabPayload
  is_default_for_source?: boolean
}

export type UpdateProfileRequest = {
  payload: CsvTabPayload
  change_note?: string
}

export type StatusMessageResponse = {
  status: string
  message: string
}

export type DatasetsPreviewRequest = {
  data_dir?: string
  upload_session_id?: string
  profile_ids: Record<SourceId, string> | Partial<Record<SourceId, string>>
}

export type DatasetsPreviewResponse = {
  status: 'ok' | 'error'
  layouts: Array<{
    source: string
    filename?: string | null
    exists: boolean
    detected_encoding?: string
    detected_delimiter?: string
    header_row_index?: number
    headers?: string[]
    rows?: number
    checksum_sha256?: string | null
    missing_required?: string[]
    missing_optional?: string[]
    warnings?: string[]
  }>
  source_samples?: Record<string, Array<Record<string, unknown>>>
  source_metrics?: Array<Record<string, unknown>>
  summary?: {
    row_count?: number
    warning_count?: number
  }
  warnings?: string[]
}

export type DatasetsIngestRequest = {
  source?: 'path'
  data_dir?: string
  upload_session_id?: string
  profile_ids: Record<SourceId, string> | Partial<Record<SourceId, string>>
}

export type DatasetsIngestResponse = {
  status: 'success' | 'error'
  dataset_version_id: string
  total_records: number
  metrics: Record<string, unknown>
  file_checksums: Record<string, string>
  warnings: string[]
}

export type MaterializeMachinesResponse = {
  artifact_id: string
  tenant_id: string
  dataset_version_id: string
  artifact_name: string
  path: string
  checksum: string | null
  row_count: number
}

export type MachineItem = {
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

export type PaginationMeta = {
  total: number
  page: number
  size: number
  has_next: boolean
  has_previous: boolean
}

export type PaginatedPayload<T> = {
  items: T[]
  meta: PaginationMeta
}

export type FilterDefinition = {
  key: string
  label: string
  severity: string
  description: string
  is_flag: boolean
}

export type MachineSummary = {
  total: number
  by_status: Record<string, number>
  by_flag: Record<string, number>
}

export type MachineTableQuery = {
  dataset_version_id: string
  page: number
  size: number
  search?: string
  pa_code?: string
  statuses?: string[]
  flags?: string[]
}

export type ReportRunRequest = {
  template_name: string
  limit?: number
}

export type ReportRunResponse = {
  template_name: string
  query: string
  row_count: number
  data: Array<Record<string, unknown>>
}
