import { api } from '@/api/client'
import { unwrapApiEnvelope, type ApiEnvelope } from '@/api/envelope'
import type {
  ClassificationDivergenceRecord,
  ClassificationModeState,
  ClassificationRunMetricRecord,
  ClassificationRuntimeMode,
  DryRunResult,
  EngineCatalogSnapshot,
  ExplainRowResult,
  ExplainSampleResult,
  JsonRecord,
  RuleSetDetailRecord,
  RuleSetPayloadV2,
  RuleSetRecord,
  RuleSetValidationPayloadResult,
  RuleSetValidationResult,
  RuleSetVersionRecord,
  SegmentPreviewResult,
  SegmentRecord,
  SegmentTemplate,
  TransformationOutputType,
  TransformationRecord,
  ViewPayload,
  ViewPreviewResult,
  ViewRecord,
  ViewRunResult,
  ViewSortSpec,
} from '@/engine_studio/types'

export async function getEngineCatalog(
  datasetVersionId: string,
  sampleSize = 10,
): Promise<EngineCatalogSnapshot> {
  const { data } = await api.get<ApiEnvelope<EngineCatalogSnapshot>>('/api/v1/engine/catalog/machines', {
    params: {
      dataset_version_id: datasetVersionId,
      sample_size: sampleSize,
    },
  })
  return unwrapApiEnvelope(data)
}

export async function listTransformations(): Promise<TransformationRecord[]> {
  const { data } = await api.get<ApiEnvelope<TransformationRecord[]>>('/api/v1/engine/transformations')
  return unwrapApiEnvelope(data)
}

export async function createTransformation(input: {
  name: string
  description?: string | null
  output_column_name: string
  expression: JsonRecord
  output_type: TransformationOutputType
}): Promise<TransformationRecord> {
  const { data } = await api.post<ApiEnvelope<TransformationRecord>>('/api/v1/engine/transformations', input)
  return unwrapApiEnvelope(data)
}

export async function updateTransformation(
  transformationId: string,
  input: {
    name?: string
    description?: string | null
    output_column_name?: string
    expression?: JsonRecord
    output_type?: TransformationOutputType
  },
): Promise<TransformationRecord> {
  const { data } = await api.put<ApiEnvelope<TransformationRecord>>(
    `/api/v1/engine/transformations/${transformationId}`,
    input,
  )
  return unwrapApiEnvelope(data)
}

export async function listSegmentTemplates(): Promise<SegmentTemplate[]> {
  const { data } = await api.get<ApiEnvelope<SegmentTemplate[]>>('/api/v1/engine/segments/templates')
  return unwrapApiEnvelope(data)
}

export async function listSegments(): Promise<SegmentRecord[]> {
  const { data } = await api.get<ApiEnvelope<SegmentRecord[]>>('/api/v1/engine/segments')
  return unwrapApiEnvelope(data)
}

export async function createSegment(input: {
  name: string
  description?: string | null
  filter_expression: JsonRecord
}): Promise<SegmentRecord> {
  const { data } = await api.post<ApiEnvelope<SegmentRecord>>('/api/v1/engine/segments', input)
  return unwrapApiEnvelope(data)
}

export async function createSegmentFromTemplate(input: {
  template_key: string
  name: string
  description?: string | null
}): Promise<SegmentRecord> {
  const { data } = await api.post<ApiEnvelope<SegmentRecord>>(
    '/api/v1/engine/segments/from-template',
    input,
  )
  return unwrapApiEnvelope(data)
}

export async function updateSegment(
  segmentId: string,
  input: {
    name?: string
    description?: string | null
    filter_expression?: JsonRecord
  },
): Promise<SegmentRecord> {
  const { data } = await api.put<ApiEnvelope<SegmentRecord>>(`/api/v1/engine/segments/${segmentId}`, input)
  return unwrapApiEnvelope(data)
}

export async function previewSegment(input: {
  dataset_version_id: string
  segment_id?: string
  expression?: JsonRecord
  limit?: number
}): Promise<SegmentPreviewResult> {
  const { data } = await api.post<ApiEnvelope<SegmentPreviewResult>>(
    '/api/v1/engine/segments/preview',
    {
      segment_id: input.segment_id,
      expression: input.expression,
      limit: input.limit,
    },
    {
      params: {
        dataset_version_id: input.dataset_version_id,
      },
    },
  )
  return unwrapApiEnvelope(data)
}

export async function listViews(): Promise<ViewRecord[]> {
  const { data } = await api.get<ApiEnvelope<ViewRecord[]>>('/api/v1/engine/views')
  return unwrapApiEnvelope(data)
}

export async function createView(input: {
  name: string
  description?: string | null
  payload: ViewPayload
}): Promise<ViewRecord> {
  const { data } = await api.post<ApiEnvelope<ViewRecord>>('/api/v1/engine/views', input)
  return unwrapApiEnvelope(data)
}

export async function updateView(
  viewId: string,
  input: {
    name?: string
    description?: string | null
    payload?: ViewPayload
  },
): Promise<ViewRecord> {
  const { data } = await api.put<ApiEnvelope<ViewRecord>>(`/api/v1/engine/views/${viewId}`, input)
  return unwrapApiEnvelope(data)
}

export async function previewView(input: {
  dataset_version_id: string
  view_id?: string
  inline_view_payload?: ViewPayload
  limit?: number
}): Promise<ViewPreviewResult> {
  const { data } = await api.post<ApiEnvelope<ViewPreviewResult>>(
    '/api/v1/engine/views/preview',
    {
      view_id: input.view_id,
      inline_view_payload: input.inline_view_payload,
      limit: input.limit,
    },
    {
      params: {
        dataset_version_id: input.dataset_version_id,
      },
    },
  )
  return unwrapApiEnvelope(data)
}

export async function runView(input: {
  dataset_version_id: string
  view_id: string
  page?: number
  size?: number
  search?: string
  sort?: ViewSortSpec
}): Promise<ViewRunResult> {
  const { data } = await api.post<ApiEnvelope<ViewRunResult>>(
    '/api/v1/engine/views/run',
    {
      view_id: input.view_id,
      page: input.page ?? 1,
      size: input.size ?? 100,
      search: input.search,
      sort: input.sort,
    },
    {
      params: {
        dataset_version_id: input.dataset_version_id,
      },
    },
  )
  return unwrapApiEnvelope(data)
}

export async function listRuleSets(includeArchived = false): Promise<RuleSetRecord[]> {
  const { data } = await api.get<ApiEnvelope<RuleSetRecord[]>>('/api/v1/engine/rulesets', {
    params: { include_archived: includeArchived },
  })
  return unwrapApiEnvelope(data)
}

export async function getRuleSet(rulesetId: string): Promise<RuleSetDetailRecord> {
  const { data } = await api.get<ApiEnvelope<RuleSetDetailRecord>>(`/api/v1/engine/rulesets/${rulesetId}`)
  return unwrapApiEnvelope(data)
}

export async function createRuleSet(input: {
  name: string
  description?: string | null
  payload: RuleSetPayloadV2
}): Promise<RuleSetRecord> {
  const { data } = await api.post<ApiEnvelope<RuleSetRecord>>('/api/v1/engine/rulesets', input)
  return unwrapApiEnvelope(data)
}

export async function updateRuleSet(
  rulesetId: string,
  input: {
    name?: string
    description?: string | null
  },
): Promise<RuleSetRecord> {
  const { data } = await api.put<ApiEnvelope<RuleSetRecord>>(`/api/v1/engine/rulesets/${rulesetId}`, input)
  return unwrapApiEnvelope(data)
}

export async function archiveRuleSet(rulesetId: string): Promise<RuleSetRecord> {
  const { data } = await api.delete<ApiEnvelope<RuleSetRecord>>(`/api/v1/engine/rulesets/${rulesetId}`)
  return unwrapApiEnvelope(data)
}

export async function listRuleSetVersions(rulesetId: string): Promise<RuleSetVersionRecord[]> {
  const { data } = await api.get<ApiEnvelope<RuleSetVersionRecord[]>>(
    `/api/v1/engine/rulesets/${rulesetId}/versions`,
    {
      params: {
        include_archived: true,
      },
    },
  )
  return unwrapApiEnvelope(data)
}

export async function createRuleSetVersion(input: {
  ruleset_id: string
  source_version?: number
  payload?: RuleSetPayloadV2
}): Promise<RuleSetVersionRecord> {
  const { data } = await api.post<ApiEnvelope<RuleSetVersionRecord>>(
    `/api/v1/engine/rulesets/${input.ruleset_id}/versions`,
    {
      source_version: input.source_version,
      payload: input.payload,
    },
  )
  return unwrapApiEnvelope(data)
}

export async function updateRuleSetVersion(input: {
  ruleset_id: string
  version: number
  payload: RuleSetPayloadV2
}): Promise<RuleSetVersionRecord> {
  const { data } = await api.put<ApiEnvelope<RuleSetVersionRecord>>(
    `/api/v1/engine/rulesets/${input.ruleset_id}/versions/${input.version}`,
    {
      payload: input.payload,
    },
  )
  return unwrapApiEnvelope(data)
}

export async function validateRuleSetVersion(input: {
  ruleset_id: string
  version: number
  column_types: Record<string, string>
}): Promise<{ version: RuleSetVersionRecord; validation: RuleSetValidationResult }> {
  const { data } = await api.post<ApiEnvelope<{ version: RuleSetVersionRecord; validation: RuleSetValidationResult }>>(
    `/api/v1/engine/rulesets/${input.ruleset_id}/versions/${input.version}/validate`,
    {
      column_types: input.column_types,
    },
  )
  return unwrapApiEnvelope(data)
}

export async function publishRuleSetVersion(input: {
  ruleset_id: string
  version: number
}): Promise<RuleSetVersionRecord> {
  const { data } = await api.post<ApiEnvelope<RuleSetVersionRecord>>(
    `/api/v1/engine/rulesets/${input.ruleset_id}/versions/${input.version}/publish`,
  )
  return unwrapApiEnvelope(data)
}

export async function rollbackRuleSet(input: {
  ruleset_id: string
  target_version?: number
}): Promise<RuleSetVersionRecord> {
  const { data } = await api.post<ApiEnvelope<RuleSetVersionRecord>>(
    `/api/v1/engine/rulesets/${input.ruleset_id}/rollback`,
    {
      target_version: input.target_version,
    },
  )
  return unwrapApiEnvelope(data)
}

export async function validateRuleSetPayload(input: {
  payload: RuleSetPayloadV2
  column_types?: Record<string, string>
}): Promise<RuleSetValidationPayloadResult> {
  const { data } = await api.post<ApiEnvelope<RuleSetValidationPayloadResult>>('/api/v1/engine/validate-ruleset', {
    payload: input.payload,
    column_types: input.column_types ?? {},
  })
  return unwrapApiEnvelope(data)
}

export async function explainRuleSetRow(input: {
  payload: RuleSetPayloadV2
  row: Record<string, unknown>
  ruleset_name?: string
  version?: number
}): Promise<ExplainRowResult> {
  const { data } = await api.post<ApiEnvelope<ExplainRowResult>>('/api/v1/engine/explain-row', {
    payload: input.payload,
    row: input.row,
    ruleset_name: input.ruleset_name,
    version: input.version,
  })
  return unwrapApiEnvelope(data)
}

export async function explainRuleSetSample(input: {
  payload: RuleSetPayloadV2
  rows: Array<Record<string, unknown>>
  limit?: number
  ruleset_name?: string
  version?: number
}): Promise<ExplainSampleResult> {
  const { data } = await api.post<ApiEnvelope<ExplainSampleResult>>('/api/v1/engine/explain-sample', {
    payload: input.payload,
    rows: input.rows,
    limit: input.limit,
    ruleset_name: input.ruleset_name,
    version: input.version,
  })
  return unwrapApiEnvelope(data)
}

export async function dryRunRuleSet(input: {
  payload: RuleSetPayloadV2
  rows: Array<Record<string, unknown>>
  mode: ClassificationRuntimeMode
  explain_sample_limit?: number
  ruleset_name?: string
  version?: number
}): Promise<DryRunResult> {
  const { data } = await api.post<ApiEnvelope<DryRunResult>>('/api/v1/engine/dry-run-ruleset', {
    payload: input.payload,
    rows: input.rows,
    mode: input.mode,
    explain_sample_limit: input.explain_sample_limit,
    ruleset_name: input.ruleset_name,
    version: input.version,
  })
  return unwrapApiEnvelope(data)
}

export async function getClassificationMode(): Promise<ClassificationModeState> {
  const { data } = await api.get<ApiEnvelope<ClassificationModeState>>('/api/v1/engine/classification/mode')
  return unwrapApiEnvelope(data)
}

export async function setClassificationMode(input: {
  mode: ClassificationRuntimeMode
  ruleset_name?: string | null
}): Promise<ClassificationModeState> {
  const { data } = await api.put<ApiEnvelope<ClassificationModeState>>('/api/v1/engine/classification/mode', {
    mode: input.mode,
    ruleset_name: input.ruleset_name ?? null,
  })
  return unwrapApiEnvelope(data)
}

export async function listClassificationDivergences(input?: {
  limit?: number
  dataset_version_id?: string | null
}): Promise<ClassificationDivergenceRecord[]> {
  const { data } = await api.get<ApiEnvelope<ClassificationDivergenceRecord[]>>(
    '/api/v1/engine/classification/divergences',
    {
      params: {
        limit: input?.limit ?? 100,
        dataset_version_id: input?.dataset_version_id ?? undefined,
      },
    },
  )
  return unwrapApiEnvelope(data)
}

export async function listClassificationMetrics(input?: {
  limit?: number
  include_without_classification?: boolean
}): Promise<ClassificationRunMetricRecord[]> {
  const { data } = await api.get<ApiEnvelope<ClassificationRunMetricRecord[]>>(
    '/api/v1/engine/classification/metrics',
    {
      params: {
        limit: input?.limit ?? 100,
        include_without_classification: input?.include_without_classification ?? false,
      },
    },
  )
  return unwrapApiEnvelope(data)
}
