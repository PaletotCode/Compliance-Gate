import { api } from '@/api/client'
import { ApiError } from '@/api/types'
import type {
  DatasetsIngestRequest,
  DatasetsIngestResponse,
  DatasetsPreviewRequest,
  DatasetsPreviewResponse,
  FilterDefinition,
  MachineItem,
  MachineSummary,
  MachineTableQuery,
  MaterializeMachinesResponse,
  PaginatedPayload,
  ReportRunRequest,
  ReportRunResponse,
} from '@/main_view/api/schemas'

type MaybeEnvelope<T> = T | { data: T; success?: boolean }

function unwrap<T>(payload: MaybeEnvelope<T>): T {
  if (payload && typeof payload === 'object' && 'data' in payload) {
    return payload.data
  }
  return payload as T
}

function buildMachinesParams(query: MachineTableQuery): URLSearchParams {
  const params = new URLSearchParams()
  params.set('dataset_version_id', query.dataset_version_id)
  params.set('page', String(query.page))
  params.set('size', String(query.size))

  if (query.search) params.set('search', query.search)
  if (query.pa_code) params.set('pa_code', query.pa_code)
  query.statuses?.forEach((status) => params.append('statuses', status))
  query.flags?.forEach((flag) => params.append('flags', flag))

  return params
}

export async function previewDatasetMachines(
  payload: DatasetsPreviewRequest,
): Promise<DatasetsPreviewResponse> {
  const { data } = await api.post<DatasetsPreviewResponse>('/api/v1/datasets/machines/preview', payload)
  return data
}

export async function ingestDatasetMachines(payload: DatasetsIngestRequest): Promise<DatasetsIngestResponse> {
  const { data } = await api.post<DatasetsIngestResponse>('/api/v1/datasets/machines/ingest', payload)
  return data
}

export async function materializeMachines(
  datasetVersionId: string,
): Promise<MaterializeMachinesResponse> {
  const { data } = await api.post<MaybeEnvelope<MaterializeMachinesResponse>>(
    '/api/v1/engine/materialize/machines',
    undefined,
    {
      params: {
        dataset_version_id: datasetVersionId,
      },
    },
  )

  return unwrap(data)
}

async function fetchMachinesTableFromEngine(
  query: MachineTableQuery,
): Promise<PaginatedPayload<MachineItem>> {
  const { data } = await api.get<MaybeEnvelope<PaginatedPayload<MachineItem>>>(
    '/api/v1/engine/tables/machines',
    {
      params: buildMachinesParams(query),
    },
  )

  return unwrap(data)
}

async function fetchMachinesTableFromMachines(
  query: MachineTableQuery,
): Promise<PaginatedPayload<MachineItem>> {
  const { data } = await api.get<MaybeEnvelope<PaginatedPayload<MachineItem>>>('/api/v1/machines/table', {
    params: buildMachinesParams(query),
  })

  return unwrap(data)
}

export async function fetchMachinesTable(query: MachineTableQuery): Promise<PaginatedPayload<MachineItem>> {
  try {
    return await fetchMachinesTableFromEngine(query)
  } catch (error) {
    if (error instanceof ApiError && error.status !== 404) {
      throw error
    }
    return fetchMachinesTableFromMachines(query)
  }
}

export async function fetchMachinesSummary(
  query: Omit<MachineTableQuery, 'page' | 'size'>,
): Promise<MachineSummary> {
  const params = new URLSearchParams()
  params.set('dataset_version_id', query.dataset_version_id)
  if (query.search) params.set('search', query.search)
  if (query.pa_code) params.set('pa_code', query.pa_code)
  query.statuses?.forEach((status) => params.append('statuses', status))
  query.flags?.forEach((flag) => params.append('flags', flag))

  const { data } = await api.get<MaybeEnvelope<MachineSummary>>('/api/v1/machines/summary', {
    params,
  })

  return unwrap(data)
}

export async function fetchMachinesFilters(): Promise<FilterDefinition[]> {
  const { data } = await api.get<MaybeEnvelope<FilterDefinition[]>>('/api/v1/machines/filters')
  return unwrap(data)
}

export async function runMachinesReport(
  datasetVersionId: string,
  payload: ReportRunRequest,
): Promise<ReportRunResponse> {
  const { data } = await api.post<MaybeEnvelope<ReportRunResponse>>('/api/v1/engine/reports/run', payload, {
    params: {
      dataset_version_id: datasetVersionId,
    },
  })

  return unwrap(data)
}

export function exportRowsAsCsv(rows: Array<Record<string, unknown>>, filename: string): void {
  if (rows.length === 0 || typeof document === 'undefined') return

  const headers = Array.from(
    rows.reduce<Set<string>>((acc, row) => {
      Object.keys(row).forEach((key) => acc.add(key))
      return acc
    }, new Set<string>()),
  )

  const lines = rows.map((row) =>
    headers
      .map((header) => {
        const raw = row[header]
        const value = Array.isArray(raw) ? raw.join('|') : raw
        const normalized = String(value ?? '').replace(/\"/g, '\"\"')
        return `\"${normalized}\"`
      })
      .join(','),
  )

  const csv = [headers.join(','), ...lines].join('\\n')
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
  const url = URL.createObjectURL(blob)
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = filename
  anchor.click()
  URL.revokeObjectURL(url)
}
