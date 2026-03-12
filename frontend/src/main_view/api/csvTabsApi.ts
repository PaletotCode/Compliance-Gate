import { ApiError } from '@/api/types'
import { api } from '@/api/client'
import { unwrapApiEnvelope, type ApiEnvelope } from '@/api/envelope'
import type {
  CreateProfileRequest,
  CsvTabProfileSchema,
  PreviewParsedRequest,
  PreviewParsedResponse,
  PreviewRawRequest,
  PreviewRawResponse,
  StatusMessageResponse,
  UpdateProfileRequest,
} from '@/main_view/api/schemas'

const CSV_TABS_BASE = '/api/v1/csv-tabs'

export async function listSources(): Promise<string[]> {
  const { data } = await api.get<ApiEnvelope<string[]>>(`${CSV_TABS_BASE}/sources`)
  return unwrapApiEnvelope(data)
}

export async function listProfiles(source?: string): Promise<CsvTabProfileSchema[]> {
  const { data } = await api.get<ApiEnvelope<CsvTabProfileSchema[]>>(`${CSV_TABS_BASE}/profiles`, {
    params: source ? { source } : undefined,
  })
  return unwrapApiEnvelope(data)
}

export async function previewRaw(payload: PreviewRawRequest): Promise<PreviewRawResponse> {
  const { data } = await api.post<ApiEnvelope<PreviewRawResponse>>(`${CSV_TABS_BASE}/preview/raw`, payload)
  return unwrapApiEnvelope(data)
}

export async function previewParsed(payload: PreviewParsedRequest): Promise<PreviewParsedResponse> {
  const { data } = await api.post<ApiEnvelope<PreviewParsedResponse>>(
    `${CSV_TABS_BASE}/preview/parsed`,
    payload,
  )
  return unwrapApiEnvelope(data)
}

export async function createProfile(payload: CreateProfileRequest): Promise<CsvTabProfileSchema> {
  const { data } = await api.post<ApiEnvelope<CsvTabProfileSchema>>(`${CSV_TABS_BASE}/profiles`, payload)
  return unwrapApiEnvelope(data)
}

export async function updateProfile(
  profileId: string,
  payload: UpdateProfileRequest,
): Promise<StatusMessageResponse> {
  const { data } = await api.put<ApiEnvelope<StatusMessageResponse>>(
    `${CSV_TABS_BASE}/profiles/${profileId}`,
    payload,
  )
  return unwrapApiEnvelope(data)
}

export async function promoteDefault(profileId: string): Promise<StatusMessageResponse> {
  const { data } = await api.post<ApiEnvelope<StatusMessageResponse>>(
    `${CSV_TABS_BASE}/profiles/${profileId}/promote-default`,
  )
  return unwrapApiEnvelope(data)
}

export function getMainViewErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 403) {
      return 'Permissão insuficiente para executar esta ação.'
    }

    if (error.status === 400 || error.status === 422) {
      return sanitizeMessage(error.message) || 'Dados inválidos para processar a operação.'
    }

    if (error.status >= 500) {
      return 'Falha interna no servidor. Tente novamente em instantes.'
    }

    if (error.status === 401) {
      return 'Sessão expirada. Faça login novamente.'
    }

    return sanitizeMessage(error.message)
  }

  if (error instanceof Error) {
    return sanitizeMessage(error.message)
  }

  return 'Erro inesperado ao processar a operação.'
}

function sanitizeMessage(message: string): string {
  return message.replace(/\s+/g, ' ').trim().slice(0, 220)
}
