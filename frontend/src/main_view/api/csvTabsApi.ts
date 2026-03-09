import { ApiError } from '@/api/types'
import { api } from '@/api/client'
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

type MaybeApiEnvelope<T> = T | { data: T }

function unwrap<T>(payload: MaybeApiEnvelope<T>): T {
  if (payload && typeof payload === 'object' && 'data' in payload) {
    return payload.data
  }
  return payload as T
}

export async function listSources(): Promise<string[]> {
  const { data } = await api.get<MaybeApiEnvelope<string[]>>(`${CSV_TABS_BASE}/sources`)
  return unwrap(data)
}

export async function listProfiles(source?: string): Promise<CsvTabProfileSchema[]> {
  const { data } = await api.get<MaybeApiEnvelope<CsvTabProfileSchema[]>>(`${CSV_TABS_BASE}/profiles`, {
    params: source ? { source } : undefined,
  })
  return unwrap(data)
}

export async function previewRaw(payload: PreviewRawRequest): Promise<PreviewRawResponse> {
  const { data } = await api.post<MaybeApiEnvelope<PreviewRawResponse>>(`${CSV_TABS_BASE}/preview/raw`, payload)
  return unwrap(data)
}

export async function previewParsed(payload: PreviewParsedRequest): Promise<PreviewParsedResponse> {
  const { data } = await api.post<MaybeApiEnvelope<PreviewParsedResponse>>(
    `${CSV_TABS_BASE}/preview/parsed`,
    payload,
  )
  return unwrap(data)
}

export async function createProfile(payload: CreateProfileRequest): Promise<CsvTabProfileSchema> {
  const { data } = await api.post<MaybeApiEnvelope<CsvTabProfileSchema>>(`${CSV_TABS_BASE}/profiles`, payload)
  return unwrap(data)
}

export async function updateProfile(
  profileId: string,
  payload: UpdateProfileRequest,
): Promise<StatusMessageResponse> {
  const { data } = await api.put<MaybeApiEnvelope<StatusMessageResponse>>(
    `${CSV_TABS_BASE}/profiles/${profileId}`,
    payload,
  )
  return unwrap(data)
}

export async function promoteDefault(profileId: string): Promise<StatusMessageResponse> {
  const { data } = await api.post<MaybeApiEnvelope<StatusMessageResponse>>(
    `${CSV_TABS_BASE}/profiles/${profileId}/promote-default`,
  )
  return unwrap(data)
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
