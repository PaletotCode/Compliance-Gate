import { ApiError } from '@/api/types'
import type { EngineErrorPayload } from '@/engine_studio/types'

type UnknownRecord = Record<string, unknown>

function asRecord(value: unknown): UnknownRecord {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return {}
  }
  return value as UnknownRecord
}

function asString(value: unknown): string | null {
  if (typeof value !== 'string') return null
  const normalized = value.trim()
  return normalized.length > 0 ? normalized : null
}

function asStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return []
  return value
    .map((item) => asString(item))
    .filter((item): item is string => Boolean(item))
    .slice(0, 3)
}

function extractErrorObject(payload: UnknownRecord): UnknownRecord {
  const detail = asRecord(payload.detail)
  if (Object.keys(detail).length > 0) return detail
  return payload
}

export function extractEngineErrorPayload(error: unknown): EngineErrorPayload {
  const fallback: EngineErrorPayload = {
    code: 'UnexpectedError',
    message: error instanceof Error ? error.message : 'Erro inesperado na Engine.',
    details: {},
    hint: 'Tente novamente após revisar os dados enviados.',
    node_path: null,
    suggestions: [],
  }

  if (!(error instanceof ApiError)) {
    return fallback
  }

  const payload = extractErrorObject(asRecord(error.details))
  const details = asRecord(payload.details)
  const code = asString(payload.code) ?? `HTTP_${error.status}`
  const message = asString(payload.message) ?? asString(error.message) ?? fallback.message
  const hint = asString(payload.hint) ?? 'Revise os campos destacados e tente novamente.'
  const nodePath = asString(payload.node_path) ?? asString(details.node_path)
  const suggestions = asStringArray(details.suggestions)

  return {
    code,
    message,
    details,
    hint,
    node_path: nodePath,
    suggestions,
  }
}

export function summarizeEngineError(error: unknown): string {
  const payload = extractEngineErrorPayload(error)
  return `[${payload.code}] ${payload.message}`
}
