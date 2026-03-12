export type ApiEnvelope<T> = T | { data: T; success?: boolean }

export function unwrapApiEnvelope<T>(payload: ApiEnvelope<T>): T {
  if (payload && typeof payload === 'object' && 'data' in payload) {
    return payload.data as T
  }
  return payload as T
}
