import axios from 'axios'
import { appConfig } from '@/lib/config'
import { session } from '@/auth/session'
import { ApiError, type ApiProblem } from './types'

const api = axios.create({
  baseURL: appConfig.apiBaseUrl,
  withCredentials: true,
})

api.interceptors.request.use((config) => {
  if (isStateChangingMethod(config.method)) {
    const csrfToken = readCookie(appConfig.csrfCookieName)
    if (csrfToken) {
      config.headers = config.headers ?? {}
      config.headers[appConfig.csrfHeaderName] = csrfToken
    }
  }
  return config
})

api.interceptors.response.use(
  (response) => response,
  (error) => {
    const problem = normalizeError(error)
    if (problem.status === 401) {
      session.notifyUnauthorized()
    }
    return Promise.reject(new ApiError(problem))
  },
)

function normalizeError(error: unknown): ApiProblem {
  if (axios.isAxiosError(error)) {
    const status = error.response?.status ?? 500
    const data = (error.response?.data ?? {}) as Record<string, unknown>
    const detail = data.detail
    const detailMessage =
      typeof detail === 'string'
        ? detail
        : Array.isArray(detail)
          ? detail
              .map((item) => (typeof item === 'string' ? item : JSON.stringify(item)))
              .join('; ')
          : undefined
    return {
      status,
      message: detailMessage || (data.message as string) || error.message || 'Unexpected error',
      code: (data.code as string) ?? undefined,
      details: data,
    }
  }
  return {
    status: 500,
    message: error instanceof Error ? error.message : 'Unexpected error',
  }
}

function readCookie(name: string): string | null {
  if (typeof document === 'undefined') return null
  const escapedName = name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  const match = document.cookie.match(new RegExp(`(?:^|; )${escapedName}=([^;]*)`))
  return match ? decodeURIComponent(match[1]) : null
}

function isStateChangingMethod(method?: string): boolean {
  if (!method) return false
  const normalized = method.toUpperCase()
  return normalized === 'POST' || normalized === 'PUT' || normalized === 'PATCH' || normalized === 'DELETE'
}

export { api }
