import axios from 'axios'
import { appConfig } from '@/lib/config'
import { session } from '@/auth/session'
import { ApiError, type ApiProblem } from './types'

const api = axios.create({
  baseURL: appConfig.apiBaseUrl,
  withCredentials: session.isCookieMode(),
})

api.interceptors.request.use((config) => {
  const token = session.getToken()
  if (token && !session.isCookieMode()) {
    config.headers = config.headers ?? {}
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

api.interceptors.response.use(
  (response) => response,
  (error) => {
    const problem = normalizeError(error)
    if (problem.status === 401 || problem.status === 403) {
      session.notifyUnauthorized()
    }
    return Promise.reject(new ApiError(problem))
  },
)

function normalizeError(error: unknown): ApiProblem {
  if (axios.isAxiosError(error)) {
    const status = error.response?.status ?? 500
    const data = (error.response?.data ?? {}) as Record<string, unknown>
    return {
      status,
      message: (data.message as string) || error.message || 'Unexpected error',
      code: (data.code as string) ?? undefined,
      details: data,
    }
  }
  return {
    status: 500,
    message: error instanceof Error ? error.message : 'Unexpected error',
  }
}

export { api }
