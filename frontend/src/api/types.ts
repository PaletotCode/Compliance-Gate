export type ApiProblem = {
  status: number
  message: string
  code?: string
  details?: Record<string, unknown>
}

export class ApiError extends Error {
  status: number
  code?: string
  details?: Record<string, unknown>

  constructor(problem: ApiProblem) {
    super(problem.message)
    this.name = 'ApiError'
    this.status = problem.status
    this.code = problem.code
    this.details = problem.details
  }
}

export type PaginatedResponse<T> = {
  items: T[]
  total: number
  page: number
  pageSize: number
}

export type ApiResponse<T> = {
  data: T
}
