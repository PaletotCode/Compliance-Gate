import { describe, expect, it } from 'vitest'
import { ApiError } from '@/api/types'
import { extractEngineErrorPayload } from '@/engine_studio/diagnostics'

describe('engine_studio diagnostics', () => {
  it('extracts structured payload with suggestions and node_path', () => {
    const error = new ApiError({
      status: 400,
      message: 'bad request',
      details: {
        detail: {
          code: 'UnknownColumn',
          message: 'Coluna inválida.',
          details: {
            column: 'hostnme',
            suggestions: ['hostname', 'host_name'],
          },
          hint: 'Use uma coluna válida.',
          node_path: 'root.left',
        },
      },
    })

    const payload = extractEngineErrorPayload(error)
    expect(payload.code).toBe('UnknownColumn')
    expect(payload.node_path).toBe('root.left')
    expect(payload.suggestions).toEqual(['hostname', 'host_name'])
    expect(payload.hint).toBe('Use uma coluna válida.')
  })

  it('returns fallback payload for non ApiError', () => {
    const payload = extractEngineErrorPayload(new Error('boom'))
    expect(payload.code).toBe('UnexpectedError')
    expect(payload.message).toContain('boom')
  })
})
