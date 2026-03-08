import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import MockAdapter from 'axios-mock-adapter'
import { api } from '@/api/client'
import { ApiError } from '@/api/types'
import { session } from '@/auth/session'

let mock: MockAdapter

beforeEach(() => {
  mock = new MockAdapter(api)
  document.cookie = 'cg_csrf=test-csrf-token'
})

afterEach(() => {
  mock.restore()
  vi.restoreAllMocks()
  document.cookie = 'cg_csrf=; Max-Age=0; path=/'
})

describe('api client', () => {
  it('attaches csrf header for state-changing requests', async () => {
    mock.onPost('/secure').reply((config) => {
      expect(config.headers?.['X-CSRF-Token']).toBe('test-csrf-token')
      return [200, { ok: true }]
    })

    await api.post('/secure', { ok: true })
  })

  it('fires unauthorized notification on 401', async () => {
    const notify = vi.spyOn(session, 'notifyUnauthorized')
    mock.onGet('/secure').reply(401, { message: 'unauthorized' })

    await expect(api.get('/secure')).rejects.toBeInstanceOf(ApiError)
    expect(notify).toHaveBeenCalled()
  })
})
