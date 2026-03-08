import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import MockAdapter from 'axios-mock-adapter'
import { api } from '@/api/client'
import { ApiError } from '@/api/types'
import { session } from '@/auth/session'

let mock: MockAdapter

beforeEach(() => {
  mock = new MockAdapter(api)
})

afterEach(() => {
  mock.restore()
  vi.restoreAllMocks()
})

describe('api client', () => {
  it('attaches bearer token when available', async () => {
    vi.spyOn(session, 'isCookieMode').mockReturnValue(false)
    vi.spyOn(session, 'getToken').mockReturnValue('test-token')

    mock.onGet('/secure').reply((config) => {
      expect(config.headers?.Authorization).toBe('Bearer test-token')
      return [200, { ok: true }]
    })

    await api.get('/secure')
  })

  it('fires unauthorized notification on 401/403', async () => {
    const notify = vi.spyOn(session, 'notifyUnauthorized')
    mock.onGet('/secure').reply(401, { message: 'unauthorized' })

    await expect(api.get('/secure')).rejects.toBeInstanceOf(ApiError)
    expect(notify).toHaveBeenCalled()
  })
})
