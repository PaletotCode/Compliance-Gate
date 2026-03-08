import { describe, expect, it, vi } from 'vitest'
import { SessionManager } from '@/auth/session'

describe('SessionManager (bearer)', () => {
  it('stores and clears bearer tokens', () => {
    const session = new SessionManager('bearer')

    session.setToken('token-123')
    expect(session.getToken()).toBe('token-123')

    session.clearToken()
    expect(session.getToken()).toBeNull()
  })

  it('notifies subscribers on unauthorized', () => {
    const session = new SessionManager('bearer')
    const handler = vi.fn()

    session.setToken('token-123')
    session.subscribeUnauthorized(handler)

    session.notifyUnauthorized()

    expect(session.getToken()).toBeNull()
    expect(handler).toHaveBeenCalledTimes(1)
  })
})

describe('SessionManager (cookie mode)', () => {
  it('skips token storage and assumes authenticated cookie session', () => {
    const session = new SessionManager('cookie')

    session.setToken('token-abc')
    expect(session.getToken()).toBeNull()
    expect(session.isAuthenticated()).toBe(true)
    expect(session.isCookieMode()).toBe(true)
  })
})
