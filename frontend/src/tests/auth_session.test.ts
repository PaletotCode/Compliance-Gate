import { describe, expect, it, vi } from 'vitest'
import { SessionManager } from '@/auth/session'

describe('SessionManager', () => {
  it('notifies subscribers on unauthorized', () => {
    const manager = new SessionManager()
    const handler = vi.fn()

    manager.subscribeUnauthorized(handler)
    manager.notifyUnauthorized()

    expect(handler).toHaveBeenCalledTimes(1)
  })

  it('returns unsubscribe function', () => {
    const manager = new SessionManager()
    const handler = vi.fn()

    const unsubscribe = manager.subscribeUnauthorized(handler)
    unsubscribe()
    manager.notifyUnauthorized()

    expect(handler).not.toHaveBeenCalled()
  })
})
