import { beforeEach, describe, expect, it, vi } from 'vitest'
import { authStore } from '@/auth/store'
import { session } from '@/auth/session'
import * as authApi from '@/auth/api'
import type { User } from '@/auth/types'

vi.mock('@/auth/api', () => ({
  login: vi.fn(),
  fetchMe: vi.fn(),
  beginMfaSetup: vi.fn(),
  confirmMfa: vi.fn(),
  resetPassword: vi.fn(),
  logout: vi.fn(),
}))

const mockedApi = vi.mocked(authApi)

const user: User = { id: '1', email: 'user@example.com', roles: ['USER'] }

beforeEach(() => {
  authStore.setState((state) => ({
    ...state,
    user: null,
    status: 'idle',
    error: null,
  }))
  vi.restoreAllMocks()
})

describe('authStore.login', () => {
  it('persists token and user', async () => {
    mockedApi.login.mockResolvedValue({ accessToken: 'token-123', user })
    const setTokenSpy = vi.spyOn(session, 'setToken')

    const result = await authStore.getState().login({ email: 'user@example.com', password: 'secret' })

    expect(result).toEqual(user)
    expect(authStore.getState().user).toEqual(user)
    expect(authStore.getState().status).toBe('authenticated')
    expect(setTokenSpy).toHaveBeenCalledWith('token-123')
  })
})

describe('authStore.ensureSession', () => {
  it('returns null when not authenticated', async () => {
    vi.spyOn(session, 'isAuthenticated').mockReturnValue(false)

    const result = await authStore.getState().ensureSession()

    expect(result).toBeNull()
    expect(authStore.getState().user).toBeNull()
  })

  it('fetches user when authenticated but user missing', async () => {
    vi.spyOn(session, 'isAuthenticated').mockReturnValue(true)
    mockedApi.fetchMe.mockResolvedValue(user)

    const result = await authStore.getState().ensureSession()

    expect(mockedApi.fetchMe).toHaveBeenCalled()
    expect(result).toEqual(user)
    expect(authStore.getState().user).toEqual(user)
  })
})

describe('authStore.logout', () => {
  it('clears local session state', async () => {
    authStore.setState((state) => ({ ...state, user, status: 'authenticated' }))
    const clearSpy = vi.spyOn(session, 'clearToken')

    await authStore.getState().logout()

    expect(clearSpy).toHaveBeenCalled()
    expect(authStore.getState().user).toBeNull()
    expect(authStore.getState().status).toBe('idle')
  })
})
