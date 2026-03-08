import { beforeEach, describe, expect, it, vi } from 'vitest'
import { authStore } from '@/auth/store'
import * as authApi from '@/auth/api'
import type { UserPublic } from '@/auth/types'

vi.mock('@/auth/api', () => ({
  login: vi.fn(),
  fetchMe: vi.fn(),
  beginMfaSetup: vi.fn(),
  confirmMfa: vi.fn(),
  resetPassword: vi.fn(),
  logout: vi.fn(),
}))

const mockedApi = vi.mocked(authApi)

const user: UserPublic = {
  id: '1',
  tenant_id: 'tenant-1',
  username: 'admin',
  role: 'TI_ADMIN',
  is_active: true,
  mfa_enabled: false,
  require_password_change: false,
}

beforeEach(() => {
  authStore.setState((state) => ({
    ...state,
    user: null,
    status: 'idle',
    error: null,
    challengeId: null,
    isLoading: false,
  }))
  vi.restoreAllMocks()
})

describe('authStore.login', () => {
  it('stores authenticated user on success', async () => {
    mockedApi.login.mockResolvedValue({
      expires_in: 3600,
      user,
    })

    const result = await authStore.getState().login({ username: 'admin', password: 'secret' })

    expect(result).toEqual({ expires_in: 3600, user })
    expect(authStore.getState().user).toEqual(user)
    expect(authStore.getState().status).toBe('authenticated')
  })

  it('switches to mfaChallenge state when backend requires TOTP', async () => {
    mockedApi.login.mockResolvedValue({ mfa_required: true, challenge_id: 'challenge-1' })

    const result = await authStore.getState().login({ username: 'admin', password: 'secret' })

    expect(result).toEqual({ mfa_required: true, challenge_id: 'challenge-1' })
    expect(authStore.getState().status).toBe('mfaChallenge')
    expect(authStore.getState().challengeId).toBe('challenge-1')
    expect(authStore.getState().user).toBeNull()
  })
})

describe('authStore.ensureSession', () => {
  it('returns null when /me fails', async () => {
    mockedApi.fetchMe.mockRejectedValue(new Error('unauthorized'))

    const result = await authStore.getState().ensureSession()

    expect(result).toBeNull()
    expect(authStore.getState().user).toBeNull()
    expect(authStore.getState().status).toBe('idle')
  })

  it('fetches user when user is missing', async () => {
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

    await authStore.getState().logout()

    expect(authStore.getState().user).toBeNull()
    expect(authStore.getState().status).toBe('idle')
  })
})
