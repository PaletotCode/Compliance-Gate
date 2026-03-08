import { create } from 'zustand'
import * as authApi from './api'
import { session } from './session'
import type {
  GenericMessageResponse,
  LoginRequest,
  LoginResponse,
  MfaConfirmRequest,
  MfaConfirmResponse,
  MfaSetupResponse,
  PasswordResetRequest,
  UserPublic,
} from './types'

export type AuthStatus = 'idle' | 'login' | 'mfaChallenge' | 'authenticated' | 'error'

export type AuthState = {
  user: UserPublic | null
  status: AuthStatus
  error: string | null
  challengeId: string | null
  isLoading: boolean
}

export type AuthActions = {
  login: (payload: LoginRequest) => Promise<LoginResponse>
  logout: () => Promise<void>
  fetchMe: () => Promise<UserPublic>
  ensureSession: () => Promise<UserPublic | null>
  beginMfaSetup: () => Promise<MfaSetupResponse>
  confirmMfa: (payload: MfaConfirmRequest) => Promise<MfaConfirmResponse>
  resetPassword: (payload: PasswordResetRequest) => Promise<GenericMessageResponse>
  clearError: () => void
}

export type AuthStore = AuthState & AuthActions
export type AuthStoreApi = typeof authStore

const initialState: AuthState = {
  user: null,
  status: 'idle',
  error: null,
  challengeId: null,
  isLoading: false,
}

export const authStore = create<AuthStore>()((set, get) => {
  session.subscribeUnauthorized(() => {
    set({ user: null, status: 'idle', error: null, challengeId: null, isLoading: false })
  })

  let activeRequestId = 0
  const beginRequest = (nextStatus?: AuthStatus) => {
    activeRequestId += 1
    const requestId = activeRequestId
    set((state) => ({
      ...state,
      isLoading: true,
      error: null,
      ...(nextStatus ? { status: nextStatus } : {}),
    }))
    return requestId
  }
  const isCurrentRequest = (requestId: number) => requestId === activeRequestId
  const finishCurrentRequest = (requestId: number, update: Partial<AuthState>) => {
    if (!isCurrentRequest(requestId)) return false
    set((state) => ({ ...state, ...update, isLoading: false }))
    return true
  }
  const failCurrentRequest = (requestId: number, error: unknown) => {
    if (!isCurrentRequest(requestId)) return
    set((state) => ({
      ...state,
      status: 'error',
      error: extractErrorMessage(error),
      isLoading: false,
    }))
  }

  return {
    ...initialState,
    login: async (payload) => {
      const requestId = beginRequest('login')
      try {
        const response = await authApi.login(payload)
        if ('mfa_required' in response) {
          finishCurrentRequest(requestId, {
            user: null,
            status: 'mfaChallenge',
            challengeId: response.challenge_id,
            error: null,
          })
          return response
        }

        finishCurrentRequest(requestId, {
          user: response.user,
          status: 'authenticated',
          challengeId: null,
          error: null,
        })
        return response
      } catch (error) {
        failCurrentRequest(requestId, error)
        throw error
      }
    },

    logout: async () => {
      const requestId = beginRequest()
      try {
        await authApi.logout()
      } finally {
        finishCurrentRequest(requestId, {
          user: null,
          status: 'idle',
          error: null,
          challengeId: null,
        })
      }
    },

    fetchMe: async () => {
      const requestId = beginRequest()
      try {
        const user = await authApi.fetchMe()
        finishCurrentRequest(requestId, {
          user,
          status: 'authenticated',
          error: null,
        })
        return user
      } catch (error) {
        failCurrentRequest(requestId, error)
        throw error
      }
    },

    ensureSession: async () => {
      if (get().user) return get().user
      try {
        return await get().fetchMe()
      } catch (error) {
        set((state) => ({
          ...state,
          user: null,
          status: 'idle',
          error: null,
          challengeId: null,
          isLoading: false,
        }))
        return null
      }
    },

    beginMfaSetup: async () => {
      const requestId = beginRequest('authenticated')
      try {
        const data = await authApi.beginMfaSetup()
        finishCurrentRequest(requestId, { status: 'authenticated', error: null })
        return data
      } catch (error) {
        failCurrentRequest(requestId, error)
        throw error
      }
    },

    confirmMfa: async (payload) => {
      const requestId = beginRequest('authenticated')
      try {
        const data = await authApi.confirmMfa(payload)
        finishCurrentRequest(requestId, { status: 'authenticated', error: null })
        return data
      } catch (error) {
        failCurrentRequest(requestId, error)
        throw error
      }
    },

    resetPassword: async (payload) => {
      const requestId = beginRequest()
      try {
        const data = await authApi.resetPassword(payload)
        finishCurrentRequest(requestId, { status: 'idle', error: null })
        return data
      } catch (error) {
        failCurrentRequest(requestId, error)
        throw error
      }
    },

    clearError: () => set((state) => ({ ...state, error: null })),
  }
})

function extractErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message
  }
  return 'Unexpected error'
}
