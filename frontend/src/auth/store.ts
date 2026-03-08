import { create } from 'zustand'
import * as authApi from './api'
import { session } from './session'
import type {
  LoginRequest,
  MfaConfirmRequest,
  MfaSetupResponse,
  PasswordResetRequest,
  SessionMode,
  User,
} from './types'

export type AuthStatus = 'idle' | 'loading' | 'authenticated' | 'error'

export type AuthState = {
  user: User | null
  status: AuthStatus
  error: string | null
  sessionMode: SessionMode
}

export type AuthActions = {
  login: (payload: LoginRequest) => Promise<User>
  logout: () => Promise<void>
  fetchMe: () => Promise<User>
  ensureSession: () => Promise<User | null>
  beginMfaSetup: () => Promise<MfaSetupResponse>
  confirmMfa: (payload: MfaConfirmRequest) => Promise<void>
  resetPassword: (payload: PasswordResetRequest) => Promise<void>
}

export type AuthStore = AuthState & AuthActions
export type AuthStoreApi = typeof authStore

const initialState: AuthState = {
  user: null,
  status: 'idle',
  error: null,
  sessionMode: session.isCookieMode() ? 'cookie' : 'bearer',
}

export const authStore = create<AuthStore>()((set, get) => {
  session.subscribeUnauthorized(() => {
    set({ user: null, status: 'idle', error: null })
  })

  return {
    ...initialState,
    login: async (payload) => {
      set({ status: 'loading', error: null })
      const response = await authApi.login(payload)

      if (response.accessToken) {
        session.setToken(response.accessToken)
      }

      const user = response.user ?? (await authApi.fetchMe())
      set({ user, status: 'authenticated', error: null })
      return user
    },

    logout: async () => {
      await authApi.logout()
      session.clearToken()
      set({ user: null, status: 'idle', error: null })
    },

    fetchMe: async () => {
      set({ status: 'loading', error: null })
      const user = await authApi.fetchMe()
      set({ user, status: 'authenticated', error: null })
      return user
    },

    ensureSession: async () => {
      if (!session.isAuthenticated()) {
        set({ user: null, status: 'idle' })
        return null
      }
      if (get().user) return get().user
      try {
        return await get().fetchMe()
      } catch (error) {
        session.clearToken()
        set({ user: null, status: 'error', error: (error as Error).message })
        return null
      }
    },

    beginMfaSetup: async () => {
      set({ status: 'loading', error: null })
      const data = await authApi.beginMfaSetup()
      set({ status: 'authenticated' })
      return data
    },

    confirmMfa: async (payload) => {
      set({ status: 'loading', error: null })
      await authApi.confirmMfa(payload)
      set({ status: 'authenticated' })
    },

    resetPassword: async (payload) => {
      set({ status: 'loading', error: null })
      await authApi.resetPassword(payload)
      set({ status: 'idle' })
    },
  }
})
