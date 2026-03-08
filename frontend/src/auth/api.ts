import { api } from '@/api/client'
import { endpoints } from '@/api/endpoints'
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

export async function login(payload: LoginRequest): Promise<LoginResponse> {
  const { data } = await api.post<LoginResponse>(endpoints.auth.login, payload)
  return data
}

export async function fetchMe(): Promise<UserPublic> {
  const { data } = await api.get<UserPublic>(endpoints.auth.me)
  return data
}

export async function beginMfaSetup(): Promise<MfaSetupResponse> {
  const { data } = await api.post<MfaSetupResponse>(endpoints.auth.mfaSetup)
  return data
}

export async function confirmMfa(payload: MfaConfirmRequest): Promise<MfaConfirmResponse> {
  const { data } = await api.post<MfaConfirmResponse>(endpoints.auth.mfaConfirm, payload)
  return data
}

export async function resetPassword(payload: PasswordResetRequest): Promise<GenericMessageResponse> {
  const { data } = await api.post<GenericMessageResponse>(endpoints.auth.passwordReset, payload)
  return data
}

export async function logout(): Promise<void> {
  try {
    await api.post(endpoints.auth.logout)
  } catch {
    // Logout should be best-effort; swallow errors to avoid blocking local cleanup
    console.warn('Logout request failed')
  }
}
