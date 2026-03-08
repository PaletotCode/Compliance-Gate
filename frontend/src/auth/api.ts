import { api } from '@/api/client'
import { endpoints } from '@/api/endpoints'
import type {
  LoginRequest,
  LoginResponse,
  MfaConfirmRequest,
  MfaSetupResponse,
  PasswordResetRequest,
  User,
} from './types'

export async function login(payload: LoginRequest): Promise<LoginResponse> {
  const { data } = await api.post<LoginResponse>(endpoints.auth.login, payload)
  return data
}

export async function fetchMe(): Promise<User> {
  const { data } = await api.get<User>(endpoints.auth.me)
  return data
}

export async function beginMfaSetup(): Promise<MfaSetupResponse> {
  const { data } = await api.post<MfaSetupResponse>(endpoints.auth.mfaSetup)
  return data
}

export async function confirmMfa(payload: MfaConfirmRequest): Promise<void> {
  await api.post(endpoints.auth.mfaConfirm, payload)
}

export async function resetPassword(payload: PasswordResetRequest): Promise<void> {
  await api.post(endpoints.auth.passwordReset, payload)
}

export async function logout(): Promise<void> {
  try {
    await api.post(endpoints.auth.logout)
  } catch (error) {
    // Logout should be best-effort; swallow errors to avoid blocking local cleanup
    console.warn('Logout request failed', error)
  }
}
