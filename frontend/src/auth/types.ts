export type Role = 'TI_ADMIN' | 'DIRECTOR' | 'TI_OPERATOR' | 'AUDITOR'

export type UserPublic = {
  id: string
  tenant_id: string
  username: string
  role: Role
  is_active: boolean
  mfa_enabled: boolean
  require_password_change: boolean
}

export type LoginRequest = {
  username: string
  password: string
  totp_code?: string
  challenge_id?: string
}

export type LoginChallengeResponse = {
  mfa_required: true
  challenge_id: string
}

export type LoginSuccessResponse = {
  expires_in: number
  user: UserPublic
}

export type LoginResponse = LoginChallengeResponse | LoginSuccessResponse

export type MfaSetupResponse = {
  otpauth_url: string
  qr_code_base64_png: string
  instructions: string
}

export type MfaConfirmRequest = {
  totp_code: string
}

export type MfaConfirmResponse = {
  recovery_codes: string[]
}

export type PasswordResetByTotpRequest = {
  username: string
  new_password: string
  totp_code: string
  recovery_code?: never
}

export type PasswordResetByRecoveryCodeRequest = {
  username: string
  new_password: string
  recovery_code: string
  totp_code?: never
}

export type PasswordResetRequest = PasswordResetByTotpRequest | PasswordResetByRecoveryCodeRequest

export type GenericMessageResponse = {
  status: 'ok'
  message: string
}

export function isLoginChallengeResponse(response: LoginResponse): response is LoginChallengeResponse {
  return 'mfa_required' in response
}

export function isLoginSuccessResponse(response: LoginResponse): response is LoginSuccessResponse {
  return 'user' in response
}
