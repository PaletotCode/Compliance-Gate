export type Role = 'TI_ADMIN' | 'DIRECTOR' | 'USER'

export type User = {
  id: string
  email: string
  name?: string
  roles: Role[]
  mfaEnabled?: boolean
}

export type LoginRequest = {
  email: string
  password: string
  mfaToken?: string
}

export type LoginResponse = {
  accessToken?: string
  refreshToken?: string
  user?: User
}

export type MfaSetupResponse = {
  secret: string
  qrCodeUrl?: string
}

export type MfaConfirmRequest = {
  code: string
}

export type PasswordResetRequest = {
  email: string
}

export type SessionMode = 'bearer' | 'cookie'
