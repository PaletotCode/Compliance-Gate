export const endpoints = {
  auth: {
    login: '/api/v1/auth/login',
    me: '/api/v1/auth/me',
    mfaSetup: '/api/v1/auth/mfa/setup',
    mfaConfirm: '/api/v1/auth/mfa/confirm',
    passwordReset: '/api/v1/auth/password/reset',
    logout: '/api/v1/auth/logout',
  },
} as const
