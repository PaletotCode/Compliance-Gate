import { redirect } from '@tanstack/react-router'
import { authStore } from './store'
import type { Role, User } from './types'

export async function requireAuth(store?: typeof authStore) {
  const state = store ?? authStore
  const user = await state.getState().ensureSession()
  if (!user) {
    throw redirect({ to: '/auth/callback', search: { reason: 'unauthenticated' } })
  }
  return user
}

export async function requireRole(role: Role, store?: typeof authStore): Promise<User> {
  const user = await requireAuth(store)
  if (!user.roles.includes(role)) {
    throw redirect({ to: '/auth/callback', search: { reason: 'forbidden' } })
  }
  return user
}
