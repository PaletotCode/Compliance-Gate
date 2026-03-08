import { redirect } from '@tanstack/react-router'
import { authStore } from './store'
import type { Role, UserPublic } from './types'

export async function requireAuth(store?: typeof authStore) {
  const state = store ?? authStore
  const user = await state.getState().ensureSession()
  if (!user) {
    throw redirect({ to: '/auth', search: { reason: 'unauthenticated' } })
  }
  return user
}

export async function requireRole(role: Role, store?: typeof authStore): Promise<UserPublic> {
  const user = await requireAuth(store)
  if (user.role !== role) {
    throw redirect({ to: '/auth', search: { reason: 'forbidden' } })
  }
  return user
}
