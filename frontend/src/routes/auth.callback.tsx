import { useEffect } from 'react'
import { createRoute } from '@tanstack/react-router'
import { session } from '@/auth/session'
import { authStore } from '@/auth/store'
import { Route as RootRoute } from './__root'

export const Route = createRoute({
  getParentRoute: () => RootRoute,
  path: '/auth/callback',
  component: CallbackHandler,
})

function CallbackHandler() {
  useEffect(() => {
    const url = new URL(window.location.href)
    const token = url.searchParams.get('token')

    if (token) {
      session.setToken(token)
      authStore.getState().ensureSession()
    }
  }, [])

  return null
}
