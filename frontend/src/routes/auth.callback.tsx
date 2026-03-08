import { useEffect } from 'react'
import { createRoute } from '@tanstack/react-router'
import { authStore } from '@/auth/store'
import { Route as RootRoute } from './__root'

export const Route = createRoute({
  getParentRoute: () => RootRoute,
  path: '/auth/callback',
  component: CallbackHandler,
})

function CallbackHandler() {
  useEffect(() => {
    authStore.getState().ensureSession()
  }, [])

  return null
}
