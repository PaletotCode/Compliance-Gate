import { createRoute, redirect } from '@tanstack/react-router'
import { AuthenticationCanvas } from '@/auth/ui/AuthenticationCanvas'
import { Route as RootRoute } from './__root'

export const Route = createRoute({
  getParentRoute: () => RootRoute,
  path: '/auth',
  beforeLoad: async ({ context }) => {
    const user = await context.auth.getState().ensureSession()
    if (user) {
      throw redirect({ to: '/app' })
    }
  },
  component: AuthenticationCanvas,
})
