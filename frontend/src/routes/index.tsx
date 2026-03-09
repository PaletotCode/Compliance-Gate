import { createRoute, redirect } from '@tanstack/react-router'
import { Route as RootRoute } from './__root'

export const Route = createRoute({
  getParentRoute: () => RootRoute,
  path: '/',
  beforeLoad: async ({ context }) => {
    const user = await context.auth.getState().ensureSession()
    if (user) {
      throw redirect({ to: '/app' })
    }
    throw redirect({ to: '/auth' })
  },
  component: () => null,
})
