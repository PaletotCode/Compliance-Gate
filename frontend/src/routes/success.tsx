import { createRoute, redirect } from '@tanstack/react-router'
import { requireAuth } from '@/auth/guard'
import { Route as RootRoute } from './__root'

export const Route = createRoute({
  getParentRoute: () => RootRoute,
  path: '/success',
  beforeLoad: async ({ context }) => {
    await requireAuth(context.auth)
    throw redirect({ to: '/app' })
  },
  component: () => null,
})
