import { createRoute } from '@tanstack/react-router'
import { requireAuth } from '@/auth/guard'
import { Route as RootRoute } from './__root'

export const Route = createRoute({
  getParentRoute: () => RootRoute,
  path: '/',
  beforeLoad: async ({ context }) => {
    await requireAuth(context.auth)
  },
  component: () => null,
})
