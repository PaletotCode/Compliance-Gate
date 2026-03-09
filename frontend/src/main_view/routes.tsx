import { createRoute, redirect } from '@tanstack/react-router'
import { MainViewRoot } from '@/main_view/MainViewRoot'
import { Route as RootRoute } from '@/routes/__root'

export const MainViewRoute = createRoute({
  getParentRoute: () => RootRoute,
  path: '/app',
  beforeLoad: async ({ context }) => {
    const user = await context.auth.getState().ensureSession()
    if (!user) {
      throw redirect({ to: '/auth', search: { reason: 'unauthenticated' } })
    }
  },
  component: MainViewRoot,
})
