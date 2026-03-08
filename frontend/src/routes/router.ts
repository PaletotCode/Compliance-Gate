import { createRouter } from '@tanstack/react-router'
import { authStore } from '@/auth/store'
import { Route as RootRoute } from './__root'
import { Route as IndexRoute } from './index'
import { Route as AuthCallbackRoute } from './auth.callback'

const routeTree = RootRoute.addChildren([IndexRoute, AuthCallbackRoute])

export const router = createRouter({
  routeTree,
  context: {
    auth: authStore,
  },
})

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router
  }
}
