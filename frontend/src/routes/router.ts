import { createRouter } from '@tanstack/react-router'
import { authStore } from '@/auth/store'
import { Route as RootRoute } from './__root'
import { Route as IndexRoute } from './index'
import { Route as AuthCallbackRoute } from './auth.callback'
import { Route as AuthRoute } from './auth'
import { MainViewRoute } from '@/main_view/routes'
import { Route as SuccessRoute } from './success'

const routeTree = RootRoute.addChildren([
  IndexRoute,
  AuthRoute,
  SuccessRoute,
  AuthCallbackRoute,
  MainViewRoute,
])

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
