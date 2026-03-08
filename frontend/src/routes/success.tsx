import { CheckCircle2 } from 'lucide-react'
import { createRoute } from '@tanstack/react-router'
import loginBackground from '@/assets/login-bg.jpg'
import { requireAuth } from '@/auth/guard'
import { Route as RootRoute } from './__root'

export const Route = createRoute({
  getParentRoute: () => RootRoute,
  path: '/success',
  beforeLoad: async ({ context }) => {
    await requireAuth(context.auth)
  },
  component: SuccessPage,
})

function SuccessPage() {
  return (
    <div className="relative min-h-screen overflow-hidden bg-black">
      <img
        src={loginBackground}
        alt="Sicoob Corporate Background"
        className="absolute inset-0 h-full w-full object-cover brightness-[0.5] contrast-125"
      />
      <div className="absolute inset-0 bg-black/35" />
      <div className="relative z-10 flex min-h-screen items-center justify-center px-6">
        <div className="w-full max-w-lg rounded-2xl border border-white/20 bg-white/10 p-10 text-center text-white backdrop-blur-[35px]">
          <div className="mx-auto mb-6 inline-flex h-24 w-24 items-center justify-center rounded-2xl bg-[#00AE9D] text-white shadow-2xl">
            <CheckCircle2 size={52} />
          </div>
          <h1 className="text-3xl font-black tracking-tight">Autenticado com sucesso</h1>
          <p className="mt-3 text-sm font-semibold text-white/85">
            Sessão ativa no Compliance Gate.
          </p>
        </div>
      </div>
      <style>{`body { overflow: hidden; background-color: #000; }`}</style>
    </div>
  )
}
