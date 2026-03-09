import { useEffect } from 'react'
import { useNavigate } from '@tanstack/react-router'
import { session } from '@/auth/session'
import { MainMenuCanvas } from '@/main_view/ui/MainMenuCanvas'
import '@/main_view/styles/main_view.css'

export function MainViewRoot() {
  const navigate = useNavigate()

  useEffect(() => {
    const unsubscribe = session.subscribeUnauthorized(() => {
      void navigate({ to: '/auth', search: { reason: 'unauthenticated' } })
    })
    return () => {
      unsubscribe()
    }
  }, [navigate])

  return <MainMenuCanvas />
}
