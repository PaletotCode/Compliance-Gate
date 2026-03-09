import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MainViewRoot } from '@/main_view/MainViewRoot'
import { mainViewStore } from '@/main_view/state/mainViewStore'

vi.mock('@tanstack/react-router', () => ({
  useNavigate: () => vi.fn(),
}))

describe('MainViewRoot smoke', () => {
  beforeEach(() => {
    mainViewStore.getState().resetState()
  })

  it('renders initial home empty state', () => {
    render(<MainViewRoot />)

    expect(screen.getByRole('button', { name: /Importar bases CSV/i })).toBeInTheDocument()
  })
})
