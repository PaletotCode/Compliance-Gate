import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { authStore } from '@/auth/store'
import { engineStudioStore } from '@/engine_studio/state'
import { MainMenuCanvas } from '@/main_view/ui/MainMenuCanvas'
import { mainViewStore } from '@/main_view/state/mainViewStore'

vi.mock('@/engine_studio/hooks', () => ({
  useEngineStudioBootstrap: () => undefined,
}))

describe('Engine Studio integration in materialized Admin Studio', () => {
  beforeEach(() => {
    mainViewStore.getState().resetState()
    engineStudioStore.setState({
      is_open: false,
      dataset_version_id: 'dataset-1',
      is_bootstrapping: false,
      table: {
        items: [],
        columns: [],
        total_rows: 0,
        page: 1,
        size: 120,
        has_next: false,
        has_previous: false,
        is_loading_initial: false,
        is_loading_more: false,
        warnings: [],
      },
    })

    authStore.setState({
      user: {
        id: 'ti-1',
        tenant_id: 'default',
        username: 'ti.admin',
        role: 'TI_ADMIN',
        is_active: true,
        mfa_enabled: false,
        require_password_change: false,
      },
      status: 'authenticated',
      error: null,
      challengeId: null,
      isLoading: false,
    })

    mainViewStore.setState((state) => ({
      ...state,
      view: 'materialized',
      pipeline: {
        ...state.pipeline,
        dataset_version_id: 'dataset-1',
      },
    }))
  })

  it('renders Engine Studio toggle and removes hardcoded status filter button', () => {
    render(<MainMenuCanvas />)

    expect(screen.getByRole('button', { name: /ABRIR ENGINE/i })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /VER STATUS/i })).not.toBeInTheDocument()
  })
})
