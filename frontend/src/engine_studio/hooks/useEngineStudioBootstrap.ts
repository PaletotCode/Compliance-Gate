import { useEffect } from 'react'
import { engineStudioStore } from '@/engine_studio/state'

type UseEngineStudioBootstrapParams = {
  enabled: boolean
  datasetVersionId: string | null
}

export function useEngineStudioBootstrap({
  enabled,
  datasetVersionId,
}: UseEngineStudioBootstrapParams): void {
  const bootstrap = engineStudioStore((state) => state.bootstrap)

  useEffect(() => {
    if (!enabled || !datasetVersionId) return
    void bootstrap(datasetVersionId)
  }, [enabled, datasetVersionId, bootstrap])
}
