import { AlertTriangle, XCircle } from 'lucide-react'
import type { EngineErrorPayload } from '@/engine_studio/types'

type EngineErrorAlertProps = {
  error: EngineErrorPayload | null
  onClear: () => void
  onHighlightNodePath: (nodePath: string | null) => void
}

export function EngineErrorAlert({
  error,
  onClear,
  onHighlightNodePath,
}: EngineErrorAlertProps) {
  if (!error) return null

  return (
    <div className="rounded-2xl border border-rose-500/35 bg-rose-900/25 p-4 space-y-3">
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-start gap-2">
          <AlertTriangle size={16} className="text-rose-300 mt-0.5 shrink-0" />
          <div>
            <p className="text-[11px] uppercase tracking-[0.12em] font-black text-rose-200">
              {error.code}
            </p>
            <p className="text-sm font-semibold text-white/95 leading-relaxed">{error.message}</p>
          </div>
        </div>

        <button
          type="button"
          onClick={onClear}
          className="p-1.5 rounded-md text-rose-200/80 hover:text-rose-100 hover:bg-rose-400/20 transition-colors"
          aria-label="Fechar erro da engine"
        >
          <XCircle size={14} />
        </button>
      </div>

      <div className="grid grid-cols-1 gap-2">
        <div className="text-[11px] text-white/80">
          <strong className="text-rose-200">Hint:</strong> {error.hint}
        </div>

        <div className="text-[11px] text-white/75 flex items-center gap-2">
          <strong className="text-rose-200">Node:</strong>
          <code className="px-2 py-1 rounded-md bg-black/35 border border-white/10">
            {error.node_path ?? '-'}
          </code>
          {error.node_path ? (
            <button
              type="button"
              className="px-2 py-1 rounded-md border border-[#00AE9D]/35 bg-[#00AE9D]/10 text-[#63e8da] text-[11px] font-semibold"
              onClick={() => onHighlightNodePath(error.node_path)}
            >
              Destacar no Builder
            </button>
          ) : null}
        </div>
      </div>
    </div>
  )
}
