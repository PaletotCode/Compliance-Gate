import { RefreshCcw } from 'lucide-react'
import type { EngineCatalogSnapshot } from '@/engine_studio/types'
import { ActionButton } from '@/main_view/components/layout/ActionButton'

type CatalogPanelProps = {
  catalog: EngineCatalogSnapshot | null
  isBootstrapping: boolean
  onRefresh: () => void
}

export function CatalogPanel({ catalog, isBootstrapping, onRefresh }: CatalogPanelProps) {
  return (
    <section className="space-y-3">
      <div className="flex items-center justify-between gap-3">
        <div>
          <h3 className="text-sm font-black tracking-[0.08em] uppercase text-white/90">
            Catálogo Materializado
          </h3>
          <p className="text-xs text-white/55">
            Tipos e cardinalidade para construir expressions sem SQL livre.
          </p>
        </div>

        <ActionButton size="sm" variant="secondary" onClick={onRefresh}>
          <RefreshCcw size={13} className={isBootstrapping ? 'animate-spin' : ''} />
          Atualizar
        </ActionButton>
      </div>

      {catalog ? (
        <div className="rounded-2xl border border-white/10 bg-black/35 p-3 space-y-3">
          <div className="grid grid-cols-2 gap-2 text-[11px] text-white/75">
            <div>
              <span className="text-white/50">Dataset:</span> {catalog.dataset_version_id}
            </div>
            <div>
              <span className="text-white/50">Linhas:</span> {catalog.row_count.toLocaleString('pt-BR')}
            </div>
          </div>

          <div className="max-h-[360px] overflow-auto custom-scrollbar rounded-xl border border-white/10">
            <table className="w-full text-xs">
              <thead className="bg-white/5 sticky top-0 z-10">
                <tr>
                  <th className="px-3 py-2 text-left text-white/60 font-black uppercase tracking-[0.09em]">Coluna</th>
                  <th className="px-3 py-2 text-left text-white/60 font-black uppercase tracking-[0.09em]">Tipo</th>
                  <th className="px-3 py-2 text-left text-white/60 font-black uppercase tracking-[0.09em]">Null %</th>
                  <th className="px-3 py-2 text-left text-white/60 font-black uppercase tracking-[0.09em]">Card.</th>
                  <th className="px-3 py-2 text-left text-white/60 font-black uppercase tracking-[0.09em]">Exemplos</th>
                </tr>
              </thead>
              <tbody>
                {catalog.columns.map((column) => (
                  <tr key={column.name} className="border-t border-white/5 align-top">
                    <td className="px-3 py-2 font-semibold text-white/90">{column.name}</td>
                    <td className="px-3 py-2 text-white/70">{column.data_type}</td>
                    <td className="px-3 py-2 text-white/70">{(column.null_rate * 100).toFixed(2)}%</td>
                    <td className="px-3 py-2 text-white/70">{column.approx_cardinality}</td>
                    <td className="px-3 py-2 text-white/65">
                      {column.sample_values.slice(0, 3).map((item) => String(item)).join(' | ') || '-'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      ) : (
        <div className="rounded-2xl border border-white/10 bg-black/35 p-6 text-center text-sm text-white/60">
          Nenhum catálogo carregado para o dataset materializado atual.
        </div>
      )}
    </section>
  )
}
