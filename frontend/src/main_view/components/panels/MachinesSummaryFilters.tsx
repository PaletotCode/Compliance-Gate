import { Search, X } from 'lucide-react'
import { ActionButton } from '@/main_view/components/layout/ActionButton'
import type { MachineFilterDefinitionState, MachineSummaryState } from '@/main_view/state/types'

type MachinesSummaryFiltersProps = {
  summary: MachineSummaryState | null
  filterDefinitions: MachineFilterDefinitionState[]
  searchInput: string
  selectedStatuses: string[]
  selectedFlags: string[]
  onSearchChange: (value: string) => void
  onToggleStatus: (value: string) => void
  onToggleFlag: (value: string) => void
  onClearFilters: () => void
}

function getLabel(key: string, defs: MachineFilterDefinitionState[]): string {
  const found = defs.find((item) => item.key === key)
  return found?.label ?? key
}

export function MachinesSummaryFilters({
  summary,
  filterDefinitions,
  searchInput,
  selectedStatuses,
  selectedFlags,
  onSearchChange,
  onToggleStatus,
  onToggleFlag,
  onClearFilters,
}: MachinesSummaryFiltersProps) {
  const statusEntries = Object.entries(summary?.by_status ?? {})
  const flagEntries = Object.entries(summary?.by_flag ?? {})

  return (
    <div className="mb-4 rounded-2xl border border-white/10 bg-black/50 backdrop-blur-2xl p-4 space-y-4">
      <div className="grid grid-cols-1 lg:grid-cols-4 gap-3">
        <div className="rounded-xl border border-white/10 bg-white/5 p-3">
          <div className="text-[10px] uppercase tracking-widest text-white/50 font-black">Total</div>
          <div className="text-xl font-black text-white mt-1">{summary?.total ?? 0}</div>
        </div>

        {statusEntries.slice(0, 3).map(([key, value]) => (
          <div key={key} className="rounded-xl border border-white/10 bg-white/5 p-3">
            <div className="text-[10px] uppercase tracking-widest text-white/50 font-black truncate">
              {getLabel(key, filterDefinitions)}
            </div>
            <div className="text-xl font-black text-white mt-1">{value}</div>
          </div>
        ))}
      </div>

      <div className="flex flex-col lg:flex-row gap-3 lg:items-center">
        <div className="relative flex-1">
          <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-white/40" />
          <input
            type="text"
            value={searchInput}
            onChange={(event) => onSearchChange(event.target.value)}
            placeholder="Pesquisar hostname, IP, usuário..."
            className="w-full h-10 pl-9 pr-3 bg-black/40 border border-white/15 rounded-lg text-sm text-white placeholder:text-white/35 focus:border-[#00AE9D] outline-none"
          />
        </div>

        <ActionButton variant="ghost" size="sm" onClick={onClearFilters}>
          <X size={14} /> Limpar filtros
        </ActionButton>
      </div>

      <div className="space-y-2">
        <div className="text-[10px] uppercase tracking-widest text-white/45 font-black">Status</div>
        <div className="flex flex-wrap gap-2">
          {statusEntries.map(([key, count]) => {
            const isSelected = selectedStatuses.includes(key)
            return (
              <button
                key={key}
                type="button"
                data-testid={`status-filter-${key}`}
                onClick={() => onToggleStatus(key)}
                className={`px-3 py-1.5 rounded-lg text-xs font-bold border transition-colors ${
                  isSelected
                    ? 'bg-[#00AE9D]/15 text-[#00AE9D] border-[#00AE9D]/40'
                    : 'bg-white/5 text-white/70 border-white/15 hover:border-white/30'
                }`}
              >
                {getLabel(key, filterDefinitions)} ({count})
              </button>
            )
          })}
        </div>
      </div>

      <div className="space-y-2">
        <div className="text-[10px] uppercase tracking-widest text-white/45 font-black">Flags</div>
        <div className="flex flex-wrap gap-2">
          {flagEntries.map(([key, count]) => {
            const isSelected = selectedFlags.includes(key)
            return (
              <button
                key={key}
                type="button"
                data-testid={`flag-filter-${key}`}
                onClick={() => onToggleFlag(key)}
                className={`px-3 py-1.5 rounded-lg text-xs font-bold border transition-colors ${
                  isSelected
                    ? 'bg-[#00AE9D]/15 text-[#00AE9D] border-[#00AE9D]/40'
                    : 'bg-white/5 text-white/70 border-white/15 hover:border-white/30'
                }`}
              >
                {getLabel(key, filterDefinitions)} ({count})
              </button>
            )
          })}
        </div>
      </div>
    </div>
  )
}
