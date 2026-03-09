import { CalendarDays, CheckSquare, FileSpreadsheet, Square } from 'lucide-react'
import { StatusBadge } from '@/main_view/components/layout/StatusBadge'
import type { SourceConfig, SourceItem } from '@/main_view/state/types'

type SourceCardProps = {
  source: SourceItem
  index: number
  config: SourceConfig
  isSelectionMode: boolean
  isSelected: boolean
  onOpenSource: (sourceId: SourceItem['id']) => void
}

export function SourceCard({
  source,
  index,
  config,
  isSelectionMode,
  isSelected,
  onOpenSource,
}: SourceCardProps) {
  return (
    <div
      onClick={() => onOpenSource(source.id)}
      className={`aspect-[4/3] rounded-3xl border bg-black/40 backdrop-blur-xl p-6 flex flex-col justify-between shadow-[0_8px_32px_rgba(0,0,0,0.3)] transition-all cursor-pointer group animate-in zoom-in-95 duration-500 fill-mode-both
                  ${
                    isSelectionMode && isSelected
                      ? 'border-[#00AE9D] shadow-[0_0_25px_rgba(0,174,157,0.2)] bg-[#00AE9D]/5'
                      : 'border-white/10 hover:border-white/30 hover:bg-black/60'
                  }`}
      style={{ animationDelay: `${index * 100}ms` }}
    >
      <div className="flex justify-between items-start">
        <div
          className={`w-12 h-12 rounded-2xl flex items-center justify-center transition-all shadow-inner group-hover:scale-105
                    ${
                      isSelectionMode && isSelected
                        ? 'bg-[#00AE9D] text-white border-none'
                        : 'bg-white/5 border border-white/10 text-white/70 group-hover:text-white'
                    }`}
        >
          <FileSpreadsheet size={24} />
        </div>

        {isSelectionMode ? (
          <div className="text-white/50">
            {isSelected ? <CheckSquare size={20} className="text-[#00AE9D]" /> : <Square size={20} />}
          </div>
        ) : (
          <StatusBadge status={config.status} />
        )}
      </div>

      <div className="space-y-2">
        <h3 className="text-base font-black text-white tracking-wide">{source.name}</h3>
        <div className="flex items-center gap-3">
          <p className="text-[10px] text-white/40 font-mono uppercase tracking-widest bg-white/5 px-2 py-1 rounded-md">
            {source.type} • 15 LINHAS
          </p>
          <p className="text-[10px] text-white/30 font-medium flex items-center gap-1">
            <CalendarDays size={10} /> {source.createdAt.split(',')[0]}
          </p>
        </div>
      </div>
    </div>
  )
}
