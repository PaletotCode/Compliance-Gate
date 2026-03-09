import { CheckCircle2, Columns, X } from 'lucide-react'

type MaterializedColumnPanelProps = {
  isOpen: boolean
  availableColumns: string[]
  activeMatCols: string[]
  onClose: () => void
  onToggleColumn: (columnKey: string) => void
}

function toLabel(key: string): string {
  return key.replace(/_/g, ' ').replace(/\b\w/g, (m) => m.toUpperCase())
}

export function MaterializedColumnPanel({
  isOpen,
  availableColumns,
  activeMatCols,
  onClose,
  onToggleColumn,
}: MaterializedColumnPanelProps) {
  if (!isOpen) {
    return null
  }

  return (
    <div className="w-[340px] border-l border-white/10 bg-black/60 backdrop-blur-2xl flex flex-col shadow-[-20px_0_40px_rgba(0,0,0,0.5)] z-20 animate-in slide-in-from-right-8 duration-300">
      <div className="p-6 border-b border-white/10 flex justify-between items-center bg-black/40">
        <div className="flex items-center gap-3">
          <Columns className="text-[#00AE9D]" size={16} />
          <h2 className="text-[11px] font-black text-white uppercase tracking-[0.15em]">
            Gerenciador de Colunas
          </h2>
        </div>
        <button
          type="button"
          onClick={onClose}
          className="text-white/50 hover:text-white transition-colors p-1.5 rounded-lg hover:bg-white/10"
        >
          <X size={16} />
        </button>
      </div>

      <div className="p-6 flex-1 overflow-auto space-y-2 custom-scrollbar">
        {availableColumns.map((column) => {
          const isChecked = activeMatCols.includes(column)
          return (
            <button
              key={column}
              type="button"
              onClick={() => onToggleColumn(column)}
              className="w-full flex items-center gap-3 p-2.5 rounded-lg hover:bg-white/5 cursor-pointer group transition-colors"
            >
              <div
                className={`w-4 h-4 rounded border flex items-center justify-center transition-colors shadow-inner shrink-0 ${
                  isChecked
                    ? 'bg-[#00AE9D] border-[#00AE9D]'
                    : 'bg-black/50 border-white/20 group-hover:border-white/40'
                }`}
              >
                {isChecked && <CheckCircle2 size={10} className="text-white" />}
              </div>
              <span className={`text-xs font-medium truncate ${isChecked ? 'text-white/90' : 'text-white/30'}`}>
                {toLabel(column)}
              </span>
            </button>
          )
        })}
      </div>
    </div>
  )
}
