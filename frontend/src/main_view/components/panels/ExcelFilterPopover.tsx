import { useMemo, useState } from 'react'
import { Check, Search } from 'lucide-react'
import { ActionButton } from '@/main_view/components/layout/ActionButton'

type ExcelFilterPopoverProps = {
  options: string[]
  selectedOptions?: string[]
  onApply: (selection: string[]) => void
  onClose: () => void
}

export function ExcelFilterPopover({
  options,
  selectedOptions,
  onApply,
  onClose,
}: ExcelFilterPopoverProps) {
  const [search, setSearch] = useState('')
  const [localSelection, setLocalSelection] = useState<Set<string>>(
    selectedOptions ? new Set(selectedOptions) : new Set(options),
  )

  const filteredOptions = useMemo(
    () => options.filter((opt) => String(opt).toLowerCase().includes(search.toLowerCase())),
    [options, search],
  )

  const toggleOption = (option: string) => {
    const nextSelection = new Set(localSelection)
    if (nextSelection.has(option)) {
      nextSelection.delete(option)
    } else {
      nextSelection.add(option)
    }
    setLocalSelection(nextSelection)
  }

  return (
    <div
      className="absolute top-full left-0 mt-2 w-56 bg-[#111]/95 backdrop-blur-2xl border border-white/10 rounded-xl shadow-[0_15px_40px_rgba(0,0,0,0.8)] z-50 flex flex-col overflow-hidden animate-in fade-in zoom-in-95 duration-200"
      onClick={(event) => event.stopPropagation()}
    >
      <div className="p-3 border-b border-white/10">
        <div className="relative">
          <Search size={12} className="absolute left-3 top-1/2 -translate-y-1/2 text-white/40" />
          <input
            type="text"
            autoFocus
            placeholder="Pesquisar..."
            className="w-full bg-black/50 border border-white/10 rounded-lg px-8 py-2 text-xs text-white outline-none focus:border-[#00AE9D] transition-colors"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
          />
        </div>
      </div>

      <div className="max-h-48 overflow-y-auto custom-scrollbar p-2">
        <button
          type="button"
          className="w-full flex items-center gap-3 p-2 hover:bg-white/5 rounded-lg cursor-pointer transition-colors border-b border-white/5 mb-1"
          onClick={() =>
            setLocalSelection(localSelection.size === options.length ? new Set() : new Set(options))
          }
        >
          <div
            className={`w-4 h-4 rounded border flex items-center justify-center transition-colors ${
              localSelection.size === options.length
                ? 'bg-[#00AE9D] border-[#00AE9D]'
                : localSelection.size > 0
                  ? 'bg-[#00AE9D]/50 border-[#00AE9D]'
                  : 'border-white/20'
            }`}
          >
            {localSelection.size > 0 && <Check size={12} className="text-white" />}
          </div>
          <span className="text-xs font-bold text-white/90">(Selecionar Tudo)</span>
        </button>

        {filteredOptions.map((option) => (
          <button
            key={option}
            type="button"
            className="w-full flex items-center gap-3 p-2 hover:bg-white/5 rounded-lg cursor-pointer transition-colors"
            onClick={() => toggleOption(option)}
          >
            <div
              className={`w-4 h-4 rounded border flex items-center justify-center transition-colors shrink-0 ${
                localSelection.has(option) ? 'bg-[#00AE9D] border-[#00AE9D]' : 'border-white/20'
              }`}
            >
              {localSelection.has(option) && <Check size={12} className="text-white" />}
            </div>
            <span className="text-xs text-white/80 truncate">{option}</span>
          </button>
        ))}

        {filteredOptions.length === 0 && (
          <div className="p-4 text-center text-xs text-white/40 italic">Nenhum resultado</div>
        )}
      </div>

      <div className="p-3 border-t border-white/10 bg-black/40 flex justify-end gap-2">
        <ActionButton variant="ghost" size="sm" onClick={onClose}>
          Cancelar
        </ActionButton>
        <ActionButton size="sm" onClick={() => onApply(Array.from(localSelection))}>
          Aplicar
        </ActionButton>
      </div>
    </div>
  )
}
