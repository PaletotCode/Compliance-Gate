import { useEffect, useMemo, useState } from 'react'
import { Check, Filter } from 'lucide-react'
import { ActionButton } from '@/main_view/components/layout/ActionButton'

type StatusOption = {
  key: string
  label: string
  count: number
}

type StatusMultiSelectPopoverProps = {
  options: StatusOption[]
  selectedKeys: string[]
  onClose: () => void
  onApply: (selectedKeys: string[]) => void
}

export function StatusMultiSelectPopover({
  options,
  selectedKeys,
  onClose,
  onApply,
}: StatusMultiSelectPopoverProps) {
  const [draft, setDraft] = useState<string[]>(selectedKeys)

  useEffect(() => {
    setDraft(selectedKeys)
  }, [selectedKeys])

  const allKeys = useMemo(() => options.map((option) => option.key), [options])

  const isAllSelected = options.length > 0 && draft.length === options.length

  const toggle = (key: string) => {
    setDraft((current) =>
      current.includes(key) ? current.filter((item) => item !== key) : [...current, key],
    )
  }

  const toggleAll = () => {
    setDraft(isAllSelected ? [] : allKeys)
  }

  return (
    <div
      className="absolute top-12 right-0 z-[70] w-[300px] rounded-xl border border-white/15 bg-[#0D0D0D]/95 backdrop-blur-2xl p-3 shadow-[0_18px_48px_rgba(0,0,0,0.65)]"
      onClick={(event) => event.stopPropagation()}
    >
      <div className="flex items-center gap-2 text-[10px] uppercase tracking-widest font-black text-white/60 mb-2 px-1">
        <Filter size={12} className="text-[#00AE9D]" /> Ver Status
      </div>

      <div className="max-h-64 overflow-auto custom-scrollbar rounded-lg border border-white/10 bg-black/30">
        <button
          type="button"
          onClick={toggleAll}
          className="w-full px-3 py-2.5 text-left text-xs border-b border-white/10 text-white/75 hover:bg-white/5 font-semibold"
        >
          {isAllSelected ? 'Desmarcar todos' : 'Selecionar todos'}
        </button>

        {options.map((option) => {
          const isChecked = draft.includes(option.key)
          return (
            <button
              key={option.key}
              type="button"
              onClick={() => toggle(option.key)}
              className="w-full px-3 py-2.5 text-left text-xs text-white/80 hover:bg-white/5 flex items-center justify-between gap-3 border-b border-white/5 last:border-b-0"
            >
              <span className="truncate">{option.label}</span>
              <span className="inline-flex items-center gap-2 shrink-0">
                <span className="text-[10px] font-mono text-white/45">{option.count}</span>
                <span
                  className={`w-4 h-4 rounded border flex items-center justify-center ${
                    isChecked
                      ? 'border-[#00AE9D] bg-[#00AE9D] text-white'
                      : 'border-white/35 text-transparent'
                  }`}
                >
                  <Check size={11} />
                </span>
              </span>
            </button>
          )
        })}

        {options.length === 0 && (
          <div className="px-3 py-4 text-xs text-white/45 text-center">Nenhum status disponível.</div>
        )}
      </div>

      <div className="mt-3 flex items-center justify-end gap-2">
        <ActionButton variant="ghost" size="sm" onClick={onClose}>
          Cancelar
        </ActionButton>
        <ActionButton
          variant="primary"
          size="sm"
          onClick={() => {
            onApply(draft)
            onClose()
          }}
        >
          Aplicar
        </ActionButton>
      </div>
    </div>
  )
}
