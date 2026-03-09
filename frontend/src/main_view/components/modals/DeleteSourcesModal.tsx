import { Trash2 } from 'lucide-react'
import { ActionButton } from '@/main_view/components/layout/ActionButton'

type DeleteSourcesModalProps = {
  isOpen: boolean
  selectedCount: number
  deleteInput: string
  requiredText: string
  onDeleteInputChange: (value: string) => void
  onCancel: () => void
  onConfirm: () => void
}

export function DeleteSourcesModal({
  isOpen,
  selectedCount,
  deleteInput,
  requiredText,
  onDeleteInputChange,
  onCancel,
  onConfirm,
}: DeleteSourcesModalProps) {
  if (!isOpen) {
    return null
  }

  return (
    <div className="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-black/80 backdrop-blur-md animate-in fade-in duration-300">
      <div className="bg-[#0A0A0A] border border-white/10 rounded-3xl p-8 max-w-lg w-full shadow-[0_20px_60px_rgba(0,0,0,0.8)] flex flex-col gap-8 animate-in zoom-in-95 duration-300">
        <div className="flex flex-col gap-3">
          <div className="w-14 h-14 rounded-full bg-red-500/10 flex items-center justify-center text-red-500 mb-2 shadow-inner">
            <Trash2 size={24} />
          </div>
          <h2 className="text-2xl font-black text-white tracking-tight">Confirmação de Exclusão</h2>
          <p className="text-sm text-white/60 leading-relaxed font-medium">
            Você está prestes a excluir definitivamente{' '}
            <strong className="text-white bg-white/10 px-1.5 py-0.5 rounded">
              {selectedCount} base(s)
            </strong>{' '}
            do Compliance Gate. Esta ação é irreversível e todos os perfis atrelados serão perdidos.
          </p>
        </div>

        <div className="bg-black/50 border border-red-500/20 p-5 rounded-2xl flex flex-col gap-4 shadow-inner">
          <label className="text-[10px] font-black text-red-400 uppercase tracking-widest">
            Para prosseguir, digite a confirmação abaixo:
          </label>
          <div className="px-4 py-3 bg-[#111] rounded-xl border border-white/5 text-sm font-mono text-white select-all text-center tracking-wide shadow-sm">
            {requiredText}
          </div>
          <input
            type="text"
            placeholder="Digite o texto aqui..."
            className="w-full bg-[#111] border border-white/10 rounded-xl h-12 px-4 text-sm text-white focus:border-red-500 outline-none transition-colors shadow-inner"
            value={deleteInput}
            onChange={(event) => onDeleteInputChange(event.target.value)}
          />
        </div>

        <div className="flex justify-end gap-3 mt-2">
          <ActionButton size="lg" variant="ghost" onClick={onCancel}>
            Cancelar
          </ActionButton>
          <ActionButton
            size="lg"
            variant="danger"
            disabled={deleteInput !== requiredText}
            onClick={onConfirm}
          >
            Sim, excluir bases
          </ActionButton>
        </div>
      </div>
    </div>
  )
}
