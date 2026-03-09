import { AlertCircle, CheckCircle2 } from 'lucide-react'
import type { ProfileStatus } from '@/main_view/state/types'

type StatusBadgeProps = {
  status: ProfileStatus
}

export function StatusBadge({ status }: StatusBadgeProps) {
  if (status === 'pronto') {
    return (
      <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-emerald-500/20 text-emerald-400 text-[9px] font-black uppercase tracking-wider border border-emerald-500/30 backdrop-blur-sm shadow-inner">
        <CheckCircle2 size={10} /> OK
      </span>
    )
  }

  return (
    <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-white/5 text-white/50 text-[9px] font-black uppercase tracking-wider border border-white/10 backdrop-blur-sm shadow-inner">
      <AlertCircle size={10} /> Pendente
    </span>
  )
}
