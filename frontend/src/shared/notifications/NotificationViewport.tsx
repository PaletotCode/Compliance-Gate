import { AlertCircle, CheckCircle2, Info, TriangleAlert, X } from 'lucide-react'
import { dismissNotification, notificationStore, type NotificationTone } from '@/shared/notifications/notificationStore'

const toneStyles: Record<NotificationTone, string> = {
  info: 'border-[#3b82f6]/35 bg-[#111827]/88 text-slate-100',
  success: 'border-[#10b981]/35 bg-[#052e2a]/92 text-emerald-100',
  warning: 'border-[#f59e0b]/35 bg-[#2b1e08]/92 text-amber-100',
  error: 'border-[#ef4444]/35 bg-[#2f1010]/92 text-rose-100',
}

const toneIconStyles: Record<NotificationTone, string> = {
  info: 'text-blue-300',
  success: 'text-emerald-300',
  warning: 'text-amber-300',
  error: 'text-rose-300',
}

function toneIcon(tone: NotificationTone) {
  if (tone === 'success') return CheckCircle2
  if (tone === 'warning') return TriangleAlert
  if (tone === 'error') return AlertCircle
  return Info
}

export function NotificationViewport() {
  const notifications = notificationStore((state) => state.notifications)

  if (notifications.length === 0) {
    return null
  }

  return (
    <div className="fixed top-5 right-5 z-[220] flex w-[min(92vw,360px)] flex-col gap-3 pointer-events-none">
      {notifications.map((item, index) => {
        const Icon = toneIcon(item.tone)
        return (
          <div
            key={item.id}
            className={`pointer-events-auto rounded-xl border px-4 py-3 shadow-[0_16px_50px_rgba(0,0,0,0.45)] backdrop-blur-xl animate-in slide-in-from-right-8 fade-in duration-300 ${toneStyles[item.tone]}`}
            style={{ animationDelay: `${index * 50}ms` }}
            role="status"
            aria-live="polite"
          >
            <div className="flex items-start gap-3">
              <Icon size={18} className={`mt-0.5 shrink-0 ${toneIconStyles[item.tone]}`} />

              <div className="min-w-0 flex-1">
                {item.title && (
                  <p className="text-[11px] font-black uppercase tracking-[0.12em] opacity-90">{item.title}</p>
                )}
                <p className="mt-0.5 text-[12px] font-semibold leading-relaxed text-white/95 break-words">{item.message}</p>
              </div>

              <button
                type="button"
                onClick={() => dismissNotification(item.id)}
                className="rounded-md p-1 text-white/65 hover:text-white hover:bg-white/10 transition-colors"
                aria-label="Fechar notificação"
              >
                <X size={14} />
              </button>
            </div>
          </div>
        )
      })}
    </div>
  )
}
