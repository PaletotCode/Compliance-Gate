import { create } from 'zustand'

export type NotificationTone = 'info' | 'success' | 'warning' | 'error'

export type NotificationItem = {
  id: string
  message: string
  title?: string
  tone: NotificationTone
}

type PushNotificationInput = {
  message: string
  title?: string
  tone?: NotificationTone
  durationMs?: number
}

type NotificationStore = {
  notifications: NotificationItem[]
  push: (input: PushNotificationInput) => string
  dismiss: (id: string) => void
  clear: () => void
}

const DEFAULT_DURATION_MS = 4200
const MAX_NOTIFICATIONS = 5
const dismissTimers = new Map<string, number>()

function scheduleDismiss(id: string, durationMs: number, dismiss: (id: string) => void): void {
  const timeoutId = window.setTimeout(() => {
    dismiss(id)
  }, Math.max(1200, durationMs))
  dismissTimers.set(id, timeoutId)
}

function clearDismissTimer(id: string): void {
  const timeoutId = dismissTimers.get(id)
  if (!timeoutId) return
  window.clearTimeout(timeoutId)
  dismissTimers.delete(id)
}

function buildNotificationId(): string {
  return `ntf-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
}

function sanitizeMessage(message: string): string {
  return message.replace(/\s+/g, ' ').trim().slice(0, 240)
}

export const notificationStore = create<NotificationStore>()((set) => ({
  notifications: [],

  push: ({ message, title, tone = 'info', durationMs = DEFAULT_DURATION_MS }) => {
    const id = buildNotificationId()
    const nextItem: NotificationItem = {
      id,
      message: sanitizeMessage(message || 'Erro inesperado.'),
      title: title ? sanitizeMessage(title) : undefined,
      tone,
    }

    set((state) => ({
      notifications: [nextItem, ...state.notifications].slice(0, MAX_NOTIFICATIONS),
    }))

    scheduleDismiss(id, durationMs, (targetId) => {
      set((state) => ({
        notifications: state.notifications.filter((item) => item.id !== targetId),
      }))
      clearDismissTimer(targetId)
    })

    return id
  },

  dismiss: (id) => {
    clearDismissTimer(id)
    set((state) => ({
      notifications: state.notifications.filter((item) => item.id !== id),
    }))
  },

  clear: () => {
    Array.from(dismissTimers.keys()).forEach(clearDismissTimer)
    set({ notifications: [] })
  },
}))

export function pushNotification(input: PushNotificationInput): string {
  return notificationStore.getState().push(input)
}

export function dismissNotification(id: string): void {
  notificationStore.getState().dismiss(id)
}
