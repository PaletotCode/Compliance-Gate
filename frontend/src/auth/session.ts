import { appConfig } from '@/lib/config'
import type { SessionMode } from './types'

const TOKEN_KEY = 'cg.auth.token'
const isBrowser = typeof window !== 'undefined'

export type TokenStore = {
  getToken: () => string | null
  setToken: (token: string) => void
  clearToken: () => void
}

export class MemoryTokenStore implements TokenStore {
  private token: string | null = null

  getToken() {
    return this.token
  }

  setToken(token: string) {
    this.token = token
  }

  clearToken() {
    this.token = null
  }
}

export class LocalStorageTokenStore implements TokenStore {
  getToken() {
    if (!isBrowser) return null
    return window.localStorage.getItem(TOKEN_KEY)
  }

  setToken(token: string) {
    if (!isBrowser) return
    window.localStorage.setItem(TOKEN_KEY, token)
  }

  clearToken() {
    if (!isBrowser) return
    window.localStorage.removeItem(TOKEN_KEY)
  }
}

type SessionSubscriber = () => void

export class SessionManager {
  private readonly mode: SessionMode
  private readonly tokenStore: TokenStore | null
  private unauthorizedSubscribers = new Set<SessionSubscriber>()

  constructor(mode: SessionMode) {
    this.mode = mode
    this.tokenStore = mode === 'bearer' ? this.resolveTokenStore() : null
  }

  private resolveTokenStore(): TokenStore {
    try {
      if (isBrowser && typeof window.localStorage !== 'undefined') {
        return new LocalStorageTokenStore()
      }
    } catch (error) {
      console.warn('LocalStorage unavailable, falling back to memory store', error)
    }
    return new MemoryTokenStore()
  }

  getToken() {
    return this.tokenStore?.getToken() ?? null
  }

  setToken(token: string) {
    if (this.mode === 'bearer') {
      this.tokenStore?.setToken(token)
    }
  }

  clearToken() {
    this.tokenStore?.clearToken()
  }

  isAuthenticated() {
    if (this.mode === 'cookie') return true
    return Boolean(this.getToken())
  }

  isCookieMode() {
    return this.mode === 'cookie'
  }

  subscribeUnauthorized(subscriber: SessionSubscriber) {
    this.unauthorizedSubscribers.add(subscriber)
    return () => this.unauthorizedSubscribers.delete(subscriber)
  }

  notifyUnauthorized() {
    this.clearToken()
    this.unauthorizedSubscribers.forEach((subscriber) => subscriber())
  }
}

export const session = new SessionManager(appConfig.sessionMode)
