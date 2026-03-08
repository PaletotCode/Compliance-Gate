type SessionSubscriber = () => void

export class SessionManager {
  private unauthorizedSubscribers = new Set<SessionSubscriber>()

  subscribeUnauthorized(subscriber: SessionSubscriber) {
    this.unauthorizedSubscribers.add(subscriber)
    return () => this.unauthorizedSubscribers.delete(subscriber)
  }

  notifyUnauthorized() {
    this.unauthorizedSubscribers.forEach((subscriber) => subscriber())
  }
}

export const session = new SessionManager()
