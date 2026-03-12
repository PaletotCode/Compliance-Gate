import { describe, expect, it } from 'vitest'

import { unwrapApiEnvelope } from '@/api/envelope'

describe('api envelope', () => {
  it('unwraps plain payloads', () => {
    const payload = { ok: true }
    expect(unwrapApiEnvelope(payload)).toEqual(payload)
  })

  it('unwraps { data } envelopes', () => {
    const payload = { data: { value: 42 }, success: true }
    expect(unwrapApiEnvelope(payload)).toEqual({ value: 42 })
  })
})
