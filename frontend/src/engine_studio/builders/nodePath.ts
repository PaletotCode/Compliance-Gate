type JsonNode = unknown

function parsePath(path: string): Array<string | number> {
  const tokens: Array<string | number> = []
  const normalized = path.trim()
  if (!normalized) return tokens

  const parts = normalized.split('.')
  parts.forEach((part) => {
    const keyMatch = part.match(/^[^\[]+/)
    if (keyMatch?.[0]) {
      tokens.push(keyMatch[0])
    }

    const indices = part.match(/\[(\d+)\]/g)
    indices?.forEach((indexToken) => {
      const value = Number(indexToken.slice(1, -1))
      if (Number.isInteger(value)) {
        tokens.push(value)
      }
    })
  })

  return tokens
}

export function getNodeByPath(root: JsonNode, path: string | null | undefined): JsonNode {
  if (!path) return null
  const tokens = parsePath(path)
  if (tokens.length === 0) return null

  let current: JsonNode = root
  for (const token of tokens) {
    if (typeof token === 'number') {
      if (!Array.isArray(current) || token < 0 || token >= current.length) {
        return null
      }
      current = current[token]
      continue
    }

    if (!current || typeof current !== 'object' || Array.isArray(current)) {
      return null
    }
    const record = current as Record<string, unknown>
    if (!(token in record)) {
      return null
    }
    current = record[token]
  }

  return current
}
