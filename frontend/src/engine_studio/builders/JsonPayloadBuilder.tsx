import { useMemo } from 'react'
import { AlertTriangle, Sparkles } from 'lucide-react'
import { getNodeByPath } from './nodePath'

type JsonPayloadBuilderProps = {
  title: string
  description?: string
  value: string
  onChange: (value: string) => void
  nodePath?: string | null
  suggestions?: string[]
  placeholder?: string
  minHeightClassName?: string
}

function prettyNode(node: unknown): string {
  if (node == null) return 'Nó não encontrado no payload atual.'
  try {
    return JSON.stringify(node, null, 2)
  } catch {
    return String(node)
  }
}

export function JsonPayloadBuilder({
  title,
  description,
  value,
  onChange,
  nodePath,
  suggestions = [],
  placeholder = '{\n  "schema_version": 1\n}',
  minHeightClassName = 'min-h-[220px]',
}: JsonPayloadBuilderProps) {
  const parsedPayload = useMemo(() => {
    try {
      return JSON.parse(value) as unknown
    } catch {
      return null
    }
  }, [value])

  const highlightedNode = useMemo(
    () => getNodeByPath(parsedPayload, nodePath),
    [parsedPayload, nodePath],
  )

  return (
    <section className="rounded-2xl border border-white/10 bg-black/40 p-4 space-y-3">
      <div className="space-y-1">
        <h4 className="text-xs font-black tracking-[0.12em] uppercase text-white/85">{title}</h4>
        {description ? <p className="text-xs text-white/55">{description}</p> : null}
      </div>

      <textarea
        value={value}
        onChange={(event) => onChange(event.target.value)}
        placeholder={placeholder}
        spellCheck={false}
        className={`w-full ${minHeightClassName} rounded-xl border border-white/15 bg-black/55 px-3 py-2 font-mono text-[11px] leading-relaxed text-white outline-none focus:border-[#00AE9D] custom-scrollbar`}
      />

      {nodePath ? (
        <div className="rounded-xl border border-amber-400/30 bg-amber-500/10 p-3 space-y-2">
          <div className="flex items-center gap-2 text-amber-200">
            <AlertTriangle size={14} />
            <span className="text-[11px] font-bold">Node Path em foco: {nodePath}</span>
          </div>
          <pre className="text-[11px] text-amber-100/90 overflow-auto custom-scrollbar max-h-[160px] whitespace-pre-wrap break-all">
            {prettyNode(highlightedNode)}
          </pre>
        </div>
      ) : null}

      {suggestions.length > 0 ? (
        <div className="rounded-xl border border-[#00AE9D]/30 bg-[#00AE9D]/10 p-3">
          <div className="flex items-center gap-2 text-[#63e8da] mb-2">
            <Sparkles size={14} />
            <span className="text-[11px] font-bold uppercase tracking-[0.1em]">Sugestões de coluna</span>
          </div>
          <div className="flex flex-wrap gap-2">
            {suggestions.map((item) => (
              <span
                key={item}
                className="px-2 py-1 rounded-lg border border-[#00AE9D]/35 bg-black/30 text-[11px] text-[#63e8da] font-semibold"
              >
                {item}
              </span>
            ))}
          </div>
        </div>
      ) : null}
    </section>
  )
}
