import { useEffect, useMemo, useState } from 'react'
import { Plus, RefreshCcw, Save } from 'lucide-react'
import { JsonPayloadBuilder } from '@/engine_studio/builders'
import type { TransformationOutputType, TransformationRecord } from '@/engine_studio/types'
import { ActionButton } from '@/main_view/components/layout/ActionButton'

type TransformationsPanelProps = {
  items: TransformationRecord[]
  highlightedNodePath: string | null
  suggestions: string[]
  onRefresh: () => Promise<void>
  onCreate: (input: {
    name: string
    description?: string | null
    output_column_name: string
    expression: Record<string, unknown>
    output_type: TransformationOutputType
  }) => Promise<void>
  onUpdate: (
    transformationId: string,
    input: {
      name?: string
      description?: string | null
      output_column_name?: string
      expression?: Record<string, unknown>
      output_type?: TransformationOutputType
    },
  ) => Promise<void>
}

type FormState = {
  name: string
  description: string
  output_column_name: string
  output_type: TransformationOutputType
  expression_json: string
}

const DEFAULT_EXPRESSION = {
  node_type: 'column_ref',
  column: 'hostname',
}

function createEmptyForm(): FormState {
  return {
    name: '',
    description: '',
    output_column_name: '',
    output_type: 'string',
    expression_json: JSON.stringify(DEFAULT_EXPRESSION, null, 2),
  }
}

function parseJson<T>(raw: string): T {
  return JSON.parse(raw) as T
}

export function TransformationsPanel({
  items,
  highlightedNodePath,
  suggestions,
  onRefresh,
  onCreate,
  onUpdate,
}: TransformationsPanelProps) {
  const [selectedId, setSelectedId] = useState<string | null>(items[0]?.id ?? null)
  const [form, setForm] = useState<FormState>(() => createEmptyForm())
  const [formError, setFormError] = useState<string | null>(null)
  const [isSaving, setSaving] = useState(false)

  useEffect(() => {
    if (!selectedId) {
      setForm(createEmptyForm())
      return
    }
    const selected = items.find((item) => item.id === selectedId)
    if (!selected) return
    setForm({
      name: selected.name,
      description: selected.description ?? '',
      output_column_name: selected.payload.output_column_name,
      output_type: selected.payload.output_type,
      expression_json: JSON.stringify(selected.payload.expression, null, 2),
    })
    setFormError(null)
  }, [selectedId, items])

  const selected = useMemo(
    () => (selectedId ? items.find((item) => item.id === selectedId) ?? null : null),
    [selectedId, items],
  )

  const handleResetForm = () => {
    setSelectedId(null)
    setForm(createEmptyForm())
    setFormError(null)
  }

  const handleSave = async () => {
    setFormError(null)
    let expression: Record<string, unknown>
    try {
      expression = parseJson<Record<string, unknown>>(form.expression_json)
    } catch {
      setFormError('Expression JSON inválido.')
      return
    }

    if (!form.name.trim() || !form.output_column_name.trim()) {
      setFormError('Nome e output_column_name são obrigatórios.')
      return
    }

    setSaving(true)
    try {
      if (selected) {
        await onUpdate(selected.id, {
          name: form.name.trim(),
          description: form.description.trim() || null,
          output_column_name: form.output_column_name.trim(),
          output_type: form.output_type,
          expression,
        })
      } else {
        await onCreate({
          name: form.name.trim(),
          description: form.description.trim() || null,
          output_column_name: form.output_column_name.trim(),
          output_type: form.output_type,
          expression,
        })
      }
      await onRefresh()
      setFormError(null)
    } catch (error) {
      setFormError(error instanceof Error ? error.message : 'Falha ao salvar transformation.')
    } finally {
      setSaving(false)
    }
  }

  return (
    <section className="space-y-3">
      <div className="flex items-center justify-between gap-3">
        <div>
          <h3 className="text-sm font-black tracking-[0.08em] uppercase text-white/90">
            Transformations
          </h3>
          <p className="text-xs text-white/55">CRUD de colunas derivadas declarativas.</p>
        </div>

        <div className="flex gap-2">
          <ActionButton variant="secondary" size="sm" onClick={() => void onRefresh()}>
            <RefreshCcw size={13} />
            Atualizar
          </ActionButton>
          <ActionButton variant="secondary" size="sm" onClick={handleResetForm}>
            <Plus size={13} />
            Nova
          </ActionButton>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-3">
        <div className="rounded-2xl border border-white/10 bg-black/35 p-3 space-y-3">
          <label className="text-[11px] uppercase tracking-[0.1em] text-white/55 font-bold">
            Transformation existente
          </label>
          <select
            className="w-full h-10 rounded-lg border border-white/15 bg-black/50 px-3 text-sm text-white outline-none focus:border-[#00AE9D]"
            value={selectedId ?? ''}
            onChange={(event) => setSelectedId(event.target.value || null)}
          >
            <option value="">Nova transformation</option>
            {items.map((item) => (
              <option key={item.id} value={item.id}>
                {item.name}
              </option>
            ))}
          </select>

          <div className="grid grid-cols-1 gap-2">
            <input
              value={form.name}
              onChange={(event) => setForm((current) => ({ ...current, name: event.target.value }))}
              placeholder="Nome da transformation"
              className="h-10 rounded-lg border border-white/15 bg-black/50 px-3 text-sm text-white outline-none focus:border-[#00AE9D]"
            />
            <input
              value={form.description}
              onChange={(event) =>
                setForm((current) => ({ ...current, description: event.target.value }))
              }
              placeholder="Descrição"
              className="h-10 rounded-lg border border-white/15 bg-black/50 px-3 text-sm text-white outline-none focus:border-[#00AE9D]"
            />
            <input
              value={form.output_column_name}
              onChange={(event) =>
                setForm((current) => ({ ...current, output_column_name: event.target.value }))
              }
              placeholder="output_column_name"
              className="h-10 rounded-lg border border-white/15 bg-black/50 px-3 text-sm text-white outline-none focus:border-[#00AE9D]"
            />
            <select
              value={form.output_type}
              onChange={(event) =>
                setForm((current) => ({
                  ...current,
                  output_type: event.target.value as TransformationOutputType,
                }))
              }
              className="h-10 rounded-lg border border-white/15 bg-black/50 px-3 text-sm text-white outline-none focus:border-[#00AE9D]"
            >
              <option value="string">string</option>
              <option value="int">int</option>
              <option value="bool">bool</option>
              <option value="date">date</option>
            </select>
          </div>

          {formError ? (
            <p className="text-xs text-rose-300 rounded-lg border border-rose-400/30 bg-rose-400/10 px-3 py-2">
              {formError}
            </p>
          ) : null}

          <ActionButton variant="primary" size="sm" onClick={() => void handleSave()} disabled={isSaving}>
            <Save size={13} />
            {isSaving ? 'Salvando...' : selected ? 'Atualizar' : 'Criar'}
          </ActionButton>
        </div>

        <JsonPayloadBuilder
          title="Expression Builder (JSON)"
          description="Cole a AST da expression declarativa da transformation."
          value={form.expression_json}
          onChange={(value) => setForm((current) => ({ ...current, expression_json: value }))}
          nodePath={highlightedNodePath}
          suggestions={suggestions}
          minHeightClassName="min-h-[260px]"
        />
      </div>
    </section>
  )
}
