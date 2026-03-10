import { useEffect, useMemo, useState } from 'react'
import { Eye, FilePlus2, Plus, RefreshCcw, Save } from 'lucide-react'
import { JsonPayloadBuilder } from '@/engine_studio/builders'
import type { SegmentPreviewResult, SegmentRecord, SegmentTemplate } from '@/engine_studio/types'
import { ActionButton } from '@/main_view/components/layout/ActionButton'

type SegmentsPanelProps = {
  items: SegmentRecord[]
  templates: SegmentTemplate[]
  preview: SegmentPreviewResult | null
  highlightedNodePath: string | null
  suggestions: string[]
  onRefresh: () => Promise<void>
  onCreate: (input: {
    name: string
    description?: string | null
    filter_expression: Record<string, unknown>
  }) => Promise<void>
  onCreateFromTemplate: (input: {
    template_key: string
    name: string
    description?: string | null
  }) => Promise<void>
  onUpdate: (
    segmentId: string,
    input: {
      name?: string
      description?: string | null
      filter_expression?: Record<string, unknown>
    },
  ) => Promise<void>
  onPreview: (input: {
    segment_id?: string
    expression?: Record<string, unknown>
    limit?: number
  }) => Promise<void>
}

type FormState = {
  name: string
  description: string
  filter_expression_json: string
}

const DEFAULT_FILTER_EXPRESSION = {
  node_type: 'function_call',
  function_name: 'is_not_null',
  arguments: [{ node_type: 'column_ref', column: 'hostname' }],
}

function createEmptyForm(): FormState {
  return {
    name: '',
    description: '',
    filter_expression_json: JSON.stringify(DEFAULT_FILTER_EXPRESSION, null, 2),
  }
}

function parseJson<T>(raw: string): T {
  return JSON.parse(raw) as T
}

export function SegmentsPanel({
  items,
  templates,
  preview,
  highlightedNodePath,
  suggestions,
  onRefresh,
  onCreate,
  onCreateFromTemplate,
  onUpdate,
  onPreview,
}: SegmentsPanelProps) {
  const [selectedId, setSelectedId] = useState<string | null>(items[0]?.id ?? null)
  const [selectedTemplate, setSelectedTemplate] = useState<string>(templates[0]?.key ?? '')
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
      filter_expression_json: JSON.stringify(selected.payload.filter_expression, null, 2),
    })
    setFormError(null)
  }, [selectedId, items])

  const selected = useMemo(
    () => (selectedId ? items.find((item) => item.id === selectedId) ?? null : null),
    [selectedId, items],
  )

  const handleSave = async () => {
    setFormError(null)
    let filterExpression: Record<string, unknown>
    try {
      filterExpression = parseJson<Record<string, unknown>>(form.filter_expression_json)
    } catch {
      setFormError('Filter expression JSON inválido.')
      return
    }

    if (!form.name.trim()) {
      setFormError('Nome do segment é obrigatório.')
      return
    }

    setSaving(true)
    try {
      if (selected) {
        await onUpdate(selected.id, {
          name: form.name.trim(),
          description: form.description.trim() || null,
          filter_expression: filterExpression,
        })
      } else {
        await onCreate({
          name: form.name.trim(),
          description: form.description.trim() || null,
          filter_expression: filterExpression,
        })
      }
      await onRefresh()
    } catch (error) {
      setFormError(error instanceof Error ? error.message : 'Falha ao salvar segment.')
    } finally {
      setSaving(false)
    }
  }

  const handlePreviewSelected = async () => {
    if (selected) {
      try {
        await onPreview({ segment_id: selected.id, limit: 15 })
      } catch (error) {
        setFormError(error instanceof Error ? error.message : 'Falha no preview do segment.')
      }
      return
    }

    try {
      const expression = parseJson<Record<string, unknown>>(form.filter_expression_json)
      await onPreview({ expression, limit: 15 })
    } catch (error) {
      if (error instanceof SyntaxError) {
        setFormError('Filter expression JSON inválido para preview.')
        return
      }
      setFormError(error instanceof Error ? error.message : 'Falha no preview do segment.')
    }
  }

  return (
    <section className="space-y-3">
      <div className="flex items-center justify-between gap-3">
        <div>
          <h3 className="text-sm font-black tracking-[0.08em] uppercase text-white/90">Segments</h3>
          <p className="text-xs text-white/55">Filtros declarativos reutilizáveis para Views.</p>
        </div>

        <div className="flex gap-2">
          <ActionButton variant="secondary" size="sm" onClick={() => void onRefresh()}>
            <RefreshCcw size={13} />
            Atualizar
          </ActionButton>
          <ActionButton variant="secondary" size="sm" onClick={() => setSelectedId(null)}>
            <Plus size={13} />
            Novo
          </ActionButton>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-3">
        <div className="rounded-2xl border border-white/10 bg-black/35 p-3 space-y-3">
          <label className="text-[11px] uppercase tracking-[0.1em] text-white/55 font-bold">
            Segment existente
          </label>
          <select
            className="w-full h-10 rounded-lg border border-white/15 bg-black/50 px-3 text-sm text-white outline-none focus:border-[#00AE9D]"
            value={selectedId ?? ''}
            onChange={(event) => setSelectedId(event.target.value || null)}
          >
            <option value="">Novo segment</option>
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
              placeholder="Nome do segment"
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
          </div>

          <div className="grid grid-cols-1 gap-2 rounded-xl border border-white/10 bg-black/25 p-3">
            <label className="text-[11px] uppercase tracking-[0.1em] text-white/55 font-bold">
              Criar por template
            </label>
            <select
              value={selectedTemplate}
              onChange={(event) => setSelectedTemplate(event.target.value)}
              className="h-10 rounded-lg border border-white/15 bg-black/50 px-3 text-sm text-white outline-none focus:border-[#00AE9D]"
            >
              {templates.map((template) => (
                <option key={template.key} value={template.key}>
                  {template.name}
                </option>
              ))}
            </select>
            <ActionButton
              variant="secondary"
              size="sm"
              onClick={() =>
                void onCreateFromTemplate({
                  template_key: selectedTemplate,
                  name: `${form.name.trim() || 'Segment'} (template)`,
                  description: form.description.trim() || null,
                })
              }
              disabled={!selectedTemplate}
            >
              <FilePlus2 size={13} />
              Criar por template
            </ActionButton>
          </div>

          {formError ? (
            <p className="text-xs text-rose-300 rounded-lg border border-rose-400/30 bg-rose-400/10 px-3 py-2">
              {formError}
            </p>
          ) : null}

          <div className="flex flex-wrap gap-2">
            <ActionButton variant="primary" size="sm" onClick={() => void handleSave()} disabled={isSaving}>
              <Save size={13} />
              {isSaving ? 'Salvando...' : selected ? 'Atualizar' : 'Criar'}
            </ActionButton>
            <ActionButton variant="secondary" size="sm" onClick={() => void handlePreviewSelected()}>
              <Eye size={13} />
              Preview
            </ActionButton>
          </div>
        </div>

        <JsonPayloadBuilder
          title="Segment Builder (JSON)"
          description="Expression booleana usada pelo segment."
          value={form.filter_expression_json}
          onChange={(value) => setForm((current) => ({ ...current, filter_expression_json: value }))}
          nodePath={highlightedNodePath}
          suggestions={suggestions}
          minHeightClassName="min-h-[260px]"
        />

        {preview ? (
          <div className="rounded-2xl border border-white/10 bg-black/35 p-3 space-y-2">
            <h4 className="text-xs font-black tracking-[0.1em] uppercase text-white/85">
              Preview do Segment
            </h4>
            <div className="grid grid-cols-3 gap-2 text-[11px] text-white/75">
              <span>Total: {preview.total_rows}</span>
              <span>Match: {preview.matched_rows}</span>
              <span>Rate: {(preview.match_rate * 100).toFixed(2)}%</span>
            </div>
            <pre className="text-[11px] text-white/70 rounded-xl border border-white/10 bg-black/40 p-3 overflow-auto max-h-[180px] custom-scrollbar">
              {JSON.stringify(preview.sample_rows, null, 2)}
            </pre>
          </div>
        ) : null}
      </div>
    </section>
  )
}
