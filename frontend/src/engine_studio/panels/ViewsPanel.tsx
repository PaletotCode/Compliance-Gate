import { useEffect, useMemo, useState } from 'react'
import { Eye, ListRestart, Plus, RefreshCcw, Save } from 'lucide-react'
import { JsonPayloadBuilder } from '@/engine_studio/builders'
import type {
  EngineCatalogSnapshot,
  ViewPreviewResult,
  ViewRecord,
} from '@/engine_studio/types'
import { ActionButton } from '@/main_view/components/layout/ActionButton'

type ViewsPanelProps = {
  catalog: EngineCatalogSnapshot | null
  items: ViewRecord[]
  selectedViewId: string | null
  tableState: {
    total_rows: number
    page: number
    size: number
    has_next: boolean
    warnings: string[]
  }
  preview: ViewPreviewResult | null
  highlightedNodePath: string | null
  suggestions: string[]
  onRefresh: () => Promise<void>
  onCreate: (input: {
    name: string
    description?: string | null
    payload: Record<string, unknown>
  }) => Promise<void>
  onUpdate: (
    viewId: string,
    input: {
      name?: string
      description?: string | null
      payload?: Record<string, unknown>
    },
  ) => Promise<void>
  onPreview: (input: {
    view_id?: string
    inline_view_payload?: Record<string, unknown>
    limit?: number
  }) => Promise<void>
  onSelectView: (viewId: string | null) => Promise<void>
  onReloadTable: () => Promise<void>
}

type FormState = {
  name: string
  description: string
  payload_json: string
}

const DEFAULT_VIEW_ROW_LIMIT = 5_000

function buildDefaultPayload(catalog: EngineCatalogSnapshot | null): Record<string, unknown> {
  const datasetVersionId = catalog?.dataset_version_id || 'dataset-version-id'
  const columns = (catalog?.columns ?? []).slice(0, 40).map((column) => ({
    kind: 'base',
    column_name: column.name,
  }))

  return {
    schema_version: 1,
    dataset_scope: {
      mode: 'dataset_version',
      dataset_version_id: datasetVersionId,
    },
    columns,
    filters: {
      segment_ids: [],
      ad_hoc_expression: null,
    },
    sort: {
      column_name: 'hostname',
      direction: 'asc',
    },
    row_limit: DEFAULT_VIEW_ROW_LIMIT,
  }
}

function createEmptyForm(catalog: EngineCatalogSnapshot | null): FormState {
  return {
    name: '',
    description: '',
    payload_json: JSON.stringify(buildDefaultPayload(catalog), null, 2),
  }
}

function parseJson<T>(raw: string): T {
  return JSON.parse(raw) as T
}

export function ViewsPanel({
  catalog,
  items,
  selectedViewId,
  tableState,
  preview,
  highlightedNodePath,
  suggestions,
  onRefresh,
  onCreate,
  onUpdate,
  onPreview,
  onSelectView,
  onReloadTable,
}: ViewsPanelProps) {
  const [localSelectedId, setLocalSelectedId] = useState<string | null>(selectedViewId)
  const [form, setForm] = useState<FormState>(() => createEmptyForm(catalog))
  const [formError, setFormError] = useState<string | null>(null)
  const [isSaving, setSaving] = useState(false)

  useEffect(() => {
    setLocalSelectedId(selectedViewId)
  }, [selectedViewId])

  useEffect(() => {
    if (!localSelectedId) {
      setForm(createEmptyForm(catalog))
      return
    }
    const selected = items.find((item) => item.id === localSelectedId)
    if (!selected) return
    setForm({
      name: selected.name,
      description: selected.description ?? '',
      payload_json: JSON.stringify(selected.payload, null, 2),
    })
    setFormError(null)
  }, [localSelectedId, items, catalog])

  const selected = useMemo(
    () => (localSelectedId ? items.find((item) => item.id === localSelectedId) ?? null : null),
    [localSelectedId, items],
  )

  const handleSave = async () => {
    setFormError(null)
    let payload: Record<string, unknown>
    try {
      payload = parseJson<Record<string, unknown>>(form.payload_json)
    } catch {
      setFormError('Payload JSON da view inválido.')
      return
    }

    if (!form.name.trim()) {
      setFormError('Nome da view é obrigatório.')
      return
    }

    setSaving(true)
    try {
      if (selected) {
        await onUpdate(selected.id, {
          name: form.name.trim(),
          description: form.description.trim() || null,
          payload,
        })
      } else {
        await onCreate({
          name: form.name.trim(),
          description: form.description.trim() || null,
          payload,
        })
      }
      await onRefresh()
    } catch (error) {
      setFormError(error instanceof Error ? error.message : 'Falha ao salvar view.')
    } finally {
      setSaving(false)
    }
  }

  const handlePreview = async () => {
    if (selected) {
      try {
        await onPreview({ view_id: selected.id, limit: 20 })
      } catch (error) {
        setFormError(error instanceof Error ? error.message : 'Falha no preview da view.')
      }
      return
    }

    try {
      const payload = parseJson<Record<string, unknown>>(form.payload_json)
      await onPreview({ inline_view_payload: payload, limit: 20 })
    } catch (error) {
      if (error instanceof SyntaxError) {
        setFormError('Payload JSON inválido para preview.')
        return
      }
      setFormError(error instanceof Error ? error.message : 'Falha no preview da view.')
    }
  }

  const handleApplyToTable = async () => {
    try {
      await onSelectView(localSelectedId)
      await onReloadTable()
    } catch (error) {
      setFormError(error instanceof Error ? error.message : 'Falha ao aplicar view na tabela.')
    }
  }

  return (
    <section className="space-y-3">
      <div className="flex items-center justify-between gap-3">
        <div>
          <h3 className="text-sm font-black tracking-[0.08em] uppercase text-white/90">Views</h3>
          <p className="text-xs text-white/55">
            Tabela final do Admin Studio executa por view declarativa.
          </p>
        </div>

        <div className="flex gap-2">
          <ActionButton variant="secondary" size="sm" onClick={() => void onRefresh()}>
            <RefreshCcw size={13} />
            Atualizar
          </ActionButton>
          <ActionButton
            variant="secondary"
            size="sm"
            onClick={() => {
              setLocalSelectedId(null)
              setForm(createEmptyForm(catalog))
            }}
          >
            <Plus size={13} />
            Nova
          </ActionButton>
        </div>
      </div>

      <div className="rounded-2xl border border-white/10 bg-black/35 p-3 space-y-3">
        <label className="text-[11px] uppercase tracking-[0.1em] text-white/55 font-bold">View existente</label>
        <select
          className="w-full h-10 rounded-lg border border-white/15 bg-black/50 px-3 text-sm text-white outline-none focus:border-[#00AE9D]"
          value={localSelectedId ?? ''}
          onChange={(event) => setLocalSelectedId(event.target.value || null)}
        >
          <option value="">Nova view</option>
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
            placeholder="Nome da view"
            className="h-10 rounded-lg border border-white/15 bg-black/50 px-3 text-sm text-white outline-none focus:border-[#00AE9D]"
          />
          <input
            value={form.description}
            onChange={(event) => setForm((current) => ({ ...current, description: event.target.value }))}
            placeholder="Descrição"
            className="h-10 rounded-lg border border-white/15 bg-black/50 px-3 text-sm text-white outline-none focus:border-[#00AE9D]"
          />
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
          <ActionButton variant="secondary" size="sm" onClick={() => void handlePreview()}>
            <Eye size={13} />
            Preview
          </ActionButton>
          <ActionButton variant="secondary" size="sm" onClick={() => void handleApplyToTable()}>
            <ListRestart size={13} />
            Aplicar na tabela final
          </ActionButton>
        </div>
      </div>

      <JsonPayloadBuilder
        title="View Builder (JSON)"
        description="Defina columns, filters (segment_ids/ad_hoc_expression) e sort."
        value={form.payload_json}
        onChange={(value) => setForm((current) => ({ ...current, payload_json: value }))}
        nodePath={highlightedNodePath}
        suggestions={suggestions}
        minHeightClassName="min-h-[280px]"
      />

      <div className="rounded-2xl border border-white/10 bg-black/35 p-3 space-y-2">
        <h4 className="text-xs font-black tracking-[0.1em] uppercase text-white/85">Execução da tabela final</h4>
        <div className="grid grid-cols-2 gap-2 text-[11px] text-white/70">
          <span>Total linhas: {tableState.total_rows}</span>
          <span>
            Página: {tableState.page} • Size: {tableState.size}
          </span>
        </div>
        {tableState.warnings.length > 0 ? (
          <ul className="text-[11px] text-amber-200/90 list-disc pl-5 space-y-1">
            {tableState.warnings.map((warning) => (
              <li key={warning}>{warning}</li>
            ))}
          </ul>
        ) : null}
      </div>

      {preview ? (
        <div className="rounded-2xl border border-white/10 bg-black/35 p-3 space-y-2">
          <h4 className="text-xs font-black tracking-[0.1em] uppercase text-white/85">Preview da View</h4>
          <div className="grid grid-cols-2 gap-2 text-[11px] text-white/75">
            <span>Total: {preview.total_rows}</span>
            <span>Retornadas: {preview.returned_rows}</span>
          </div>
          <pre className="text-[11px] text-white/70 rounded-xl border border-white/10 bg-black/40 p-3 overflow-auto max-h-[180px] custom-scrollbar">
            {JSON.stringify(preview.sample_rows, null, 2)}
          </pre>
        </div>
      ) : null}
    </section>
  )
}
