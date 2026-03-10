import { useEffect, useMemo, useState } from 'react'
import {
  CheckCircle2,
  Eye,
  FileDiff,
  FilePlus2,
  History,
  PlayCircle,
  RefreshCcw,
  RotateCcw,
  Save,
  Send,
} from 'lucide-react'
import { JsonPayloadBuilder } from '@/engine_studio/builders'
import { inferColumnTypesFromCatalog } from '@/engine_studio/state'
import type {
  ClassificationRuntimeMode,
  DryRunResult,
  EngineCatalogSnapshot,
  ExplainRowResult,
  ExplainSampleResult,
  RuleSetDetailRecord,
  RuleSetPayloadV2,
  RuleSetRecord,
  RuleSetValidationPayloadResult,
} from '@/engine_studio/types'
import { ActionButton } from '@/main_view/components/layout/ActionButton'

type RuleSetsPanelProps = {
  catalog: EngineCatalogSnapshot | null
  rulesets: RuleSetRecord[]
  selectedRuleSetId: string | null
  selectedRuleSetDetail: RuleSetDetailRecord | null
  validationResult: RuleSetValidationPayloadResult | null
  explainRowResult: ExplainRowResult | null
  explainSampleResult: ExplainSampleResult | null
  dryRunResult: DryRunResult | null
  highlightedNodePath: string | null
  suggestions: string[]
  onRefresh: () => Promise<void>
  onSelectRuleSet: (rulesetId: string | null) => Promise<void>
  onCreateRuleSet: (input: {
    name: string
    description?: string | null
    payload: RuleSetPayloadV2
  }) => Promise<void>
  onUpdateRuleSet: (
    rulesetId: string,
    input: {
      name?: string
      description?: string | null
    },
  ) => Promise<void>
  onCreateVersion: (input: {
    ruleset_id: string
    source_version?: number
    payload?: RuleSetPayloadV2
  }) => Promise<void>
  onUpdateVersion: (input: {
    ruleset_id: string
    version: number
    payload: RuleSetPayloadV2
  }) => Promise<void>
  onValidateVersion: (input: {
    ruleset_id: string
    version: number
    column_types: Record<string, string>
  }) => Promise<unknown>
  onPublishVersion: (input: { ruleset_id: string; version: number }) => Promise<void>
  onRollback: (input: { ruleset_id: string; target_version?: number }) => Promise<void>
  onValidatePayload: (input: {
    payload: RuleSetPayloadV2
    column_types?: Record<string, string>
  }) => Promise<unknown>
  onExplainRow: (input: {
    payload: RuleSetPayloadV2
    row: Record<string, unknown>
    ruleset_name?: string
    version?: number
  }) => Promise<unknown>
  onExplainSample: (input: {
    payload: RuleSetPayloadV2
    rows: Array<Record<string, unknown>>
    limit?: number
    ruleset_name?: string
    version?: number
  }) => Promise<unknown>
  onDryRun: (input: {
    payload: RuleSetPayloadV2
    rows: Array<Record<string, unknown>>
    mode: ClassificationRuntimeMode
    explain_sample_limit?: number
    ruleset_name?: string
    version?: number
  }) => Promise<unknown>
  onHighlightNodePath: (nodePath: string | null) => void
}

type FormState = {
  name: string
  description: string
  payload_json: string
  row_json: string
  sample_rows_json: string
  mode: ClassificationRuntimeMode
}

const DEFAULT_RULESET_PAYLOAD: RuleSetPayloadV2 = {
  schema_version: 2,
  blocks: [
    {
      kind: 'primary',
      entries: [
        {
          rule_key: 'default_primary',
          priority: 1,
          condition: {
            node_type: 'function_call',
            function_name: 'is_not_null',
            arguments: [{ node_type: 'column_ref', column: 'hostname' }],
          },
          output: {
            primary_status: 'COMPLIANT',
            primary_status_label: 'Compliant',
          },
        },
      ],
    },
  ],
}

const DEFAULT_ROW = {
  hostname: 'HOST-01',
  pa_code: 'PA-1',
  has_ad: true,
  has_uem: true,
  has_edr: true,
  has_asset: true,
}

function createEmptyForm(): FormState {
  return {
    name: '',
    description: '',
    payload_json: JSON.stringify(DEFAULT_RULESET_PAYLOAD, null, 2),
    row_json: JSON.stringify(DEFAULT_ROW, null, 2),
    sample_rows_json: JSON.stringify([DEFAULT_ROW], null, 2),
    mode: 'declarative',
  }
}

function parseJson<T>(raw: string): T {
  return JSON.parse(raw) as T
}

function toVersionLabel(version: number, status: string): string {
  return `v${version} • ${status}`
}

export function RuleSetsPanel({
  catalog,
  rulesets,
  selectedRuleSetId,
  selectedRuleSetDetail,
  validationResult,
  explainRowResult,
  explainSampleResult,
  dryRunResult,
  highlightedNodePath,
  suggestions,
  onRefresh,
  onSelectRuleSet,
  onCreateRuleSet,
  onUpdateRuleSet,
  onCreateVersion,
  onUpdateVersion,
  onValidateVersion,
  onPublishVersion,
  onRollback,
  onValidatePayload,
  onExplainRow,
  onExplainSample,
  onDryRun,
  onHighlightNodePath,
}: RuleSetsPanelProps) {
  const [form, setForm] = useState<FormState>(() => createEmptyForm())
  const [selectedVersion, setSelectedVersion] = useState<number | null>(null)
  const [formError, setFormError] = useState<string | null>(null)
  const [isWorking, setWorking] = useState(false)

  const selectedRuleSet = useMemo(
    () => (selectedRuleSetId ? rulesets.find((item) => item.id === selectedRuleSetId) ?? null : null),
    [selectedRuleSetId, rulesets],
  )

  useEffect(() => {
    if (!selectedRuleSetDetail) {
      setForm(createEmptyForm())
      setSelectedVersion(null)
      return
    }

    const latestVersion = [...selectedRuleSetDetail.versions].sort((a, b) => b.version - a.version)[0]
    setSelectedVersion(latestVersion?.version ?? null)
    setForm((current) => ({
      ...current,
      name: selectedRuleSetDetail.name,
      description: selectedRuleSetDetail.description ?? '',
      payload_json: JSON.stringify(latestVersion?.payload ?? DEFAULT_RULESET_PAYLOAD, null, 2),
    }))
  }, [selectedRuleSetDetail])

  useEffect(() => {
    if (!selectedRuleSetDetail || selectedVersion == null) return
    const version = selectedRuleSetDetail.versions.find((item) => item.version === selectedVersion)
    if (!version) return
    setForm((current) => ({
      ...current,
      payload_json: JSON.stringify(version.payload, null, 2),
    }))
  }, [selectedVersion, selectedRuleSetDetail])

  const parsedPayload = useMemo(() => {
    try {
      return parseJson<RuleSetPayloadV2>(form.payload_json)
    } catch {
      return null
    }
  }, [form.payload_json])

  const parsedRow = useMemo(() => {
    try {
      return parseJson<Record<string, unknown>>(form.row_json)
    } catch {
      return null
    }
  }, [form.row_json])

  const parsedSampleRows = useMemo(() => {
    try {
      return parseJson<Array<Record<string, unknown>>>(form.sample_rows_json)
    } catch {
      return null
    }
  }, [form.sample_rows_json])

  const columnTypes = useMemo(() => inferColumnTypesFromCatalog(catalog), [catalog])

  const handleCreateOrUpdateRuleSet = async () => {
    if (!parsedPayload) {
      setFormError('Payload JSON do RuleSet inválido.')
      return
    }

    if (!form.name.trim()) {
      setFormError('Nome do RuleSet é obrigatório.')
      return
    }

    setFormError(null)
    setWorking(true)
    try {
      if (selectedRuleSet) {
        await onUpdateRuleSet(selectedRuleSet.id, {
          name: form.name.trim(),
          description: form.description.trim() || null,
        })
      } else {
        await onCreateRuleSet({
          name: form.name.trim(),
          description: form.description.trim() || null,
          payload: parsedPayload,
        })
      }
      await onRefresh()
    } catch (error) {
      setFormError(error instanceof Error ? error.message : 'Falha ao salvar RuleSet.')
    } finally {
      setWorking(false)
    }
  }

  const handleCreateVersion = async () => {
    if (!selectedRuleSet) return
    setWorking(true)
    try {
      await onCreateVersion({
        ruleset_id: selectedRuleSet.id,
        source_version: selectedVersion ?? undefined,
      })
      await onRefresh()
    } catch (error) {
      setFormError(error instanceof Error ? error.message : 'Falha ao criar versão.')
    } finally {
      setWorking(false)
    }
  }

  const handleUpdateVersion = async () => {
    if (!selectedRuleSet || selectedVersion == null || !parsedPayload) return
    setWorking(true)
    try {
      await onUpdateVersion({
        ruleset_id: selectedRuleSet.id,
        version: selectedVersion,
        payload: parsedPayload,
      })
      await onRefresh()
    } catch (error) {
      setFormError(error instanceof Error ? error.message : 'Falha ao atualizar versão.')
    } finally {
      setWorking(false)
    }
  }

  const handleValidateVersion = async () => {
    if (!selectedRuleSet || selectedVersion == null) return
    try {
      await onValidateVersion({
        ruleset_id: selectedRuleSet.id,
        version: selectedVersion,
        column_types: columnTypes,
      })
      await onRefresh()
    } catch (error) {
      setFormError(error instanceof Error ? error.message : 'Falha ao validar versão.')
    }
  }

  const handlePublishVersion = async () => {
    if (!selectedRuleSet || selectedVersion == null) return
    try {
      await onPublishVersion({
        ruleset_id: selectedRuleSet.id,
        version: selectedVersion,
      })
      await onRefresh()
    } catch (error) {
      setFormError(error instanceof Error ? error.message : 'Falha ao publicar versão.')
    }
  }

  const handleRollback = async () => {
    if (!selectedRuleSet) return
    try {
      await onRollback({
        ruleset_id: selectedRuleSet.id,
        target_version: selectedVersion ?? undefined,
      })
      await onRefresh()
    } catch (error) {
      setFormError(error instanceof Error ? error.message : 'Falha ao executar rollback.')
    }
  }

  const handleValidatePayload = async () => {
    if (!parsedPayload) {
      setFormError('Payload JSON inválido para validar.')
      return
    }
    setFormError(null)
    try {
      await onValidatePayload({
        payload: parsedPayload,
        column_types: columnTypes,
      })
    } catch (error) {
      setFormError(error instanceof Error ? error.message : 'Falha ao validar payload.')
    }
  }

  const handleExplainRow = async () => {
    if (!parsedPayload || !parsedRow) {
      setFormError('Payload/row JSON inválido para explain-row.')
      return
    }
    setFormError(null)
    try {
      await onExplainRow({
        payload: parsedPayload,
        row: parsedRow,
        ruleset_name: form.name.trim() || undefined,
        version: selectedVersion ?? undefined,
      })
    } catch (error) {
      setFormError(error instanceof Error ? error.message : 'Falha ao executar explain-row.')
    }
  }

  const handleExplainSample = async () => {
    if (!parsedPayload || !parsedSampleRows) {
      setFormError('Payload/sample JSON inválido para explain-sample.')
      return
    }
    setFormError(null)
    try {
      await onExplainSample({
        payload: parsedPayload,
        rows: parsedSampleRows,
        limit: 5,
        ruleset_name: form.name.trim() || undefined,
        version: selectedVersion ?? undefined,
      })
    } catch (error) {
      setFormError(error instanceof Error ? error.message : 'Falha ao executar explain-sample.')
    }
  }

  const handleDryRun = async () => {
    if (!parsedPayload || !parsedSampleRows) {
      setFormError('Payload/sample JSON inválido para dry-run.')
      return
    }
    setFormError(null)
    try {
      await onDryRun({
        payload: parsedPayload,
        rows: parsedSampleRows,
        mode: form.mode,
        explain_sample_limit: 3,
        ruleset_name: form.name.trim() || undefined,
        version: selectedVersion ?? undefined,
      })
    } catch (error) {
      setFormError(error instanceof Error ? error.message : 'Falha ao executar dry-run.')
    }
  }

  return (
    <section className="space-y-3">
      <div className="flex items-center justify-between gap-3">
        <div>
          <h3 className="text-sm font-black tracking-[0.08em] uppercase text-white/90">RuleSets</h3>
          <p className="text-xs text-white/55">
            Versionamento, validação, publish/rollback e explain da classificação declarativa.
          </p>
        </div>

        <ActionButton variant="secondary" size="sm" onClick={() => void onRefresh()}>
          <RefreshCcw size={13} />
          Atualizar
        </ActionButton>
      </div>

      <div className="rounded-2xl border border-white/10 bg-black/35 p-3 space-y-3">
        <label className="text-[11px] uppercase tracking-[0.1em] text-white/55 font-bold">RuleSet existente</label>
        <select
          className="w-full h-10 rounded-lg border border-white/15 bg-black/50 px-3 text-sm text-white outline-none focus:border-[#00AE9D]"
          value={selectedRuleSetId ?? ''}
          onChange={(event) => void onSelectRuleSet(event.target.value || null)}
        >
          <option value="">Novo RuleSet</option>
          {rulesets.map((item) => (
            <option key={item.id} value={item.id}>
              {item.name} (v{item.active_version})
            </option>
          ))}
        </select>

        {selectedRuleSetDetail?.versions?.length ? (
          <select
            className="w-full h-10 rounded-lg border border-white/15 bg-black/50 px-3 text-sm text-white outline-none focus:border-[#00AE9D]"
            value={selectedVersion ?? ''}
            onChange={(event) => setSelectedVersion(Number(event.target.value))}
          >
            {selectedRuleSetDetail.versions
              .slice()
              .sort((left, right) => right.version - left.version)
              .map((version) => (
                <option key={version.version} value={version.version}>
                  {toVersionLabel(version.version, version.status)}
                </option>
              ))}
          </select>
        ) : null}

        <div className="grid grid-cols-1 gap-2">
          <input
            value={form.name}
            onChange={(event) => setForm((current) => ({ ...current, name: event.target.value }))}
            placeholder="Nome do RuleSet"
            className="h-10 rounded-lg border border-white/15 bg-black/50 px-3 text-sm text-white outline-none focus:border-[#00AE9D]"
          />
          <input
            value={form.description}
            onChange={(event) => setForm((current) => ({ ...current, description: event.target.value }))}
            placeholder="Descrição"
            className="h-10 rounded-lg border border-white/15 bg-black/50 px-3 text-sm text-white outline-none focus:border-[#00AE9D]"
          />
        </div>

        <div className="flex flex-wrap gap-2">
          <ActionButton variant="primary" size="sm" onClick={() => void handleCreateOrUpdateRuleSet()} disabled={isWorking}>
            <Save size={13} />
            {selectedRuleSet ? 'Salvar Metadata' : 'Criar RuleSet'}
          </ActionButton>
          {selectedRuleSet ? (
            <>
              <ActionButton variant="secondary" size="sm" onClick={() => void handleCreateVersion()}>
                <FilePlus2 size={13} />
                Nova Versão
              </ActionButton>
              <ActionButton variant="secondary" size="sm" onClick={() => void handleUpdateVersion()}>
                <FileDiff size={13} />
                Atualizar Versão
              </ActionButton>
              <ActionButton variant="secondary" size="sm" onClick={() => void handleValidateVersion()}>
                <CheckCircle2 size={13} />
                Validar Versão
              </ActionButton>
              <ActionButton variant="secondary" size="sm" onClick={() => void handlePublishVersion()}>
                <Send size={13} />
                Publicar
              </ActionButton>
              <ActionButton variant="secondary" size="sm" onClick={() => void handleRollback()}>
                <History size={13} />
                Rollback
              </ActionButton>
            </>
          ) : null}
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
          <ActionButton variant="secondary" size="sm" onClick={() => void handleValidatePayload()}>
            <CheckCircle2 size={13} />
            validate-ruleset
          </ActionButton>
          <ActionButton variant="secondary" size="sm" onClick={() => void handleExplainRow()}>
            <Eye size={13} />
            explain-row
          </ActionButton>
          <ActionButton variant="secondary" size="sm" onClick={() => void handleExplainSample()}>
            <Eye size={13} />
            explain-sample
          </ActionButton>
          <ActionButton variant="secondary" size="sm" onClick={() => void handleDryRun()}>
            <PlayCircle size={13} />
            dry-run-ruleset
          </ActionButton>
        </div>

        <div className="flex items-center gap-2">
          <label className="text-[11px] text-white/65">Modo dry-run:</label>
          <select
            value={form.mode}
            onChange={(event) =>
              setForm((current) => ({
                ...current,
                mode: event.target.value as ClassificationRuntimeMode,
              }))
            }
            className="h-9 rounded-lg border border-white/15 bg-black/50 px-2 text-xs text-white outline-none focus:border-[#00AE9D]"
          >
            <option value="legacy">legacy</option>
            <option value="shadow">shadow</option>
            <option value="declarative">declarative</option>
          </select>
        </div>

        {formError ? (
          <p className="text-xs text-rose-300 rounded-lg border border-rose-400/30 bg-rose-400/10 px-3 py-2">
            {formError}
          </p>
        ) : null}
      </div>

      <JsonPayloadBuilder
        title="RuleSet Builder (JSON)"
        description="Payload completo do RuleSet v2 (special/primary/flags)."
        value={form.payload_json}
        onChange={(value) => setForm((current) => ({ ...current, payload_json: value }))}
        nodePath={highlightedNodePath}
        suggestions={suggestions}
        minHeightClassName="min-h-[280px]"
      />

      <JsonPayloadBuilder
        title="Explain Row Input"
        value={form.row_json}
        onChange={(value) => setForm((current) => ({ ...current, row_json: value }))}
        nodePath={highlightedNodePath}
        minHeightClassName="min-h-[150px]"
      />

      <JsonPayloadBuilder
        title="Explain Sample Input"
        value={form.sample_rows_json}
        onChange={(value) => setForm((current) => ({ ...current, sample_rows_json: value }))}
        nodePath={highlightedNodePath}
        minHeightClassName="min-h-[150px]"
      />

      {validationResult ? (
        <div className="rounded-2xl border border-white/10 bg-black/35 p-3 space-y-2">
          <h4 className="text-xs font-black tracking-[0.1em] uppercase text-white/85 flex items-center gap-2">
            <RotateCcw size={13} />
            validate-ruleset resultado
          </h4>
          <div className="text-[11px] text-white/75">
            is_valid: {String(validationResult.is_valid)} | errors: {validationResult.summary.error_count} |
            warnings: {validationResult.summary.warning_count}
          </div>

          <div className="space-y-2">
            {[...validationResult.issues, ...validationResult.warnings].map((issue, index) => (
              <button
                type="button"
                key={`${issue.code}-${index}`}
                className="w-full text-left rounded-xl border border-white/10 bg-black/40 p-3 hover:border-[#00AE9D]/35 transition-colors"
                onClick={() => onHighlightNodePath(issue.node_path ?? null)}
              >
                <div className="text-[11px] font-bold text-white/85">
                  [{issue.stage}] {issue.code}
                </div>
                <div className="text-xs text-white/70">{issue.message}</div>
                <div className="text-[11px] text-[#63e8da] mt-1">node_path: {issue.node_path ?? '-'}</div>
              </button>
            ))}
          </div>
        </div>
      ) : null}

      {explainRowResult ? (
        <div className="rounded-2xl border border-white/10 bg-black/35 p-3 space-y-2">
          <h4 className="text-xs font-black tracking-[0.1em] uppercase text-white/85">explain-row resultado</h4>
          <div className="text-[11px] text-white/75">
            status: {explainRowResult.final_output.primary_status} | flags:{' '}
            {explainRowResult.final_output.flags.join(', ') || '-'}
          </div>
          <pre className="text-[11px] text-white/70 rounded-xl border border-white/10 bg-black/40 p-3 overflow-auto max-h-[180px] custom-scrollbar">
            {JSON.stringify(explainRowResult, null, 2)}
          </pre>
        </div>
      ) : null}

      {explainSampleResult ? (
        <div className="rounded-2xl border border-white/10 bg-black/35 p-3 space-y-2">
          <h4 className="text-xs font-black tracking-[0.1em] uppercase text-white/85">explain-sample resultado</h4>
          <div className="text-[11px] text-white/75">
            total_rows: {explainSampleResult.total_rows} | explained_rows: {explainSampleResult.explained_rows}
          </div>
          <pre className="text-[11px] text-white/70 rounded-xl border border-white/10 bg-black/40 p-3 overflow-auto max-h-[180px] custom-scrollbar">
            {JSON.stringify(explainSampleResult.rows, null, 2)}
          </pre>
        </div>
      ) : null}

      {dryRunResult ? (
        <div className="rounded-2xl border border-white/10 bg-black/35 p-3 space-y-2">
          <h4 className="text-xs font-black tracking-[0.1em] uppercase text-white/85">dry-run resultado</h4>
          <div className="text-[11px] text-white/75">
            mode: {dryRunResult.mode} | rows: {dryRunResult.rows_scanned} | divergences:{' '}
            {dryRunResult.divergences}
          </div>
          <pre className="text-[11px] text-white/70 rounded-xl border border-white/10 bg-black/40 p-3 overflow-auto max-h-[180px] custom-scrollbar">
            {JSON.stringify(dryRunResult, null, 2)}
          </pre>
        </div>
      ) : null}
    </section>
  )
}
