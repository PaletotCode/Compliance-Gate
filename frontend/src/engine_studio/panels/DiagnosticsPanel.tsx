import { useMemo, useState } from 'react'
import { GaugeCircle, RefreshCcw, ShieldAlert } from 'lucide-react'
import type {
  ClassificationDivergenceRecord,
  ClassificationModeState,
  ClassificationRunMetricRecord,
  ClassificationRuntimeMode,
  RuleSetRecord,
} from '@/engine_studio/types'
import { ActionButton } from '@/main_view/components/layout/ActionButton'

type DiagnosticsPanelProps = {
  modeState: ClassificationModeState | null
  rulesets: RuleSetRecord[]
  divergences: ClassificationDivergenceRecord[]
  runMetrics: ClassificationRunMetricRecord[]
  onRefresh: () => Promise<void>
  onSetRuntimeMode: (input: {
    mode: ClassificationRuntimeMode
    ruleset_name?: string | null
  }) => Promise<void>
}

export function DiagnosticsPanel({
  modeState,
  rulesets,
  divergences,
  runMetrics,
  onRefresh,
  onSetRuntimeMode,
}: DiagnosticsPanelProps) {
  const [selectedMode, setSelectedMode] = useState<ClassificationRuntimeMode>(modeState?.mode ?? 'legacy')
  const [selectedRuleSetName, setSelectedRuleSetName] = useState<string>(modeState?.ruleset_name ?? '')
  const [isSubmitting, setSubmitting] = useState(false)

  const publishedRuleSets = useMemo(
    () => rulesets.filter((item) => item.published_version != null),
    [rulesets],
  )

  const applyMode = async () => {
    setSubmitting(true)
    try {
      await onSetRuntimeMode({
        mode: selectedMode,
        ruleset_name: selectedMode === 'legacy' ? null : selectedRuleSetName || null,
      })
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <section className="space-y-3">
      <div className="flex items-center justify-between gap-3">
        <div>
          <h3 className="text-sm font-black tracking-[0.08em] uppercase text-white/90">Diagnostics</h3>
          <p className="text-xs text-white/55">Modo de runtime, divergências shadow e métricas de execução.</p>
        </div>

        <ActionButton variant="secondary" size="sm" onClick={() => void onRefresh()}>
          <RefreshCcw size={13} />
          Atualizar
        </ActionButton>
      </div>

      <div className="rounded-2xl border border-white/10 bg-black/35 p-3 space-y-3">
        <div className="flex items-center gap-2 text-[#63e8da]">
          <GaugeCircle size={14} />
          <span className="text-[11px] font-black uppercase tracking-[0.12em]">Modo de execução</span>
        </div>

        <div className="grid grid-cols-1 gap-2">
          <select
            value={selectedMode}
            onChange={(event) => setSelectedMode(event.target.value as ClassificationRuntimeMode)}
            className="h-10 rounded-lg border border-white/15 bg-black/50 px-3 text-sm text-white outline-none focus:border-[#00AE9D]"
          >
            <option value="legacy">legacy</option>
            <option value="shadow">shadow</option>
            <option value="declarative">declarative</option>
          </select>

          <select
            value={selectedRuleSetName}
            onChange={(event) => setSelectedRuleSetName(event.target.value)}
            className="h-10 rounded-lg border border-white/15 bg-black/50 px-3 text-sm text-white outline-none focus:border-[#00AE9D]"
            disabled={selectedMode === 'legacy'}
          >
            <option value="">Selecione RuleSet publicado</option>
            {publishedRuleSets.map((item) => (
              <option key={item.id} value={item.name}>
                {item.name} (v{item.published_version})
              </option>
            ))}
          </select>
        </div>

        <div className="text-[11px] text-white/70 rounded-xl border border-white/10 bg-black/30 p-3">
          Atual: <strong>{modeState?.mode ?? '-'}</strong> • ruleset:{' '}
          <strong>{modeState?.ruleset_name ?? '-'}</strong> • source: <strong>{modeState?.source ?? '-'}</strong>
        </div>

        <ActionButton variant="primary" size="sm" disabled={isSubmitting} onClick={() => void applyMode()}>
          {isSubmitting ? 'Aplicando...' : 'Aplicar modo'}
        </ActionButton>
      </div>

      <div className="rounded-2xl border border-white/10 bg-black/35 p-3 space-y-2">
        <div className="flex items-center gap-2 text-amber-200">
          <ShieldAlert size={14} />
          <span className="text-[11px] font-black uppercase tracking-[0.12em]">Divergências recentes</span>
        </div>
        <div className="max-h-[220px] overflow-auto custom-scrollbar rounded-xl border border-white/10">
          <table className="w-full text-[11px]">
            <thead className="bg-white/5 sticky top-0 z-10">
              <tr>
                <th className="px-2 py-2 text-left text-white/60">Host</th>
                <th className="px-2 py-2 text-left text-white/60">Legacy</th>
                <th className="px-2 py-2 text-left text-white/60">Declarative</th>
                <th className="px-2 py-2 text-left text-white/60">Criado</th>
              </tr>
            </thead>
            <tbody>
              {divergences.map((item) => (
                <tr key={item.id} className="border-t border-white/5">
                  <td className="px-2 py-2 text-white/80">{item.hostname ?? '-'}</td>
                  <td className="px-2 py-2 text-white/70">{item.legacy_primary_status ?? '-'}</td>
                  <td className="px-2 py-2 text-white/70">{item.declarative_primary_status ?? '-'}</td>
                  <td className="px-2 py-2 text-white/60">{item.created_at}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="rounded-2xl border border-white/10 bg-black/35 p-3 space-y-2">
        <h4 className="text-[11px] font-black uppercase tracking-[0.12em] text-white/75">Métricas de execução</h4>
        <div className="max-h-[220px] overflow-auto custom-scrollbar rounded-xl border border-white/10">
          <table className="w-full text-[11px]">
            <thead className="bg-white/5 sticky top-0 z-10">
              <tr>
                <th className="px-2 py-2 text-left text-white/60">Run</th>
                <th className="px-2 py-2 text-left text-white/60">Mode</th>
                <th className="px-2 py-2 text-left text-white/60">Rows</th>
                <th className="px-2 py-2 text-left text-white/60">ms</th>
                <th className="px-2 py-2 text-left text-white/60">Div</th>
              </tr>
            </thead>
            <tbody>
              {runMetrics.map((item) => (
                <tr key={item.run_id} className="border-t border-white/5">
                  <td className="px-2 py-2 text-white/80">{item.run_id}</td>
                  <td className="px-2 py-2 text-white/70">{item.mode ?? '-'}</td>
                  <td className="px-2 py-2 text-white/70">{item.rows_scanned ?? '-'}</td>
                  <td className="px-2 py-2 text-white/70">{item.elapsed_ms ?? '-'}</td>
                  <td className="px-2 py-2 text-white/70">{item.divergences ?? '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  )
}
