import { useMemo } from 'react'
import { ChevronRight, PanelRightClose } from 'lucide-react'
import { engineStudioStore } from '@/engine_studio/state'
import { CatalogPanel } from '@/engine_studio/panels/CatalogPanel'
import { DiagnosticsPanel } from '@/engine_studio/panels/DiagnosticsPanel'
import { RuleSetsPanel } from '@/engine_studio/panels/RuleSetsPanel'
import { SegmentsPanel } from '@/engine_studio/panels/SegmentsPanel'
import { TransformationsPanel } from '@/engine_studio/panels/TransformationsPanel'
import { ViewsPanel } from '@/engine_studio/panels/ViewsPanel'
import { EngineErrorAlert } from './EngineErrorAlert'
import { StudioPanelTabs } from './StudioPanelTabs'

export function EngineStudioDock() {
  const isOpen = engineStudioStore((state) => state.is_open)
  const activePanel = engineStudioStore((state) => state.active_panel)
  const datasetVersionId = engineStudioStore((state) => state.dataset_version_id)
  const isBootstrapping = engineStudioStore((state) => state.is_bootstrapping)
  const catalog = engineStudioStore((state) => state.catalog)
  const transformations = engineStudioStore((state) => state.transformations)
  const segments = engineStudioStore((state) => state.segments)
  const segmentTemplates = engineStudioStore((state) => state.segment_templates)
  const views = engineStudioStore((state) => state.views)
  const rulesets = engineStudioStore((state) => state.rulesets)
  const selectedViewId = engineStudioStore((state) => state.selected_view_id)
  const selectedRuleSetId = engineStudioStore((state) => state.selected_ruleset_id)
  const selectedRuleSetDetail = engineStudioStore((state) => state.selected_ruleset_detail)
  const table = engineStudioStore((state) => state.table)
  const segmentPreview = engineStudioStore((state) => state.segment_preview)
  const viewPreview = engineStudioStore((state) => state.view_preview)
  const modeState = engineStudioStore((state) => state.mode_state)
  const divergences = engineStudioStore((state) => state.divergences)
  const runMetrics = engineStudioStore((state) => state.run_metrics)
  const validationResult = engineStudioStore((state) => state.last_validation_payload)
  const explainRowResult = engineStudioStore((state) => state.last_explain_row)
  const explainSampleResult = engineStudioStore((state) => state.last_explain_sample)
  const dryRunResult = engineStudioStore((state) => state.last_dry_run)
  const highlightedNodePath = engineStudioStore((state) => state.highlighted_node_path)
  const lastError = engineStudioStore((state) => state.last_error)

  const setOpen = engineStudioStore((state) => state.setOpen)
  const setActivePanel = engineStudioStore((state) => state.setActivePanel)
  const setHighlightedNodePath = engineStudioStore((state) => state.setHighlightedNodePath)
  const clearError = engineStudioStore((state) => state.clearError)
  const refreshCatalog = engineStudioStore((state) => state.refreshCatalog)
  const refreshTransformations = engineStudioStore((state) => state.refreshTransformations)
  const createTransformation = engineStudioStore((state) => state.createTransformation)
  const updateTransformation = engineStudioStore((state) => state.updateTransformation)
  const refreshSegments = engineStudioStore((state) => state.refreshSegments)
  const createSegment = engineStudioStore((state) => state.createSegment)
  const createSegmentFromTemplate = engineStudioStore((state) => state.createSegmentFromTemplate)
  const updateSegment = engineStudioStore((state) => state.updateSegment)
  const previewSegment = engineStudioStore((state) => state.previewSegment)
  const refreshViews = engineStudioStore((state) => state.refreshViews)
  const createView = engineStudioStore((state) => state.createView)
  const updateView = engineStudioStore((state) => state.updateView)
  const previewView = engineStudioStore((state) => state.previewView)
  const selectView = engineStudioStore((state) => state.selectView)
  const reloadTable = engineStudioStore((state) => state.reloadTable)
  const refreshRuleSets = engineStudioStore((state) => state.refreshRuleSets)
  const createRuleSet = engineStudioStore((state) => state.createRuleSet)
  const updateRuleSet = engineStudioStore((state) => state.updateRuleSet)
  const loadRuleSetDetail = engineStudioStore((state) => state.loadRuleSetDetail)
  const createRuleSetVersion = engineStudioStore((state) => state.createRuleSetVersion)
  const updateRuleSetVersion = engineStudioStore((state) => state.updateRuleSetVersion)
  const validateRuleSetVersion = engineStudioStore((state) => state.validateRuleSetVersion)
  const publishRuleSetVersion = engineStudioStore((state) => state.publishRuleSetVersion)
  const rollbackRuleSet = engineStudioStore((state) => state.rollbackRuleSet)
  const validateRuleSetPayload = engineStudioStore((state) => state.validateRuleSetPayload)
  const explainRuleSetRow = engineStudioStore((state) => state.explainRuleSetRow)
  const explainRuleSetSample = engineStudioStore((state) => state.explainRuleSetSample)
  const dryRunRuleSet = engineStudioStore((state) => state.dryRunRuleSet)
  const refreshDiagnostics = engineStudioStore((state) => state.refreshDiagnostics)
  const setRuntimeMode = engineStudioStore((state) => state.setRuntimeMode)

  const suggestions = useMemo(() => {
    const fromError = lastError?.suggestions ?? []
    const fromValidation =
      validationResult?.issues
        .flatMap((issue) => {
          const raw = issue.details?.suggestions
          if (!Array.isArray(raw)) return []
          return raw
            .map((item) => (typeof item === 'string' ? item.trim() : ''))
            .filter((item) => item.length > 0)
        })
        .slice(0, 3) ?? []
    return Array.from(new Set([...fromError, ...fromValidation])).slice(0, 3)
  }, [lastError, validationResult])

  if (!isOpen) {
    return null
  }

  return (
    <aside
      data-testid="engine-studio-dock"
      className="w-[460px] border-l border-white/10 bg-black/65 backdrop-blur-2xl flex flex-col z-20 animate-in slide-in-from-right-8 duration-300"
    >
      <header className="p-4 border-b border-white/10 bg-black/35 space-y-3">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-[12px] font-black uppercase tracking-[0.14em] text-white">Engine Studio</h2>
            <p className="text-[11px] text-white/60 mt-0.5">
              Dataset: <span className="text-white/85">{datasetVersionId ?? '-'}</span>
            </p>
          </div>

          <button
            type="button"
            onClick={() => setOpen(false)}
            className="h-8 px-2.5 rounded-lg border border-white/15 bg-white/5 text-white/70 hover:text-white hover:bg-white/10 transition-colors flex items-center gap-1"
          >
            <PanelRightClose size={14} />
            Fechar
          </button>
        </div>

        <StudioPanelTabs activePanel={activePanel} onSelectPanel={setActivePanel} />

        {isBootstrapping ? (
          <div className="text-[11px] text-[#63e8da] flex items-center gap-2">
            <ChevronRight size={12} className="animate-pulse" />
            Carregando contexto declarativo...
          </div>
        ) : null}
      </header>

      <div className="p-4 space-y-3 overflow-auto custom-scrollbar">
        <EngineErrorAlert
          error={lastError}
          onClear={clearError}
          onHighlightNodePath={(nodePath) => setHighlightedNodePath(nodePath)}
        />

        {activePanel === 'catalog' ? (
          <CatalogPanel
            catalog={catalog}
            isBootstrapping={isBootstrapping}
            onRefresh={() => void refreshCatalog()}
          />
        ) : null}

        {activePanel === 'transformations' ? (
          <TransformationsPanel
            items={transformations}
            highlightedNodePath={highlightedNodePath}
            suggestions={suggestions}
            onRefresh={refreshTransformations}
            onCreate={createTransformation}
            onUpdate={updateTransformation}
          />
        ) : null}

        {activePanel === 'segments' ? (
          <SegmentsPanel
            items={segments}
            templates={segmentTemplates}
            preview={segmentPreview}
            highlightedNodePath={highlightedNodePath}
            suggestions={suggestions}
            onRefresh={refreshSegments}
            onCreate={createSegment}
            onCreateFromTemplate={createSegmentFromTemplate}
            onUpdate={updateSegment}
            onPreview={previewSegment}
          />
        ) : null}

        {activePanel === 'views' ? (
          <ViewsPanel
            catalog={catalog}
            items={views}
            selectedViewId={selectedViewId}
            tableState={{
              total_rows: table.total_rows,
              page: table.page,
              size: table.size,
              has_next: table.has_next,
              warnings: table.warnings,
            }}
            preview={viewPreview}
            highlightedNodePath={highlightedNodePath}
            suggestions={suggestions}
            onRefresh={refreshViews}
            onCreate={createView}
            onUpdate={updateView}
            onPreview={previewView}
            onSelectView={selectView}
            onReloadTable={reloadTable}
          />
        ) : null}

        {activePanel === 'rulesets' ? (
          <RuleSetsPanel
            catalog={catalog}
            rulesets={rulesets}
            selectedRuleSetId={selectedRuleSetId}
            selectedRuleSetDetail={selectedRuleSetDetail}
            validationResult={validationResult}
            explainRowResult={explainRowResult}
            explainSampleResult={explainSampleResult}
            dryRunResult={dryRunResult}
            highlightedNodePath={highlightedNodePath}
            suggestions={suggestions}
            onRefresh={refreshRuleSets}
            onSelectRuleSet={loadRuleSetDetail}
            onCreateRuleSet={createRuleSet}
            onUpdateRuleSet={updateRuleSet}
            onCreateVersion={createRuleSetVersion}
            onUpdateVersion={updateRuleSetVersion}
            onValidateVersion={validateRuleSetVersion}
            onPublishVersion={publishRuleSetVersion}
            onRollback={rollbackRuleSet}
            onValidatePayload={validateRuleSetPayload}
            onExplainRow={explainRuleSetRow}
            onExplainSample={explainRuleSetSample}
            onDryRun={dryRunRuleSet}
            onHighlightNodePath={setHighlightedNodePath}
          />
        ) : null}

        {activePanel === 'diagnostics' ? (
          <DiagnosticsPanel
            modeState={modeState}
            rulesets={rulesets}
            divergences={divergences}
            runMetrics={runMetrics}
            onRefresh={refreshDiagnostics}
            onSetRuntimeMode={setRuntimeMode}
          />
        ) : null}
      </div>
    </aside>
  )
}
