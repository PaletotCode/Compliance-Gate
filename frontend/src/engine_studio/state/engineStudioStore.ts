import { create } from 'zustand'
import {
  archiveRuleSet,
  createRuleSet,
  createRuleSetVersion,
  createSegment,
  createSegmentFromTemplate,
  createTransformation,
  createView,
  dryRunRuleSet,
  explainRuleSetRow,
  explainRuleSetSample,
  getClassificationMode,
  getEngineCatalog,
  listClassificationDivergences,
  listClassificationMetrics,
  listRuleSets,
  listSegments,
  listSegmentTemplates,
  listTransformations,
  listViews,
  previewSegment,
  previewView,
  publishRuleSetVersion,
  rollbackRuleSet,
  runView,
  setClassificationMode,
  updateRuleSet,
  updateRuleSetVersion,
  updateSegment,
  updateTransformation,
  updateView,
  validateRuleSetPayload,
  validateRuleSetVersion,
  getRuleSet,
} from '@/engine_studio/api'
import { extractEngineErrorPayload } from '@/engine_studio/diagnostics'
import type {
  ClassificationRuntimeMode,
  EngineCatalogSnapshot,
  JsonRecord,
  RuleSetPayloadV2,
  RuleSetVersionRecord,
  ViewPayload,
} from '@/engine_studio/types'
import { pushNotification } from '@/shared/notifications/notificationStore'
import type { EngineStudioStore, EngineStudioTableState } from './types'

const DEFAULT_VIEW_NAME = 'Admin Studio Table'

function createInitialTableState(): EngineStudioTableState {
  return {
    items: [],
    columns: [],
    total_rows: 0,
    page: 0,
    size: 120,
    has_next: false,
    has_previous: false,
    is_loading_initial: false,
    is_loading_more: false,
    warnings: [],
  }
}

function buildDefaultViewPayload(catalog: EngineCatalogSnapshot): ViewPayload {
  const columns = catalog.columns.slice(0, 60).map((column) => ({
    kind: 'base' as const,
    column_name: column.name,
  }))

  return {
    schema_version: 1,
    dataset_scope: {
      mode: 'dataset_version',
      dataset_version_id: catalog.dataset_version_id,
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
    row_limit: 100000,
  }
}

function notifyEngineSuccess(message: string): void {
  pushNotification({
    tone: 'success',
    title: 'Engine Studio',
    message,
  })
}

function ensureRuleSetPayload(payload: Record<string, unknown>): RuleSetPayloadV2 {
  return payload as unknown as RuleSetPayloadV2
}

function ensureJsonRecord(payload: Record<string, unknown> | undefined): JsonRecord | undefined {
  if (!payload) return undefined
  return payload as unknown as JsonRecord
}

export const engineStudioStore = create<EngineStudioStore>()((set, get) => ({
  is_open: true,
  active_panel: 'catalog',
  dataset_version_id: null,
  is_bootstrapping: false,
  catalog: null,
  transformations: [],
  segments: [],
  segment_templates: [],
  views: [],
  rulesets: [],
  selected_view_id: null,
  selected_ruleset_id: null,
  selected_ruleset_detail: null,
  table: createInitialTableState(),
  segment_preview: null,
  view_preview: null,
  mode_state: null,
  divergences: [],
  run_metrics: [],
  last_validation_payload: null,
  last_explain_row: null,
  last_explain_sample: null,
  last_dry_run: null,
  highlighted_node_path: null,
  last_error: null,

  setOpen: (isOpen) => set({ is_open: isOpen }),

  toggleOpen: () => set((state) => ({ is_open: !state.is_open })),

  setActivePanel: (panel) => set({ active_panel: panel }),

  setHighlightedNodePath: (nodePath) => set({ highlighted_node_path: nodePath }),

  clearError: () => set({ last_error: null }),

  bootstrap: async (datasetVersionId) => {
    if (!datasetVersionId) return

    set({
      is_bootstrapping: true,
      dataset_version_id: datasetVersionId,
      last_error: null,
    })

    try {
      const [
        catalog,
        transformations,
        segmentTemplates,
        segments,
        rulesets,
        modeState,
        divergences,
        runMetrics,
      ] = await Promise.all([
        getEngineCatalog(datasetVersionId, 10),
        listTransformations(),
        listSegmentTemplates(),
        listSegments(),
        listRuleSets(false),
        getClassificationMode(),
        listClassificationDivergences({ dataset_version_id: datasetVersionId, limit: 50 }),
        listClassificationMetrics({ limit: 50 }),
      ])

      let views = await listViews()
      if (views.length === 0) {
        const defaultView = await createView({
          name: DEFAULT_VIEW_NAME,
          description: 'View padrão do Admin Studio para tabela materializada.',
          payload: buildDefaultViewPayload(catalog),
        })
        views = [defaultView]
      }

      const previousSelected = get().selected_view_id
      const nextSelected =
        (previousSelected && views.some((item) => item.id === previousSelected) && previousSelected) ||
        views[0]?.id ||
        null

      set({
        catalog,
        transformations,
        segment_templates: segmentTemplates,
        segments,
        views,
        rulesets,
        mode_state: modeState,
        divergences,
        run_metrics: runMetrics,
        selected_view_id: nextSelected,
      })

      await get().reloadTable()
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({ last_error: payload })
      pushNotification({
        tone: 'error',
        title: `Engine Studio • ${payload.code}`,
        message: payload.message,
      })
    } finally {
      set({ is_bootstrapping: false })
    }
  },

  refreshCatalog: async (datasetVersionId) => {
    const resolvedDatasetVersionId = datasetVersionId || get().dataset_version_id
    if (!resolvedDatasetVersionId) {
      throw new Error('dataset_version_id ausente para catálogo.')
    }

    try {
      const catalog = await getEngineCatalog(resolvedDatasetVersionId, 10)
      set({ catalog, dataset_version_id: resolvedDatasetVersionId })
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({ last_error: payload })
      throw error
    }
  },

  refreshTransformations: async () => {
    try {
      const transformations = await listTransformations()
      set({ transformations })
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({ last_error: payload })
      throw error
    }
  },

  createTransformation: async (input) => {
    try {
      await createTransformation({
        ...input,
        expression: ensureJsonRecord(input.expression) ?? {},
      })
      await get().refreshTransformations()
      notifyEngineSuccess('Transformation criada com sucesso.')
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({
        last_error: payload,
        highlighted_node_path: payload.node_path,
      })
      throw error
    }
  },

  updateTransformation: async (transformationId, input) => {
    try {
      await updateTransformation(transformationId, {
        ...input,
        expression: ensureJsonRecord(input.expression),
      })
      await get().refreshTransformations()
      notifyEngineSuccess('Transformation atualizada com sucesso.')
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({
        last_error: payload,
        highlighted_node_path: payload.node_path,
      })
      throw error
    }
  },

  refreshSegments: async () => {
    try {
      const [segments, templates] = await Promise.all([listSegments(), listSegmentTemplates()])
      set({
        segments,
        segment_templates: templates,
      })
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({ last_error: payload })
      throw error
    }
  },

  createSegment: async (input) => {
    try {
      await createSegment({
        ...input,
        filter_expression: ensureJsonRecord(input.filter_expression) ?? {},
      })
      await get().refreshSegments()
      notifyEngineSuccess('Segment criado com sucesso.')
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({
        last_error: payload,
        highlighted_node_path: payload.node_path,
      })
      throw error
    }
  },

  createSegmentFromTemplate: async (input) => {
    try {
      await createSegmentFromTemplate(input)
      await get().refreshSegments()
      notifyEngineSuccess('Segment criado a partir do template.')
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({ last_error: payload })
      throw error
    }
  },

  updateSegment: async (segmentId, input) => {
    try {
      await updateSegment(segmentId, {
        ...input,
        filter_expression: ensureJsonRecord(input.filter_expression),
      })
      await get().refreshSegments()
      notifyEngineSuccess('Segment atualizado com sucesso.')
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({
        last_error: payload,
        highlighted_node_path: payload.node_path,
      })
      throw error
    }
  },

  previewSegment: async (input) => {
    const datasetVersionId = get().dataset_version_id
    if (!datasetVersionId) {
      throw new Error('dataset_version_id ausente para preview de segment.')
    }

    try {
      const result = await previewSegment({
        dataset_version_id: datasetVersionId,
        segment_id: input.segment_id,
        expression: ensureJsonRecord(input.expression),
        limit: input.limit,
      })
      set({ segment_preview: result })
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({
        last_error: payload,
        highlighted_node_path: payload.node_path,
      })
      throw error
    }
  },

  refreshViews: async () => {
    try {
      const views = await listViews()
      const selected = get().selected_view_id
      const selected_view_id =
        (selected && views.some((item) => item.id === selected) && selected) || views[0]?.id || null
      set({
        views,
        selected_view_id,
      })
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({ last_error: payload })
      throw error
    }
  },

  createView: async (input) => {
    try {
      const created = await createView({
        name: input.name,
        description: input.description ?? null,
        payload: input.payload as unknown as ViewPayload,
      })
      await get().refreshViews()
      set({ selected_view_id: created.id })
      await get().reloadTable()
      notifyEngineSuccess('View criada com sucesso.')
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({
        last_error: payload,
        highlighted_node_path: payload.node_path,
      })
      throw error
    }
  },

  updateView: async (viewId, input) => {
    try {
      await updateView(viewId, {
        name: input.name,
        description: input.description,
        payload: input.payload as unknown as ViewPayload | undefined,
      })
      await get().refreshViews()
      await get().reloadTable()
      notifyEngineSuccess('View atualizada com sucesso.')
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({
        last_error: payload,
        highlighted_node_path: payload.node_path,
      })
      throw error
    }
  },

  previewView: async (input) => {
    const datasetVersionId = get().dataset_version_id
    if (!datasetVersionId) {
      throw new Error('dataset_version_id ausente para preview de view.')
    }

    try {
      const result = await previewView({
        dataset_version_id: datasetVersionId,
        view_id: input.view_id,
        inline_view_payload: input.inline_view_payload as unknown as ViewPayload | undefined,
        limit: input.limit,
      })
      set({ view_preview: result })
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({
        last_error: payload,
        highlighted_node_path: payload.node_path,
      })
      throw error
    }
  },

  selectView: async (viewId) => {
    set({ selected_view_id: viewId })
    await get().reloadTable()
  },

  reloadTable: async () => {
    const datasetVersionId = get().dataset_version_id
    const viewId = get().selected_view_id
    if (!datasetVersionId || !viewId) {
      set({ table: createInitialTableState() })
      return
    }

    set((state) => ({
      table: {
        ...state.table,
        is_loading_initial: true,
        is_loading_more: false,
      },
    }))

    try {
      const result = await runView({
        dataset_version_id: datasetVersionId,
        view_id: viewId,
        page: 1,
        size: get().table.size,
      })

      set((state) => ({
        table: {
          ...state.table,
          items: result.items,
          columns: result.columns,
          total_rows: result.total_rows,
          page: result.page,
          size: result.size,
          has_next: result.has_next,
          has_previous: result.has_previous,
          warnings: result.warnings,
          is_loading_initial: false,
          is_loading_more: false,
        },
      }))
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set((state) => ({
        last_error: payload,
        table: {
          ...state.table,
          is_loading_initial: false,
          is_loading_more: false,
        },
      }))
      throw error
    }
  },

  fetchNextTablePage: async () => {
    const { dataset_version_id: datasetVersionId, selected_view_id: viewId, table } = get()
    if (!datasetVersionId || !viewId) return
    if (table.is_loading_more || table.is_loading_initial || !table.has_next) return

    set((state) => ({
      table: {
        ...state.table,
        is_loading_more: true,
      },
    }))

    try {
      const result = await runView({
        dataset_version_id: datasetVersionId,
        view_id: viewId,
        page: table.page + 1,
        size: table.size,
      })

      set((state) => ({
        table: {
          ...state.table,
          items: [...state.table.items, ...result.items],
          columns: result.columns,
          total_rows: result.total_rows,
          page: result.page,
          size: result.size,
          has_next: result.has_next,
          has_previous: result.has_previous,
          warnings: result.warnings,
          is_loading_more: false,
        },
      }))
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set((state) => ({
        last_error: payload,
        table: {
          ...state.table,
          is_loading_more: false,
        },
      }))
      throw error
    }
  },

  refreshRuleSets: async () => {
    try {
      const rulesets = await listRuleSets(false)
      const selectedRulesetId = get().selected_ruleset_id
      const nextSelected =
        (selectedRulesetId &&
          rulesets.some((item) => item.id === selectedRulesetId) &&
          selectedRulesetId) ||
        rulesets[0]?.id ||
        null
      set({
        rulesets,
        selected_ruleset_id: nextSelected,
      })
      if (nextSelected) {
        await get().loadRuleSetDetail(nextSelected)
      } else {
        set({ selected_ruleset_detail: null })
      }
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({ last_error: payload })
      throw error
    }
  },

  createRuleSet: async (input) => {
    try {
      const record = await createRuleSet(input)
      await get().refreshRuleSets()
      set({
        selected_ruleset_id: record.id,
      })
      await get().loadRuleSetDetail(record.id)
      notifyEngineSuccess('RuleSet criado com sucesso.')
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({
        last_error: payload,
        highlighted_node_path: payload.node_path,
      })
      throw error
    }
  },

  updateRuleSet: async (rulesetId, input) => {
    try {
      await updateRuleSet(rulesetId, input)
      await get().refreshRuleSets()
      await get().loadRuleSetDetail(rulesetId)
      notifyEngineSuccess('RuleSet atualizado com sucesso.')
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({ last_error: payload })
      throw error
    }
  },

  archiveRuleSet: async (rulesetId) => {
    try {
      await archiveRuleSet(rulesetId)
      await get().refreshRuleSets()
      notifyEngineSuccess('RuleSet arquivado.')
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({ last_error: payload })
      throw error
    }
  },

  loadRuleSetDetail: async (rulesetId) => {
    if (!rulesetId) {
      set({
        selected_ruleset_id: null,
        selected_ruleset_detail: null,
      })
      return
    }

    try {
      const detail = await getRuleSet(rulesetId)
      set({
        selected_ruleset_id: rulesetId,
        selected_ruleset_detail: detail,
      })
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({ last_error: payload })
      throw error
    }
  },

  createRuleSetVersion: async (input) => {
    try {
      await createRuleSetVersion({
        ruleset_id: input.ruleset_id,
        source_version: input.source_version,
        payload: input.payload,
      })
      await get().loadRuleSetDetail(input.ruleset_id)
      await get().refreshRuleSets()
      notifyEngineSuccess('Nova versão criada.')
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({
        last_error: payload,
        highlighted_node_path: payload.node_path,
      })
      throw error
    }
  },

  updateRuleSetVersion: async (input) => {
    try {
      await updateRuleSetVersion(input)
      await get().loadRuleSetDetail(input.ruleset_id)
      await get().refreshRuleSets()
      notifyEngineSuccess('Versão do RuleSet atualizada.')
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({
        last_error: payload,
        highlighted_node_path: payload.node_path,
      })
      throw error
    }
  },

  validateRuleSetVersion: async (input) => {
    try {
      const result = await validateRuleSetVersion(input)
      await get().loadRuleSetDetail(input.ruleset_id)
      await get().refreshRuleSets()
      if (!result.validation.is_valid && result.validation.issues.length > 0) {
        const firstIssue = result.validation.issues[0]
        set({ highlighted_node_path: firstIssue.node_path ?? null })
      }
      notifyEngineSuccess('Validação de versão concluída.')
      return result.version
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({
        last_error: payload,
        highlighted_node_path: payload.node_path,
      })
      return null
    }
  },

  publishRuleSetVersion: async (input) => {
    try {
      await publishRuleSetVersion(input)
      await get().loadRuleSetDetail(input.ruleset_id)
      await get().refreshRuleSets()
      notifyEngineSuccess('Versão publicada com sucesso.')
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({ last_error: payload })
      throw error
    }
  },

  rollbackRuleSet: async (input) => {
    try {
      await rollbackRuleSet(input)
      await get().loadRuleSetDetail(input.ruleset_id)
      await get().refreshRuleSets()
      notifyEngineSuccess('Rollback concluído com sucesso.')
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({ last_error: payload })
      throw error
    }
  },

  validateRuleSetPayload: async (input) => {
    try {
      const result = await validateRuleSetPayload({
        payload: ensureRuleSetPayload(input.payload as unknown as Record<string, unknown>),
        column_types: input.column_types ?? {},
      })
      const firstIssue = result.issues[0] ?? result.warnings[0] ?? null
      set({
        last_validation_payload: result,
        highlighted_node_path: firstIssue?.node_path ?? null,
      })
      return result
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({
        last_error: payload,
        highlighted_node_path: payload.node_path,
      })
      return null
    }
  },

  explainRuleSetRow: async (input) => {
    try {
      const result = await explainRuleSetRow(input)
      set({ last_explain_row: result })
      return result
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({
        last_error: payload,
        highlighted_node_path: payload.node_path,
      })
      return null
    }
  },

  explainRuleSetSample: async (input) => {
    try {
      const result = await explainRuleSetSample(input)
      set({ last_explain_sample: result })
      return result
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({
        last_error: payload,
        highlighted_node_path: payload.node_path,
      })
      return null
    }
  },

  dryRunRuleSet: async (input) => {
    try {
      const result = await dryRunRuleSet(input)
      const firstWarning = result.warnings[0] ?? null
      set({
        last_dry_run: result,
        highlighted_node_path: firstWarning?.node_path ?? null,
      })
      return result
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({
        last_error: payload,
        highlighted_node_path: payload.node_path,
      })
      return null
    }
  },

  refreshDiagnostics: async () => {
    try {
      const datasetVersionId = get().dataset_version_id
      const [modeState, divergences, runMetrics] = await Promise.all([
        getClassificationMode(),
        listClassificationDivergences({
          limit: 100,
          dataset_version_id: datasetVersionId,
        }),
        listClassificationMetrics({ limit: 100 }),
      ])
      set({
        mode_state: modeState,
        divergences,
        run_metrics: runMetrics,
      })
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({ last_error: payload })
      throw error
    }
  },

  setRuntimeMode: async (input) => {
    try {
      await setClassificationMode(input)
      await get().refreshDiagnostics()
      notifyEngineSuccess('Modo de execução atualizado.')
    } catch (error) {
      const payload = extractEngineErrorPayload(error)
      set({ last_error: payload })
      throw error
    }
  },
}))

export function inferColumnTypesFromCatalog(
  catalog: EngineCatalogSnapshot | null,
): Record<string, string> {
  if (!catalog) return {}
  return catalog.columns.reduce<Record<string, string>>((acc, column) => {
    acc[column.name] = column.data_type
    return acc
  }, {})
}

export function pickLatestRuleSetVersion(
  versions: RuleSetVersionRecord[],
): RuleSetVersionRecord | null {
  if (versions.length === 0) return null
  const sorted = [...versions].sort((left, right) => right.version - left.version)
  return sorted[0] ?? null
}

export function pickPublishedRuleSetVersion(
  versions: RuleSetVersionRecord[],
): RuleSetVersionRecord | null {
  return versions.find((item) => item.status === 'published') ?? null
}

export function normalizeRuntimeMode(value: string | null | undefined): ClassificationRuntimeMode {
  if (value === 'shadow' || value === 'declarative' || value === 'legacy') {
    return value
  }
  return 'legacy'
}
