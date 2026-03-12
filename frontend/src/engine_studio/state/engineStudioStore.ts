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
  SegmentRecord,
  TransformationRecord,
  ViewColumnSpec,
  ViewRecord,
  ViewPayload,
  ViewSortSpec,
} from '@/engine_studio/types'
import { pushNotification } from '@/shared/notifications/notificationStore'
import type { EngineStudioStore, EngineStudioTableState } from './types'

const DEFAULT_VIEW_NAME = 'Admin Studio Table'
const MAX_ENGINE_VIEW_ROW_LIMIT = 10_000
const DEFAULT_ENGINE_VIEW_ROW_LIMIT = 5_000
const VIEW_NORMALIZATION_FALLBACK_COLUMNS = 60
const REPAIRABLE_VIEW_GUARDRAIL_REASONS = new Set([
  'view_column_not_found',
  'dataset_scope_mismatch',
  'sort_column_not_selected',
  'transformation_not_found',
  'segment_not_found',
  'empty_view_columns',
])

type ViewNormalizationContext = {
  datasetVersionId: string
  baseColumnNames: Set<string>
  fallbackBaseColumns: string[]
  transformationIds: Set<string>
  transformationOutputById: Map<string, string>
  segmentIds: Set<string>
}

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
    row_limit: DEFAULT_ENGINE_VIEW_ROW_LIMIT,
  }
}

function clampViewRowLimit(value: unknown): number {
  const numericValue = Number(value)
  if (!Number.isFinite(numericValue)) {
    return DEFAULT_ENGINE_VIEW_ROW_LIMIT
  }
  const normalized = Math.trunc(numericValue)
  return Math.min(Math.max(normalized, 1), MAX_ENGINE_VIEW_ROW_LIMIT)
}

function normalizeViewPayloadGuardrails(payload: ViewPayload, fallbackDatasetVersionId: string): ViewPayload {
  const resolvedDatasetVersionId =
    fallbackDatasetVersionId ||
    (payload.dataset_scope?.mode === 'dataset_version'
      ? payload.dataset_scope.dataset_version_id
      : '') ||
    'dataset-version-id'

  return {
    ...payload,
    dataset_scope: {
      mode: 'dataset_version',
      dataset_version_id: resolvedDatasetVersionId,
    },
    row_limit: clampViewRowLimit(payload.row_limit),
  }
}

function resolvePayloadDatasetVersionId(payload: ViewPayload | undefined): string | null {
  if (!payload) return null
  if (payload.dataset_scope?.mode !== 'dataset_version') return null
  return payload.dataset_scope.dataset_version_id || null
}

function uniqueStrings(values: string[]): string[] {
  return [...new Set(values)]
}

function buildViewNormalizationContext(input: {
  datasetVersionId: string
  catalog: EngineCatalogSnapshot | null
  transformations: TransformationRecord[]
  segments: SegmentRecord[]
}): ViewNormalizationContext {
  const { datasetVersionId, catalog, transformations, segments } = input
  const hasCatalogForDataset =
    Boolean(catalog) && catalog?.dataset_version_id === datasetVersionId

  const catalogColumns = hasCatalogForDataset
    ? (catalog?.columns ?? []).map((column) => column.name)
    : []

  return {
    datasetVersionId,
    baseColumnNames: new Set(catalogColumns),
    fallbackBaseColumns: catalogColumns.slice(0, VIEW_NORMALIZATION_FALLBACK_COLUMNS),
    transformationIds: new Set(transformations.map((item) => item.id)),
    transformationOutputById: new Map(
      transformations.map((item) => [item.id, item.payload.output_column_name]),
    ),
    segmentIds: new Set(segments.map((item) => item.id)),
  }
}

function resolveSelectableColumnName(
  column: ViewColumnSpec,
  transformationOutputById: Map<string, string>,
): string | null {
  if (column.kind === 'base') {
    return column.column_name
  }
  const alias = column.alias?.trim()
  if (alias) return alias
  return transformationOutputById.get(column.transformation_id) ?? null
}

function sanitizeViewPayload(
  payload: ViewPayload,
  context: ViewNormalizationContext,
): ViewPayload {
  const normalized = normalizeViewPayloadGuardrails(payload, context.datasetVersionId)
  const dedup = new Set<string>()
  const sanitizedColumns: ViewColumnSpec[] = []

  for (const column of normalized.columns ?? []) {
    if (column.kind === 'base') {
      if (
        context.baseColumnNames.size > 0 &&
        !context.baseColumnNames.has(column.column_name)
      ) {
        continue
      }
      const dedupKey = `base:${column.column_name}`
      if (dedup.has(dedupKey)) continue
      dedup.add(dedupKey)
      sanitizedColumns.push(column)
      continue
    }

    if (!context.transformationIds.has(column.transformation_id)) {
      continue
    }
    const alias = column.alias?.trim() || null
    const dedupKey = `derived:${column.transformation_id}:${alias ?? ''}`
    if (dedup.has(dedupKey)) continue
    dedup.add(dedupKey)
    sanitizedColumns.push(
      alias
        ? {
            kind: 'derived',
            transformation_id: column.transformation_id,
            alias,
          }
        : {
            kind: 'derived',
            transformation_id: column.transformation_id,
          },
    )
  }

  if (sanitizedColumns.length === 0 && context.fallbackBaseColumns.length > 0) {
    sanitizedColumns.push(
      ...context.fallbackBaseColumns.map((name) => ({
        kind: 'base' as const,
        column_name: name,
      })),
    )
  }

  const selectableColumns = new Set(
    sanitizedColumns
      .map((column) =>
        resolveSelectableColumnName(column, context.transformationOutputById),
      )
      .filter((name): name is string => Boolean(name)),
  )

  const currentSort = normalized.sort
  let sanitizedSort: ViewSortSpec | null = null
  if (currentSort) {
    if (selectableColumns.has(currentSort.column_name)) {
      sanitizedSort = {
        column_name: currentSort.column_name,
        direction: currentSort.direction === 'desc' ? 'desc' : 'asc',
      }
    } else if (selectableColumns.size > 0) {
      const fallbackSortColumn = selectableColumns.has('hostname')
        ? 'hostname'
        : [...selectableColumns][0]
      sanitizedSort = {
        column_name: fallbackSortColumn,
        direction: 'asc',
      }
    }
  }

  const filters = normalized.filters
  const sanitizedSegmentIds = uniqueStrings(
    (filters?.segment_ids ?? []).filter((segmentId) =>
      context.segmentIds.has(segmentId),
    ),
  )

  return {
    ...normalized,
    columns: sanitizedColumns,
    filters: {
      segment_ids: sanitizedSegmentIds,
      ad_hoc_expression: filters?.ad_hoc_expression ?? null,
    },
    sort: sanitizedSort,
  }
}

async function normalizeViewsGuardrails(
  views: ViewRecord[],
  context: ViewNormalizationContext,
): Promise<ViewRecord[]> {
  const normalizedViews: ViewRecord[] = []
  for (const view of views) {
    const normalizedPayload = sanitizeViewPayload(view.payload, context)
    if (JSON.stringify(normalizedPayload) === JSON.stringify(view.payload)) {
      normalizedViews.push(view)
      continue
    }

    try {
      const updated = await updateView(view.id, {
        payload: normalizedPayload,
      })
      normalizedViews.push(updated)
    } catch {
      normalizedViews.push(view)
      pushNotification({
        tone: 'warning',
        title: 'Engine Studio',
        message: `Não foi possível normalizar a view "${view.name}".`,
      })
    }
  }

  return normalizedViews
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
      const viewContext = buildViewNormalizationContext({
        datasetVersionId,
        catalog,
        transformations,
        segments,
      })
      views = await normalizeViewsGuardrails(views, viewContext)

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
      const datasetVersionId = get().dataset_version_id
      let views = await listViews()
      if (datasetVersionId) {
        const state = get()
        const viewContext = buildViewNormalizationContext({
          datasetVersionId,
          catalog: state.catalog,
          transformations: state.transformations,
          segments: state.segments,
        })
        views = await normalizeViewsGuardrails(views, viewContext)
      }
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
      const fallbackDatasetVersionId =
        get().dataset_version_id ??
        resolvePayloadDatasetVersionId(input.payload as ViewPayload) ??
        'dataset-version-id'
      const state = get()
      const viewContext = buildViewNormalizationContext({
        datasetVersionId: fallbackDatasetVersionId,
        catalog: state.catalog,
        transformations: state.transformations,
        segments: state.segments,
      })
      const created = await createView({
        name: input.name,
        description: input.description ?? null,
        payload: sanitizeViewPayload(input.payload as unknown as ViewPayload, viewContext),
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
      const fallbackDatasetVersionId =
        get().dataset_version_id ??
        resolvePayloadDatasetVersionId(input.payload as ViewPayload)
      const state = get()
      const viewContext = fallbackDatasetVersionId
        ? buildViewNormalizationContext({
            datasetVersionId: fallbackDatasetVersionId,
            catalog: state.catalog,
            transformations: state.transformations,
            segments: state.segments,
          })
        : null
      await updateView(viewId, {
        name: input.name,
        description: input.description,
        payload:
          input.payload && viewContext
            ? sanitizeViewPayload(input.payload as unknown as ViewPayload, viewContext)
            : (input.payload as unknown as ViewPayload | undefined),
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
      const state = get()
      const viewContext = buildViewNormalizationContext({
        datasetVersionId,
        catalog: state.catalog,
        transformations: state.transformations,
        segments: state.segments,
      })
      const result = await previewView({
        dataset_version_id: datasetVersionId,
        view_id: input.view_id,
        inline_view_payload: input.inline_view_payload
          ? sanitizeViewPayload(input.inline_view_payload as unknown as ViewPayload, viewContext)
          : undefined,
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
      const reason =
        typeof payload.details.reason === 'string' ? payload.details.reason : null

      if (reason && REPAIRABLE_VIEW_GUARDRAIL_REASONS.has(reason)) {
        try {
          await get().refreshViews()
          const repairedSelectedViewId = get().selected_view_id
          if (repairedSelectedViewId) {
            const repaired = await runView({
              dataset_version_id: datasetVersionId,
              view_id: repairedSelectedViewId,
              page: 1,
              size: get().table.size,
            })
            set((state) => ({
              last_error: null,
              table: {
                ...state.table,
                items: repaired.items,
                columns: repaired.columns,
                total_rows: repaired.total_rows,
                page: repaired.page,
                size: repaired.size,
                has_next: repaired.has_next,
                has_previous: repaired.has_previous,
                warnings: repaired.warnings,
                is_loading_initial: false,
                is_loading_more: false,
              },
            }))
            pushNotification({
              tone: 'warning',
              title: 'Engine Studio',
              message: 'View ajustada automaticamente para o dataset atual.',
            })
            return
          }
        } catch {
          // fallback para erro original
        }
      }

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
