import { create } from 'zustand'
import {
  createProfile,
  listProfiles,
  listSources,
  previewParsed,
  previewRaw,
  promoteDefault,
  updateProfile,
} from '@/main_view/api/csvTabsApi'
import {
  exportRowsAsCsv,
  fetchMachinesFilters,
  fetchMachinesSummary,
  fetchMachinesTable,
  ingestDatasetMachines,
  materializeMachines,
  previewDatasetMachines,
  runMachinesReport,
} from '@/main_view/api/pipelineApi'
import type { CsvTabPayload } from '@/main_view/api/schemas'
import { INITIAL_SOURCES } from '@/main_view/mocks/mockData'
import type {
  ExcelFiltersState,
  MachineFilterDefinitionState,
  MachineSummaryState,
  MachinesGridState,
  MainViewMode,
  PipelineState,
  SourceConfig,
  SourceId,
  SourceItem,
  SourceRuntimeMap,
  SourceRuntimeState,
  SourceWorkflowStatus,
} from '@/main_view/state/types'

type MainViewState = {
  view: MainViewMode
  activeTab: SourceId
  sources: SourceItem[]
  sourceStates: SourceRuntimeMap
  configs: Record<SourceId, SourceConfig>
  editingTab: SourceId | null
  editTabName: string
  isSelectionMode: boolean
  selectedSources: SourceId[]
  isDeleteModalOpen: boolean
  deleteInput: string
  excelFilters: ExcelFiltersState
  openFilterMenu: string | null
  isColPanelOpen: boolean
  activeMatCols: string[]
  materializedColumns: string[]
  pipeline: PipelineState
  machinesGrid: MachinesGridState
}

type MainViewActions = {
  setView: (view: MainViewMode) => void
  setActiveTab: (sourceId: SourceId) => void
  setSelectionMode: (value: boolean) => void
  handleImportAll: () => Promise<void>
  handleOpenSource: (sourceId: SourceId) => Promise<void>
  refreshRawPreview: (sourceId: SourceId) => Promise<void>
  setHeaderRow: (sourceId: SourceId, headerRow: number) => Promise<void>
  handleSaveProfile: (sourceId: SourceId) => Promise<void>
  handlePromoteDefault: (sourceId: SourceId) => Promise<void>
  setSicColumn: (sourceId: SourceId, value: string) => void
  toggleSelectedColumn: (sourceId: SourceId, column: string) => void
  startEditingTab: (source: SourceItem) => void
  setEditTabName: (value: string) => void
  saveTabName: (sourceId: SourceId) => void
  toggleSelectionMode: () => void
  setDeleteModalOpen: (value: boolean) => void
  setDeleteInput: (value: string) => void
  handleDeleteConfirm: () => void
  setOpenFilterMenu: (value: string | null) => void
  applyExcelFilter: (tab: keyof ExcelFiltersState, col: string, selection: string[]) => void
  setColPanelOpen: (value: boolean) => void
  toggleMatCol: (colKey: string) => void
  handleRunIngest: () => Promise<void>
  reloadMachinesGrid: () => Promise<void>
  fetchNextMachinesPage: () => Promise<void>
  refreshMachinesSummaryAndFilters: () => Promise<void>
  setMachinesSearchInput: (value: string) => void
  commitMachinesSearch: () => Promise<void>
  setStatusFilters: (statuses: string[]) => Promise<void>
  toggleStatusFilter: (status: string) => Promise<void>
  toggleFlagFilter: (flag: string) => Promise<void>
  clearMachinesFilters: () => Promise<void>
  exportMachines: () => Promise<void>
  resetState: () => void
}

export type MainViewStore = MainViewState & MainViewActions

const SOURCE_ORDER: SourceId[] = ['AD', 'UEM', 'EDR', 'ASSET']
const DATASET_VERSION_STORAGE_KEY = 'cg.main_view.dataset_version_id'
const DEFAULT_MACHINE_VISIBLE_COLUMNS = [
  'hostname',
  'primary_status_label',
  'pa_code',
  'flags',
  'has_ad',
  'has_uem',
  'has_edr',
  'has_asset',
]

const SOURCE_NAME_BY_ID: Record<SourceId, string> = {
  AD: 'Active Directory',
  UEM: 'Workspace ONE (UEM)',
  EDR: 'CrowdStrike (EDR)',
  ASSET: 'GLPI (Ativos)',
}

const SOURCE_CREATED_AT_BY_ID: Record<SourceId, string> = {
  AD: '08 Mar 2026, 11:30',
  UEM: '08 Mar 2026, 11:32',
  EDR: '08 Mar 2026, 11:35',
  ASSET: '08 Mar 2026, 11:40',
}

function normalizeHeaderKey(raw: string): string {
  return raw.trim().replace(/^\uFEFF/, '').toUpperCase()
}

function isSourceId(value: string): value is SourceId {
  return SOURCE_ORDER.includes(value as SourceId)
}

function getPreviewContext(): { data_dir?: string; upload_session_id?: string } {
  const dataDir = String(import.meta.env.VITE_MAIN_VIEW_DATA_DIR ?? '').trim()
  const uploadSessionId = String(import.meta.env.VITE_MAIN_VIEW_UPLOAD_SESSION_ID ?? '').trim()

  return {
    ...(dataDir ? { data_dir: dataDir } : {}),
    ...(uploadSessionId ? { upload_session_id: uploadSessionId } : {}),
  }
}

function readPersistedDatasetVersionId(): string | null {
  if (typeof window === 'undefined') return null
  const value = window.localStorage.getItem(DATASET_VERSION_STORAGE_KEY)
  return value && value.trim() ? value : null
}

function persistDatasetVersionId(value: string | null): void {
  if (typeof window === 'undefined') return
  if (value) {
    window.localStorage.setItem(DATASET_VERSION_STORAGE_KEY, value)
  } else {
    window.localStorage.removeItem(DATASET_VERSION_STORAGE_KEY)
  }
}

function createRuntimeState(): SourceRuntimeMap {
  return {
    AD: {
      profile_id: null,
      payload: { header_row: 0, sic_column: '', selected_columns: [] },
      status: 'not_configured',
      is_default_for_source: false,
      raw_preview: null,
      parsed_preview: null,
      is_loading_raw: false,
      is_saving_profile: false,
    },
    UEM: {
      profile_id: null,
      payload: { header_row: 0, sic_column: '', selected_columns: [] },
      status: 'not_configured',
      is_default_for_source: false,
      raw_preview: null,
      parsed_preview: null,
      is_loading_raw: false,
      is_saving_profile: false,
    },
    EDR: {
      profile_id: null,
      payload: { header_row: 0, sic_column: '', selected_columns: [] },
      status: 'not_configured',
      is_default_for_source: false,
      raw_preview: null,
      parsed_preview: null,
      is_loading_raw: false,
      is_saving_profile: false,
    },
    ASSET: {
      profile_id: null,
      payload: { header_row: 0, sic_column: '', selected_columns: [] },
      status: 'not_configured',
      is_default_for_source: false,
      raw_preview: null,
      parsed_preview: null,
      is_loading_raw: false,
      is_saving_profile: false,
    },
  }
}

function createInitialExcelFilters(): ExcelFiltersState {
  return { AD: {}, UEM: {}, EDR: {}, ASSET: {}, MATERIALIZED: {} }
}

function createInitialPipelineState(): PipelineState {
  const datasetVersionId = readPersistedDatasetVersionId()
  return {
    dataset_version_id: datasetVersionId,
    ingest_status: 'idle',
    materialize_status: 'idle',
    ingest_message: null,
    materialize_message: null,
    ingest_total_records: null,
    materialize_row_count: null,
    materialize_checksum: null,
  }
}

function createInitialMachinesGridState(): MachinesGridState {
  return {
    rows: [],
    page: 0,
    size: 120,
    total: 0,
    has_next: false,
    is_loading_initial: false,
    is_loading_more: false,
    summary: null,
    filter_definitions: [],
    selected_statuses: [],
    selected_flags: [],
    search_input: '',
    search_query: '',
    is_exporting: false,
  }
}

function mapWorkflowToBadge(status: SourceWorkflowStatus): SourceConfig['status'] {
  return status === 'ready' ? 'pronto' : 'pendente'
}

function toSourceConfig(runtime: SourceRuntimeState): SourceConfig {
  return {
    status: mapWorkflowToBadge(runtime.status),
    headerRow: runtime.payload.header_row,
    sicColumn: runtime.payload.sic_column,
    selectedCols: runtime.payload.selected_columns,
  }
}

function createInitialConfigs(sourceStates: SourceRuntimeMap): Record<SourceId, SourceConfig> {
  return {
    AD: toSourceConfig(sourceStates.AD),
    UEM: toSourceConfig(sourceStates.UEM),
    EDR: toSourceConfig(sourceStates.EDR),
    ASSET: toSourceConfig(sourceStates.ASSET),
  }
}

function createSourceItem(sourceId: SourceId): SourceItem {
  return {
    id: sourceId,
    name: SOURCE_NAME_BY_ID[sourceId],
    type: 'CSV',
    createdAt: SOURCE_CREATED_AT_BY_ID[sourceId],
  }
}

function ensureKnownSources(rawSources: string[]): SourceId[] {
  const known = rawSources.filter(isSourceId)
  if (known.length === 0) {
    return SOURCE_ORDER
  }

  const knownSet = new Set(known)
  return SOURCE_ORDER.filter((id) => knownSet.has(id))
}

function deriveWorkflowStatus(runtime: SourceRuntimeState): SourceWorkflowStatus {
  if (!runtime.profile_id) {
    return 'not_configured'
  }

  const parsedHasRows = (runtime.parsed_preview?.sample_rows.length ?? 0) > 0
  const parsedHasWarnings = (runtime.parsed_preview?.warnings.length ?? 0) > 0

  if (runtime.is_default_for_source && parsedHasRows && !parsedHasWarnings) {
    return 'ready'
  }

  if (runtime.is_default_for_source) {
    return 'default'
  }

  return 'configured'
}

function toApiPayload(runtime: SourceRuntimeState): CsvTabPayload {
  return {
    header_row: runtime.payload.header_row,
    sic_column: runtime.payload.sic_column,
    selected_columns: runtime.payload.selected_columns,
  }
}

function patchSourceState(
  state: MainViewState,
  sourceId: SourceId,
  updater: (current: SourceRuntimeState) => SourceRuntimeState,
): Pick<MainViewState, 'sourceStates' | 'configs'> {
  const current = state.sourceStates[sourceId]
  const next = updater(current)

  return {
    sourceStates: {
      ...state.sourceStates,
      [sourceId]: next,
    },
    configs: {
      ...state.configs,
      [sourceId]: toSourceConfig(next),
    },
  }
}

function buildProfileIdsMap(state: MainViewState): Partial<Record<SourceId, string>> {
  return state.sources.reduce<Partial<Record<SourceId, string>>>((acc, source) => {
    const profileId = state.sourceStates[source.id].profile_id
    if (profileId) {
      acc[source.id] = profileId
    }
    return acc
  }, {})
}

function validateProfileIdsForPipeline(state: MainViewState): void {
  const missing = state.sources
    .filter((source) => !state.sourceStates[source.id].profile_id)
    .map((source) => source.name)

  if (missing.length > 0) {
    throw new Error(`Perfis ausentes para: ${missing.join(', ')}.`)
  }
}

function pickMaterializedColumns(rows: MachinesGridState['rows']): string[] {
  if (rows.length === 0) return []
  const keys = Object.keys(rows[0]).filter((key) => key !== 'id')
  const preferred = DEFAULT_MACHINE_VISIBLE_COLUMNS.filter((key) => keys.includes(key))
  if (preferred.length > 0) return preferred
  return keys.slice(0, 10)
}

function createInitialState(): MainViewState {
  const sourceStates = createRuntimeState()

  return {
    view: 'home-empty',
    activeTab: 'AD',
    sources: INITIAL_SOURCES,
    sourceStates,
    configs: createInitialConfigs(sourceStates),
    editingTab: null,
    editTabName: '',
    isSelectionMode: false,
    selectedSources: [],
    isDeleteModalOpen: false,
    deleteInput: '',
    excelFilters: createInitialExcelFilters(),
    openFilterMenu: null,
    isColPanelOpen: false,
    activeMatCols: DEFAULT_MACHINE_VISIBLE_COLUMNS,
    materializedColumns: [],
    pipeline: createInitialPipelineState(),
    machinesGrid: createInitialMachinesGridState(),
  }
}

async function loadMachinesSummaryAndFilters(
  setState: (recipe: (state: MainViewState) => Partial<MainViewState>) => void,
  getState: () => MainViewState,
): Promise<void> {
  const { pipeline, machinesGrid } = getState()
  if (!pipeline.dataset_version_id) return

  const [summary, filterDefinitions] = await Promise.all([
    fetchMachinesSummary({
      dataset_version_id: pipeline.dataset_version_id,
      search: machinesGrid.search_query || undefined,
      statuses: machinesGrid.selected_statuses.length > 0 ? machinesGrid.selected_statuses : undefined,
      flags: machinesGrid.selected_flags.length > 0 ? machinesGrid.selected_flags : undefined,
    }),
    fetchMachinesFilters(),
  ])

  setState((state) => ({
    machinesGrid: {
      ...state.machinesGrid,
      summary: summary as MachineSummaryState,
      filter_definitions: filterDefinitions as MachineFilterDefinitionState[],
    },
  }))
}

export const mainViewStore = create<MainViewStore>()((set, get) => ({
  ...createInitialState(),

  setView: (view) => set({ view }),

  setActiveTab: (sourceId) => set({ activeTab: sourceId }),

  setSelectionMode: (value) =>
    set((state) => ({
      isSelectionMode: value,
      selectedSources: value ? state.selectedSources : [],
    })),

  handleImportAll: async () => {
    const remoteSources = await listSources()
    const sourceIds = ensureKnownSources(remoteSources)

    set((state) => {
      const nextSourceStates: SourceRuntimeMap = {
        ...state.sourceStates,
      }
      const nextConfigs: Record<SourceId, SourceConfig> = { ...state.configs }

      sourceIds.forEach((sourceId) => {
        nextConfigs[sourceId] = toSourceConfig(nextSourceStates[sourceId])
      })

      return {
        sources: sourceIds.map(createSourceItem),
        sourceStates: nextSourceStates,
        configs: nextConfigs,
        view: 'home-filled',
        isSelectionMode: false,
        selectedSources: [],
      }
    })
  },

  handleOpenSource: async (sourceId) => {
    const { isSelectionMode } = get()
    if (isSelectionMode) {
      set((state) => {
        const isSelected = state.selectedSources.includes(sourceId)
        return {
          selectedSources: isSelected
            ? state.selectedSources.filter((id) => id !== sourceId)
            : [...state.selectedSources, sourceId],
        }
      })
      return
    }

    set({ activeTab: sourceId, view: 'viewer' })

    const profiles = await listProfiles(sourceId)
    const currentProfile = profiles.find((profile) => profile.is_default_for_source) ?? profiles[0] ?? null
    const currentPayload = currentProfile?.payload
    const normalizedPayload: CsvTabPayload = {
      header_row: Math.max(0, currentPayload?.header_row ?? 0),
      sic_column: currentPayload?.sic_column ?? '',
      selected_columns: currentPayload?.selected_columns ?? [],
    }

    set((state) =>
      patchSourceState(state, sourceId, (current) => {
        const next: SourceRuntimeState = {
          ...current,
          profile_id: currentProfile?.id ?? null,
          payload: normalizedPayload,
          is_default_for_source: Boolean(currentProfile?.is_default_for_source),
        }

        return {
          ...next,
          status: deriveWorkflowStatus(next),
        }
      }),
    )

    await get().refreshRawPreview(sourceId)
  },

  refreshRawPreview: async (sourceId) => {
    set((state) =>
      patchSourceState(state, sourceId, (current) => ({
        ...current,
        is_loading_raw: true,
      })),
    )

    const runtime = get().sourceStates[sourceId]
    const response = await previewRaw({
      source: sourceId,
      header_row_override: runtime.payload.header_row,
      ...getPreviewContext(),
    })

    if (response.status !== 'ok') {
      throw new Error(response.error || 'Falha ao carregar preview bruto.')
    }

    set((state) =>
      patchSourceState(state, sourceId, (current) => {
        const headers =
          response.original_headers && response.original_headers.length > 0
            ? response.original_headers
            : response.detected_headers ?? []
        const headerByNormalized = new Map(headers.map((header) => [normalizeHeaderKey(header), header]))
        const selectedColumnsRaw =
          current.payload.selected_columns.length > 0 ? current.payload.selected_columns : [...headers]
        const selectedColumns = Array.from(
          new Set(
            selectedColumnsRaw
              .map((column) => headerByNormalized.get(normalizeHeaderKey(column)))
              .filter((column): column is string => Boolean(column)),
          ),
        )
        const mappedSicColumn = headerByNormalized.get(normalizeHeaderKey(current.payload.sic_column)) ?? ''

        const next: SourceRuntimeState = {
          ...current,
          payload: {
            ...current.payload,
            selected_columns: selectedColumns,
            sic_column: mappedSicColumn,
          },
          raw_preview: {
            headers,
            sample_rows: response.sample_rows,
            warnings: response.warnings,
          },
          is_loading_raw: false,
        }

        return {
          ...next,
          status: deriveWorkflowStatus(next),
        }
      }),
    )
  },

  setHeaderRow: async (sourceId, headerRow) => {
    set((state) =>
      patchSourceState(state, sourceId, (current) => ({
        ...current,
        payload: {
          ...current.payload,
          header_row: Math.max(0, headerRow),
        },
      })),
    )

    await get().refreshRawPreview(sourceId)
  },

  handleSaveProfile: async (sourceId) => {
    const sourceState = get().sourceStates[sourceId]

    if (!sourceState.payload.sic_column) {
      throw new Error('Selecione a coluna SIC antes de salvar o perfil.')
    }

    if (!sourceState.payload.selected_columns.length) {
      throw new Error('Selecione ao menos uma coluna antes de salvar o perfil.')
    }

    set((state) =>
      patchSourceState(state, sourceId, (current) => ({
        ...current,
        is_saving_profile: true,
      })),
    )

    try {
      const current = get().sourceStates[sourceId]
      let profileId = current.profile_id
      let isDefault = current.is_default_for_source

      if (!profileId) {
        const created = await createProfile({
          source: sourceId,
          scope: 'PRIVATE',
          name: `${SOURCE_NAME_BY_ID[sourceId]} - Perfil TI`,
          payload: toApiPayload(current),
          is_default_for_source: false,
        })
        profileId = created.id
        isDefault = created.is_default_for_source
      } else {
        await updateProfile(profileId, {
          payload: toApiPayload(current),
          change_note: 'Atualizado via Main View TI',
        })
      }

      const parsed = await previewParsed({
        source: sourceId,
        profile_id: profileId,
        ...getPreviewContext(),
      })

      if (parsed.status !== 'ok') {
        throw new Error(parsed.error || 'Falha ao validar parse do perfil.')
      }

      if (!isDefault) {
        await promoteDefault(profileId)
        isDefault = true
      }

      set((state) =>
        patchSourceState(state, sourceId, (currentState) => {
          const next: SourceRuntimeState = {
            ...currentState,
            profile_id: profileId,
            is_default_for_source: isDefault,
            parsed_preview: {
              sample_rows: parsed.sample_rows,
              warnings: parsed.warnings,
            },
            is_saving_profile: false,
          }

          return {
            ...next,
            status: deriveWorkflowStatus(next),
          }
        }),
      )
    } catch (error) {
      set((state) =>
        patchSourceState(state, sourceId, (current) => ({
          ...current,
          is_saving_profile: false,
        })),
      )
      throw error
    }
  },

  handlePromoteDefault: async (sourceId) => {
    const sourceState = get().sourceStates[sourceId]
    if (!sourceState.profile_id) {
      throw new Error('Perfil ainda não foi criado para esta fonte.')
    }

    await promoteDefault(sourceState.profile_id)

    set((state) =>
      patchSourceState(state, sourceId, (current) => {
        const next: SourceRuntimeState = {
          ...current,
          is_default_for_source: true,
        }

        return {
          ...next,
          status: deriveWorkflowStatus(next),
        }
      }),
    )
  },

  setSicColumn: (sourceId, value) =>
    set((state) =>
      patchSourceState(state, sourceId, (current) => ({
        ...current,
        payload: {
          ...current.payload,
          sic_column: value,
        },
      })),
    ),

  toggleSelectedColumn: (sourceId, column) =>
    set((state) =>
      patchSourceState(state, sourceId, (current) => {
        const selectedCols = current.payload.selected_columns
        const nextCols = selectedCols.includes(column)
          ? selectedCols.filter((col) => col !== column)
          : [...selectedCols, column]

        return {
          ...current,
          payload: {
            ...current.payload,
            selected_columns: nextCols,
          },
        }
      }),
    ),

  startEditingTab: (source) => set({ editingTab: source.id, editTabName: source.name }),

  setEditTabName: (value) => set({ editTabName: value }),

  saveTabName: (sourceId) =>
    set((state) => ({
      sources: state.sources.map((source) =>
        source.id === sourceId ? { ...source, name: state.editTabName || source.name } : source,
      ),
      editingTab: null,
    })),

  toggleSelectionMode: () =>
    set((state) => ({
      isSelectionMode: !state.isSelectionMode,
      selectedSources: state.isSelectionMode ? [] : state.selectedSources,
    })),

  setDeleteModalOpen: (value) => set({ isDeleteModalOpen: value }),

  setDeleteInput: (value) => set({ deleteInput: value }),

  handleDeleteConfirm: () =>
    set((state) => {
      const nextSources = state.sources.filter((source) => !state.selectedSources.includes(source.id))
      const nextActiveTab = nextSources.find((source) => source.id === state.activeTab)?.id

      return {
        sources: nextSources,
        selectedSources: [],
        isSelectionMode: false,
        isDeleteModalOpen: false,
        deleteInput: '',
        view: nextSources.length === 0 ? 'home-empty' : state.view,
        activeTab: nextActiveTab ?? nextSources[0]?.id ?? 'AD',
      }
    }),

  setOpenFilterMenu: (value) => set({ openFilterMenu: value }),

  applyExcelFilter: (tab, col, selection) =>
    set((state) => ({
      excelFilters: {
        ...state.excelFilters,
        [tab]: {
          ...state.excelFilters[tab],
          [col]: selection,
        },
      },
    })),

  setColPanelOpen: (value) => set({ isColPanelOpen: value }),

  toggleMatCol: (colKey) =>
    set((state) => ({
      activeMatCols: state.activeMatCols.includes(colKey)
        ? state.activeMatCols.filter((col) => col !== colKey)
        : [...state.activeMatCols, colKey],
    })),

  handleRunIngest: async () => {
    const state = get()
    validateProfileIdsForPipeline(state)
    const profileIds = buildProfileIdsMap(state)

    set((current) => ({
      pipeline: {
        ...current.pipeline,
        ingest_status: 'running',
        materialize_status: 'idle',
        ingest_message: 'Executando preview + ingest...',
        materialize_message: null,
      },
    }))

    try {
      const preview = await previewDatasetMachines({
        profile_ids: profileIds,
        ...getPreviewContext(),
      })

      if (preview.status !== 'ok') {
        throw new Error('Preview do dataset retornou erro.')
      }

      const ingest = await ingestDatasetMachines({
        source: 'path',
        profile_ids: profileIds,
        ...getPreviewContext(),
      })

      if (ingest.status !== 'success') {
        throw new Error('Ingest retornou erro.')
      }

      persistDatasetVersionId(ingest.dataset_version_id)

      set((current) => ({
        pipeline: {
          ...current.pipeline,
          dataset_version_id: ingest.dataset_version_id,
          ingest_status: 'success',
          ingest_message: `Ingest OK • dataset_version_id=${ingest.dataset_version_id}`,
          ingest_total_records: ingest.total_records,
          materialize_status: 'running',
          materialize_message: 'Executando materialização...',
        },
      }))

      const materialized = await materializeMachines(ingest.dataset_version_id)

      set((current) => ({
        view: 'materialized',
        pipeline: {
          ...current.pipeline,
          dataset_version_id: ingest.dataset_version_id,
          materialize_status: 'success',
          materialize_message: `Materialize OK • rows=${materialized.row_count}`,
          materialize_row_count: materialized.row_count,
          materialize_checksum: materialized.checksum,
        },
      }))

      await Promise.all([get().refreshMachinesSummaryAndFilters(), get().reloadMachinesGrid()])
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Falha no pipeline'
      set((current) => ({
        pipeline: {
          ...current.pipeline,
          ingest_status: current.pipeline.ingest_status === 'running' ? 'error' : current.pipeline.ingest_status,
          materialize_status:
            current.pipeline.materialize_status === 'running' ? 'error' : current.pipeline.materialize_status,
          ingest_message:
            current.pipeline.ingest_status === 'running' ? `Ingest ERRO • ${message}` : current.pipeline.ingest_message,
          materialize_message:
            current.pipeline.materialize_status === 'running'
              ? `Materialize ERRO • ${message}`
              : current.pipeline.materialize_message,
        },
      }))
      throw error
    }
  },

  reloadMachinesGrid: async () => {
    const { pipeline, machinesGrid } = get()
    if (!pipeline.dataset_version_id) {
      throw new Error('dataset_version_id não definido. Rode ingest + materialize primeiro.')
    }

    set((state) => ({
      machinesGrid: {
        ...state.machinesGrid,
        is_loading_initial: true,
        rows: [],
        page: 0,
        total: 0,
        has_next: false,
      },
    }))

    const page = 1
    const response = await fetchMachinesTable({
      dataset_version_id: pipeline.dataset_version_id,
      page,
      size: machinesGrid.size,
      search: machinesGrid.search_query || undefined,
      statuses: machinesGrid.selected_statuses.length > 0 ? machinesGrid.selected_statuses : undefined,
      flags: machinesGrid.selected_flags.length > 0 ? machinesGrid.selected_flags : undefined,
    })

    set((state) => {
      const availableColumns = pickMaterializedColumns(response.items)
      const nextActiveColumns =
        state.activeMatCols.length > 0
          ? state.activeMatCols.filter((col) => availableColumns.includes(col))
          : availableColumns

      return {
        materializedColumns: availableColumns,
        activeMatCols: nextActiveColumns.length > 0 ? nextActiveColumns : availableColumns,
        machinesGrid: {
          ...state.machinesGrid,
          rows: response.items,
          page,
          total: response.meta.total,
          has_next: response.meta.has_next,
          is_loading_initial: false,
          is_loading_more: false,
        },
      }
    })
  },

  fetchNextMachinesPage: async () => {
    const { pipeline, machinesGrid } = get()
    if (!pipeline.dataset_version_id || !machinesGrid.has_next || machinesGrid.is_loading_more) {
      return
    }

    const nextPage = machinesGrid.page + 1
    set((state) => ({
      machinesGrid: {
        ...state.machinesGrid,
        is_loading_more: true,
      },
    }))

    try {
      const response = await fetchMachinesTable({
        dataset_version_id: pipeline.dataset_version_id,
        page: nextPage,
        size: machinesGrid.size,
        search: machinesGrid.search_query || undefined,
        statuses: machinesGrid.selected_statuses.length > 0 ? machinesGrid.selected_statuses : undefined,
        flags: machinesGrid.selected_flags.length > 0 ? machinesGrid.selected_flags : undefined,
      })

      set((state) => ({
        machinesGrid: {
          ...state.machinesGrid,
          rows: [...state.machinesGrid.rows, ...response.items],
          page: nextPage,
          total: response.meta.total,
          has_next: response.meta.has_next,
          is_loading_more: false,
        },
      }))
    } catch (error) {
      set((state) => ({
        machinesGrid: {
          ...state.machinesGrid,
          is_loading_more: false,
        },
      }))
      throw error
    }
  },

  refreshMachinesSummaryAndFilters: async () => {
    await loadMachinesSummaryAndFilters(
      (recipe) => set((state) => recipe(state)),
      () => get(),
    )
  },

  setMachinesSearchInput: (value) =>
    set((state) => ({
      machinesGrid: {
        ...state.machinesGrid,
        search_input: value,
      },
    })),

  commitMachinesSearch: async () => {
    set((state) => ({
      machinesGrid: {
        ...state.machinesGrid,
        search_query: state.machinesGrid.search_input.trim(),
      },
    }))

    await Promise.all([get().reloadMachinesGrid(), get().refreshMachinesSummaryAndFilters()])
  },

  setStatusFilters: async (statuses) => {
    const deduplicated = Array.from(new Set(statuses))
    set((state) => ({
      machinesGrid: {
        ...state.machinesGrid,
        selected_statuses: deduplicated,
      },
    }))

    await Promise.all([get().reloadMachinesGrid(), get().refreshMachinesSummaryAndFilters()])
  },

  toggleStatusFilter: async (status) => {
    set((state) => {
      const exists = state.machinesGrid.selected_statuses.includes(status)
      return {
        machinesGrid: {
          ...state.machinesGrid,
          selected_statuses: exists
            ? state.machinesGrid.selected_statuses.filter((item) => item !== status)
            : [...state.machinesGrid.selected_statuses, status],
        },
      }
    })

    await Promise.all([get().reloadMachinesGrid(), get().refreshMachinesSummaryAndFilters()])
  },

  toggleFlagFilter: async (flag) => {
    set((state) => {
      const exists = state.machinesGrid.selected_flags.includes(flag)
      return {
        machinesGrid: {
          ...state.machinesGrid,
          selected_flags: exists
            ? state.machinesGrid.selected_flags.filter((item) => item !== flag)
            : [...state.machinesGrid.selected_flags, flag],
        },
      }
    })

    await Promise.all([get().reloadMachinesGrid(), get().refreshMachinesSummaryAndFilters()])
  },

  clearMachinesFilters: async () => {
    set((state) => ({
      machinesGrid: {
        ...state.machinesGrid,
        selected_statuses: [],
        selected_flags: [],
        search_input: '',
        search_query: '',
      },
    }))

    await Promise.all([get().reloadMachinesGrid(), get().refreshMachinesSummaryAndFilters()])
  },

  exportMachines: async () => {
    const { pipeline, machinesGrid } = get()
    if (!pipeline.dataset_version_id) {
      throw new Error('dataset_version_id não definido para exportação.')
    }

    set((state) => ({
      machinesGrid: {
        ...state.machinesGrid,
        is_exporting: true,
      },
    }))

    try {
      const report = await runMachinesReport(pipeline.dataset_version_id, {
        template_name: 'machines_status_summary',
        limit: 20000,
      })

      const rows = report.data.length > 0 ? report.data : machinesGrid.rows
      if (!rows.length) {
        throw new Error('Nenhum dado disponível para exportar.')
      }

      exportRowsAsCsv(rows, `machines_${pipeline.dataset_version_id}.csv`)
    } finally {
      set((state) => ({
        machinesGrid: {
          ...state.machinesGrid,
          is_exporting: false,
        },
      }))
    }
  },

  resetState: () => {
    persistDatasetVersionId(null)
    set(createInitialState())
  },
}))
