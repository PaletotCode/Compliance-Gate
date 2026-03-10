import { useEffect, useMemo, useState } from 'react'
import { Columns, Download, ListChecks, PanelRight, Play, Plus, Trash2 } from 'lucide-react'
import { authStore } from '@/auth/store'
import { EngineStudioDock } from '@/engine_studio/components'
import { useEngineStudioBootstrap } from '@/engine_studio/hooks'
import { engineStudioStore } from '@/engine_studio/state'
import { getMainViewErrorMessage } from '@/main_view/api/csvTabsApi'
import { SourceCard } from '@/main_view/components/cards/SourceCard'
import { ActionButton } from '@/main_view/components/layout/ActionButton'
import { DeleteSourcesModal } from '@/main_view/components/modals/DeleteSourcesModal'
import { MaterializedColumnPanel } from '@/main_view/components/panels/MaterializedColumnPanel'
import { ViewerConfigPanel } from '@/main_view/components/panels/ViewerConfigPanel'
import { MachinesVirtualGrid } from '@/main_view/components/table/MachinesVirtualGrid'
import { SourceDataTable } from '@/main_view/components/table/SourceDataTable'
import { MainTabsBar } from '@/main_view/components/tabs/MainTabsBar'
import { REQUIRED_DELETE_TEXT } from '@/main_view/mocks/mockData'
import { mainViewStore } from '@/main_view/state/mainViewStore'
import type { MachineTableRow } from '@/main_view/state/types'
import { pushNotification } from '@/shared/notifications/notificationStore'

export function MainMenuCanvas() {
  const [isViewerConfigCollapsed, setViewerConfigCollapsed] = useState(false)
  const currentUser = authStore((state) => state.user)
  const isTiAdmin = currentUser?.role === 'TI_ADMIN'
  const studioTable = engineStudioStore((state) => state.table)
  const studioIsOpen = engineStudioStore((state) => state.is_open)
  const toggleStudioOpen = engineStudioStore((state) => state.toggleOpen)
  const fetchNextStudioTablePage = engineStudioStore((state) => state.fetchNextTablePage)
  const studioIsBootstrapping = engineStudioStore((state) => state.is_bootstrapping)

  const {
    view,
    activeTab,
    sources,
    sourceStates,
    configs,
    editingTab,
    editTabName,
    isSelectionMode,
    selectedSources,
    isDeleteModalOpen,
    deleteInput,
    excelFilters,
    openFilterMenu,
    isColPanelOpen,
    activeMatCols,
    materializedColumns,
    pipeline,
    machinesGrid,
    setView,
    setSelectionMode,
    handleImportAll,
    handleOpenSource,
    setHeaderRow,
    handleSaveProfile,
    setSicColumn,
    toggleSelectedColumn,
    startEditingTab,
    setEditTabName,
    saveTabName,
    toggleSelectionMode,
    setDeleteModalOpen,
    setDeleteInput,
    handleDeleteConfirm,
    setOpenFilterMenu,
    applyExcelFilter,
    setColPanelOpen,
    toggleMatCol,
    syncMaterializedColumnsFromRows,
    handleRunIngest,
    reloadMachinesGrid,
    fetchNextMachinesPage,
    refreshMachinesSummaryAndFilters,
    exportMachines,
  } = mainViewStore()

  useEngineStudioBootstrap({
    enabled: view === 'materialized' && isTiAdmin,
    datasetVersionId: pipeline.dataset_version_id,
  })

  const readyCount = sources.filter((source) => configs[source.id]?.status === 'pronto').length

  const isAllReady = readyCount === sources.length && sources.length > 0
  const readinessRatio = sources.length > 0 ? readyCount / sources.length : 0

  const runSafeAction = async (action: () => Promise<void>) => {
    try {
      await action()
    } catch (error) {
      pushNotification({
        tone: 'error',
        title: 'Falha na operação',
        message: getMainViewErrorMessage(error),
      })
    }
  }

  useEffect(() => {
    if (view !== 'materialized') return
    if (!pipeline.dataset_version_id) return
    if (isTiAdmin) return
    if (machinesGrid.rows.length > 0 || machinesGrid.is_loading_initial) return

    void runSafeAction(async () => {
      await Promise.all([refreshMachinesSummaryAndFilters(), reloadMachinesGrid()])
    })
  }, [
    view,
    pipeline.dataset_version_id,
    isTiAdmin,
    machinesGrid.rows.length,
    machinesGrid.is_loading_initial,
    refreshMachinesSummaryAndFilters,
    reloadMachinesGrid,
  ])

  const studioRows = useMemo<MachineTableRow[]>(() => {
    return studioTable.items.map((rawRow, index) => {
      const flagsRaw = rawRow.flags
      const normalizedFlags = Array.isArray(flagsRaw)
        ? flagsRaw.map((item) => String(item))
        : []

      const normalizedRow: MachineTableRow = {
        id: String(rawRow.id ?? rawRow.machine_id ?? rawRow.hostname ?? `row-${index + 1}`),
        hostname: String(rawRow.hostname ?? rawRow.machine_id ?? `ROW-${index + 1}`),
        pa_code: String(rawRow.pa_code ?? ''),
        primary_status: String(rawRow.primary_status ?? ''),
        primary_status_label: String(rawRow.primary_status_label ?? rawRow.primary_status ?? ''),
        flags: normalizedFlags,
        has_ad: Boolean(rawRow.has_ad),
        has_uem: Boolean(rawRow.has_uem),
        has_edr: Boolean(rawRow.has_edr),
        has_asset: Boolean(rawRow.has_asset),
      }

      return {
        ...(rawRow as Record<string, unknown>),
        ...normalizedRow,
      } as MachineTableRow
    })
  }, [studioTable.items])

  const materializedRows = isTiAdmin ? studioRows : machinesGrid.rows
  const materializedTotalRows = isTiAdmin ? studioTable.total_rows : machinesGrid.total
  const materializedHasNextPage = isTiAdmin ? studioTable.has_next : machinesGrid.has_next
  const materializedIsLoadingInitial = isTiAdmin
    ? studioTable.is_loading_initial || studioIsBootstrapping
    : machinesGrid.is_loading_initial
  const materializedIsLoadingMore = isTiAdmin ? studioTable.is_loading_more : machinesGrid.is_loading_more

  useEffect(() => {
    if (view !== 'materialized') return
    syncMaterializedColumnsFromRows(materializedRows)
  }, [view, materializedRows, syncMaterializedColumnsFromRows])

  const activeSource = sources.find((source) => source.id === activeTab)
  const activeRuntime = sourceStates[activeTab]
  const activeData = activeRuntime?.raw_preview?.sample_rows ?? []
  const activeColumns = activeRuntime?.raw_preview?.headers ?? []
  const activeConfig = configs[activeTab]

  const materializedTopbarActions = (
    <>
      {isTiAdmin ? (
        <ActionButton variant={studioIsOpen ? 'primary' : 'secondary'} size="sm" onClick={toggleStudioOpen}>
          <PanelRight size={14} />
          {studioIsOpen ? 'OCULTAR ENGINE' : 'ABRIR ENGINE'}
        </ActionButton>
      ) : null}

      <ActionButton variant={isColPanelOpen ? 'primary' : 'secondary'} size="sm" onClick={() => setColPanelOpen(!isColPanelOpen)}>
        <Columns size={14} /> GERENCIAR COLUNAS
      </ActionButton>

      <ActionButton
        variant="primary"
        size="sm"
        disabled={machinesGrid.is_exporting}
        onClick={() => void runSafeAction(exportMachines)}
      >
        <Download size={14} /> {machinesGrid.is_exporting ? 'EXPORTANDO...' : 'EXPORTAR'}
      </ActionButton>
    </>
  )

  const renderHome = () => (
    <div className="flex-1 p-8 lg:p-12 animate-in fade-in zoom-in-95 duration-500 max-w-7xl mx-auto w-full flex flex-col justify-center">
      {view === 'home-empty' ? (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
          <button
            type="button"
            data-testid="import-csv-bases"
            onClick={() => void runSafeAction(handleImportAll)}
            className="aspect-[4/3] rounded-3xl border-2 border-dashed border-white/20 hover:border-[#00AE9D]/60 bg-white/5 backdrop-blur-md hover:bg-[#00AE9D]/10 flex flex-col items-center justify-center gap-5 transition-all group shadow-xl"
          >
            <div className="w-14 h-14 rounded-full bg-white/5 backdrop-blur-xl group-hover:bg-[#00AE9D]/20 border border-white/10 group-hover:border-[#00AE9D]/40 flex items-center justify-center text-white/50 group-hover:text-[#00AE9D] transition-all transform group-hover:scale-110">
              <Plus size={28} />
            </div>
            <span className="text-sm font-bold text-white/70 group-hover:text-white tracking-wide">
              Importar bases CSV
            </span>
          </button>
        </div>
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6 content-start h-full pt-10">
          {sources.map((source, index) => (
            <SourceCard
              key={source.id}
              source={source}
              index={index}
              config={configs[source.id]}
              isSelectionMode={isSelectionMode}
              isSelected={selectedSources.includes(source.id)}
              onOpenSource={(sourceId) => void runSafeAction(() => handleOpenSource(sourceId))}
            />
          ))}
        </div>
      )}

      {isSelectionMode && (
        <div className="absolute bottom-10 left-1/2 -translate-x-1/2 flex items-center gap-4 bg-[#111]/90 backdrop-blur-2xl p-4 rounded-2xl border border-white/10 shadow-[0_20px_50px_rgba(0,0,0,0.8)] animate-in slide-in-from-bottom-8">
          <span className="text-sm font-bold text-white px-2">
            {selectedSources.length} base(s) selecionada(s)
          </span>
          <div className="h-6 w-px bg-white/10" />
          <ActionButton variant="ghost" onClick={() => setSelectionMode(false)}>
            Cancelar
          </ActionButton>
          <ActionButton
            variant="danger"
            disabled={selectedSources.length === 0}
            onClick={() => setDeleteModalOpen(true)}
          >
            <Trash2 size={16} /> Apagar Selecionadas
          </ActionButton>
        </div>
      )}

      <DeleteSourcesModal
        isOpen={isDeleteModalOpen}
        selectedCount={selectedSources.length}
        deleteInput={deleteInput}
        requiredText={REQUIRED_DELETE_TEXT}
        onDeleteInputChange={setDeleteInput}
        onCancel={() => {
          setDeleteModalOpen(false)
          setDeleteInput('')
        }}
        onConfirm={handleDeleteConfirm}
      />
    </div>
  )

  const renderViewer = () => {
    if (!activeSource || !activeConfig) {
      return null
    }

    return (
      <div className="flex flex-col h-[calc(100vh-64px)] w-full relative animate-in fade-in duration-300">
        <div className="flex flex-1 overflow-hidden relative min-w-0">
          <div className="flex-1 min-w-0 flex flex-col bg-black/20 backdrop-blur-sm relative z-10 p-6 animate-in fade-in duration-300">
            <SourceDataTable
              key={`source-table-${activeTab}`}
              activeTab={activeTab}
              headers={activeColumns}
              rows={activeData}
              config={activeConfig}
              filters={excelFilters[activeTab] ?? {}}
              openFilterMenu={openFilterMenu}
              onOpenFilterMenu={setOpenFilterMenu}
              onApplyFilter={(column, selection) => applyExcelFilter(activeTab, column, selection)}
            />
          </div>

          <ViewerConfigPanel
            source={activeSource}
            config={activeConfig}
            columns={activeColumns}
            isCollapsed={isViewerConfigCollapsed}
            onToggleCollapse={() => setViewerConfigCollapsed((current) => !current)}
            onHeaderRowChange={(value) => void runSafeAction(() => setHeaderRow(activeTab, value))}
            onSicColumnChange={(value) => setSicColumn(activeTab, value)}
            onToggleColumn={(column) => toggleSelectedColumn(activeTab, column)}
            isSavingProfile={activeRuntime?.is_saving_profile ?? false}
            onSaveProfile={() => void runSafeAction(() => handleSaveProfile(activeTab))}
          />
        </div>
      </div>
    )
  }

  const renderMaterialized = () => (
    <div className="flex h-[calc(100vh-64px)] w-full animate-in fade-in duration-500 overflow-hidden relative">
      <div className="flex-1 flex flex-col p-6 bg-black/20 backdrop-blur-sm overflow-hidden z-10 relative animate-in fade-in duration-300">
        <MachinesVirtualGrid
          rows={materializedRows}
          visibleColumns={activeMatCols}
          filters={excelFilters.MATERIALIZED ?? {}}
          openFilterMenu={openFilterMenu}
          totalRows={materializedTotalRows}
          hasNextPage={materializedHasNextPage}
          isLoadingInitial={materializedIsLoadingInitial}
          isLoadingMore={materializedIsLoadingMore}
          onOpenFilterMenu={setOpenFilterMenu}
          onApplyFilter={(column, selection) => applyExcelFilter('MATERIALIZED', column, selection)}
          onReachEnd={() => {
            if (isTiAdmin) {
              void runSafeAction(fetchNextStudioTablePage)
              return
            }
            void runSafeAction(fetchNextMachinesPage)
          }}
        />
      </div>

      {isTiAdmin ? <EngineStudioDock /> : null}

      <MaterializedColumnPanel
        isOpen={isColPanelOpen}
        availableColumns={materializedColumns}
        activeMatCols={activeMatCols}
        onClose={() => setColPanelOpen(false)}
        onToggleColumn={toggleMatCol}
      />
    </div>
  )

  return (
    <div
      className="cg-main-view min-h-screen bg-[#020202] text-white selection:bg-[#00AE9D]/30 flex flex-col overflow-hidden relative"
      onClick={() => setOpenFilterMenu(null)}
    >
      <div className="fixed inset-0 pointer-events-none z-0 overflow-hidden">
        <div className="absolute top-[-30%] left-[-10%] w-[60%] h-[60%] bg-[#00AE9D]/5 blur-[150px] rounded-full" />
        <div className="absolute bottom-[-30%] right-[-10%] w-[60%] h-[60%] bg-[#00AE9D]/5 blur-[150px] rounded-full" />
      </div>

      <header className="h-16 shrink-0 border-b border-white/10 flex items-center bg-black/60 backdrop-blur-2xl backdrop-saturate-150 z-50 shadow-md relative">
        <div className="flex items-center gap-6 shrink-0 justify-start w-[240px] pl-6">
          <button
            type="button"
            className="flex items-center gap-3 cursor-pointer group"
            onClick={() => setView('home-filled')}
          >
            <div className="w-9 h-9 rounded-lg bg-gradient-to-br from-[#00AE9D] to-[#008f81] flex items-center justify-center text-white font-black shadow-[0_0_20px_rgba(0,174,157,0.3)] group-hover:shadow-[0_0_25px_rgba(0,174,157,0.5)] transition-shadow shrink-0">
              CG
            </div>
            <div className="flex flex-col hidden sm:flex truncate">
              <span className="font-black text-[13px] tracking-tight leading-none text-white">SICOOB</span>
              <span className="text-[8px] font-black text-[#00AE9D] tracking-[0.25em] uppercase mt-0.5 opacity-90 truncate">
                Compliance Gate
              </span>
            </div>
          </button>
        </div>

        <div className="flex-1 min-w-0 flex items-center justify-start pr-6 pl-4">
          <MainTabsBar
            view={view}
            sources={sources}
            activeTab={activeTab}
            configs={configs}
            editingTab={editingTab}
            editTabName={editTabName}
            onBackHome={() => setView('home-filled')}
            onSelectTab={(sourceId) => void runSafeAction(() => handleOpenSource(sourceId))}
            onStartEditing={startEditingTab}
            onEditTabNameChange={setEditTabName}
            onSaveTabName={saveTabName}
            rightSlot={view === 'materialized' ? materializedTopbarActions : null}
          />
        </div>

        <div
          className={`flex items-center justify-end gap-4 shrink-0 pr-6 ${
            view === 'home-filled' ? 'w-auto' : 'w-[240px]'
          }`}
        >
          <div className="flex items-center gap-3 mr-2 animate-in fade-in duration-300">
            {view === 'home-filled' && (
              <div className="hidden lg:flex items-center gap-3 px-3 h-9 rounded-lg bg-white/5 border border-white/10 shadow-inner mr-2 shrink-0">
                <span className="text-[9px] font-bold text-white/50 tracking-widest uppercase">Prontidão</span>
                <div className="h-4 w-px bg-white/10" />
                <div className="flex items-center gap-2">
                  <div className="relative w-4 h-4 flex items-center justify-center">
                    <svg className="w-full h-full transform -rotate-90 absolute inset-0">
                      <circle
                        cx="8"
                        cy="8"
                        r="7"
                        stroke="currentColor"
                        strokeWidth="2"
                        fill="transparent"
                        className="text-white/10"
                      />
                      <circle
                        cx="8"
                        cy="8"
                        r="7"
                        stroke="currentColor"
                        strokeWidth="2"
                        fill="transparent"
                        strokeDasharray={`${2 * Math.PI * 7}`}
                        strokeDashoffset={`${2 * Math.PI * 7 * (1 - readinessRatio)}`}
                        className="text-[#00AE9D] transition-all duration-1000 ease-out"
                      />
                    </svg>
                  </div>
                  <span className="text-[10px] font-black text-white">
                    {readyCount}/{sources.length}
                  </span>
                </div>
              </div>
            )}

            {view === 'home-filled' && sources.length > 0 && (
              <ActionButton
                variant="secondary"
                size="sm"
                className={isSelectionMode ? 'bg-white/15 text-white border-white/20' : ''}
                onClick={toggleSelectionMode}
              >
                <ListChecks size={14} /> GERENCIAR FONTES
              </ActionButton>
            )}

            {view === 'home-filled' && (
              <ActionButton
                variant={isAllReady ? 'primary' : 'secondary'}
                size="sm"
                disabled={!isAllReady || pipeline.ingest_status === 'running' || pipeline.materialize_status === 'running'}
                onClick={() => void runSafeAction(handleRunIngest)}
                className="main-view-run-pipeline-btn"
              >
                <Play size={14} className={isAllReady ? 'text-white' : 'text-white/50'} />
                EXECUTAR PIPELINE
              </ActionButton>
            )}

          </div>

          {view === 'home-filled' && (pipeline.ingest_message || pipeline.materialize_message) && (
            <div className="hidden xl:flex flex-col gap-0.5 text-[9px] font-mono text-white/65 px-3 py-2 rounded-lg border border-white/10 bg-white/5 max-w-[250px] truncate">
              {pipeline.ingest_message && <span className="truncate">{pipeline.ingest_message}</span>}
              {pipeline.materialize_message && <span className="truncate">{pipeline.materialize_message}</span>}
            </div>
          )}

          {view !== 'home-filled' && (
            <div className="flex items-center gap-3 shrink-0">
              <div className="text-right hidden md:block">
                <div className="text-[11px] font-black text-white tracking-wide">TI.Administrador</div>
                <div className="text-[9px] font-mono text-white/40 uppercase tracking-widest mt-0.5">
                  Sessão Ativa
                </div>
              </div>
              <div className="w-9 h-9 shrink-0 rounded-full bg-white/10 border border-white/20 flex items-center justify-center backdrop-blur-md shadow-inner">
                <span className="text-[10px] font-black text-[#00AE9D]">TI</span>
              </div>
            </div>
          )}
        </div>
      </header>

      <main className="flex-1 overflow-hidden relative flex z-10">
        {view.startsWith('home') && renderHome()}
        {view === 'viewer' && renderViewer()}
        {view === 'materialized' && renderMaterialized()}
      </main>
    </div>
  )
}
