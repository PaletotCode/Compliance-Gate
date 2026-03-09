import { ArrowLeft, FileSpreadsheet } from 'lucide-react'
import type { ReactNode } from 'react'
import type { MainViewMode, SourceConfig, SourceId, SourceItem } from '@/main_view/state/types'

type MainTabsBarProps = {
  view: MainViewMode
  sources: SourceItem[]
  activeTab: SourceId
  configs: Record<SourceId, SourceConfig>
  editingTab: SourceId | null
  editTabName: string
  onBackHome: () => void
  onSelectTab: (sourceId: SourceId) => void
  onStartEditing: (source: SourceItem) => void
  onEditTabNameChange: (value: string) => void
  onSaveTabName: (sourceId: SourceId) => void
  rightSlot?: ReactNode
}

export function MainTabsBar({
  view,
  sources,
  activeTab,
  configs,
  editingTab,
  editTabName,
  onBackHome,
  onSelectTab,
  onStartEditing,
  onEditTabNameChange,
  onSaveTabName,
  rightSlot,
}: MainTabsBarProps) {
  if (view === 'home-empty' || view === 'home-filled') {
    return null
  }

  return (
    <div className="flex items-center w-full p-1 bg-white/5 border border-white/10 rounded-xl shadow-inner transition-all animate-in fade-in zoom-in-95">
      <button
        onClick={onBackHome}
        className="flex items-center justify-center gap-2 px-4 h-9 rounded-lg bg-white/10 text-white hover:bg-white/20 hover:text-white border border-white/5 shadow-sm transition-all shrink-0 group mr-1 whitespace-nowrap"
      >
        <ArrowLeft size={14} className="group-hover:-translate-x-1 transition-transform" />
        <span className="text-[11px] font-bold tracking-wide">Início</span>
      </button>

      <div className="w-px h-5 bg-white/10 mx-1 shrink-0" />

      <div className="flex-1 flex items-center overflow-x-auto hide-scrollbar scroll-smooth">
        {view === 'viewer' &&
          sources.map((source) => (
            <button
              key={source.id}
              onClick={() => onSelectTab(source.id)}
              className={`flex items-center gap-2 px-4 h-9 rounded-lg transition-all min-w-[130px] shrink-0 group relative overflow-hidden animate-in fade-in duration-300
                      ${
                        activeTab === source.id
                          ? 'bg-white/10 text-white shadow-md border border-white/10'
                          : 'bg-transparent text-white/40 hover:text-white hover:bg-white/5 border border-transparent'
                      }`}
            >
              {activeTab === source.id && (
                <div className="absolute inset-0 bg-gradient-to-b from-white/5 to-transparent opacity-50 pointer-events-none" />
              )}
              <FileSpreadsheet
                size={13}
                className={
                  activeTab === source.id
                    ? 'text-[#00AE9D] relative z-10'
                    : 'opacity-50 group-hover:opacity-100 relative z-10'
                }
              />

              {editingTab === source.id ? (
                <input
                  autoFocus
                  className="bg-[#222] border border-[#00AE9D] text-white px-2 rounded-md text-xs outline-none w-full font-bold shadow-inner relative z-10 h-7"
                  value={editTabName}
                  onChange={(event) => onEditTabNameChange(event.target.value)}
                  onBlur={() => onSaveTabName(source.id)}
                  onKeyDown={(event) => {
                    if (event.key === 'Enter') {
                      onSaveTabName(source.id)
                    }
                  }}
                  onClick={(event) => event.stopPropagation()}
                />
              ) : (
                <div
                  className="flex items-center gap-2 flex-1 min-w-0 cursor-text relative z-10"
                  onDoubleClick={(event) => {
                    event.stopPropagation()
                    onStartEditing(source)
                  }}
                  title="Duplo clique para renomear"
                >
                  <span className="text-[11px] font-bold truncate tracking-wide">{source.name}</span>
                </div>
              )}

              <div className="ml-auto shrink-0 pl-1 relative z-10">
                <div
                  className={`w-1.5 h-1.5 rounded-full ${
                    configs[source.id].status === 'pronto'
                      ? 'bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.8)]'
                      : 'bg-white/20'
                  }`}
                />
              </div>
            </button>
          ))}
      </div>

      {rightSlot ? (
        <>
          <div className="w-px h-5 bg-white/10 mx-1 shrink-0" />
          <div className="flex items-center gap-2 shrink-0 pr-1">{rightSlot}</div>
        </>
      ) : null}
    </div>
  )
}
