import { Braces, Bug, Database, FileCode2, Filter, GanttChartSquare } from 'lucide-react'
import type { LucideIcon } from 'lucide-react'
import type { EngineStudioPanelKey } from '@/engine_studio/state'

type StudioPanelTabItem = {
  key: EngineStudioPanelKey
  label: string
  icon: LucideIcon
}

const TABS: StudioPanelTabItem[] = [
  { key: 'catalog', label: 'Catálogo', icon: Database },
  { key: 'transformations', label: 'Transformations', icon: Braces },
  { key: 'segments', label: 'Segments', icon: Filter },
  { key: 'views', label: 'Views', icon: FileCode2 },
  { key: 'rulesets', label: 'RuleSets', icon: GanttChartSquare },
  { key: 'diagnostics', label: 'Diagnostics', icon: Bug },
]

type StudioPanelTabsProps = {
  activePanel: EngineStudioPanelKey
  onSelectPanel: (panel: EngineStudioPanelKey) => void
}

export function StudioPanelTabs({ activePanel, onSelectPanel }: StudioPanelTabsProps) {
  return (
    <div className="flex flex-wrap gap-2">
      {TABS.map((tab) => {
        const Icon = tab.icon
        const active = tab.key === activePanel
        return (
          <button
            key={tab.key}
            type="button"
            onClick={() => onSelectPanel(tab.key)}
            className={`h-8 px-3 rounded-lg border text-[11px] font-bold flex items-center gap-1.5 transition-colors ${
              active
                ? 'bg-[#00AE9D]/20 border-[#00AE9D]/40 text-[#63e8da]'
                : 'bg-white/5 border-white/10 text-white/70 hover:text-white hover:border-white/25'
            }`}
          >
            <Icon size={13} />
            {tab.label}
          </button>
        )
      })}
    </div>
  )
}
