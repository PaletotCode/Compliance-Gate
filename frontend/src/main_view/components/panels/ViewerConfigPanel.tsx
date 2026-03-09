import { CalendarDays, CheckCircle2, ChevronLeft, ChevronRight, Key, ListChecks, Settings } from 'lucide-react'
import { ActionButton } from '@/main_view/components/layout/ActionButton'
import type { SourceConfig, SourceItem } from '@/main_view/state/types'

type ViewerConfigPanelProps = {
  source: SourceItem
  config: SourceConfig
  columns: string[]
  isCollapsed: boolean
  onToggleCollapse: () => void
  onHeaderRowChange: (value: number) => void
  onSicColumnChange: (value: string) => void
  onToggleColumn: (column: string) => void
  isSavingProfile: boolean
  onSaveProfile: () => void
}

export function ViewerConfigPanel({
  source,
  config,
  columns,
  isCollapsed,
  onToggleCollapse,
  onHeaderRowChange,
  onSicColumnChange,
  onToggleColumn,
  isSavingProfile,
  onSaveProfile,
}: ViewerConfigPanelProps) {
  if (isCollapsed) {
    return (
      <div className="w-[56px] shrink-0 border-l border-white/10 bg-black/60 backdrop-blur-2xl flex flex-col items-center py-4 gap-4 shadow-[-20px_0_40px_rgba(0,0,0,0.5)] z-20 transition-[width] duration-300">
        <button
          type="button"
          onClick={onToggleCollapse}
          className="w-8 h-8 rounded-lg border border-white/20 bg-black/50 hover:bg-white/10 text-white/75 flex items-center justify-center"
          title="Expandir configurações"
        >
          <ChevronLeft size={16} />
        </button>
        <span className="text-[10px] tracking-[0.2em] font-black text-white/40 [writing-mode:vertical-rl] rotate-180">
          CONFIG
        </span>
      </div>
    )
  }

  return (
    <div className="w-[340px] shrink-0 border-l border-white/10 bg-black/60 backdrop-blur-2xl flex flex-col shadow-[-20px_0_40px_rgba(0,0,0,0.5)] z-20 transition-[width] duration-300">
      <div className="p-6 flex-1 overflow-auto space-y-8 custom-scrollbar">
        <div className="flex items-center justify-between gap-3 border-b border-white/10 pb-4">
          <div className="flex items-center gap-3">
            <Settings className="text-[#00AE9D]" size={18} />
            <h2 className="text-xs font-black text-white uppercase tracking-[0.15em]">Configuração</h2>
          </div>
          <button
            type="button"
            onClick={onToggleCollapse}
            className="w-8 h-8 rounded-lg border border-white/20 bg-black/50 hover:bg-white/10 text-white/75 flex items-center justify-center"
            title="Encolher configurações"
          >
            <ChevronRight size={16} />
          </button>
        </div>

        <div className="flex flex-col gap-1 p-4 bg-white/5 rounded-xl border border-white/5 shadow-inner">
          <span className="text-[9px] text-white/50 font-black uppercase tracking-widest flex items-center gap-2">
            <CalendarDays size={10} /> Importado em
          </span>
          <span className="text-xs font-medium text-white/90">{source.createdAt}</span>
        </div>

        <div className="space-y-4">
          <label className="text-[9px] font-black text-white/50 uppercase tracking-[1.5px] flex items-center gap-2">
            <ListChecks size={12} /> Linha de Cabeçalho
          </label>
          <div className="grid grid-cols-[auto,1fr,auto] items-center gap-2">
            <button
              type="button"
              onClick={() => onHeaderRowChange(Math.max(0, config.headerRow - 1))}
              className="h-10 w-10 rounded-lg border border-white/20 bg-black/40 hover:bg-white/10 text-white/75 text-lg font-bold"
            >
              -
            </button>
            <input
              type="number"
              min={0}
              step={1}
              value={config.headerRow}
              onChange={(event) => {
                const parsed = Number.parseInt(event.target.value, 10)
                onHeaderRowChange(Number.isFinite(parsed) ? Math.max(0, parsed) : 0)
              }}
              className="h-10 w-full px-3 bg-black/40 border border-white/20 rounded-lg text-xs text-white focus:border-[#00AE9D] outline-none"
            />
            <button
              type="button"
              onClick={() => onHeaderRowChange(config.headerRow + 1)}
              className="h-10 w-10 rounded-lg border border-white/20 bg-black/40 hover:bg-white/10 text-white/75 text-lg font-bold"
            >
              +
            </button>
          </div>
          <p className="text-[10px] text-white/40 leading-relaxed font-medium">
            Ajuste a linha que contém os nomes das colunas para corrigir previews vazios.
          </p>
        </div>

        <div className="space-y-4">
          <label className="text-[9px] font-black text-[#00AE9D] uppercase tracking-[1.5px] flex items-center gap-2">
            <Key size={12} /> Coluna SIC (Join Key)
          </label>
          <select
            className="w-full h-10 px-3 bg-[#00AE9D]/5 border border-[#00AE9D]/30 rounded-lg text-xs text-white focus:border-[#00AE9D] focus:ring-1 focus:ring-[#00AE9D] outline-none transition-all shadow-inner appearance-none cursor-pointer"
            value={config.sicColumn}
            onChange={(event) => onSicColumnChange(event.target.value)}
          >
            <option value="" className="bg-[#111]" disabled>
              Selecionar chave primária...
            </option>
            {columns.map((column) => (
              <option key={column} value={column} className="bg-[#111]">
                {column}
              </option>
            ))}
          </select>
          <p className="text-[10px] text-white/40 leading-relaxed font-medium">
            Atuará como chave de cruzamento universal (ex: Hostname, Serial).
          </p>
        </div>

        <div className="space-y-4">
          <label className="text-[9px] font-black text-white/50 uppercase tracking-[1.5px] flex items-center gap-2">
            <ListChecks size={12} /> Seleção de Colunas
          </label>
          <div className="space-y-1.5 max-h-64 overflow-y-auto pr-2 custom-scrollbar bg-black/20 rounded-xl p-3 border border-white/5 shadow-inner">
            {columns.map((column) => (
              <button
                key={column}
                type="button"
                onClick={() => onToggleColumn(column)}
                className="w-full flex items-center gap-3 p-2.5 rounded-lg hover:bg-white/5 cursor-pointer group transition-colors"
              >
                <div
                  className={`w-4 h-4 rounded border flex items-center justify-center transition-colors shadow-inner shrink-0 ${
                    config.selectedCols.includes(column)
                      ? 'bg-[#00AE9D] border-[#00AE9D]'
                      : 'bg-black/50 border-white/20 group-hover:border-white/40'
                  }`}
                >
                  {config.selectedCols.includes(column) && <CheckCircle2 size={10} className="text-white" />}
                </div>
                <span
                  className={`text-xs font-medium truncate ${
                    config.selectedCols.includes(column) ? 'text-white/90' : 'text-white/30'
                  }`}
                >
                  {column}
                </span>
              </button>
            ))}
          </div>
        </div>
      </div>

      <div className="p-6 border-t border-white/10 bg-black/40 backdrop-blur-xl">
        <ActionButton
          onClick={onSaveProfile}
          variant="primary"
          size="md"
          className="w-full"
          disabled={!config.sicColumn || isSavingProfile}
        >
          {isSavingProfile
            ? 'SALVANDO...'
            : config.status === 'pronto'
              ? 'ATUALIZAR PERFIL'
              : 'SALVAR PERFIL'}
        </ActionButton>
      </div>
    </div>
  )
}
