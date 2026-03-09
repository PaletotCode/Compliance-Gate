import { CheckCircle2, Filter, Key } from 'lucide-react'
import { ExcelFilterPopover } from '@/main_view/components/panels/ExcelFilterPopover'

type MaterializedRow = Record<string, string>

type MaterializedDataTableProps = {
  rows: MaterializedRow[]
  activeMatCols: string[]
  filters: Record<string, string[]>
  openFilterMenu: string | null
  onOpenFilterMenu: (value: string | null) => void
  onApplyFilter: (column: string, selection: string[]) => void
}

function getUniqueValues(rows: MaterializedRow[], column: string): string[] {
  return [...new Set(rows.map((row) => row[column]))]
}

export function MaterializedDataTable({
  rows,
  activeMatCols,
  filters,
  openFilterMenu,
  onOpenFilterMenu,
  onApplyFilter,
}: MaterializedDataTableProps) {
  const filteredRows = rows.filter((row) =>
    Object.entries(filters).every(([column, selectedValues]) => {
      if (!selectedValues) return true
      return selectedValues.includes(row[column])
    }),
  )

  const isSicFiltered =
    Boolean(filters.SIC_CHAVE) && filters.SIC_CHAVE.length !== getUniqueValues(rows, 'SIC_CHAVE').length

  return (
    <div className="flex-1 overflow-hidden rounded-2xl border border-white/10 shadow-[0_8px_32px_rgba(0,0,0,0.5)] backdrop-blur-xl bg-[#0A0A0A]/80 flex flex-col relative">
      <div className="absolute top-4 right-6 z-30 inline-flex items-center gap-2 px-3 py-1.5 rounded-lg bg-[#00AE9D]/10 text-[#00AE9D] text-[9px] font-black uppercase tracking-widest border border-[#00AE9D]/20 backdrop-blur-md pointer-events-none shadow-[0_0_20px_rgba(0,174,157,0.1)]">
        <CheckCircle2 size={12} /> CMDB (15 registros unificados)
      </div>

      <div className="flex-1 overflow-auto custom-scrollbar relative z-10">
        <table className="w-full text-sm text-left whitespace-nowrap">
          <thead className="bg-[#111] border-b border-white/10 text-white/60 text-[10px] uppercase tracking-[0.15em] font-black sticky top-0 z-20 backdrop-blur-2xl shadow-sm">
            <tr>
              <th className="px-6 py-4 w-16 align-middle border-r border-white/5">#</th>

              <th className="px-4 py-3 border-r border-white/5 align-middle">
                <div className="flex items-center justify-between gap-4 relative">
                  <div className="flex items-center gap-2 text-[#00AE9D]">
                    <Key size={12} /> SIC (Chave)
                  </div>
                  <button
                    type="button"
                    onClick={(event) => {
                      event.stopPropagation()
                      onOpenFilterMenu(openFilterMenu === 'MAT_SIC_CHAVE' ? null : 'MAT_SIC_CHAVE')
                    }}
                    className={`p-1.5 rounded-md transition-all hover:bg-white/10 ${
                      isSicFiltered ? 'text-[#00AE9D] bg-[#00AE9D]/10' : 'text-white/30 hover:text-white'
                    }`}
                  >
                    <Filter size={12} className={isSicFiltered ? 'fill-[#00AE9D]/20' : ''} />
                  </button>

                  {openFilterMenu === 'MAT_SIC_CHAVE' && (
                    <ExcelFilterPopover
                      options={getUniqueValues(rows, 'SIC_CHAVE')}
                      selectedOptions={filters.SIC_CHAVE}
                      onClose={() => onOpenFilterMenu(null)}
                      onApply={(selection) => {
                        onApplyFilter('SIC_CHAVE', selection)
                        onOpenFilterMenu(null)
                      }}
                    />
                  )}
                </div>
              </th>

              {activeMatCols.map((columnKey) => {
                const sourceId = columnKey.split('_')[0]
                const columnName = columnKey.substring(sourceId.length + 1)
                const uniqueValues = getUniqueValues(rows, columnKey)
                const isFiltered =
                  Boolean(filters[columnKey]) && filters[columnKey].length !== uniqueValues.length
                const menuKey = `MAT_${columnKey}`

                return (
                  <th key={columnKey} className="px-4 py-3 border-r border-white/5 align-middle">
                    <div className="flex items-center justify-between gap-4 relative">
                      <div className="flex items-center gap-2">
                        <span className="opacity-50 text-[9px] font-mono tracking-widest bg-white/5 px-1.5 py-0.5 rounded-md">
                          {sourceId}
                        </span>
                        {columnName}
                      </div>

                      <button
                        type="button"
                        onClick={(event) => {
                          event.stopPropagation()
                          onOpenFilterMenu(openFilterMenu === menuKey ? null : menuKey)
                        }}
                        className={`p-1.5 rounded-md transition-all hover:bg-white/10 ${
                          isFiltered || openFilterMenu === menuKey
                            ? 'text-[#00AE9D] bg-[#00AE9D]/10'
                            : 'text-white/30 hover:text-white'
                        }`}
                      >
                        <Filter size={12} className={isFiltered ? 'fill-[#00AE9D]/20' : ''} />
                      </button>

                      {openFilterMenu === menuKey && (
                        <ExcelFilterPopover
                          options={uniqueValues}
                          selectedOptions={filters[columnKey]}
                          onClose={() => onOpenFilterMenu(null)}
                          onApply={(selection) => {
                            onApplyFilter(columnKey, selection)
                            onOpenFilterMenu(null)
                          }}
                        />
                      )}
                    </div>
                  </th>
                )
              })}
            </tr>
          </thead>

          <tbody className="text-white/80 divide-y divide-white/5">
            {filteredRows.length === 0 ? (
              <tr>
                <td colSpan={activeMatCols.length + 2} className="px-6 py-12 text-center text-white/40 italic text-xs">
                  Nenhum registro encontrado para os filtros aplicados.
                </td>
              </tr>
            ) : (
              filteredRows.map((row, rowIndex) => (
                <tr key={rowIndex} className="hover:bg-white/5 transition-colors">
                  <td className="px-6 py-4 font-mono text-white/30 text-xs border-r border-white/5">
                    {rowIndex + 1}
                  </td>
                  <td className="px-6 py-4 border-r border-white/5 font-mono text-[#00AE9D] font-bold">
                    {row.SIC_CHAVE}
                  </td>

                  {activeMatCols.map((columnKey) => (
                    <td key={columnKey} className="px-4 py-4 border-r border-white/5">
                      {columnKey.includes('Estado') || columnKey.includes('Status') ? (
                        <span
                          className={`px-2 py-1 rounded-md text-[9px] font-black uppercase tracking-wider backdrop-blur-sm border ${
                            row[columnKey] === 'Em Manutenção'
                              ? 'bg-amber-500/10 text-amber-500 border-amber-500/20'
                              : 'bg-emerald-500/10 text-emerald-500 border-emerald-500/20'
                          }`}
                        >
                          {row[columnKey]}
                        </span>
                      ) : (
                        row[columnKey]
                      )}
                    </td>
                  ))}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
