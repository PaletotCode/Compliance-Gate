import { Filter, Key } from 'lucide-react'
import { ExcelFilterPopover } from '@/main_view/components/panels/ExcelFilterPopover'
import type { SourceConfig, SourceId, SourceRecord } from '@/main_view/state/types'

type SourceDataTableProps = {
  activeTab: SourceId
  headers: string[]
  rows: SourceRecord[]
  config: SourceConfig
  filters: Record<string, string[]>
  openFilterMenu: string | null
  onOpenFilterMenu: (value: string | null) => void
  onApplyFilter: (column: string, selection: string[]) => void
}

function normalizeCellValue(value: unknown): string {
  if (Array.isArray(value)) return value.join(', ')
  if (typeof value === 'boolean') return value ? 'Sim' : 'Não'
  if (value == null) return '-'
  if (typeof value === 'object') return JSON.stringify(value)
  const normalized = String(value).trim()
  return normalized.length > 0 ? normalized : '-'
}

export function SourceDataTable({
  activeTab,
  headers,
  rows,
  config,
  filters,
  openFilterMenu,
  onOpenFilterMenu,
  onApplyFilter,
}: SourceDataTableProps) {
  const columns = headers.length > 0 ? headers : rows.length > 0 ? Object.keys(rows[0]) : []

  const filteredRows = rows.filter((row) =>
    Object.entries(filters).every(([column, selectedValues]) => {
      if (!selectedValues) return true
      return selectedValues.includes(normalizeCellValue(row[column]))
    }),
  )

  return (
    <div className="flex-1 rounded-2xl overflow-hidden border border-white/10 shadow-[0_8px_32px_rgba(0,0,0,0.4)] backdrop-blur-md bg-[#0A0A0A]/80 flex flex-col">
      <div className="flex-1 overflow-auto custom-scrollbar relative">
        <table className="w-full text-sm text-left whitespace-nowrap">
          <thead className="bg-[#111] border-b border-white/10 text-white/60 text-[10px] uppercase tracking-[0.15em] font-black sticky top-0 z-20 backdrop-blur-xl">
            <tr>
              <th className="px-6 py-4 w-16 align-middle border-r border-white/5">#</th>
              {columns.map((column) => {
                const uniqueValues = [...new Set(rows.map((item) => normalizeCellValue(item[column])))]
                const isFiltered =
                  Boolean(filters[column]) && filters[column].length !== uniqueValues.length
                const filterKey = `${activeTab}_${column}`

                return (
                  <th
                    key={column}
                    className={`px-4 py-3 border-r border-white/5 align-middle ${
                      !config.selectedCols.includes(column) ? 'opacity-30' : ''
                    }`}
                  >
                    <div className="flex items-center justify-between gap-4 relative">
                      <div
                        className={`flex items-center gap-2 ${
                          !config.selectedCols.includes(column) ? 'line-through' : ''
                        } text-white/80`}
                      >
                        {column === config.sicColumn && <Key size={12} className="text-[#00AE9D]" />}
                        {column}
                      </div>

                      <button
                        type="button"
                        onClick={(event) => {
                          event.stopPropagation()
                          onOpenFilterMenu(openFilterMenu === filterKey ? null : filterKey)
                        }}
                        className={`p-1.5 rounded-md transition-all hover:bg-white/10 ${
                          isFiltered || openFilterMenu === filterKey
                            ? 'text-[#00AE9D] bg-[#00AE9D]/10'
                            : 'text-white/30 hover:text-white'
                        }`}
                      >
                        <Filter size={12} className={isFiltered ? 'fill-[#00AE9D]/20' : ''} />
                      </button>

                      {openFilterMenu === filterKey && (
                        <ExcelFilterPopover
                          options={uniqueValues}
                          selectedOptions={filters[column]}
                          onClose={() => onOpenFilterMenu(null)}
                          onApply={(selection) => {
                            onApplyFilter(column, selection)
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
                <td colSpan={columns.length + 1} className="px-6 py-12 text-center text-white/40 italic text-xs">
                  Nenhum registro encontrado para os filtros atuais.
                </td>
              </tr>
            ) : (
              filteredRows.map((row, rowIndex) => (
                <tr key={rowIndex} className="hover:bg-white/5 transition-colors">
                  <td className="px-6 py-4 font-mono text-white/30 text-xs border-r border-white/5">
                    {rowIndex + 1}
                  </td>
                  {columns.map((column) => (
                    <td
                      key={column}
                      className={`px-4 py-4 border-r border-white/5 ${
                        !config.selectedCols.includes(column) ? 'opacity-30' : ''
                      }`}
                    >
                      {normalizeCellValue(row[column])}
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
