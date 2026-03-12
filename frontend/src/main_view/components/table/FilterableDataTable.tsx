import { useMemo, useRef } from 'react'
import { Filter, Key } from 'lucide-react'
import { ExcelFilterPopover } from '@/main_view/components/panels/ExcelFilterPopover'

type FilterableDataTableProps = {
  headers: string[]
  rows: Array<Record<string, unknown>>
  filters: Record<string, string[]>
  openFilterMenu: string | null
  onOpenFilterMenu: (value: string | null) => void
  onApplyFilter: (column: string, selection: string[]) => void
  filterScopeKey: string
  selectedColumns?: string[]
  sicColumn?: string
  columnLabels?: Record<string, string>
  tableTestId?: string
  counterTestId?: string
  scrollTestId?: string
  totalRows?: number
  isLoadingInitial?: boolean
  hasNextPage?: boolean
  isLoadingMore?: boolean
  onReachEnd?: () => void
  loadingMessage?: string
  emptyMessage?: string
  noColumnsMessage?: string
  rowKey?: (row: Record<string, unknown>, index: number) => string
}

export function formatTableCellValue(value: unknown): string {
  if (Array.isArray(value)) return value.join(', ')
  if (typeof value === 'boolean') return value ? 'Sim' : 'Não'
  if (value == null) return '-'
  if (typeof value === 'object') return JSON.stringify(value)
  const normalized = String(value).trim()
  return normalized.length > 0 ? normalized : '-'
}

export function FilterableDataTable({
  headers,
  rows,
  filters,
  openFilterMenu,
  onOpenFilterMenu,
  onApplyFilter,
  filterScopeKey,
  selectedColumns,
  sicColumn,
  columnLabels,
  tableTestId,
  counterTestId,
  scrollTestId,
  totalRows,
  isLoadingInitial = false,
  hasNextPage = false,
  isLoadingMore = false,
  onReachEnd,
  loadingMessage = 'Carregando tabela...',
  emptyMessage = 'Nenhum registro encontrado para os filtros atuais.',
  noColumnsMessage = 'Nenhuma coluna disponível para exibição.',
  rowKey,
}: FilterableDataTableProps) {
  const scrollRef = useRef<HTMLDivElement | null>(null)
  const columns = headers.length > 0 ? headers : rows.length > 0 ? Object.keys(rows[0]) : []
  const shouldDimBySelection = Array.isArray(selectedColumns) && selectedColumns.length > 0

  const uniqueValuesByColumn = useMemo<Record<string, string[]>>(
    () =>
      columns.reduce<Record<string, string[]>>((acc, column) => {
        acc[column] = [...new Set(rows.map((row) => formatTableCellValue(row[column])))]
        return acc
      }, {}),
    [columns, rows],
  )

  const filteredRows = useMemo(
    () =>
      rows.filter((row) =>
        Object.entries(filters).every(([column, selectedValues]) => {
          if (!selectedValues || selectedValues.length === 0) return true
          return selectedValues.includes(formatTableCellValue(row[column]))
        }),
      ),
    [filters, rows],
  )

  const handleScroll = () => {
    if (!onReachEnd || !hasNextPage || isLoadingMore || isLoadingInitial) return
    const node = scrollRef.current
    if (!node) return
    const distanceToBottom = node.scrollHeight - node.scrollTop - node.clientHeight
    if (distanceToBottom <= 180) {
      onReachEnd()
    }
  }

  if (isLoadingInitial) {
    return (
      <div className="flex-1 rounded-2xl border border-white/10 bg-[#0A0A0A]/80 backdrop-blur-md p-8 text-center text-white/60 text-sm">
        {loadingMessage}
      </div>
    )
  }

  if (columns.length === 0) {
    return (
      <div className="flex-1 rounded-2xl border border-white/10 bg-[#0A0A0A]/80 backdrop-blur-md p-8 text-center text-white/60 text-sm">
        {noColumnsMessage}
      </div>
    )
  }

  return (
    <div
      data-testid={tableTestId}
      className="flex-1 rounded-2xl overflow-hidden border border-white/10 shadow-[0_8px_32px_rgba(0,0,0,0.4)] backdrop-blur-md bg-[#0A0A0A]/80 flex flex-col"
    >
      <div data-testid={counterTestId} className="hidden">
        {filteredRows.length} / {totalRows ?? rows.length}
      </div>

      <div
        ref={scrollRef}
        data-testid={scrollTestId}
        className="flex-1 overflow-auto custom-scrollbar relative"
        onScroll={handleScroll}
      >
        <table className="w-full text-sm text-left whitespace-nowrap">
          <thead className="bg-[#111] border-b border-white/10 text-white/60 text-[10px] uppercase tracking-[0.15em] font-black sticky top-0 z-20 backdrop-blur-xl">
            <tr>
              <th className="px-6 py-4 w-16 align-middle border-r border-white/5">#</th>
              {columns.map((column) => {
                const uniqueValues = uniqueValuesByColumn[column] ?? []
                const currentFilter = filters[column]
                const isFiltered = Boolean(currentFilter) && currentFilter.length > 0 && currentFilter.length !== uniqueValues.length
                const filterKey = `${filterScopeKey}_${column}`
                const isSelected = !shouldDimBySelection || selectedColumns.includes(column)
                const displayLabel = columnLabels?.[column] ?? column

                return (
                  <th
                    key={column}
                    className={`px-4 py-3 border-r border-white/5 align-middle ${isSelected ? '' : 'opacity-30'}`}
                  >
                    <div className="flex items-center justify-between gap-4 relative">
                      <div className={`flex items-center gap-2 ${isSelected ? '' : 'line-through'} text-white/80`}>
                        {sicColumn === column ? <Key size={12} className="text-[#00AE9D]" /> : null}
                        {displayLabel}
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

                      {openFilterMenu === filterKey ? (
                        <ExcelFilterPopover
                          options={uniqueValues}
                          selectedOptions={currentFilter}
                          onClose={() => onOpenFilterMenu(null)}
                          onApply={(selection) => {
                            onApplyFilter(column, selection)
                            onOpenFilterMenu(null)
                          }}
                        />
                      ) : null}
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
                  {emptyMessage}
                </td>
              </tr>
            ) : (
              filteredRows.map((row, rowIndex) => (
                <tr key={rowKey ? rowKey(row, rowIndex) : rowIndex} className="hover:bg-white/5 transition-colors">
                  <td className="px-6 py-4 font-mono text-white/30 text-xs border-r border-white/5">{rowIndex + 1}</td>
                  {columns.map((column) => {
                    const isSelected = !shouldDimBySelection || selectedColumns.includes(column)
                    return (
                      <td key={column} className={`px-4 py-4 border-r border-white/5 ${isSelected ? '' : 'opacity-30'}`}>
                        {formatTableCellValue(row[column])}
                      </td>
                    )
                  })}
                </tr>
              ))
            )}
          </tbody>
        </table>

        {isLoadingMore ? (
          <div className="absolute bottom-0 left-0 right-0 text-center text-xs text-white/60 py-3 bg-black/60 backdrop-blur-md">
            Carregando mais registros...
          </div>
        ) : null}
      </div>
    </div>
  )
}
