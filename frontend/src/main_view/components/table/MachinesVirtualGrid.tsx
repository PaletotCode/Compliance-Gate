import { useEffect, useMemo, useRef } from 'react'
import { Filter } from 'lucide-react'
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  useReactTable,
  type ColumnDef,
} from '@tanstack/react-table'
import { useVirtualizer } from '@tanstack/react-virtual'
import { ExcelFilterPopover } from '@/main_view/components/panels/ExcelFilterPopover'
import type { MachineTableRow } from '@/main_view/state/types'

type MachinesVirtualGridProps = {
  rows: MachineTableRow[]
  visibleColumns: string[]
  filters: Record<string, string[]>
  openFilterMenu: string | null
  totalRows: number
  hasNextPage: boolean
  isLoadingInitial: boolean
  isLoadingMore: boolean
  onOpenFilterMenu: (value: string | null) => void
  onApplyFilter: (column: string, selection: string[]) => void
  onReachEnd: () => void
}

const columnHelper = createColumnHelper<MachineTableRow>()

const HEADER_LABELS: Record<string, string> = {
  hostname: 'Hostname',
  pa_code: 'PA',
  primary_status: 'Status',
  primary_status_label: 'Status Label',
  flags: 'Flags',
  has_ad: 'AD',
  has_uem: 'UEM',
  has_edr: 'EDR',
  has_asset: 'ASSET',
  model: 'Modelo',
  ip: 'IP',
  tags: 'Tags',
  main_user: 'Usuário',
  ad_os: 'AD OS',
  uem_serial: 'Serial UEM',
  edr_serial: 'Serial EDR',
  chassis: 'Chassis',
}

function toLabel(key: string): string {
  return HEADER_LABELS[key] ?? key.replace(/_/g, ' ').replace(/\b\w/g, (m) => m.toUpperCase())
}

function toFilterMenuKey(key: string): string {
  return `MATERIALIZED_${key}`
}

function readValue(row: MachineTableRow, key: string): unknown {
  return (row as Record<string, unknown>)[key]
}

function formatCellValue(value: unknown): string {
  if (Array.isArray(value)) return value.join(', ')
  if (typeof value === 'boolean') return value ? 'Sim' : 'Não'
  if (value == null) return '-'
  if (typeof value === 'object') return JSON.stringify(value)
  const normalized = String(value).trim()
  return normalized.length > 0 ? normalized : '-'
}

export function MachinesVirtualGrid({
  rows,
  visibleColumns,
  filters,
  openFilterMenu,
  totalRows,
  hasNextPage,
  isLoadingInitial,
  isLoadingMore,
  onOpenFilterMenu,
  onApplyFilter,
  onReachEnd,
}: MachinesVirtualGridProps) {
  const parentRef = useRef<HTMLDivElement | null>(null)

  const valueOptionsByColumn = useMemo<Record<string, string[]>>(() => {
    return visibleColumns.reduce<Record<string, string[]>>((acc, key) => {
      acc[key] = [...new Set(rows.map((row) => formatCellValue(readValue(row, key))))]
      return acc
    }, {})
  }, [rows, visibleColumns])

  const filteredRows = useMemo(
    () =>
      rows.filter((row) =>
        Object.entries(filters).every(([column, selectedValues]) => {
          if (!selectedValues || selectedValues.length === 0) return true
          return selectedValues.includes(formatCellValue(readValue(row, column)))
        }),
      ),
    [filters, rows],
  )

  const columns = useMemo<ColumnDef<MachineTableRow, unknown>[]>(() => {
    const dynamic = visibleColumns.map((key) =>
      columnHelper.accessor((row) => readValue(row, key), {
        id: key,
        header: () => {
          const uniqueValues = valueOptionsByColumn[key] ?? []
          const isFiltered =
            Boolean(filters[key]) &&
            filters[key].length > 0 &&
            filters[key].length !== uniqueValues.length
          const menuKey = toFilterMenuKey(key)

          return (
            <div className="flex items-center justify-between gap-2 relative overflow-visible">
              <span className="truncate">{toLabel(key)}</span>
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
                  selectedOptions={filters[key]}
                  onClose={() => onOpenFilterMenu(null)}
                  onApply={(selection) => {
                    onApplyFilter(key, selection)
                    onOpenFilterMenu(null)
                  }}
                />
              )}
            </div>
          )
        },
        cell: (info) => formatCellValue(info.getValue()),
      }),
    )

    return [
      columnHelper.display({
        id: 'row_index',
        header: '#',
        cell: (info) => info.row.index + 1,
      }),
      ...dynamic,
    ]
  }, [visibleColumns, valueOptionsByColumn, filters, onOpenFilterMenu, openFilterMenu, onApplyFilter])

  const table = useReactTable({
    data: filteredRows,
    columns,
    getCoreRowModel: getCoreRowModel(),
  })

  const virtualizer = useVirtualizer({
    count: table.getRowModel().rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 44,
    overscan: 16,
  })

  const virtualRows = virtualizer.getVirtualItems()
  const rowModel = table.getRowModel().rows

  useEffect(() => {
    const lastItem = virtualRows[virtualRows.length - 1]
    if (!lastItem) return

    if (lastItem.index >= rowModel.length - 20 && hasNextPage && !isLoadingMore && !isLoadingInitial) {
      onReachEnd()
    }
  }, [virtualRows, rowModel.length, hasNextPage, isLoadingMore, isLoadingInitial, onReachEnd])

  const gridTemplateColumns = `80px repeat(${Math.max(columns.length - 1, 1)}, minmax(170px, 1fr))`

  if (isLoadingInitial) {
    return (
      <div className="flex-1 rounded-2xl border border-white/10 bg-[#0A0A0A]/80 backdrop-blur-md p-8 text-center text-white/60 text-sm">
        Carregando tabela principal...
      </div>
    )
  }

  if (columns.length <= 1) {
    return (
      <div className="flex-1 rounded-2xl border border-white/10 bg-[#0A0A0A]/80 backdrop-blur-md p-8 text-center text-white/60 text-sm">
        Nenhuma coluna selecionada no gerenciador.
      </div>
    )
  }

  return (
    <div
      data-testid="machines-virtual-grid"
      className="flex-1 rounded-2xl overflow-hidden border border-white/10 shadow-[0_8px_32px_rgba(0,0,0,0.45)] backdrop-blur-md bg-[#0A0A0A]/80 flex flex-col"
    >
      <div data-testid="machines-grid-counter" className="hidden">
        {filteredRows.length} / {totalRows}
      </div>

      <div
        className="grid bg-[#111] border-b border-white/10 text-white/60 text-[10px] uppercase tracking-[0.15em] font-black sticky top-0 z-20"
        style={{ gridTemplateColumns }}
        title={`${filteredRows.length} de ${totalRows} registros`}
      >
        {table.getFlatHeaders().map((header) => (
          <div key={header.id} className="px-4 py-3 border-r border-white/5 truncate relative overflow-visible">
            {flexRender(header.column.columnDef.header, header.getContext())}
          </div>
        ))}
      </div>

      <div
        ref={parentRef}
        data-testid="machines-grid-scroll"
        className="flex-1 overflow-auto custom-scrollbar relative"
      >
        <div style={{ height: `${virtualizer.getTotalSize()}px`, position: 'relative' }}>
          {virtualRows.map((virtualRow) => {
            const row = rowModel[virtualRow.index]
            return (
              <div
                key={row.id}
                className="absolute left-0 right-0 grid text-sm text-white/85 border-b border-white/5 hover:bg-white/5 transition-colors"
                style={{
                  gridTemplateColumns,
                  transform: `translateY(${virtualRow.start}px)`,
                  height: `${virtualRow.size}px`,
                }}
              >
                {row.getVisibleCells().map((cell) => (
                  <div key={cell.id} className="px-4 py-3 border-r border-white/5 truncate">
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </div>
                ))}
              </div>
            )
          })}
        </div>

        {isLoadingMore && (
          <div className="absolute bottom-0 left-0 right-0 text-center text-xs text-white/60 py-3 bg-black/60 backdrop-blur-md">
            Carregando mais registros...
          </div>
        )}
      </div>
    </div>
  )
}
