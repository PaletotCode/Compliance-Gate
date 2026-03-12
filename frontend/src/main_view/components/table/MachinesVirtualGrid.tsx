import { useMemo } from 'react'
import { FilterableDataTable } from '@/main_view/components/table/FilterableDataTable'
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

function readValue(row: MachineTableRow, key: string): unknown {
  const asRecord = row as Record<string, unknown>
  if (key in asRecord) {
    return asRecord[key]
  }

  const selectedData = asRecord.selected_data
  if (selectedData && typeof selectedData === 'object') {
    const selectedRecord = selectedData as Record<string, unknown>
    if (key in selectedRecord) {
      return selectedRecord[key]
    }
  }

  return undefined
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
  const normalizedRows = useMemo(
    () =>
      rows.map((row, index) => {
        const normalized: Record<string, unknown> = {
          __row_id: String((row as Record<string, unknown>).id ?? `row-${index}`),
        }
        visibleColumns.forEach((key) => {
          normalized[key] = readValue(row, key)
        })
        return normalized
      }),
    [rows, visibleColumns],
  )

  if (isLoadingInitial) {
    return (
      <div className="flex-1 rounded-2xl border border-white/10 bg-[#0A0A0A]/80 backdrop-blur-md p-8 text-center text-white/60 text-sm">
        Carregando tabela principal...
      </div>
    )
  }

  if (visibleColumns.length === 0) {
    return (
      <div className="flex-1 rounded-2xl border border-white/10 bg-[#0A0A0A]/80 backdrop-blur-md p-8 text-center text-white/60 text-sm">
        Nenhuma coluna selecionada no gerenciador.
      </div>
    )
  }

  return (
    <FilterableDataTable
      headers={visibleColumns}
      rows={normalizedRows}
      filters={filters}
      openFilterMenu={openFilterMenu}
      onOpenFilterMenu={onOpenFilterMenu}
      onApplyFilter={onApplyFilter}
      filterScopeKey="MATERIALIZED"
      columnLabels={HEADER_LABELS}
      tableTestId="machines-virtual-grid"
      counterTestId="machines-grid-counter"
      scrollTestId="machines-grid-scroll"
      totalRows={totalRows}
      hasNextPage={hasNextPage}
      isLoadingMore={isLoadingMore}
      onReachEnd={onReachEnd}
      loadingMessage="Carregando tabela principal..."
      noColumnsMessage="Nenhuma coluna selecionada no gerenciador."
      rowKey={(row) => String(row.__row_id ?? '')}
    />
  )
}
