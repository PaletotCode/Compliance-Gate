import { FilterableDataTable } from '@/main_view/components/table/FilterableDataTable'
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
  return (
    <FilterableDataTable
      headers={headers}
      rows={rows as Array<Record<string, unknown>>}
      filters={filters}
      openFilterMenu={openFilterMenu}
      onOpenFilterMenu={onOpenFilterMenu}
      onApplyFilter={onApplyFilter}
      filterScopeKey={activeTab}
      selectedColumns={config.selectedCols}
      sicColumn={config.sicColumn}
      noColumnsMessage="Nenhuma coluna detectada para esta fonte."
    />
  )
}
