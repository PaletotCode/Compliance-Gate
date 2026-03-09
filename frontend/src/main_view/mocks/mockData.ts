import type { SourceId, SourceItem, SourceMockData } from '@/main_view/state/types'

export const REQUIRED_DELETE_TEXT = 'Eu TI consinto em apagar'

export const INITIAL_SOURCES: SourceItem[] = [
  { id: 'AD', name: 'Active Directory', type: 'CSV', createdAt: '08 Mar 2026, 11:30' },
  { id: 'UEM', name: 'Workspace ONE (UEM)', type: 'CSV', createdAt: '08 Mar 2026, 11:32' },
  { id: 'EDR', name: 'CrowdStrike (EDR)', type: 'CSV', createdAt: '08 Mar 2026, 11:35' },
  { id: 'ASSET', name: 'GLPI (Ativos)', type: 'CSV', createdAt: '08 Mar 2026, 11:40' },
]

export function generateMockData(): SourceMockData {
  const ad = Array.from({ length: 15 }).map((_, i) => ({
    'Computer Name': `BR-LT-${String(i + 1).padStart(3, '0')}`,
    'DNS Name': `br-lt-${String(i + 1).padStart(3, '0')}.sicoob.local`,
    'Operating System': i % 3 === 0 ? 'Windows 10' : 'Windows 11',
    Version: i % 2 === 0 ? '22H2' : '21H2',
    'Last Logon': `2026-03-${String((i % 7) + 1).padStart(2, '0')}`,
  }))

  const uem = Array.from({ length: 15 }).map((_, i) => ({
    'Friendly Name': `BR-LT-${String(i + 1).padStart(3, '0')}`,
    Username: `usuario.${i + 1}`,
    'Serial Number': `PF2XYZ${i}`,
    'Last Seen': `2026-03-${String((i % 8) + 1).padStart(2, '0')}`,
    OS: i % 3 === 0 ? 'Win10' : 'Win11',
    Model: i % 2 === 0 ? 'ThinkPad T14' : 'Dell Latitude',
  }))

  const edr = Array.from({ length: 15 }).map((_, i) => ({
    Hostname: `BR-LT-${String(i + 1).padStart(3, '0')}`,
    'Last Seen': `2026-03-${String((i % 5) + 1).padStart(2, '0')}`,
    'Local IP': `10.0.0.${40 + i}`,
    'OS Version': i % 3 === 0 ? 'Windows 10' : 'Windows 11',
    'Sensor Tags': 'TI, Compliance',
    'Serial Number': `PF2XYZ${i}`,
  }))

  const asset = Array.from({ length: 15 }).map((_, i) => ({
    'Nome do ativo': `BR-LT-${String(i + 1).padStart(3, '0')}`,
    Usuário: `usuario.${i + 1}`,
    'Estado do ativo': i % 5 === 0 ? 'Em Manutenção' : 'Ativo',
    Fornecedor: i % 2 === 0 ? 'Lenovo' : 'Dell',
    'Data Aquisição': `2023-0${(i % 9) + 1}-15`,
  }))

  return { AD: ad, UEM: uem, EDR: edr, ASSET: asset }
}

export const MOCK_DATA: SourceMockData = generateMockData()

export const DEFAULT_ACTIVE_MAT_COLS: string[] = [
  'AD_Operating System',
  'UEM_Username',
  'EDR_Local IP',
  'ASSET_Estado do ativo',
]

export function getSourceColumns(sourceId: SourceId): string[] {
  const rows = MOCK_DATA[sourceId]
  if (!rows || rows.length === 0) return []
  return Object.keys(rows[0] ?? {})
}
