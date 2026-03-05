import { MachineItem } from '../api/types';
import { ColumnDef } from '../components/column-picker';

export function exportToCsv(data: MachineItem[], columns: ColumnDef[], filename = 'machines_export.csv') {
    if (!data || !data.length) {
        alert('No data to export.');
        return;
    }

    // Filter only visible columns
    const visibleCols = columns.filter(c => c.visible);

    // Create header row
    const headers = visibleCols.map(c => `"${c.label}"`).join(',');
  
  // Create data rows
  const rows = data.map(item => {
    return visibleCols.map(col => {
      let val: any = item[col.id as keyof MachineItem];
      
      // Formatting specific fields
      if (col.id === 'flags' && Array.isArray(val)) {
        val = val.join(' | ');
      }
      if (typeof val === 'boolean') {
        val = val ? 'True' : 'False';
      }
      
      // Escape quotes and wrap in quotes
      val = val === null || val === undefined ? '' : String(val);
      val = val.replace(/"/g, '""');
      return `"${val}"`;
    }).join(',');
  });

  const csvContent = [headers, ...rows].join('\n');
  
  // Create blob and force download
  const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  
  const link = document.createElement('a');
  link.setAttribute('href', url);
  link.setAttribute('download', `${filename.replace('.csv', '')}_${new Date().getTime()}.csv`);
  link.style.display = 'none';
  
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
}
