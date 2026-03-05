import { api } from '../api/client';
import { MachineItem, MachineFilterParams } from '../api/types';
import { ColumnDef, DEFAULT_COLUMNS } from './column-picker';
import { exportToCsv } from '../utils/export';

// Register sub components just in case
import './status-chip';
import './column-picker';

export class DataGrid extends HTMLElement {
    #items: MachineItem[] = [];
    #columns: ColumnDef[] = [...DEFAULT_COLUMNS];
    #isLoading = false;
    #currentFilters: MachineFilterParams = {};

    constructor() {
        super();
        this.attachShadow({ mode: 'open' });
    }

    connectedCallback() {
        this.render();

        // Listen to global events
        document.addEventListener('filters-applied', (e: any) => this.handleFilters(e.detail));
        document.addEventListener('columns-changed', (e: any) => this.handleColumns(e.detail.columns));

        // Initial fetch (if columns load before grid, we might miss the event, so we read local storage too)
        const storedCols = localStorage.getItem('cg-columns');
        if (storedCols) {
            try {
                const parsed = JSON.parse(storedCols);
                this.#columns = DEFAULT_COLUMNS.map(def => {
                    const found = parsed.find((p: any) => p.id === def.id);
                    return found ? { ...def, visible: found.visible } : def;
                });
            } catch (e) { /* ignore */ }
        }

        this.fetchData();
    }

    handleFilters(filters: MachineFilterParams) {
        this.#currentFilters = filters;
        this.fetchData();
    }

    handleColumns(columns: ColumnDef[]) {
        this.#columns = columns;
        this.renderTable();
    }

    async fetchData() {
        this.#isLoading = true;
        this.renderTable(); // Update loading state

        try {
            // Simulate spreadsheet by fetching a large chunk
            const result = await api.machines.getTable(this.#currentFilters);
            this.#items = result.items;
        } catch (e) {
            console.error('Data grid fetch error:', e);
        } finally {
            this.#isLoading = false;
            this.renderTable();
        }
    }

    handleExport() {
        exportToCsv(this.#items, this.#columns, 'compliance_gate_machines');
    }

    // --- Rendering logic ---

    renderCell(col: ColumnDef, item: MachineItem): string {
        const val = item[col.id as keyof MachineItem];

        if (col.id === 'primary_status_label') {
            return `<cg-status-chip severity="${item.primary_status}" label="${val || 'Unknown'}"></cg-status-chip>`;
    }
    
    if (col.id === 'flags' && Array.isArray(val)) {
      if (val.length === 0) return '<span class="muted">-</span>';
      return val.map(f => `<cg-status-chip severity="WARNING" label="${f}"></cg-status-chip>`).join(' ');
    }
    
    if (typeof val === 'boolean') {
      return val 
        ? '<span class="bool-true">Yes</span>' 
        : '<span class="bool-false">No</span>';
    }

    if (val === null || val === undefined || val === '') {
      return '<span class="muted">-</span>';
    }

    return String(val);
  }

  renderTable() {
    const container = this.shadowRoot?.querySelector('#table-container');
    if (!container) return;

    if (this.#isLoading) {
      container.innerHTML = `<div class="center-msg">Loading data...</div>`;
      return;
    }

    if (this.#items.length === 0) {
      container.innerHTML = `<div class="center-msg">No machines found for current filters.</div>`;
      return;
    }

    const visibleCols = this.#columns.filter(c => c.visible);

    const thead = `
      <thead>
        <tr>
          ${visibleCols.map(c => `<th>${c.label}</th>`).join('')}
        </tr>
      </thead>
    `;

    const tbody = `
      <tbody>
        ${this.#items.map(item => `
          <tr>
            ${visibleCols.map(c => `<td>${this.renderCell(c, item)}</td>`).join('')}
          </tr>
        `).join('')}
      </tbody>
    `;

    container.innerHTML = `
      <table>
        ${thead}
        ${tbody}
      </table>
    `;
  }

  render() {
    if (!this.shadowRoot) return;

    this.shadowRoot.innerHTML = `
      <style>
        :host {
          display: flex;
          flex-direction: column;
          height: 100%;
          font-family: var(--font-family-base);
          background: var(--color-bg-panel);
          border: 1px solid var(--color-border);
          border-radius: var(--radius-md);
          overflow: hidden;
          box-shadow: var(--shadow-sm);
        }

        .toolbar {
          padding: var(--spacing-3) var(--spacing-4);
          border-bottom: 1px solid var(--color-border);
          display: flex;
          justify-content: space-between;
          align-items: center;
          background: #fdfdfd;
        }
        
        .toolbar-left {
          display: flex;
          gap: var(--spacing-3);
          align-items: center;
        }

        .toolbar-title {
          font-size: var(--font-size-md);
          font-weight: 600;
          color: var(--color-text-main);
        }

        button.export {
          padding: var(--spacing-2) var(--spacing-4);
          background: var(--color-primary);
          color: white;
          border: none;
          border-radius: var(--radius-sm);
          font-weight: 500;
          font-size: var(--font-size-sm);
          cursor: pointer;
          transition: background 0.2s;
        }
        button.export:hover {
          background: var(--color-primary-hover);
        }

        .table-wrapper {
          flex: 1;
          overflow: auto;
          position: relative;
        }

        table {
          width: 100%;
          border-collapse: collapse;
          text-align: left;
        }

        th {
          position: sticky;
          top: 0;
          background: var(--color-bg-panel);
          color: var(--color-text-muted);
          font-size: 11px;
          text-transform: uppercase;
          letter-spacing: 0.5px;
          padding: var(--spacing-3) var(--spacing-4);
          border-bottom: 2px solid var(--color-border);
          z-index: 10;
          white-space: nowrap;
        }

        td {
          padding: var(--spacing-3) var(--spacing-4);
          border-bottom: 1px solid var(--color-border);
          font-size: var(--font-size-sm);
          color: var(--color-text-main);
          white-space: nowrap;
        }

        tr:hover td {
          background-color: var(--color-bg-hover);
        }

        .muted {
          color: var(--color-text-muted);
          opacity: 0.5;
        }
        
        .bool-true { color: var(--color-status-success-text); font-weight: bold; }
        .bool-false { color: var(--color-status-danger-text); }

        .center-msg {
          padding: var(--spacing-8);
          text-align: center;
          color: var(--color-text-muted);
          font-size: var(--font-size-md);
        }
      </style>

      <div class="toolbar">
        <div class="toolbar-left">
          <div class="toolbar-title">Data Grid</div>
          <cg-column-picker></cg-column-picker>
        </div>
        <button class="export" id="export-csv">Export CSV</button>
      </div>

      <div class="table-wrapper" id="table-container">
        <!-- Rendered by JS -->
      </div>
    `;

    this.shadowRoot.querySelector('#export-csv')?.addEventListener('click', () => this.handleExport());
    this.renderTable(); // Initial state
  }
}

customElements.define('cg-data-grid', DataGrid);
