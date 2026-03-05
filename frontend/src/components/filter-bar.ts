import { api } from '../api/client';
import { FilterDefinition, MachineSummary } from '../api/types';

export class FilterBar extends HTMLElement {
    #definitions: FilterDefinition[] = [];
    #summary: MachineSummary | null = null;

    #selectedStatuses: Set<string> = new Set();
    #selectedFlags: Set<string> = new Set();
    #searchTerm: string = '';

    constructor() {
        super();
        this.attachShadow({ mode: 'open' });
    }

    async connectedCallback() {
        try {
            this.#definitions = await api.machines.getFilters();
            this.render();
            this.attachListeners();

            // Auto-fetch initial summary empty
            await this.refreshSummary();
        } catch (e) {
            console.error('Failed to load filters', e);
        }
    }

    async refreshSummary() {
        try {
            this.#summary = await api.machines.getSummary(this.getFilterParams());
            this.render();
            this.attachListeners();
        } catch (e) {
            console.error('Failed to get summary', e);
        }
    }

    getFilterParams() {
        return {
            search: this.#searchTerm || undefined,
            statuses: this.#selectedStatuses.size > 0 ? Array.from(this.#selectedStatuses) : undefined,
            flags: this.#selectedFlags.size > 0 ? Array.from(this.#selectedFlags) : undefined,
        };
    }

    emitApply() {
        // Also re-fetch summary when filters change
        this.refreshSummary();

        this.dispatchEvent(new CustomEvent('filters-applied', {
            detail: this.getFilterParams(),
            bubbles: true,
            composed: true
        }));
    }

    handleToggle(type: 'status' | 'flag', key: string) {
        const targetSet = type === 'status' ? this.#selectedStatuses : this.#selectedFlags;

        if (targetSet.has(key)) {
            targetSet.delete(key);
        } else {
            targetSet.add(key);
        }

        this.render();
        this.attachListeners();
        this.emitApply(); // Auto apply on click for premium feel
    }

    handleSearch(term: string) {
        this.#searchTerm = term;
        // Debouced apply would be better, but we leave it for simple 'Enter' key or button
    }

    clear() {
        this.#selectedStatuses.clear();
        this.#selectedFlags.clear();
        this.#searchTerm = '';

        const searchInput = this.shadowRoot?.querySelector('#search') as HTMLInputElement;
        if (searchInput) searchInput.value = '';

        this.render();
        this.attachListeners();
        this.emitApply();
    }

    attachListeners() {
        if (!this.shadowRoot) return;

        this.shadowRoot.querySelectorAll('.filter-item.status').forEach(btn => {
            btn.addEventListener('click', () => this.handleToggle('status', btn.getAttribute('data-key')!));
        });

        this.shadowRoot.querySelectorAll('.filter-item.flag').forEach(btn => {
            btn.addEventListener('click', () => this.handleToggle('flag', btn.getAttribute('data-key')!));
        });

        const searchInput = this.shadowRoot.querySelector('#search') as HTMLInputElement;
        searchInput?.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                this.handleSearch(searchInput.value);
                this.emitApply();
            }
        });

        this.shadowRoot.querySelector('#clear-btn')?.addEventListener('click', () => this.clear());
    }

    render() {
        if (!this.shadowRoot || this.#definitions.length === 0) return;

        const statuses = this.#definitions.filter(d => !d.is_flag);
        const flags = this.#definitions.filter(d => d.is_flag);

        const renderItems = (items: FilterDefinition[], type: 'status' | 'flag', selectedSet: Set<string>, counts: Record<string, number>) => {
            return items.map(def => {
                const selected = selectedSet.has(def.key);
                const count = counts[def.key] ?? 0;
                return `
          <button 
            class="filter-item ${type} ${selected ? 'selected' : ''}" 
            data-key="${def.key}"
            title="${def.description}"
          >
            ${def.label} <span class="badge">${count}</span>
          </button>
        `;
      }).join('');
    };

    const statusCounts = this.#summary?.by_status || {};
    const flagCounts = this.#summary?.by_flag || {};

    const totalStr = this.#summary !== null 
      ? `<span class="total-badge">${this.#summary.total} machines match</span>`
      : '';

    this.shadowRoot.innerHTML = `
      <style>
        :host {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-3);
          font-family: var(--font-family-base);
        }

        .row {
          display: flex;
          align-items: center;
          gap: var(--spacing-3);
          flex-wrap: wrap;
        }

        .label {
          font-size: var(--font-size-xs);
          font-weight: 600;
          color: var(--color-text-muted);
          text-transform: uppercase;
          min-width: 60px;
        }

        .search-box {
          padding: var(--spacing-2) var(--spacing-3);
          border: 1px solid var(--color-border);
          border-radius: var(--radius-sm);
          font-size: var(--font-size-sm);
          min-width: 250px;
          outline: none;
        }
        .search-box:focus {
          border-color: var(--color-primary);
        }

        .filter-item {
          padding: 4px 12px;
          border: 1px solid var(--color-border);
          border-radius: 16px;
          background: var(--color-bg-panel);
          color: var(--color-text-main);
          font-size: var(--font-size-sm);
          cursor: pointer;
          display: flex;
          align-items: center;
          gap: var(--spacing-2);
          transition: all 0.2s;
        }

        .filter-item:hover {
          background: var(--color-bg-hover);
        }

        .filter-item.selected {
          background: var(--color-primary);
          color: #fff;
          border-color: var(--color-primary);
        }
        
        .filter-item.selected .badge {
          background: rgba(255,255,255,0.2);
          color: #fff;
        }

        .badge {
          background: #e9ecef;
          padding: 2px 6px;
          border-radius: 10px;
          font-size: 10px;
          font-weight: bold;
          color: var(--color-text-muted);
        }

        button.outline {
          padding: var(--spacing-2) var(--spacing-4);
          border: 1px solid var(--color-border);
          border-radius: var(--radius-sm);
          background: transparent;
          color: var(--color-text-main);
          font-weight: 500;
          font-size: var(--font-size-sm);
          cursor: pointer;
          transition: background 0.2s;
        }
        button.outline:hover {
          background: var(--color-bg-hover);
        }

        .header-row {
          display: flex;
          justify-content: space-between;
          align-items: center;
        }

        .total-badge {
          font-size: var(--font-size-sm);
          font-weight: 600;
          color: var(--color-text-main);
          background: var(--color-status-info-bg);
          color: var(--color-status-info-text);
          padding: 4px 12px;
          border-radius: 12px;
        }
      </style>

      <div class="header-row">
        <div class="row style="flex: 1">
          <input type="text" id="search" class="search-box" placeholder="Search hostname... (Press Enter)" value="${this.#searchTerm}" />
          <button class="outline" id="clear-btn">Clear All</button>
        </div>
        ${totalStr}
      </div>

      <div class="row">
        <div class="label">Status</div>
        ${renderItems(statuses, 'status', this.#selectedStatuses, statusCounts)}
      </div>
      
      <div class="row">
        <div class="label">Flags</div>
        ${renderItems(flags, 'flag', this.#selectedFlags, flagCounts)}
      </div>
    `;
  }
}

customElements.define('cg-filter-bar', FilterBar);
