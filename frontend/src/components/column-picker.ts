export interface ColumnDef {
    id: string;
    label: string;
    visible: boolean;
}

export const DEFAULT_COLUMNS: ColumnDef[] = [
    { id: 'hostname', label: 'Hostname', visible: true },
    { id: 'pa_code', label: 'PA Code', visible: true },
    { id: 'has_ad', label: 'AD', visible: true },
    { id: 'has_uem', label: 'UEM', visible: true },
    { id: 'has_edr', label: 'EDR', visible: true },
    { id: 'has_asset', label: 'ASSET', visible: true },
    { id: 'uem_serial', label: 'UEM Serial', visible: true },
    { id: 'edr_serial', label: 'EDR Serial', visible: true },
    { id: 'main_user', label: 'User', visible: true },
    { id: 'primary_status_label', label: 'Status', visible: true },
    { id: 'flags', label: 'Flags', visible: true },
    { id: 'model', label: 'Model', visible: false },
    { id: 'ip', label: 'IP', visible: false },
    { id: 'ad_os', label: 'AD OS', visible: false },
];

export class ColumnPicker extends HTMLElement {
    #columns: ColumnDef[] = [];
    #isOpen = false;

    constructor() {
        super();
        this.attachShadow({ mode: 'open' });
    }

    connectedCallback() {
        // Load preference from local storage or use defaults
        const stored = localStorage.getItem('cg-columns');
        if (stored) {
            try {
                const parsed = JSON.parse(stored);
                // Merge with defaults to ensure all exist
                this.#columns = DEFAULT_COLUMNS.map(def => {
                    const found = parsed.find((p: any) => p.id === def.id);
                    return found ? { ...def, visible: found.visible } : def;
                });
            } catch (e) {
                this.#columns = [...DEFAULT_COLUMNS];
            }
        } else {
            this.#columns = [...DEFAULT_COLUMNS];
        }

        this.render();

        // Add event listeners
        this.shadowRoot?.querySelector('.toggle-btn')?.addEventListener('click', () => {
            this.#isOpen = !this.#isOpen;
            const popup = this.shadowRoot?.querySelector('.popup');
            if (popup) {
                popup.classList.toggle('open', this.#isOpen);
            }
        });

        // Close when clicking outside
        document.addEventListener('click', (e) => {
            if (this.#isOpen && !this.contains(e.target as Node)) {
                this.#isOpen = false;
                this.shadowRoot?.querySelector('.popup')?.classList.remove('open');
            }
        });

        // Fire initial event
        this.emitChange();
    }

    handleToggle(id: string, event: Event) {
        const checkbox = event.target as HTMLInputElement;
        const col = this.#columns.find(c => c.id === id);
        if (col) {
            col.visible = checkbox.checked;
            this.saveAndEmit();
        }
    }

    saveAndEmit() {
        localStorage.setItem('cg-columns', JSON.stringify(this.#columns));
        this.emitChange();
    }

    emitChange() {
        this.dispatchEvent(new CustomEvent('columns-changed', {
            detail: { columns: this.#columns },
            bubbles: true,
            composed: true
        }));
    }

    render() {
        if (!this.shadowRoot) return;

        const itemsHtml = this.#columns.map(col => `
      <label class="item">
        <input type="checkbox" value="${col.id}" ${col.visible ? 'checked' : ''} />
        <span>${col.label}</span>
      </label>
    `).join('');

    this.shadowRoot.innerHTML = `
      <style>
        :host {
          position: relative;
          display: inline-block;
          font-family: var(--font-family-base);
        }

        .toggle-btn {
          padding: var(--spacing-2) var(--spacing-4);
          background: var(--color-bg-panel);
          border: 1px solid var(--color-border);
          border-radius: var(--radius-sm);
          color: var(--color-text-main);
          font-size: var(--font-size-sm);
          font-weight: 500;
          cursor: pointer;
          display: flex;
          align-items: center;
          gap: var(--spacing-2);
          transition: background 0.2s;
        }

        .toggle-btn:hover {
          background: var(--color-bg-hover);
        }

        .popup {
          display: none;
          position: absolute;
          top: 100%;
          right: 0;
          margin-top: var(--spacing-1);
          background: var(--color-bg-panel);
          border: 1px solid var(--color-border);
          border-radius: var(--radius-md);
          box-shadow: var(--shadow-md);
          padding: var(--spacing-3);
          min-width: 200px;
          z-index: 100;
          max-height: 300px;
          overflow-y: auto;
        }

        .popup.open {
          display: flex;
          flex-direction: column;
          gap: var(--spacing-2);
        }

        .item {
          display: flex;
          align-items: center;
          gap: var(--spacing-2);
          font-size: var(--font-size-sm);
          color: var(--color-text-main);
          cursor: pointer;
          padding: var(--spacing-1) 0;
        }

        .item input {
          cursor: pointer;
        }
        
        .header {
          font-size: var(--font-size-xs);
          color: var(--color-text-muted);
          text-transform: uppercase;
          letter-spacing: 0.5px;
          margin-bottom: var(--spacing-2);
          font-weight: 600;
        }
      </style>

      <button class="toggle-btn">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <path d="M9 3H5a2 2 0 0 0-2 2v4a2 2 0 0 0 2 2h4a2 2 0 0 0 2-2V5a2 2 0 0 0-2-2z"></path>
          <path d="M19 3h-4a2 2 0 0 0-2 2v4a2 2 0 0 0 2 2h4a2 2 0 0 0 2-2V5a2 2 0 0 0-2-2z"></path>
          <path d="M9 15H5a2 2 0 0 0-2 2v4a2 2 0 0 0 2 2h4a2 2 0 0 0 2-2v-4a2 2 0 0 0-2-2z"></path>
          <path d="M19 15h-4a2 2 0 0 0-2 2v4a2 2 0 0 0 2 2h4a2 2 0 0 0 2-2v-4a2 2 0 0 0-2-2z"></path>
        </svg>
        Columns
      </button>

      <div class="popup">
        <div class="header">Toggle Columns</div>
        ${itemsHtml}
      </div>
    `;

    // Attach listeners to checkboxes
    const inputs = this.shadowRoot.querySelectorAll('input[type="checkbox"]');
    inputs.forEach(input => {
      input.addEventListener('change', (e) => this.handleToggle((e.target as HTMLInputElement).value, e));
    });
  }
}

customElements.define('cg-column-picker', ColumnPicker);
