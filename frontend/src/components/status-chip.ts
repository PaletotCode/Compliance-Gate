const SEVERITY_COLORS: Record<string, { bg: string, text: string }> = {
    SUCCESS: { bg: 'var(--color-status-success-bg)', text: 'var(--color-status-success-text)' },
    WARNING: { bg: 'var(--color-status-warning-bg)', text: 'var(--color-status-warning-text)' },
    DANGER: { bg: 'var(--color-status-danger-bg)', text: 'var(--color-status-danger-text)' },
    INFO: { bg: 'var(--color-status-info-bg)', text: 'var(--color-status-info-text)' },
    OFFLINE: { bg: 'var(--color-status-offline-bg)', text: 'var(--color-status-offline-text)' }
};

export class StatusChip extends HTMLElement {
    static get observedAttributes() {
        return ['label', 'severity'];
    }

    constructor() {
        super();
        this.attachShadow({ mode: 'open' });
    }

    connectedCallback() {
        this.render();
    }

    attributeChangedCallback() {
        this.render();
    }

    render() {
        if (!this.shadowRoot) return;

        const label = this.getAttribute('label') || 'Unknown';
        let severity = this.getAttribute('severity') || 'INFO';

        // OFFLINE is a special status key usually returning INFO/WARNING, but we want it distinct 
        if (label.includes('OFFLINE')) {
            severity = 'OFFLINE';
        }

        const colors = SEVERITY_COLORS[severity] || SEVERITY_COLORS.INFO;

        this.shadowRoot.innerHTML = `
      <style>
        .chip {
          display: inline-flex;
          align-items: center;
          padding: 2px 8px;
          border-radius: 12px;
          font-size: 11px;
          font-weight: 600;
          letter-spacing: 0.3px;
          text-transform: uppercase;
          background-color: ${colors.bg};
          color: ${colors.text};
          white-space: nowrap;
        }
      </style>
      <span class="chip" title="${label}">${label}</span>
    `;
    }
}

customElements.define('cg-status-chip', StatusChip);
