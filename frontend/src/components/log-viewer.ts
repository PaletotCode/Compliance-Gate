import { api } from '../api/client';

export class LogViewer extends HTMLElement {
    #autoRefreshInterval: number | null = null;
    #isOpen = false;

    constructor() {
        super();
        this.attachShadow({ mode: 'open' });
    }

    connectedCallback() {
        this.render();

        // Listen for global open/close toggles
        document.addEventListener('toggle-logs', this.togglePanel.bind(this));

        this.shadowRoot?.querySelector('#refresh-logs')?.addEventListener('click', () => this.fetchLogs());
        this.shadowRoot?.querySelector('#clear-logs')?.addEventListener('click', () => {
            const container = this.shadowRoot?.querySelector('.logs-container');
            if (container) container.innerHTML = '<div class="empty">Logs cleared in view...</div>';
        });
    }

    disconnectedCallback() {
        document.removeEventListener('toggle-logs', this.togglePanel.bind(this));
        if (this.#autoRefreshInterval) clearInterval(this.#autoRefreshInterval);
    }

    togglePanel() {
        this.#isOpen = !this.#isOpen;
        if (this.#isOpen) {
            this.fetchLogs();
            // Auto refresh every 5s while open
            this.#autoRefreshInterval = window.setInterval(() => this.fetchLogs(), 5000);
        } else {
            if (this.#autoRefreshInterval) clearInterval(this.#autoRefreshInterval);
        }
    }

    async fetchLogs() {
        try {
            const [logs, samples] = await Promise.all([
                api.machines.getDebugLogs(200),
                api.machines.getDebugSample(5) // Just grab latest 5 samples to avoid massive payload
            ]);

            this.renderLogsList(logs, samples);
        } catch (e) {
            this.renderError(e instanceof Error ? e.message : 'Unknown error');
        }
    }

    renderLogsList(logs: any[], samples: any[]) {
        const container = this.shadowRoot?.querySelector('.logs-container');
        if (!container) return;

        if (logs.length === 0) {
            container.innerHTML = '<div class="empty">No backend logs available yet.</div>';
            return;
        }

        // Newest first
        const reversed = [...logs].reverse();

        const html = reversed.map(log => {
            const time = new Date(log.timestamp).toLocaleTimeString();
            let detailsStr = '';
            if (log.details && Object.keys(log.details).length > 0) {
                detailsStr = `<pre class="details">${JSON.stringify(log.details, null, 2)}</pre>`;
            }
            return `
        <div class="log-entry">
          <div class="log-header">
            <span class="time">${time}</span>
            <span class="stage">${log.stage}</span>
          </div>
          <div class="msg">${log.message}</div>
          ${detailsStr}
        </div>
      `;
        }).join('');

        const sampleHtml = samples.length > 0 ? `
      <div class="log-entry sample">
        <div class="log-header"><span class="stage">LATEST JOIN SAMPLES (Top 5)</span></div>
        <pre class="details">${JSON.stringify(samples, null, 2)}</pre>
      </div>
    ` : '';

        container.innerHTML = sampleHtml + html;
    }

    renderError(msg: string) {
        const container = this.shadowRoot?.querySelector('.logs-container');
        if (container) {
            container.innerHTML = `<div class="empty error">Error fetching logs:<br>${msg}</div>`;
        }
    }

    render() {
        if (!this.shadowRoot) return;

        this.shadowRoot.innerHTML = `
      <style>
        :host {
          display: flex;
          flex-direction: column;
          height: 100%;
          background: var(--color-bg-panel);
          font-family: var(--font-family-base);
          border-left: 1px solid var(--color-border);
        }
        
        .header {
          padding: var(--spacing-4);
          border-bottom: 1px solid var(--color-border);
          display: flex;
          justify-content: space-between;
          align-items: center;
        }
        
        h2 {
          font-size: var(--font-size-md);
          margin: 0;
        }

        .actions {
          display: flex;
          gap: var(--spacing-2);
        }

        button {
          padding: 4px 8px;
          font-size: var(--font-size-xs);
          border: 1px solid var(--color-border);
          border-radius: var(--radius-sm);
          background: transparent;
          color: var(--color-text-main);
          cursor: pointer;
        }
        
        button:hover {
          background: var(--color-bg-hover);
        }

        .logs-container {
          flex: 1;
          overflow-y: auto;
          padding: var(--spacing-4);
          display: flex;
          flex-direction: column;
          gap: var(--spacing-3);
          background: #fafbfc;
        }

        .empty {
          font-size: var(--font-size-sm);
          color: var(--color-text-muted);
          text-align: center;
          margin-top: var(--spacing-6);
        }
        .error { color: var(--color-status-danger-text); }

        .log-entry {
          background: #fff;
          border: 1px solid var(--color-border);
          border-radius: var(--radius-sm);
          padding: var(--spacing-3);
          box-shadow: 0 1px 2px rgba(9, 30, 66, 0.05);
        }

        .log-entry.sample {
          background: var(--color-status-warning-bg);
          border-color: #f7d87b;
        }

        .log-header {
          display: flex;
          justify-content: space-between;
          font-size: 10px;
          color: var(--color-text-muted);
          margin-bottom: var(--spacing-2);
          text-transform: uppercase;
        }

        .stage {
          font-weight: bold;
          color: var(--color-primary);
        }

        .msg {
          font-size: var(--font-size-sm);
          color: var(--color-text-main);
        }

        .details {
          margin-top: var(--spacing-2);
          background: var(--color-bg-base);
          padding: var(--spacing-2);
          border-radius: var(--radius-sm);
          font-size: 11px;
          color: var(--color-text-muted);
          overflow-x: auto;
        }
      </style>
      
      <div class="header">
        <h2>Backend Logs</h2>
        <div class="actions">
          <button id="refresh-logs">Refresh</button>
          <button id="clear-logs">Clear</button>
        </div>
      </div>
      
      <div class="logs-container">
        <div class="empty">Click 'Refresh' or wait to fetch backend logs.</div>
      </div>
    `;
    }
}

customElements.define('cg-log-viewer', LogViewer);
