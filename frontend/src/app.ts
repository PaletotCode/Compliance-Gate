class CgApp extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: 'open' });
  }

  connectedCallback() {
    this.render();
  }

  render() {
    if (!this.shadowRoot) return;

    this.shadowRoot.innerHTML = `
      <style>
        :host {
          display: flex;
          flex-direction: column;
          height: 100vh;
          width: 100vw;
          overflow: hidden;
        }

        header {
          background-color: var(--color-bg-panel);
          border-bottom: 1px solid var(--color-border);
          padding: var(--spacing-4) var(--spacing-6);
          display: flex;
          align-items: center;
          justify-content: space-between;
          z-index: 10;
          box-shadow: var(--shadow-sm);
        }

        h1 {
          font-size: var(--font-size-lg);
          margin: 0;
          display: flex;
          align-items: center;
          gap: var(--spacing-2);
        }

        .layout-container {
          display: flex;
          flex: 1;
          overflow: hidden;
        }

        .main-content {
          flex: 1;
          display: flex;
          flex-direction: column;
          overflow: hidden;
        }

        /* Filter bar area */
        .filters {
          padding: var(--spacing-4) var(--spacing-6);
          background-color: var(--color-bg-panel);
          border-bottom: 1px solid var(--color-border);
          display: flex;
          flex-direction: column;
          gap: var(--spacing-3);
        }

        /* Grid area */
        .workspace {
          flex: 1;
          overflow: auto;
          background-color: var(--color-bg-base);
          padding: var(--spacing-4);
        }

        .side-panel {
          width: 350px;
          border-left: 1px solid var(--color-border);
          background-color: var(--color-bg-panel);
          display: flex;
          flex-direction: column;
        }

        .hidden {
          display: none !important;
        }

        button.outline {
          padding: var(--spacing-2) var(--spacing-4);
          border: 1px solid var(--color-border);
          border-radius: var(--radius-sm);
          background: transparent;
          color: var(--color-text-main);
          font-weight: 500;
          transition: background 0.2s;
        }
        button.outline:hover {
          background: var(--color-bg-hover);
        }
      </style>

      <header>
        <h1>Compliance Gate <span>Validation</span></h1>
        <div class="actions">
          <button class="outline" id="toggle-logs">Toggle Logs</button>
        </div>
      </header>

      <div class="layout-container">
        <main class="main-content">
          <div class="filters">
            <cg-filter-bar></cg-filter-bar>
          </div>
          <div class="workspace">
            <cg-data-grid></cg-data-grid>
          </div>
        </main>
        
        <aside class="side-panel hidden" id="log-panel">
          <cg-log-viewer></cg-log-viewer>
        </aside>
      </div>
    `;

    // Setup toggler
    const toggleBtn = this.shadowRoot.getElementById('toggle-logs');
    const logPanel = this.shadowRoot.getElementById('log-panel');

    toggleBtn?.addEventListener('click', () => {
      logPanel?.classList.toggle('hidden');
    });
  }
}

customElements.define('cg-app', CgApp);
