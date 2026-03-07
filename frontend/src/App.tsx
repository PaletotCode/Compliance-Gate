import { useState } from 'react';
import SetupView from './SetupView';
import DashboardView from './DashboardView';

function App() {
  const [currentView, setCurrentView] = useState<'SETUP' | 'DASHBOARD'>('DASHBOARD');

  return (
    <div className="min-h-screen flex flex-col">
      <header className="bg-slate-900 text-white shadow p-4 flex justify-between items-center">
        <h1 className="text-xl font-bold tracking-tight">Compliance Gate <span className="text-sm font-normal text-slate-400">| Debug UI</span></h1>

        <nav className="flex space-x-2">
          <button
            onClick={() => setCurrentView('DASHBOARD')}
            className={`px-3 py-1.5 rounded text-sm ${currentView === 'DASHBOARD' ? 'bg-white text-slate-900 font-medium' : 'text-slate-300 hover:bg-slate-800'}`}
          >
            Dashboard Final
          </button>
          <button
            onClick={() => setCurrentView('SETUP')}
            className={`px-3 py-1.5 rounded text-sm ${currentView === 'SETUP' ? 'bg-white text-slate-900 font-medium' : 'text-slate-300 hover:bg-slate-800'}`}
          >
            Configurar CSVs
          </button>
        </nav>
      </header>

      <main className="flex-1 overflow-hidden flex flex-col bg-slate-100">
        {currentView === 'DASHBOARD' ? <DashboardView /> : <SetupView />}
      </main>
    </div>
  );
}

export default App;
