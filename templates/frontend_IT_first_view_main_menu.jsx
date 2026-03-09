import React, { useState, useMemo } from 'react';
import { 
  Plus, 
  CheckCircle2, 
  AlertCircle, 
  Settings, 
  Play, 
  Download, 
  FileSpreadsheet,
  Columns,
  Key,
  ListChecks,
  ArrowLeft,
  Pencil,
  Search,
  X,
  Filter,
  Check,
  Trash2,
  CheckSquare,
  Square,
  CalendarDays,
  Database
} from 'lucide-react';

// --- MOCK DATA GENERATORS ---
const generateMockData = () => {
  const ad = Array.from({ length: 15 }).map((_, i) => ({
    "Computer Name": `BR-LT-${String(i + 1).padStart(3, '0')}`,
    "DNS Name": `br-lt-${String(i + 1).padStart(3, '0')}.sicoob.local`,
    "Operating System": i % 3 === 0 ? "Windows 10" : "Windows 11",
    "Version": i % 2 === 0 ? "22H2" : "21H2",
    "Last Logon": `2026-03-${String((i % 7) + 1).padStart(2, '0')}`
  }));

  const uem = Array.from({ length: 15 }).map((_, i) => ({
    "Friendly Name": `BR-LT-${String(i + 1).padStart(3, '0')}`,
    "Username": `usuario.${i + 1}`,
    "Serial Number": `PF2XYZ${i}`,
    "Last Seen": `2026-03-${String((i % 8) + 1).padStart(2, '0')}`,
    "OS": i % 3 === 0 ? "Win10" : "Win11",
    "Model": i % 2 === 0 ? "ThinkPad T14" : "Dell Latitude"
  }));

  const edr = Array.from({ length: 15 }).map((_, i) => ({
    "Hostname": `BR-LT-${String(i + 1).padStart(3, '0')}`,
    "Last Seen": `2026-03-${String((i % 5) + 1).padStart(2, '0')}`,
    "Local IP": `10.0.0.${40 + i}`,
    "OS Version": i % 3 === 0 ? "Windows 10" : "Windows 11",
    "Sensor Tags": "TI, Compliance",
    "Serial Number": `PF2XYZ${i}`
  }));

  const asset = Array.from({ length: 15 }).map((_, i) => ({
    "Nome do ativo": `BR-LT-${String(i + 1).padStart(3, '0')}`,
    "Usuário": `usuario.${i + 1}`,
    "Estado do ativo": i % 5 === 0 ? "Em Manutenção" : "Ativo",
    "Fornecedor": i % 2 === 0 ? "Lenovo" : "Dell",
    "Data Aquisição": `2023-0${(i % 9) + 1}-15`
  }));

  return { AD: ad, UEM: uem, EDR: edr, ASSET: asset };
};

const mockData = generateMockData();

const INITIAL_SOURCES = [
  { id: 'AD', name: 'Active Directory', type: 'CSV', createdAt: '08 Mar 2026, 11:30' },
  { id: 'UEM', name: 'Workspace ONE (UEM)', type: 'CSV', createdAt: '08 Mar 2026, 11:32' },
  { id: 'EDR', name: 'CrowdStrike (EDR)', type: 'CSV', createdAt: '08 Mar 2026, 11:35' },
  { id: 'ASSET', name: 'GLPI (Ativos)', type: 'CSV', createdAt: '08 Mar 2026, 11:40' },
];

const REQUIRED_DELETE_TEXT = "Eu TI consinto em apagar";

// --- DESIGN SYSTEM: COMPONENTES PADRONIZADOS ---

const Button = ({ children, onClick, variant = 'primary', size = 'md', className = '', disabled = false }) => {
  // Contrato reforçado: whitespace-nowrap e shrink-0 garantem que o botão JAMAIS seja esmagado
  const baseStyle = "font-bold flex items-center justify-center transition-all active:scale-[0.98] disabled:opacity-50 disabled:pointer-events-none tracking-wide rounded-lg outline-none focus-visible:ring-2 focus-visible:ring-offset-2 focus-visible:ring-offset-black focus-visible:ring-[#00AE9D] whitespace-nowrap shrink-0";
  
  const variants = {
    primary: "bg-[#00AE9D] text-white hover:bg-[#00AE9D]/90 hover:shadow-[0_0_15px_rgba(0,174,157,0.4)] border border-transparent",
    secondary: "bg-white/10 backdrop-blur-md text-white hover:bg-white/20 border border-white/10 shadow-sm",
    ghost: "text-white/60 hover:text-white hover:bg-white/5 border border-transparent",
    danger: "bg-red-500 text-white hover:bg-red-600 hover:shadow-[0_0_15px_rgba(239,68,68,0.4)] border border-transparent",
  };

  const sizes = {
    sm: "h-9 px-4 text-[11px] gap-2",    // Altura rigorosamente travada em h-9 para a TopBar
    md: "h-10 px-4 text-xs gap-2",      
    lg: "h-12 px-6 text-sm gap-3",      
  };

  return (
    <button onClick={onClick} disabled={disabled} className={`${baseStyle} ${variants[variant]} ${sizes[size]} ${className}`}>
      {children}
    </button>
  );
};

const StatusBadge = ({ status }) => {
  if (status === 'pronto') return <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-emerald-500/20 text-emerald-400 text-[9px] font-black uppercase tracking-wider border border-emerald-500/30 backdrop-blur-sm shadow-inner"><CheckCircle2 size={10}/> Pronto</span>;
  return <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-white/5 text-white/50 text-[9px] font-black uppercase tracking-wider border border-white/10 backdrop-blur-sm shadow-inner"><AlertCircle size={10}/> Pendente</span>;
};

// --- COMPONENTE DE FILTRO ESTILO EXCEL ---
const ExcelFilterPopover = ({ options, selectedOptions, onApply, onClose }) => {
  const [search, setSearch] = useState('');
  const [localSelection, setLocalSelection] = useState(selectedOptions ? new Set(selectedOptions) : new Set(options));

  const filteredOptions = options.filter(opt => String(opt).toLowerCase().includes(search.toLowerCase()));

  const toggleOption = (opt) => {
    const newSel = new Set(localSelection);
    if (newSel.has(opt)) newSel.delete(opt);
    else newSel.add(opt);
    setLocalSelection(newSel);
  };

  return (
    <div className="absolute top-full left-0 mt-2 w-56 bg-[#111]/95 backdrop-blur-2xl border border-white/10 rounded-xl shadow-[0_15px_40px_rgba(0,0,0,0.8)] z-50 flex flex-col overflow-hidden animate-in fade-in zoom-in-95 duration-200" onClick={e => e.stopPropagation()}>
      <div className="p-3 border-b border-white/10">
        <div className="relative">
          <Search size={12} className="absolute left-3 top-1/2 -translate-y-1/2 text-white/40" />
          <input
            type="text"
            autoFocus
            placeholder="Pesquisar..."
            className="w-full bg-black/50 border border-white/10 rounded-lg px-8 py-2 text-xs text-white outline-none focus:border-[#00AE9D] transition-colors"
            value={search}
            onChange={e => setSearch(e.target.value)}
          />
        </div>
      </div>
      
      <div className="max-h-48 overflow-y-auto custom-scrollbar p-2">
        <label className="flex items-center gap-3 p-2 hover:bg-white/5 rounded-lg cursor-pointer transition-colors border-b border-white/5 mb-1"
               onClick={() => setLocalSelection(localSelection.size === options.length ? new Set() : new Set(options))}>
          <div className={`w-4 h-4 rounded border flex items-center justify-center transition-colors
            ${localSelection.size === options.length ? 'bg-[#00AE9D] border-[#00AE9D]' : localSelection.size > 0 ? 'bg-[#00AE9D]/50 border-[#00AE9D]' : 'border-white/20'}`}>
            {localSelection.size > 0 && <Check size={12} className="text-white" />}
          </div>
          <span className="text-xs font-bold text-white/90">(Selecionar Tudo)</span>
        </label>
        
        {filteredOptions.map(opt => (
          <label key={String(opt)} className="flex items-center gap-3 p-2 hover:bg-white/5 rounded-lg cursor-pointer transition-colors"
                 onClick={() => toggleOption(opt)}>
            <div className={`w-4 h-4 rounded border flex items-center justify-center transition-colors shrink-0
              ${localSelection.has(opt) ? 'bg-[#00AE9D] border-[#00AE9D]' : 'border-white/20'}`}>
              {localSelection.has(opt) && <Check size={12} className="text-white" />}
            </div>
            <span className="text-xs text-white/80 truncate">{opt}</span>
          </label>
        ))}
        {filteredOptions.length === 0 && (
          <div className="p-4 text-center text-xs text-white/40 italic">Nenhum resultado</div>
        )}
      </div>

      <div className="p-3 border-t border-white/10 bg-black/40 flex justify-end gap-2">
        <Button variant="ghost" size="sm" onClick={onClose}>Cancelar</Button>
        <Button variant="primary" size="sm" onClick={() => onApply(Array.from(localSelection))}>Aplicar</Button>
      </div>
    </div>
  );
};


// --- MAIN APP COMPONENT ---

export default function ComplianceGatePrototype() {
  const [view, setView] = useState('home-empty'); 
  const [activeTab, setActiveTab] = useState('AD');
  
  const [sources, setSources] = useState(INITIAL_SOURCES);
  const [editingTab, setEditingTab] = useState(null);
  const [editTabName, setEditTabName] = useState('');

  // Estados de Seleção e Deleção (Multi-select)
  const [isSelectionMode, setIsSelectionMode] = useState(false);
  const [selectedSources, setSelectedSources] = useState([]);
  const [isDeleteModalOpen, setIsDeleteModalOpen] = useState(false);
  const [deleteInput, setDeleteInput] = useState('');

  // Column Filters State (Excel Style)
  const [excelFilters, setExcelFilters] = useState({ AD: {}, UEM: {}, EDR: {}, ASSET: {}, MATERIALIZED: {} });
  const [openFilterMenu, setOpenFilterMenu] = useState(null); 
  
  // Materialized Table Dynamic Columns State
  const [isColPanelOpen, setIsColPanelOpen] = useState(false);
  const [activeMatCols, setActiveMatCols] = useState([
    'AD_Operating System', 'UEM_Username', 'EDR_Local IP', 'ASSET_Estado do ativo'
  ]);

  const toggleMatCol = (colKey) => {
    setActiveMatCols(prev => 
      prev.includes(colKey) ? prev.filter(c => c !== colKey) : [...prev, colKey]
    );
  };
  
  // Config state
  const [configs, setConfigs] = useState({
    AD: { status: 'pendente', headerRow: 1, sicColumn: '', selectedCols: [] },
    UEM: { status: 'pendente', headerRow: 1, sicColumn: '', selectedCols: [] },
    EDR: { status: 'pendente', headerRow: 1, sicColumn: '', selectedCols: [] },
    ASSET: { status: 'pendente', headerRow: 1, sicColumn: '', selectedCols: [] },
  });

  // Simulated Materialized Data
  const materializedData = useMemo(() => {
    return Array.from({ length: 15 }).map((_, i) => {
      const row = { SIC_CHAVE: `BR-LT-${String(i + 1).padStart(3, '0')}` };
      sources.forEach(source => {
        const sourceData = mockData[source.id]?.[i];
        if(sourceData) {
          Object.keys(sourceData).forEach(col => {
            row[`${source.id}_${col}`] = sourceData[col];
          });
        }
      });
      return row;
    });
  }, [sources]);

  const handleImportAll = () => {
    setSources(INITIAL_SOURCES);
    setConfigs(prev => {
      const newConfigs = { ...prev };
      INITIAL_SOURCES.forEach(s => {
        newConfigs[s.id] = { status: 'pendente', headerRow: 1, sicColumn: '', selectedCols: Object.keys(mockData[s.id][0]) };
      });
      return newConfigs;
    });
    setView('home-filled');
    setIsSelectionMode(false);
    setSelectedSources([]);
  };

  const handleOpenSource = (sourceId) => {
    if (isSelectionMode) {
      setSelectedSources(prev => 
        prev.includes(sourceId) ? prev.filter(id => id !== sourceId) : [...prev, sourceId]
      );
    } else {
      setActiveTab(sourceId);
      setView('viewer');
    }
  };

  const handleSaveProfile = (sourceId) => {
    setConfigs(prev => ({
      ...prev,
      [sourceId]: { ...prev[sourceId], status: 'pronto' }
    }));
  };

  const startEditingTab = (e, source) => {
    e.stopPropagation();
    setEditingTab(source.id);
    setEditTabName(source.name);
  };

  const saveTabName = (sourceId) => {
    setSources(prev => prev.map(s => s.id === sourceId ? { ...s, name: editTabName || s.name } : s));
    setEditingTab(null);
  };

  const handleDeleteConfirm = () => {
    const newSources = sources.filter(s => !selectedSources.includes(s.id));
    setSources(newSources);
    setSelectedSources([]);
    setIsSelectionMode(false);
    setIsDeleteModalOpen(false);
    setDeleteInput('');
    if (newSources.length === 0) {
      setView('home-empty');
    } else if (!newSources.find(s => s.id === activeTab)) {
      setActiveTab(newSources[0].id);
    }
  };

  const readyCount = Object.values(configs).filter(c => c.status === 'pronto').length;
  const isAllReady = readyCount === sources.length && sources.length > 0;

  const handleRunIngest = () => {
    setView('materialized');
  };

  // --- SUB-VIEWS ---

  const renderHome = () => (
    <div className="flex-1 p-8 lg:p-12 animate-in fade-in zoom-in-95 duration-500 max-w-7xl mx-auto w-full flex flex-col justify-center">
      {view === 'home-empty' ? (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
          <button 
            onClick={handleImportAll}
            className="aspect-[4/3] rounded-3xl border-2 border-dashed border-white/20 hover:border-[#00AE9D]/60 bg-white/5 backdrop-blur-md hover:bg-[#00AE9D]/10 flex flex-col items-center justify-center gap-5 transition-all group shadow-xl"
          >
            <div className="w-14 h-14 rounded-full bg-white/5 backdrop-blur-xl group-hover:bg-[#00AE9D]/20 border border-white/10 group-hover:border-[#00AE9D]/40 flex items-center justify-center text-white/50 group-hover:text-[#00AE9D] transition-all transform group-hover:scale-110">
              <Plus size={28} />
            </div>
            <span className="text-sm font-bold text-white/70 group-hover:text-white tracking-wide">Importar bases CSV</span>
          </button>
        </div>
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6 content-start h-full pt-10">
          {sources.map((source, idx) => {
            const isSelected = selectedSources.includes(source.id);
            return (
              <div 
                key={source.id} 
                onClick={() => handleOpenSource(source.id)}
                className={`aspect-[4/3] rounded-3xl border bg-black/40 backdrop-blur-xl p-6 flex flex-col justify-between shadow-[0_8px_32px_rgba(0,0,0,0.3)] transition-all cursor-pointer group animate-in zoom-in-95 duration-500 fill-mode-both
                  ${isSelectionMode && isSelected 
                    ? 'border-[#00AE9D] shadow-[0_0_25px_rgba(0,174,157,0.2)] bg-[#00AE9D]/5' 
                    : 'border-white/10 hover:border-white/30 hover:bg-black/60'}`}
                style={{ animationDelay: `${idx * 100}ms` }}
              >
                <div className="flex justify-between items-start">
                  <div className={`w-12 h-12 rounded-2xl flex items-center justify-center transition-all shadow-inner group-hover:scale-105
                    ${isSelectionMode && isSelected ? 'bg-[#00AE9D] text-white border-none' : 'bg-white/5 border border-white/10 text-white/70 group-hover:text-white'}`}>
                    <FileSpreadsheet size={24} />
                  </div>
                  
                  {isSelectionMode ? (
                    <div className="text-white/50">
                      {isSelected ? <CheckSquare size={20} className="text-[#00AE9D]"/> : <Square size={20}/>}
                    </div>
                  ) : (
                    <StatusBadge status={configs[source.id].status} />
                  )}
                </div>
                
                <div className="space-y-2">
                  <h3 className="text-base font-black text-white tracking-wide">{source.name}</h3>
                  <div className="flex items-center gap-3">
                    <p className="text-[10px] text-white/40 font-mono uppercase tracking-widest bg-white/5 px-2 py-1 rounded-md">{source.type} • 15 LINHAS</p>
                    <p className="text-[10px] text-white/30 font-medium flex items-center gap-1"><CalendarDays size={10}/> {source.createdAt.split(',')[0]}</p>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}

      {/* FLOATING ACTION BAR FOR DELETE */}
      {isSelectionMode && (
         <div className="absolute bottom-10 left-1/2 -translate-x-1/2 flex items-center gap-4 bg-[#111]/90 backdrop-blur-2xl p-4 rounded-2xl border border-white/10 shadow-[0_20px_50px_rgba(0,0,0,0.8)] animate-in slide-in-from-bottom-8">
            <span className="text-sm font-bold text-white px-2">
              {selectedSources.length} base(s) selecionada(s)
            </span>
            <div className="h-6 w-px bg-white/10" />
            <Button variant="ghost" onClick={() => { setIsSelectionMode(false); setSelectedSources([]); }}>
              Cancelar
            </Button>
            <Button 
              variant="danger" 
              disabled={selectedSources.length === 0}
              onClick={() => setIsDeleteModalOpen(true)}
            >
              <Trash2 size={16}/> Apagar Selecionadas
            </Button>
         </div>
      )}

      {/* DELETE CONFIRMATION MODAL */}
      {isDeleteModalOpen && (
        <div className="fixed inset-0 z-[100] flex items-center justify-center p-4 bg-black/80 backdrop-blur-md animate-in fade-in duration-300">
          <div className="bg-[#0A0A0A] border border-white/10 rounded-3xl p-8 max-w-lg w-full shadow-[0_20px_60px_rgba(0,0,0,0.8)] flex flex-col gap-8 animate-in zoom-in-95 duration-300">
            <div className="flex flex-col gap-3">
              <div className="w-14 h-14 rounded-full bg-red-500/10 flex items-center justify-center text-red-500 mb-2 shadow-inner">
                <Trash2 size={24} />
              </div>
              <h2 className="text-2xl font-black text-white tracking-tight">Confirmação de Exclusão</h2>
              <p className="text-sm text-white/60 leading-relaxed font-medium">
                Você está prestes a excluir definitivamente <strong className="text-white bg-white/10 px-1.5 py-0.5 rounded">{selectedSources.length} base(s)</strong> do Compliance Gate. Esta ação é irreversível e todos os perfis atrelados serão perdidos.
              </p>
            </div>

            <div className="bg-black/50 border border-red-500/20 p-5 rounded-2xl flex flex-col gap-4 shadow-inner">
              <label className="text-[10px] font-black text-red-400 uppercase tracking-widest">
                Para prosseguir, digite a confirmação abaixo:
              </label>
              <div className="px-4 py-3 bg-[#111] rounded-xl border border-white/5 text-sm font-mono text-white select-all text-center tracking-wide shadow-sm">
                {REQUIRED_DELETE_TEXT}
              </div>
              <input 
                type="text" 
                placeholder="Digite o texto aqui..."
                className="w-full bg-[#111] border border-white/10 rounded-xl h-12 px-4 text-sm text-white focus:border-red-500 outline-none transition-colors shadow-inner"
                value={deleteInput}
                onChange={e => setDeleteInput(e.target.value)}
              />
            </div>

            <div className="flex justify-end gap-3 mt-2">
               <Button size="lg" variant="ghost" onClick={() => { setIsDeleteModalOpen(false); setDeleteInput(''); }}>
                 Cancelar
               </Button>
               <Button 
                 size="lg"
                 variant="danger" 
                 disabled={deleteInput !== REQUIRED_DELETE_TEXT}
                 onClick={handleDeleteConfirm}
               >
                 Sim, excluir bases
               </Button>
            </div>
          </div>
        </div>
      )}
    </div>
  );

  const renderViewer = () => {
    const activeSource = sources.find(s => s.id === activeTab);
    if (!activeSource) return null;

    const activeData = mockData[activeTab] || [];
    const columns = activeData.length > 0 ? Object.keys(activeData[0]) : [];
    const config = configs[activeTab];
    const currentFilters = excelFilters[activeTab] || {};
    
    const filteredData = activeData.filter(row => {
      return Object.entries(currentFilters).every(([col, selectedValues]) => {
        if (!selectedValues) return true;
        return selectedValues.includes(row[col]);
      });
    });

    return (
      <div className="flex flex-col h-[calc(100vh-64px)] w-full relative animate-in fade-in duration-300">
        <div className="flex flex-1 overflow-hidden relative">

          {/* AREA ESQUERDA: Tabela */}
          <div className="flex-1 flex flex-col bg-black/20 backdrop-blur-sm relative z-10 p-6">
            
            <div className="flex-1 rounded-2xl overflow-hidden border border-white/10 shadow-[0_8px_32px_rgba(0,0,0,0.4)] backdrop-blur-md bg-[#0A0A0A]/80 flex flex-col">
              <div className="flex-1 overflow-auto custom-scrollbar relative">
                <table className="w-full text-sm text-left whitespace-nowrap">
                  <thead className="bg-[#111] border-b border-white/10 text-white/60 text-[10px] uppercase tracking-[0.15em] font-black sticky top-0 z-20 backdrop-blur-xl">
                    <tr>
                      <th className="px-6 py-4 w-16 align-middle border-r border-white/5">#</th>
                      {columns.map(col => {
                        const isFiltered = currentFilters[col] && currentFilters[col].length !== [...new Set(activeData.map(r => r[col]))].length;
                        const filterKey = `${activeTab}_${col}`;

                        return (
                          <th key={col} className={`px-4 py-3 border-r border-white/5 align-middle ${!config.selectedCols.includes(col) ? 'opacity-30' : ''}`}>
                            <div className="flex items-center justify-between gap-4 relative">
                              <div className={`flex items-center gap-2 ${!config.selectedCols.includes(col) ? 'line-through' : ''} text-white/80`}>
                                {col === config.sicColumn && <Key size={12} className="text-[#00AE9D]" />}
                                {col}
                              </div>
                              
                              <button 
                                onClick={(e) => { e.stopPropagation(); setOpenFilterMenu(openFilterMenu === filterKey ? null : filterKey); }}
                                className={`p-1.5 rounded-md transition-all hover:bg-white/10 ${isFiltered || openFilterMenu === filterKey ? 'text-[#00AE9D] bg-[#00AE9D]/10' : 'text-white/30 hover:text-white'}`}
                              >
                                <Filter size={12} className={isFiltered ? "fill-[#00AE9D]/20" : ""} />
                              </button>

                              {openFilterMenu === filterKey && (
                                <ExcelFilterPopover 
                                  options={[...new Set(activeData.map(r => r[col]))]}
                                  selectedOptions={currentFilters[col]}
                                  onClose={() => setOpenFilterMenu(null)}
                                  onApply={(selection) => {
                                    setExcelFilters(prev => ({
                                      ...prev,
                                      [activeTab]: { ...prev[activeTab], [col]: selection }
                                    }));
                                    setOpenFilterMenu(null);
                                  }}
                                />
                              )}
                            </div>
                          </th>
                        );
                      })}
                    </tr>
                  </thead>
                  <tbody className="text-white/80 divide-y divide-white/5">
                    {filteredData.length === 0 ? (
                      <tr>
                        <td colSpan={columns.length + 1} className="px-6 py-12 text-center text-white/40 italic text-xs">
                          Nenhum registro encontrado para os filtros atuais.
                        </td>
                      </tr>
                    ) : (
                      filteredData.map((row, i) => (
                        <tr key={i} className="hover:bg-white/5 transition-colors">
                          <td className="px-6 py-4 font-mono text-white/30 text-xs border-r border-white/5">{i + 1}</td>
                          {columns.map(col => (
                            <td key={col} className={`px-4 py-4 border-r border-white/5 ${!config.selectedCols.includes(col) ? 'opacity-30' : ''}`}>
                              {row[col]}
                            </td>
                          ))}
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          {/* Viewer Config Panel */}
          <div className="w-[340px] shrink-0 border-l border-white/10 bg-black/60 backdrop-blur-2xl flex flex-col shadow-[-20px_0_40px_rgba(0,0,0,0.5)] z-20">
            <div className="p-6 flex-1 overflow-auto space-y-8 custom-scrollbar">
              
              <div className="flex items-center gap-3 border-b border-white/10 pb-4">
                <Settings className="text-[#00AE9D]" size={18} />
                <h2 className="text-xs font-black text-white uppercase tracking-[0.15em]">Configuração</h2>
              </div>

              {/* METADATA APPLE STYLE */}
              <div className="flex flex-col gap-1 p-4 bg-white/5 rounded-xl border border-white/5 shadow-inner">
                <span className="text-[9px] text-white/50 font-black uppercase tracking-widest flex items-center gap-2"><CalendarDays size={10}/> Importado em</span>
                <span className="text-xs font-medium text-white/90">{activeSource.createdAt}</span>
              </div>

              <div className="space-y-4">
                <label className="text-[9px] font-black text-[#00AE9D] uppercase tracking-[1.5px] flex items-center gap-2">
                  <Key size={12} /> Coluna SIC (Join Key)
                </label>
                <select 
                  className="w-full h-10 px-3 bg-[#00AE9D]/5 border border-[#00AE9D]/30 rounded-lg text-xs text-white focus:border-[#00AE9D] focus:ring-1 focus:ring-[#00AE9D] outline-none transition-all shadow-inner appearance-none cursor-pointer"
                  value={config.sicColumn}
                  onChange={(e) => setConfigs(prev => ({...prev, [activeTab]: {...prev[activeTab], sicColumn: e.target.value}}))}
                >
                  <option value="" className="bg-[#111]" disabled>Selecionar chave primária...</option>
                  {columns.map(col => (
                    <option key={col} value={col} className="bg-[#111]">{col}</option>
                  ))}
                </select>
                <p className="text-[10px] text-white/40 leading-relaxed font-medium">
                  Atuará como chave de cruzamento universal (ex: Hostname, Serial).
                </p>
              </div>

              <div className="space-y-4">
                <label className="text-[9px] font-black text-white/50 uppercase tracking-[1.5px] flex items-center gap-2">
                  <ListChecks size={12} /> Seleção de Colunas
                </label>
                <div className="space-y-1.5 max-h-64 overflow-y-auto pr-2 custom-scrollbar bg-black/20 rounded-xl p-3 border border-white/5 shadow-inner">
                  {columns.map(col => (
                    <label key={col} className="flex items-center gap-3 p-2.5 rounded-lg hover:bg-white/5 cursor-pointer group transition-colors">
                      <div className={`w-4 h-4 rounded border flex items-center justify-center transition-colors shadow-inner shrink-0
                        ${config.selectedCols.includes(col) ? 'bg-[#00AE9D] border-[#00AE9D]' : 'bg-black/50 border-white/20 group-hover:border-white/40'}`}>
                        {config.selectedCols.includes(col) && <CheckCircle2 size={10} className="text-white" />}
                      </div>
                      <span className={`text-xs font-medium truncate ${config.selectedCols.includes(col) ? 'text-white/90' : 'text-white/30'}`}>
                        {col}
                      </span>
                    </label>
                  ))}
                </div>
              </div>
            </div>

            <div className="p-6 border-t border-white/10 bg-black/40 backdrop-blur-xl">
              <Button 
                onClick={() => handleSaveProfile(activeTab)} 
                variant="primary"
                size="md"
                className="w-full"
                disabled={!config.sicColumn}
              >
                {config.status === 'pronto' ? 'ATUALIZAR PERFIL' : 'SALVAR PERFIL'}
              </Button>
            </div>
          </div>

        </div>
      </div>
    );
  };

  const renderMaterialized = () => {
    const currentFilters = excelFilters['MATERIALIZED'] || {};

    const filteredMatData = materializedData.filter(row => {
      return Object.entries(currentFilters).every(([col, selectedValues]) => {
        if (!selectedValues) return true;
        return selectedValues.includes(row[col]);
      });
    });

    const getUniqueValues = (colKey) => [...new Set(materializedData.map(r => r[colKey]))];

    return (
      <div className="flex h-[calc(100vh-64px)] w-full animate-in fade-in duration-500 overflow-hidden relative">
        <div className="flex-1 flex flex-col p-6 bg-black/20 backdrop-blur-sm overflow-hidden z-10 relative">
          <div className="flex-1 overflow-hidden rounded-2xl border border-white/10 shadow-[0_8px_32px_rgba(0,0,0,0.5)] backdrop-blur-xl bg-[#0A0A0A]/80 flex flex-col relative">
            
            <div className="absolute top-4 right-6 z-30 inline-flex items-center gap-2 px-3 py-1.5 rounded-lg bg-[#00AE9D]/10 text-[#00AE9D] text-[9px] font-black uppercase tracking-widest border border-[#00AE9D]/20 backdrop-blur-md pointer-events-none shadow-[0_0_20px_rgba(0,174,157,0.1)]">
              <CheckCircle2 size={12} /> CMDB (15 registros unificados)
            </div>

            <div className="flex-1 overflow-auto custom-scrollbar relative z-10">
              <table className="w-full text-sm text-left whitespace-nowrap">
                <thead className="bg-[#111] border-b border-white/10 text-white/60 text-[10px] uppercase tracking-[0.15em] font-black sticky top-0 z-20 backdrop-blur-2xl shadow-sm">
                  <tr>
                    <th className="px-6 py-4 w-16 align-middle border-r border-white/5">#</th>
                    
                    <th className="px-4 py-3 border-r border-white/5 align-middle">
                      <div className="flex items-center justify-between gap-4 relative">
                        <div className="flex items-center gap-2 text-[#00AE9D]">
                          <Key size={12} /> SIC (Chave)
                        </div>
                        <button 
                          onClick={(e) => { e.stopPropagation(); setOpenFilterMenu(openFilterMenu === 'MAT_SIC_CHAVE' ? null : 'MAT_SIC_CHAVE'); }}
                          className={`p-1.5 rounded-md transition-all hover:bg-white/10 ${currentFilters['SIC_CHAVE'] && currentFilters['SIC_CHAVE'].length !== getUniqueValues('SIC_CHAVE').length ? 'text-[#00AE9D] bg-[#00AE9D]/10' : 'text-white/30 hover:text-white'}`}
                        >
                          <Filter size={12} className={currentFilters['SIC_CHAVE'] && currentFilters['SIC_CHAVE'].length !== getUniqueValues('SIC_CHAVE').length ? "fill-[#00AE9D]/20" : ""} />
                        </button>

                        {openFilterMenu === 'MAT_SIC_CHAVE' && (
                          <ExcelFilterPopover 
                            options={getUniqueValues('SIC_CHAVE')}
                            selectedOptions={currentFilters['SIC_CHAVE']}
                            onClose={() => setOpenFilterMenu(null)}
                            onApply={(selection) => {
                              setExcelFilters(prev => ({ ...prev, MATERIALIZED: { ...prev.MATERIALIZED, 'SIC_CHAVE': selection } }));
                              setOpenFilterMenu(null);
                            }}
                          />
                        )}
                      </div>
                    </th>

                    {activeMatCols.map(colKey => {
                      const sourceId = colKey.split('_')[0];
                      const colName = colKey.substring(sourceId.length + 1);
                      const isFiltered = currentFilters[colKey] && currentFilters[colKey].length !== getUniqueValues(colKey).length;

                      return (
                        <th key={colKey} className="px-4 py-3 border-r border-white/5 align-middle">
                          <div className="flex items-center justify-between gap-4 relative">
                            <div className="flex items-center gap-2">
                              <span className="opacity-50 text-[9px] font-mono tracking-widest bg-white/5 px-1.5 py-0.5 rounded-md">{sourceId}</span>
                              {colName}
                            </div>
                            
                            <button 
                              onClick={(e) => { e.stopPropagation(); setOpenFilterMenu(openFilterMenu === `MAT_${colKey}` ? null : `MAT_${colKey}`); }}
                              className={`p-1.5 rounded-md transition-all hover:bg-white/10 ${isFiltered || openFilterMenu === `MAT_${colKey}` ? 'text-[#00AE9D] bg-[#00AE9D]/10' : 'text-white/30 hover:text-white'}`}
                            >
                              <Filter size={12} className={isFiltered ? "fill-[#00AE9D]/20" : ""} />
                            </button>

                            {openFilterMenu === `MAT_${colKey}` && (
                              <ExcelFilterPopover 
                                options={getUniqueValues(colKey)}
                                selectedOptions={currentFilters[colKey]}
                                onClose={() => setOpenFilterMenu(null)}
                                onApply={(selection) => {
                                  setExcelFilters(prev => ({ ...prev, MATERIALIZED: { ...prev.MATERIALIZED, [colKey]: selection } }));
                                  setOpenFilterMenu(null);
                                }}
                              />
                            )}
                          </div>
                        </th>
                      )
                    })}
                  </tr>
                </thead>
                <tbody className="text-white/80 divide-y divide-white/5">
                  {filteredMatData.length === 0 ? (
                    <tr>
                      <td colSpan={activeMatCols.length + 2} className="px-6 py-12 text-center text-white/40 italic text-xs">
                        Nenhum registro encontrado para os filtros aplicados.
                      </td>
                    </tr>
                  ) : (
                    filteredMatData.map((row, i) => (
                      <tr key={i} className="hover:bg-white/5 transition-colors">
                        <td className="px-6 py-4 font-mono text-white/30 text-xs border-r border-white/5">{i + 1}</td>
                        <td className="px-6 py-4 border-r border-white/5 font-mono text-[#00AE9D] font-bold">{row.SIC_CHAVE}</td>
                        
                        {activeMatCols.map(colKey => (
                          <td key={colKey} className="px-4 py-4 border-r border-white/5">
                            {colKey.includes('Estado') || colKey.includes('Status') ? (
                               <span className={`px-2 py-1 rounded-md text-[9px] font-black uppercase tracking-wider backdrop-blur-sm border ${row[colKey] === "Em Manutenção" ? 'bg-amber-500/10 text-amber-500 border-amber-500/20' : 'bg-emerald-500/10 text-emerald-500 border-emerald-500/20'}`}>
                                 {row[colKey]}
                               </span>
                            ) : (
                               row[colKey]
                            )}
                          </td>
                        ))}
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>

        {/* Dynamic Column Manager Right Panel */}
        {isColPanelOpen && (
          <div className="w-[340px] border-l border-white/10 bg-black/60 backdrop-blur-2xl flex flex-col shadow-[-20px_0_40px_rgba(0,0,0,0.5)] z-20 animate-in slide-in-from-right-8 duration-300">
             <div className="p-6 border-b border-white/10 flex justify-between items-center bg-black/40">
                <div className="flex items-center gap-3">
                   <Columns className="text-[#00AE9D]" size={16} />
                   <h2 className="text-[11px] font-black text-white uppercase tracking-[0.15em]">Gerenciador de Colunas</h2>
                </div>
                <button onClick={() => setIsColPanelOpen(false)} className="text-white/50 hover:text-white transition-colors p-1.5 rounded-lg hover:bg-white/10"><X size={16}/></button>
             </div>
             
             <div className="p-6 flex-1 overflow-auto space-y-6 custom-scrollbar">
                {sources.map(source => (
                   <div key={source.id} className="space-y-3">
                      <h3 className="text-[10px] font-black text-[#00AE9D] uppercase tracking-widest">{source.name}</h3>
                      <div className="space-y-1 bg-black/30 rounded-xl p-3 border border-white/5 shadow-inner">
                         {Object.keys(mockData[source.id][0] || {}).map(col => {
                            const colKey = `${source.id}_${col}`;
                            const isChecked = activeMatCols.includes(colKey);
                            return (
                               <label key={colKey} onClick={() => toggleMatCol(colKey)} className="flex items-center gap-3 p-2.5 rounded-lg hover:bg-white/5 cursor-pointer group transition-colors">
                                 <div className={`w-4 h-4 rounded border flex items-center justify-center transition-colors shadow-inner shrink-0
                                   ${isChecked ? 'bg-[#00AE9D] border-[#00AE9D]' : 'bg-black/50 border-white/20 group-hover:border-white/40'}`}>
                                   {isChecked && <CheckCircle2 size={10} className="text-white" />}
                                 </div>
                                 <span className={`text-xs font-medium truncate ${isChecked ? 'text-white/90' : 'text-white/30'}`}>
                                   {col}
                                 </span>
                               </label>
                            );
                         })}
                      </div>
                   </div>
                ))}
             </div>
          </div>
        )}
      </div>
    );
  };

  return (
    <div className="min-h-screen bg-[#020202] text-white font-sans selection:bg-[#00AE9D]/30 flex flex-col overflow-hidden relative" onClick={() => setOpenFilterMenu(null)}>
      
      {/* Background Deep Layers (Noise/Gradients) */}
      <div className="fixed inset-0 pointer-events-none z-0 overflow-hidden">
        <div className="absolute top-[-30%] left-[-10%] w-[60%] h-[60%] bg-[#00AE9D]/5 blur-[150px] rounded-full" />
        <div className="absolute bottom-[-30%] right-[-10%] w-[60%] h-[60%] bg-[#00AE9D]/5 blur-[150px] rounded-full" />
      </div>

      {/* GLOBAL UNIFIED HEADER */}
      {/* Grid refatorado para alinhar perfeitamente: Left (240px) | Center (flex-1) | Right (340px) */}
      <header className="h-16 shrink-0 border-b border-white/10 flex items-center bg-black/60 backdrop-blur-2xl backdrop-saturate-150 z-50 shadow-md relative">
        
        {/* Left: Branding */}
        <div className="flex items-center gap-6 shrink-0 justify-start w-[240px] pl-6">
          <div className="flex items-center gap-3 cursor-pointer group" onClick={() => setView('home-filled')}>
            <div className="w-9 h-9 rounded-lg bg-gradient-to-br from-[#00AE9D] to-[#008f81] flex items-center justify-center text-white font-black shadow-[0_0_20px_rgba(0,174,157,0.3)] group-hover:shadow-[0_0_25px_rgba(0,174,157,0.5)] transition-shadow shrink-0">
              CG
            </div>
            <div className="flex flex-col hidden sm:flex truncate">
              <span className="font-black text-[13px] tracking-tight leading-none text-white">SICOOB</span>
              <span className="text-[8px] font-black text-[#00AE9D] tracking-[0.25em] uppercase mt-0.5 opacity-90 truncate">Compliance Gate</span>
            </div>
          </div>
        </div>

        {/* CENTER: INTEGRATED TABS (TopBar Dinâmica e Estável com largura 100% da sua área para não pular) */}
        <div className="flex-1 min-w-0 flex items-center justify-start pr-6 pl-4">
          {view !== 'home-empty' && view !== 'home-filled' && (
            <div className="flex items-center w-full p-1 bg-white/5 border border-white/10 rounded-xl shadow-inner transition-all animate-in fade-in zoom-in-95">
              
              {/* FIXED FIRST OPTION: Voltar ao Início */}
              <button 
                onClick={() => setView('home-filled')}
                className="flex items-center justify-center gap-2 px-4 h-9 rounded-lg bg-white/10 text-white hover:bg-white/20 hover:text-white border border-white/5 shadow-sm transition-all shrink-0 group mr-1 whitespace-nowrap"
              >
                <ArrowLeft size={14} className="group-hover:-translate-x-1 transition-transform" />
                <span className="text-[11px] font-bold tracking-wide">Início</span>
              </button>

              {/* Divisor Visual */}
              <div className="w-px h-5 bg-white/10 mx-1 shrink-0" />

              {/* TABS ROLLOVER (Abas que entram/saem fluidamente) */}
              <div className="flex-1 flex items-center overflow-x-auto hide-scrollbar scroll-smooth">
                {view === 'viewer' && sources.map(source => (
                  <button
                    key={source.id}
                    onClick={() => setActiveTab(source.id)}
                    className={`flex items-center gap-2 px-4 h-9 rounded-lg transition-all min-w-[130px] shrink-0 group relative overflow-hidden animate-in fade-in duration-300
                      ${activeTab === source.id 
                        ? 'bg-white/10 text-white shadow-md border border-white/10' 
                        : 'bg-transparent text-white/40 hover:text-white hover:bg-white/5 border border-transparent'}`}
                  >
                    {activeTab === source.id && (
                      <div className="absolute inset-0 bg-gradient-to-b from-white/5 to-transparent opacity-50 pointer-events-none" />
                    )}
                    <FileSpreadsheet size={13} className={activeTab === source.id ? 'text-[#00AE9D] relative z-10' : 'opacity-50 group-hover:opacity-100 relative z-10'} />
                    
                    {editingTab === source.id ? (
                      <input
                        autoFocus
                        className="bg-[#222] border border-[#00AE9D] text-white px-2 rounded-md text-xs outline-none w-full font-bold shadow-inner relative z-10 h-7"
                        value={editTabName}
                        onChange={e => setEditTabName(e.target.value)}
                        onBlur={() => saveTabName(source.id)}
                        onKeyDown={e => e.key === 'Enter' && saveTabName(source.id)}
                        onClick={e => e.stopPropagation()}
                      />
                    ) : (
                      <div 
                        className="flex items-center gap-2 flex-1 min-w-0 cursor-text relative z-10" 
                        onDoubleClick={(e) => startEditingTab(e, source)}
                        title="Duplo clique para renomear"
                      >
                        <span className="text-[11px] font-bold truncate tracking-wide">{source.name}</span>
                      </div>
                    )}
                    
                    <div className="ml-auto shrink-0 pl-1 relative z-10">
                      <div className={`w-1.5 h-1.5 rounded-full ${configs[source.id].status === 'pronto' ? 'bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.8)]' : 'bg-white/20'}`} />
                    </div>
                  </button>
                ))}

                {/* CONTEXT BADGE (Se Materialized) */}
                {view === 'materialized' && (
                  <div className="flex items-center gap-2 px-4 h-9 rounded-lg bg-[#00AE9D]/10 text-[#00AE9D] border border-[#00AE9D]/20 transition-all shrink-0 ml-1 animate-in fade-in">
                      <Database size={13} />
                      <span className="text-[11px] font-bold tracking-wide">CMDB Unificado</span>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>

        {/* Right: Contextual Actions & Profile */}
        {/* Usando largura exata de 340px com padding pr-6. Como o centro tem pr-6, a linha invisível bate 364px exatos na direita. */}
        <div className="flex items-center justify-end gap-4 shrink-0 w-[340px] pr-6">
          
          <div className="flex items-center gap-3 mr-2 animate-in fade-in duration-300">
            
            {view === 'home-filled' && (
              <div className="hidden lg:flex items-center gap-3 px-3 h-9 rounded-lg bg-white/5 border border-white/10 shadow-inner mr-2 shrink-0">
                <span className="text-[9px] font-bold text-white/50 tracking-widest uppercase">Prontidão</span>
                <div className="h-4 w-px bg-white/10" />
                <div className="flex items-center gap-2">
                  <div className="relative w-4 h-4 flex items-center justify-center">
                    <svg className="w-full h-full transform -rotate-90 absolute inset-0">
                      <circle cx="8" cy="8" r="7" stroke="currentColor" strokeWidth="2" fill="transparent" className="text-white/10" />
                      <circle cx="8" cy="8" r="7" stroke="currentColor" strokeWidth="2" fill="transparent" 
                        strokeDasharray={`${2 * Math.PI * 7}`} 
                        strokeDashoffset={`${2 * Math.PI * 7 * (1 - readyCount/sources.length)}`}
                        className="text-[#00AE9D] transition-all duration-1000 ease-out" />
                    </svg>
                  </div>
                  <span className="text-[10px] font-black text-white">{readyCount}/{sources.length}</span>
                </div>
              </div>
            )}

            {view === 'home-filled' && sources.length > 0 && (
              <Button 
                variant="ghost" 
                size="sm"
                className={isSelectionMode ? "bg-white/10 text-white" : ""}
                onClick={() => {
                  setIsSelectionMode(!isSelectionMode);
                  if (isSelectionMode) setSelectedSources([]); 
                }}
              >
                <ListChecks size={14} /> GERENCIAR FONTES
              </Button>
            )}

            {view === 'home-filled' && (
              <Button 
                variant={isAllReady ? "primary" : "secondary"} 
                size="sm"
                disabled={!isAllReady}
                onClick={handleRunIngest}
              >
                <Play size={14} className={isAllReady ? "text-white" : "text-white/50"} />
                EXECUTAR INGEST
              </Button>
            )}

            {view === 'materialized' && (
              <>
                <Button 
                  variant={isColPanelOpen ? "primary" : "secondary"} 
                  size="sm"
                  onClick={() => setIsColPanelOpen(!isColPanelOpen)}
                >
                  <Columns size={14} /> GERENCIAR COLUNAS
                </Button>
                
                <div className="h-4 w-px bg-white/10 mx-1 hidden sm:block" />
                
                <Button 
                  variant="primary" 
                  size="sm"
                >
                  <Download size={14} /> EXPORTAR
                </Button>
              </>
            )}
          </div>

          <div className="h-6 w-px bg-white/10 hidden sm:block" />

          {/* Profile Minimal */}
          <div className="flex items-center gap-3 shrink-0">
            <div className="text-right hidden md:block">
              <div className="text-[11px] font-black text-white tracking-wide">TI.Administrador</div>
              <div className="text-[9px] font-mono text-white/40 uppercase tracking-widest mt-0.5">Sessão Ativa</div>
            </div>
            <div className="w-9 h-9 shrink-0 rounded-full bg-white/10 border border-white/20 flex items-center justify-center backdrop-blur-md shadow-inner">
               <span className="text-[10px] font-black text-[#00AE9D]">TI</span>
            </div>
          </div>
        </div>
      </header>

      {/* MAIN CONTENT AREA */}
      <main className="flex-1 overflow-hidden relative flex z-10">
        {view.startsWith('home') && renderHome()}
        {view === 'viewer' && renderViewer()}a
        {view === 'materialized' && renderMaterialized()}
      </main>

      <style dangerouslySetInnerHTML={{__html: `
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;900&display=swap');
        body { font-family: 'Inter', sans-serif; background-color: #020202; }
        
        /* Custom Scrollbar Premium */
        .custom-scrollbar::-webkit-scrollbar { width: 8px; height: 8px; }
        .custom-scrollbar::-webkit-scrollbar-track { background: transparent; }
        .custom-scrollbar::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.08); border-radius: 10px; border: 2px solid transparent; background-clip: padding-box; }
        .custom-scrollbar::-webkit-scrollbar-thumb:hover { background-color: rgba(255,255,255,0.15); border: 2px solid transparent; background-clip: padding-box; }
        
        /* Ocultar scrollbar para as abas integradas mantendo a usabilidade de scroll horizontal */
        .hide-scrollbar::-webkit-scrollbar { display: none; }
        .hide-scrollbar { -ms-overflow-style: none; scrollbar-width: none; }
      `}} />
    </div>
  );
}