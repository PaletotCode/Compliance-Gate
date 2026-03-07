import { useState, useEffect } from 'react';
import { api } from './api';
import type { RawPreviewResponse, CsvTabProfile, CsvTabConfig } from './api/types';

const SOURCES = ['AD', 'UEM', 'EDR', 'ASSET'];

export default function SetupView() {
    const [activeSource, setActiveSource] = useState(SOURCES[0]);
    const [profile, setProfile] = useState<CsvTabProfile | null>(null);

    const [preview, setPreview] = useState<RawPreviewResponse | null>(null);
    const [headerRow, setHeaderRow] = useState(0);
    const [sicCol, setSicCol] = useState('');
    const [loading, setLoading] = useState(false);
    const [saving, setSaving] = useState(false);
    const [toastMsg, setToastMsg] = useState<{ text: string; type: 'success' | 'error' | 'info' } | null>(null);
    const [isDirty, setIsDirty] = useState(false);

    function showToast(text: string, type: 'success' | 'error' | 'info' = 'success') {
        setToastMsg({ text, type });
        setTimeout(() => setToastMsg(null), 3000);
    }

    useEffect(() => {
        const savedHr = profile?.payload?.header_row ?? 0;
        const savedSic = profile?.payload?.sic_column ?? '';
        setIsDirty(headerRow !== savedHr || sicCol !== savedSic);
    }, [headerRow, sicCol, profile]);

    useEffect(() => {
        loadSourceProfile(activeSource);
    }, [activeSource]);

    async function loadSourceProfile(src: string) {
        setLoading(true);
        setPreview(null);
        try {
            const profiles = await api.csv.getProfiles(src);
            let defaultProf = profiles.find(p => p.is_default_for_source);

            if (defaultProf) {
                // Fetch full payload
                defaultProf = await api.csv.getProfile(defaultProf.id);
                setProfile(defaultProf);
                setHeaderRow(defaultProf.payload?.header_row ?? 0);
                setSicCol(defaultProf.payload?.sic_column ?? '');
                await fetchPreview(src, defaultProf.payload?.header_row ?? 0);
            } else {
                setProfile(null);
                setHeaderRow(0);
                setSicCol('');
                await fetchPreview(src, 0);
            }
        } catch (e) {
            console.error(e);
            showToast('Failed to load profile', 'error');
        } finally {
            setLoading(false);
        }
    }

    async function fetchPreview(src: string, hr: number) {
        setLoading(true);
        try {
            const res = await api.csv.previewRaw(src, hr);
            setPreview(res);
            // Auto-select SIC if empty and we have headers
            if (!sicCol && res.detected_headers.length > 0) {
                setSicCol(res.detected_headers[0]);
            }
        } catch (e) {
            console.error(e);
        } finally {
            setLoading(false);
        }
    }

    function handleHeaderChange(e: React.ChangeEvent<HTMLInputElement>) {
        const val = parseInt(e.target.value) || 0;
        setHeaderRow(val);
    }

    function handleApplyHeader() {
        fetchPreview(activeSource, headerRow);
    }

    async function handleSave() {
        if (!preview) return;
        setSaving(true);
        try {
            const payload: CsvTabConfig = {
                header_row: headerRow,
                sic_column: sicCol,
                selected_columns: preview.detected_headers, // Save all columns for transparency
            };

            if (profile?.id) {
                await api.csv.updateProfile(profile.id, { payload });
            } else {
                await api.csv.saveProfile(activeSource, payload);
            }
            showToast('Perfil salvo com sucesso.');
            loadSourceProfile(activeSource);
        } catch (e) {
            showToast('Erro ao salvar', 'error');
        } finally {
            setSaving(false);
        }
    }

    async function handleForceIngest() {
        try {
            // Send active profiles if needed, or backend can auto-detect defaults.
            // Let's rely on backend defaults for now.
            showToast('Iniciando ingestão...', 'info');
            await api.datasets.ingest({});
            showToast('Ingestão concluída e salva na nova versão (Latest)!');
        } catch (e: any) {
            showToast('Erro no Ingest: ' + e.message, 'error');
        }
    }

    return (
        <div className="flex h-full">
            {/* Sidebar Sources */}
            <div className="w-64 bg-white border-r border-slate-200 flex flex-col">
                <div className="p-4 border-b border-slate-200 bg-slate-50">
                    <h2 className="font-semibold text-slate-700">Fontes CSV</h2>
                </div>
                <div className="flex-1 overflow-y-auto p-2">
                    {SOURCES.map(s => (
                        <button
                            key={s}
                            onClick={() => setActiveSource(s)}
                            className={`w-full text-left p-3 rounded mb-1 ${activeSource === s ? 'bg-blue-50 text-blue-700 font-medium' : 'hover:bg-slate-100 text-slate-600'}`}
                        >
                            {s}
                        </button>
                    ))}
                </div>
            </div>

            {/* Main Content */}
            <div className="flex-1 flex flex-col overflow-hidden relative">

                {/* Toast Notification */}
                {toastMsg && (
                    <div className={`absolute top-4 inset-x-0 mx-auto w-max px-4 py-2 rounded shadow-lg z-50 text-sm font-medium transition-all transform animate-in fade-in slide-in-from-top-4 ${toastMsg.type === 'success' ? 'bg-green-100 text-green-800 border border-green-200' :
                        toastMsg.type === 'info' ? 'bg-blue-100 text-blue-800 border border-blue-200' :
                            'bg-red-100 text-red-800 border border-red-200'
                        }`}>
                        {toastMsg.text}
                    </div>
                )}
                {/* Toolbar */}
                <div className="bg-white border-b border-slate-200 p-4 shrink-0 flex items-center justify-between shadow-sm z-10">
                    <div className="flex items-center space-x-6">
                        <h2 className="text-xl font-bold text-slate-800 shrink-0">Ajuste de {activeSource}</h2>

                        <div className="flex items-center space-x-2 bg-slate-50 p-2 rounded border border-slate-200">
                            <label className="text-sm font-medium text-slate-600">Header Row (0-idx):</label>
                            <input
                                type="number"
                                min="0"
                                value={headerRow}
                                onChange={handleHeaderChange}
                                className="w-16 p-1 border rounded text-center"
                            />
                            <button onClick={handleApplyHeader} className="btn-outline btn-sm">Aplicar</button>
                        </div>

                        <div className="flex items-center space-x-2 bg-slate-50 p-2 rounded border border-slate-200">
                            <label className="text-sm font-medium text-slate-600">Coluna SIC (Join):</label>
                            <select
                                value={sicCol}
                                onChange={e => setSicCol(e.target.value)}
                                className="w-48 p-1 border rounded"
                            >
                                {preview?.detected_headers.map(h => (
                                    <option key={h} value={h}>{h}</option>
                                ))}
                            </select>
                        </div>
                    </div>

                    <div className="flex space-x-3">
                        <button onClick={handleForceIngest} className="btn-outline border-blue-600 text-blue-600 hover:bg-blue-50 bg-white">
                            ▶ Processar Ingestão Global
                        </button>
                        <div className="relative">
                            {isDirty && (
                                <span className="absolute -top-1 -right-1 flex h-3 w-3 z-10" title="Alterações não salvas">
                                    <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-red-400 opacity-75"></span>
                                    <span className="relative inline-flex rounded-full h-3 w-3 bg-red-500"></span>
                                </span>
                            )}
                            <button onClick={handleSave} disabled={saving} className={`btn-primary ${isDirty ? 'ring-2 ring-red-500 ring-offset-1' : ''}`}>
                                {saving ? 'Salvando...' : (isDirty ? 'Salvar Perfil *' : 'Salvar Perfil')}
                            </button>
                        </div>
                    </div>
                </div>

                {/* Spreadsheet Area */}
                <div className="flex-1 overflow-auto bg-slate-100 p-6 relative">
                    {loading ? (
                        <div className="flex items-center justify-center h-full text-slate-500">Lendo CSV...</div>
                    ) : !preview?.exists ? (
                        <div className="flex items-center justify-center h-full text-red-500 flex-col">
                            <span className="text-4xl mb-4">📄❌</span>
                            <span>CSV não encontrado na raiz: {activeSource}_*.csv</span>
                        </div>
                    ) : (
                        <div className="bg-white shadow ring-1 ring-slate-200 rounded min-w-max">
                            <table className="w-full text-sm text-left border-collapse">
                                <thead>
                                    <tr>
                                        <th className="bg-slate-200 border border-slate-300 p-2 text-slate-500 w-12 text-center sticky top-0 left-0 z-20">#</th>
                                        {preview.detected_headers.map((h, i) => (
                                            <th key={i} className={`bg-slate-100 border border-slate-300 p-2 font-semibold text-slate-700 whitespace-nowrap sticky top-0 z-10 ${h === sicCol ? 'ring-2 ring-inset ring-blue-500 bg-blue-50' : ''}`}>
                                                {h}
                                                {h === sicCol && <span className="ml-2 text-xs text-blue-600">★ SIC</span>}
                                            </th>
                                        ))}
                                    </tr>
                                </thead>
                                <tbody>
                                    {preview.sample_rows.map((row, rIdx) => (
                                        <tr key={rIdx} className="hover:bg-yellow-50">
                                            <td className="bg-slate-100 border border-slate-300 p-2 text-slate-500 text-center sticky left-0 z-10">{rIdx + headerRow + 1}</td>
                                            {preview.detected_headers.map((h, cIdx) => (
                                                <td key={cIdx} className={`border border-slate-300 p-2 whitespace-nowrap overflow-hidden max-w-[300px] text-ellipsis ${h === sicCol ? 'bg-blue-50/30' : ''}`}>
                                                    {/* Usamos original_headers para acessar os dados no JSON cru (o polars exporta com nome original, mas detectou com maiusculas e sem BOM) */}
                                                    <span title={row[preview.original_headers[cIdx]]}>{row[preview.original_headers[cIdx]] ?? '-'}</span>
                                                </td>
                                            ))}
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
