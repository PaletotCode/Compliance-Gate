import { useState, useEffect } from 'react';
import { api } from './api';
import type { MachineItemSchema } from './api/types';

// O Dashboard_fix.ts tem a ordem estrita exigida pelo usuário:
const FINAL_COLUMNS = [
    "HOSTNAME", "STATUS ATUAL", "AD", "UEM", "EDR", "ASSET",
    "U.S AD", "U.S UEM", "U.S EDR", "USUÁRIO", "status_usuariologado",
    "USUÁRIOS VERIFICADOS", "OS", "LEGADO", "status_check_win11",
    "SERIAL EDR", "SERIAL UEM", "CHASSIS"
];

// Mapping original values from our Schema to these columns
// This maps column names to backend property accessors if possible.
const COL_MAPPING: Record<string, (r: MachineItemSchema) => any> = {
    "HOSTNAME": r => r.hostname,
    "STATUS ATUAL": r => r.primary_status_label,
    "AD": r => r.has_ad ? '✅' : '❌',
    "UEM": r => r.has_uem ? '✅' : '❌',
    "EDR": r => r.has_edr ? '✅' : '❌',
    "ASSET": r => r.has_asset ? '✅' : '❌',
    "U.S AD": r => r.us_ad || '-',
    "U.S UEM": r => r.us_uem || '-',
    "U.S EDR": r => r.us_edr || '-',
    "USUÁRIO": r => r.main_user || '-',
    "status_usuariologado": r => r.uem_extra_user_logado || '-',
    "USUÁRIOS VERIFICADOS": () => '-', // Needs feature parity with Excel comments
    "OS": r => r.ad_os || r.edr_os || '-',
    "LEGADO": r => r.flags?.includes("LEGACY") ? 'SIM' : '-',
    "status_check_win11": r => r.status_check_win11 || '-',
    "SERIAL EDR": r => r.edr_serial || '-',
    "SERIAL UEM": r => r.uem_serial || '-',
    "CHASSIS": r => r.chassis || '-'
};

export default function DashboardView() {
    const [data, setData] = useState<MachineItemSchema[]>([]);
    const [loading, setLoading] = useState(false);
    const [total, setTotal] = useState(0);
    const [page, setPage] = useState(1);
    const pageSize = 50;

    useEffect(() => {
        fetchData();
    }, [page]);

    async function fetchData() {
        setLoading(true);
        try {
            const res = await api.machines.getTable('latest', page, pageSize);
            setData(res.data.items);
            setTotal(res.data.meta.total);
        } catch (e) {
            console.error(e);
            alert('Erro ao buscar dados do Dashboard');
        } finally {
            setLoading(false);
        }
    }

    return (
        <div className="flex-1 flex flex-col h-full">
            <div className="bg-white border-b border-slate-200 p-4 shrink-0 flex justify-between items-center z-10">
                <div>
                    <h2 className="text-xl font-bold text-slate-800">Outputs Finais (Ingestão)</h2>
                    <p className="text-slate-500 text-sm">{total} registros no total na base (Latest)</p>
                </div>
                <button onClick={fetchData} className="btn-outline">Recarregar</button>
            </div>

            <div className="flex-1 overflow-auto bg-slate-100 p-6">
                {loading ? (
                    <div className="flex items-center justify-center h-full">Carregando Tabela Central...</div>
                ) : (
                    <div className="bg-white shadow ring-1 ring-slate-200 rounded min-w-max inline-block">
                        <table className="w-full text-sm text-left border-collapse">
                            <thead>
                                <tr>
                                    {FINAL_COLUMNS.map(col => (
                                        <th key={col} className="bg-slate-800 text-white font-semibold border border-slate-700 p-2 whitespace-nowrap sticky top-0">
                                            {col}
                                        </th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody>
                                {data.map((row, i) => (
                                    <tr key={row.id || i} className="hover:bg-slate-50 border-b border-slate-200">
                                        {FINAL_COLUMNS.map(col => {
                                            const rawVal = COL_MAPPING[col] ? COL_MAPPING[col](row) : '-';
                                            const val = (rawVal === null || rawVal === undefined || rawVal === '') ? '-' : rawVal;

                                            let colorClass = '';
                                            if (val === '✅') colorClass = 'text-green-600 bg-green-50 text-center font-bold';
                                            else if (val === '❌') colorClass = 'text-red-600 bg-red-50 text-center font-bold';

                                            return (
                                                <td key={col} className={`border-r border-slate-200 p-2 whitespace-nowrap max-w-[200px] overflow-hidden text-ellipsis ${colorClass}`} title={String(val)}>
                                                    {val}
                                                </td>
                                            );
                                        })}
                                    </tr>
                                ))}
                                {data.length === 0 && (
                                    <tr><td colSpan={FINAL_COLUMNS.length} className="text-center p-8 text-slate-500">Nenhum dado encontrado. Faça a ingestão na tela de Setup primeiro.</td></tr>
                                )}
                            </tbody>
                        </table>
                    </div>
                )}
            </div>

            {/* Pagination Controls */}
            <div className="bg-white border-t border-slate-200 p-4 shrink-0 flex items-center justify-between z-10">
                <div className="text-sm text-slate-600">
                    Mostrando página {page} de {Math.ceil(total / pageSize) || 1}
                </div>
                <div className="flex space-x-2">
                    <button
                        onClick={() => setPage(p => Math.max(1, p - 1))}
                        disabled={page === 1 || loading}
                        className="btn-outline btn-sm"
                    >
                        Anterior
                    </button>
                    <button
                        onClick={() => setPage(p => p + 1)}
                        disabled={page * pageSize >= total || loading}
                        className="btn-outline btn-sm"
                    >
                        Próxima
                    </button>
                </div>
            </div>
        </div>
    );
}
