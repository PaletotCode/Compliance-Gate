/// <reference types="vite/client" />

import {
  FilterDefinition,
  MachineFilterParams,
  MachineItem,
  MachineSummary,
  PaginatedResult
} from './types';

// Depending on the dev setup (Docker proxy or local env), Vite proxy handles /api
const BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api/v1';

async function fetchApi<T>(endpoint: string, params?: Record<string, any>): Promise<T> {
  let url = `${BASE_URL}${endpoint}`;

  if (params) {
    const searchParams = new URLSearchParams();
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined && value !== null && value !== '') {
        if (Array.isArray(value)) {
          value.forEach(v => searchParams.append(key, v));
        } else {
          searchParams.append(key, value.toString());
        }
      }
    }
    const query = searchParams.toString();
    if (query) {
      url += `?${query}`;
    }
  }

  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`API request failed: ${response.statusText}`);
  }

  const json = await response.json();
  // Using our standard wrapper parsing
  return json.body?.data ?? json.data;
}

export const api = {
  machines: {
    getFilters: () => fetchApi<FilterDefinition[]>('/machines/filters'),

    getSummary: (params?: MachineFilterParams) =>
      fetchApi<MachineSummary>('/machines/summary', params),

    getTable: (params?: MachineFilterParams) =>
      fetchApi<PaginatedResult<MachineItem>>('/machines/table', {
        page: 1,
        page_size: 500, // Load a chunk big enough to emulate the spreadsheet
        ...params
      }),

    getDebugLogs: (limit = 200) => fetchApi<any[]>('/machines/debug/logs', { limit }),
    getDebugSample: (limit = 50) => fetchApi<any[]>('/machines/debug/sample', { limit }),
  }
};
