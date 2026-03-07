import { fetchApi } from './client';
import type { CsvTabProfile, RawPreviewResponse, PaginatedResponse, MachineItemSchema } from './types';

export const api = {
    csv: {
        getSources: () => fetchApi<string[]>('/csv-tabs/sources'),
        getProfiles: (source: string) => fetchApi<CsvTabProfile[]>(`/csv-tabs/profiles?source=${source}`),
        getProfile: (id: string) => fetchApi<CsvTabProfile>(`/csv-tabs/profiles/${id}`),
        saveProfile: (source: string, payload: any) =>
            fetchApi<CsvTabProfile>('/csv-tabs/profiles', {
                method: 'POST',
                body: JSON.stringify({
                    source,
                    name: `Setup ${source}`,
                    scope: 'TENANT',
                    is_default_for_source: true,
                    payload
                })
            }),
        updateProfile: (id: string, payload: any) =>
            fetchApi<CsvTabProfile>(`/csv-tabs/profiles/${id}`, {
                method: 'PUT',
                body: JSON.stringify(payload)
            }),
        previewRaw: (source: string, header_row?: number) =>
            fetchApi<RawPreviewResponse>('/csv-tabs/preview/raw', {
                method: 'POST',
                body: JSON.stringify({ source, header_row_override: header_row })
            })
    },
    datasets: {
        ingest: (profileIds: Record<string, string>) =>
            fetchApi<any>('/datasets/machines/ingest', {
                method: 'POST',
                body: JSON.stringify({ profile_ids: profileIds })
            })
    },
    machines: {
        getTable: (datasetId = 'latest', page = 1, size = 100) =>
            fetchApi<PaginatedResponse<MachineItemSchema>>(`/machines/table?dataset_version_id=${datasetId}&page=${page}&size=${size}`)
    }
};
