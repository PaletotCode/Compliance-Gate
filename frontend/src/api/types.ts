export interface CsvTabConfig {
    header_row: number;
    delimiter?: string;
    encoding?: string;
    sic_column: string;
    selected_columns: string[];
    alias_map?: Record<string, string>;
    normalize_key_strategy?: string;
}

export interface CsvTabProfile {
    id: string;
    tenant_id: string;
    source: string;
    scope: string;
    name: string;
    active_version: number;
    is_default_for_source: boolean;
    payload?: CsvTabConfig;
}

export interface RawPreviewResponse {
    status: string;
    source: string;
    exists: boolean;
    detected_encoding: string;
    detected_delimiter: string;
    header_row_index: number;
    detected_headers: string[];
    original_headers: string[];
    rows_total_read: number;
    sample_rows: Record<string, any>[];
    warnings: string[];
    error?: string;
}

export interface MachineItemSchema {
    id: string;
    hostname: string;
    pa_code: string;
    primary_status: string;
    primary_status_label: string;
    flags: string[];
    has_ad: boolean;
    has_uem: boolean;
    has_edr: boolean;
    has_asset: boolean;
    us_ad?: string | null;
    us_uem?: string | null;
    us_edr?: string | null;
    main_user?: string | null;
    uem_extra_user_logado?: string | null;
    ad_os?: string | null;
    edr_os?: string | null;
    status_check_win11?: string | null;
    edr_serial?: string | null;
    uem_serial?: string | null;
    chassis?: string | null;
    selected_data?: Record<string, any>;
}

export interface PaginatedResponse<T> {
    data: {
        items: T[];
        meta: {
            total: number;
            page: number;
            size: number;
        };
    };
}
