// Domain schemas reflecting backend

export interface FilterDefinition {
    key: string;
    label: string;
    severity: 'INFO' | 'WARNING' | 'DANGER' | 'SUCCESS';
    description: string;
    is_flag: boolean;
}

export interface MachineItem {
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
    model: string | null;
    ip: string | null;
    tags: string | null;
    main_user: string | null;
    ad_os: string | null;
}

export interface MachineSummary {
    total: number;
    by_status: Record<string, number>;
    by_flag: Record<string, number>;
}

// API Responses
export interface ApiResponse<T> {
    status: number;
    ms: number;
    body: {
        success: boolean;
        data: T;
        errors: any | null;
    };
}

export interface PaginationMeta {
    total: number;
    page: number;
    size: number;
    has_next: boolean;
    has_previous: boolean;
}

export interface PaginatedResult<T> {
    items: T[];
    meta: PaginationMeta;
}

// Filter Options
export interface MachineFilterParams {
    search?: string;
    pa_code?: string;
    statuses?: string[];
    flags?: string[];
    page?: number;
    page_size?: number;
}
