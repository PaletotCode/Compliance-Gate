const BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000/api/v1';

export async function fetchApi<T>(endpoint: string, options: RequestInit = {}): Promise<T> {
    const url = `${BASE_URL}${endpoint}`;
    const defaultHeaders: Record<string, string> = {
        'Accept': 'application/json',
    };

    if (options.body && typeof options.body === 'string') {
        defaultHeaders['Content-Type'] = 'application/json';
    }

    const response = await fetch(url, {
        ...options,
        headers: {
            ...defaultHeaders,
            ...options.headers,
        },
    });

    if (!response.ok) {
        let errorMsg = `HTTP Error ${response.status}`;
        try {
            const errBody = await response.json();
            errorMsg = errBody.detail || errorMsg;
        } catch {
            // Ignore
        }
        throw new Error(errorMsg);
    }

    return response.json();
}
