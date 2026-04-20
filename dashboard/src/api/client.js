/**
 * api/client.js
 * All API calls for the OpsResolver dashboard.
 * Base URL is driven by VITE_API_BASE environment variable:
 *   - .env.development → http://localhost:8000
 *   - .env.production  → /api  (proxied by Nginx)
 */

const BASE = import.meta.env.VITE_API_BASE || '';

// ── Generic fetch wrapper ──────────────────────────────────────────────────

async function request(path, options = {}) {
    const url = `${BASE}${path}`;
    const res = await fetch(url, {
        headers: { 'Content-Type': 'application/json', ...options.headers },
        ...options,
    });

    if (!res.ok) {
        const body = await res.text();
        throw new Error(`API ${res.status}: ${body}`);
    }

    return res.json();
}

// ── Helpers ────────────────────────────────────────────────────────────────

/** Convert a plain object into a query string, skipping null/undefined/empty */
function buildQuery(params = {}) {
    const q = new URLSearchParams();
    for (const [k, v] of Object.entries(params)) {
        if (v !== null && v !== undefined && v !== '') {
            q.append(k, v);
        }
    }
    const str = q.toString();
    return str ? `?${str}` : '';
}

// ── Dashboard stats endpoints ──────────────────────────────────────────────

/**
 * GET /stats/summary
 * @param {{ from_date?: string, to_date?: string }} [dateRange]
 * Returns: { total, resolved, unresolved, technical, business, platform, today }
 */
export async function getSummary(dateRange = {}) {
    return request(`/stats/summary${buildQuery(dateRange)}`);
}

/**
 * GET /stats/by-application
 * @param {{ from_date?: string, to_date?: string }} [dateRange]
 * Returns: { data: [{ application_name, total, resolved, unresolved }] }
 */
export async function getByApplication(dateRange = {}) {
    return request(`/stats/by-application${buildQuery(dateRange)}`);
}

/**
 * GET /stats/trends
 * @param {{ from_date?: string, to_date?: string, application_name?: string }} [filters]
 * Returns: { granularity, data: [{ bucket, total, technical, business, platform, unknown }] }
 */
export async function getTrends(filters = {}) {
    return request(`/stats/trends${buildQuery(filters)}`);
}

// ── Records ────────────────────────────────────────────────────────────────

/**
 * GET /applications
 * Returns: { applications: string[] }
 */
export async function getApplications() {
    return request('/applications');
}

/**
 * GET /getsolutions  (with optional filters + pagination)
 * @param {Object} filters
 * @param {string}  filters.app_name
 * @param {string}  filters.error_type   'technical' | 'business'
 * @param {string}  filters.status       'resolved' | 'unresolved'
 * @param {string}  filters.from_date    YYYY-MM-DD
 * @param {string}  filters.to_date      YYYY-MM-DD
 * @param {string}  filters.search       keyword
 * @param {number}  filters.page         default 1
 * @param {number}  filters.page_size    default 20
 *
 * Returns: { total, page, page_size, total_pages, records: [] }
 */
export async function getSolutions(filters = {}) {
    return request(`/getsolutions${buildQuery(filters)}`);
}

// ── Solution submission ────────────────────────────────────────────────────

/**
 * POST /submitopssolution
 * Submits an ops-verified solution for an error record.
 *
 * @param {Object} payload
 * @param {number}  payload.errorId            - DB record id
 * @param {string|null} payload.customSolution - null if selecting an LLM solution
 * @param {string|null} payload.solutionId     - "1" | "2" | "3" (LLM solution number)
 * @param {string}  payload.solutionTimestamp  - ISO timestamp of submission
 *
 * Returns: { message, status }
 */
export async function submitSolution(payload) {
    return request('/submitopssolution', {
        method: 'POST',
        body: JSON.stringify({
            errorId: payload.errorId,
            customSolution: payload.customSolution || null,
            solutionId: payload.solutionId || null,
            solutionTimestamp: payload.solutionTimestamp || new Date().toISOString(),
        }),
    });
}
