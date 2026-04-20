/**
 * pages/KnowledgeBase.jsx
 * Read-only table of all resolved errors — searchable by keyword, app, error code.
 * Polls every 60s.
 */
import { useState, useCallback, useEffect } from 'react';
import { usePolling } from '../hooks/usePolling';
import { getSolutions, getApplications } from '../api/client';
import RecordsTable from '../components/RecordsTable';
import DetailPanel from '../components/DetailPanel';

const DEFAULT_FILTERS = { status: 'resolved', page: 1, page_size: 20 };

export default function KnowledgeBase({ onLastUpdated }) {
    const [filters, setFilters] = useState(DEFAULT_FILTERS);
    const [selected, setSelected] = useState(null);
    const [apps, setApps] = useState([]);
    const [search, setSearch] = useState('');
    const [appFilter, setApp] = useState('');

    useEffect(() => {
        getApplications().then(r => setApps(r.applications || [])).catch(() => { });
    }, []);

    const fetchFn = useCallback(async () => {
        const res = await getSolutions(filters);
        onLastUpdated?.(new Date());
        return res;
    }, [filters, onLastUpdated]);

    const { data, loading, error, refresh } = usePolling(fetchFn, 60_000);

    // Apply search + app filter together
    useEffect(() => {
        setFilters({ status: 'resolved', page: 1, page_size: 20, search, app_name: appFilter });
    }, [search, appFilter]);

    return (
        <div className="page">
            <div className="page-header flex-between">
                <div>
                    <h2>Knowledge Base</h2>
                    <p>All errors with an approved ops solution — searchable reference for the team</p>
                </div>
                <span className="badge badge-success" style={{ fontSize: '0.78rem', padding: '6px 14px' }}>
                    {data?.total ?? '…'} resolved
                </span>
            </div>

            {error && <div className="error-banner">{error}</div>}

            {/* Compact filter bar */}
            <div className="filter-bar mb-20">
                <div className="search-wrapper">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                        <circle cx="11" cy="11" r="8" /><path d="M21 21l-4.35-4.35" />
                    </svg>
                    <input
                        className="search-input"
                        placeholder="Search error code or description…"
                        value={search}
                        onChange={e => setSearch(e.target.value)}
                    />
                </div>
                <div className="filter-group">
                    <span className="filter-label">App</span>
                    <select className="filter-select" value={appFilter} onChange={e => setApp(e.target.value)}>
                        <option value="">All applications</option>
                        {apps.map(a => <option key={a} value={a}>{a}</option>)}
                    </select>
                </div>
                {(search || appFilter) && (
                    <button className="btn btn-ghost btn-sm" onClick={() => { setSearch(''); setApp(''); }}>
                        ✕ Clear
                    </button>
                )}
            </div>

            <RecordsTable
                records={data?.records || []}
                total={data?.total}
                page={data?.page || 1}
                pageSize={data?.page_size || 20}
                onPageChange={p => setFilters(f => ({ ...f, page: p }))}
                onRowClick={setSelected}
                loading={loading}
            />

            {/* Detail panel — read-only since all are resolved */}
            <DetailPanel
                record={selected}
                onClose={() => setSelected(null)}
                onSubmitted={refresh}
            />
        </div>
    );
}
