/**
 * pages/Records.jsx
 * Filterable, paginated error records table with slide-in detail panel.
 * Auto-refreshes every 30s. Re-fetches immediately on filter change.
 */
import { useState, useCallback, useEffect } from 'react';
import { usePolling } from '../hooks/usePolling';
import { getSolutions, getApplications } from '../api/client';
import FilterBar from '../components/FilterBar';
import RecordsTable from '../components/RecordsTable';
import DetailPanel from '../components/DetailPanel';

const DEFAULT_FILTERS = { page: 1, page_size: 20 };

export default function Records({ onLastUpdated }) {
    const [filters, setFilters] = useState(DEFAULT_FILTERS);
    const [selected, setSelected] = useState(null);
    const [applications, setApps] = useState([]);

    // Load application list once
    useEffect(() => {
        getApplications().then(r => setApps(r.applications || [])).catch(() => { });
    }, []);

    const fetchFn = useCallback(async () => {
        const res = await getSolutions(filters);
        onLastUpdated?.(new Date());
        return res;
    }, [filters, onLastUpdated]);

    const { data, loading, error, refresh } = usePolling(fetchFn, 30_000);

    const handleFilterChange = (newFilters) => setFilters(newFilters);
    const handlePageChange = (page) => setFilters(f => ({ ...f, page }));

    return (
        <div className="page">
            <div className="page-header flex-between">
                <div>
                    <h2>Error Records</h2>
                    <p>All errors processed by the pipeline — click a row to view details and submit a solution</p>
                </div>
                <button className="btn btn-secondary btn-sm" onClick={refresh}>↻ Refresh</button>
            </div>

            {error && <div className="error-banner">{error}</div>}

            <FilterBar
                filters={filters}
                onFilterChange={handleFilterChange}
                applications={applications}
            />

            <RecordsTable
                records={data?.records || []}
                total={data?.total}
                page={data?.page || 1}
                pageSize={data?.page_size || 20}
                onPageChange={handlePageChange}
                onRowClick={setSelected}
                loading={loading}
            />

            <DetailPanel
                record={selected}
                onClose={() => setSelected(null)}
                onSubmitted={() => { refresh(); setSelected(null); }}
            />
        </div>
    );
}
