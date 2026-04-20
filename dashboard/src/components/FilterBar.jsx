/**
 * FilterBar.jsx
 * Filter controls above the Records table.
 * Props: filters (object), onFilterChange (fn), applications (string[])
 */
import TimeRangePicker from './TimeRangePicker';

export default function FilterBar({ filters, onFilterChange, applications = [] }) {
    const set = (key, val) => onFilterChange({ ...filters, [key]: val, page: 1 });

    function handleTimeRange({ from_date, to_date }) {
        onFilterChange({ ...filters, from_date, to_date, page: 1 });
    }

    function handleTimeClear() {
        const { from_date, to_date, ...rest } = filters;
        onFilterChange({ ...rest, page: 1 });
    }

    return (
        <div className="filter-bar">

            {/* Search */}
            <div className="search-wrapper">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <circle cx="11" cy="11" r="8" /><path d="M21 21l-4.35-4.35" />
                </svg>
                <input
                    className="search-input"
                    placeholder="Search code or description…"
                    value={filters.search || ''}
                    onChange={e => set('search', e.target.value)}
                />
            </div>

            {/* Application */}
            <div className="filter-group">
                <span className="filter-label">App</span>
                <select className="filter-select" value={filters.app_name || ''} onChange={e => set('app_name', e.target.value)}>
                    <option value="">All</option>
                    {applications.map(a => <option key={a} value={a}>{a}</option>)}
                </select>
            </div>

            {/* Error type */}
            <div className="filter-group">
                <span className="filter-label">Type</span>
                <div className="toggle-group">
                    {['', 'platform', 'technical', 'business'].map(t => (
                        <button
                            key={t}
                            className={`toggle-btn ${(filters.error_type || '') === t ? 'active' : ''}`}
                            onClick={() => set('error_type', t)}
                        >
                            {t === '' ? 'All' : t.charAt(0).toUpperCase() + t.slice(1)}
                        </button>
                    ))}
                </div>
            </div>

            {/* Status */}
            <div className="filter-group">
                <span className="filter-label">Status</span>
                <div className="toggle-group">
                    {['', 'resolved', 'unresolved'].map(s => (
                        <button
                            key={s}
                            className={`toggle-btn ${(filters.status || '') === s ? 'active' : ''}`}
                            onClick={() => set('status', s)}
                        >
                            {s === '' ? 'All' : s.charAt(0).toUpperCase() + s.slice(1)}
                        </button>
                    ))}
                </div>
            </div>

            {/* Time range picker */}
            <TimeRangePicker
                value={{ from_date: filters.from_date, to_date: filters.to_date }}
                onChange={handleTimeRange}
                onClear={handleTimeClear}
            />

            {/* Clear all */}
            {(() => { const { page, page_size, ...active } = filters; return Object.values(active).some(Boolean); })() && (
                <button
                    className="btn btn-ghost btn-sm"
                    onClick={() => onFilterChange({ page: 1, page_size: 20 })}
                >
                    ✕ Clear
                </button>
            )}
        </div>
    );
}
