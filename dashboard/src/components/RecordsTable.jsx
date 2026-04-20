/**
 * RecordsTable.jsx
 * Paginated sortable table of error records.
 * Props:
 *   records      [] — array of row objects from /getsolutions
 *   total        number — total record count (for pagination)
 *   page         number — current page (1-indexed)
 *   pageSize     number — records per page
 *   onPageChange fn(newPage)
 *   onRowClick   fn(record)
 *   loading      bool
 */
import { format, parseISO } from 'date-fns';

function TypeBadge({ type }) {
    if (!type) return <span className="badge badge-neutral">—</span>;
    const cls = type === 'technical' ? 'badge-technical' : 'badge-business';
    return <span className={`badge ${cls}`}>{type}</span>;
}

function StatusBadge({ resolved }) {
    return resolved
        ? <span className="badge badge-success">✓ Resolved</span>
        : <span className="badge badge-warning">⏳ Pending</span>;
}

function formatTs(ts) {
    if (!ts) return '—';
    try { return format(parseISO(String(ts)), 'MMM d, yyyy HH:mm'); }
    catch { return String(ts).slice(0, 16); }
}

export default function RecordsTable({ records = [], total, page, pageSize, onPageChange, onRowClick, loading }) {
    const totalPages = Math.max(1, Math.ceil((total || 0) / (pageSize || 20)));

    if (loading && !records.length) {
        return <div className="spinner-wrap"><div className="spinner" /></div>;
    }

    return (
        <div className="table-wrapper">
            <div className="table-scroll">
                <table>
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>Application</th>
                            <th>Error Code</th>
                            <th>Type</th>
                            <th>Occurrences</th>
                            <th>Timestamp</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody>
                        {records.length === 0 ? (
                            <tr><td colSpan={7} className="table-empty">No records found</td></tr>
                        ) : records.map(r => (
                            <tr key={r.id} onClick={() => onRowClick && onRowClick(r)}>
                                <td className="text-muted text-sm">{r.id}</td>
                                <td style={{ maxWidth: 180 }} className="truncate">{r.application_name || '—'}</td>
                                <td><code className="font-mono">{r.error_code || '—'}</code></td>
                                <td><TypeBadge type={r.error_type} /></td>
                                <td style={{ textAlign: 'center' }}>
                                    <span style={{
                                        background: r.occurrence_count > 10 ? 'var(--danger-bg)' : 'var(--bg-surface)',
                                        color: r.occurrence_count > 10 ? 'var(--danger)' : 'var(--text-secondary)',
                                        padding: '2px 8px', borderRadius: 999, fontSize: '0.75rem', fontWeight: 600
                                    }}>
                                        {r.occurrence_count ?? 1}
                                    </span>
                                </td>
                                <td className="text-secondary text-sm">{formatTs(r.error_timestamp)}</td>
                                <td><StatusBadge resolved={r.is_resolved} /></td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>

            {/* Pagination */}
            <div className="pagination">
                <span>{total ?? 0} total records</span>
                <div className="pagination-controls">
                    <button
                        className="btn btn-ghost btn-sm"
                        disabled={page <= 1}
                        onClick={() => onPageChange(page - 1)}
                    >‹ Prev</button>
                    <span style={{ padding: '0 8px', fontSize: '0.78rem', color: 'var(--text-secondary)' }}>
                        Page {page} of {totalPages}
                    </span>
                    <button
                        className="btn btn-ghost btn-sm"
                        disabled={page >= totalPages}
                        onClick={() => onPageChange(page + 1)}
                    >Next ›</button>
                </div>
            </div>
        </div>
    );
}
