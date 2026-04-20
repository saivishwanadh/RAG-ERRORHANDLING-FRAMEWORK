/**
 * pages/Overview.jsx
 * Dashboard home – stat cards + charts
 * TimeRangePicker lives in the page-header, right-aligned next to the title.
 */
import { useState, useCallback } from 'react';
import { usePolling } from '../hooks/usePolling';
import { getSummary, getByApplication } from '../api/client';
import StatCard from '../components/StatCard';
import AppBarChart from '../components/AppBarChart';
import DonutChart from '../components/DonutChart';
import TimeRangePicker from '../components/TimeRangePicker';

export default function Overview({ onLastUpdated }) {
    const [timeRange, setTimeRange] = useState(null); // { from_date, to_date } | null

    const from = timeRange?.from_date || null;
    const to = timeRange?.to_date || null;

    const summaryFetch = useCallback(async () => {
        const s = await getSummary({ from_date: from, to_date: to });
        onLastUpdated?.(new Date());
        return s;
    }, [onLastUpdated, from, to]);

    const appFetch = useCallback(() => getByApplication({ from_date: from, to_date: to }), [from, to]);

    const { data: summary, loading: sl } = usePolling(summaryFetch, 60_000);
    const { data: appData } = usePolling(appFetch, 60_000);

    const s = summary || {};

    const tech = Number(s.technical) || 0;
    const biz = Number(s.business) || 0;
    const platform = Number(s.platform) || 0;
    const unknown = Math.max(0, (Number(s.total) || 0) - tech - biz - platform);
    const donutData = [
        { key: 'platform', name: 'Platform Errors', value: platform },
        { key: 'technical', name: 'Technical Errors', value: tech },
        { key: 'business', name: 'Business Errors', value: biz },
        { key: 'unknown', name: 'Unknown Errors', value: unknown },
    ];

    return (
        <div className="page">
            {/* Page header with TimeRangePicker right-aligned */}
            <div className="page-header" style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between' }}>
                <div>
                    <h2>Overview</h2>
                    <p>Live summary of all application errors processed by the pipeline</p>
                </div>
                <TimeRangePicker
                    value={timeRange}
                    onChange={setTimeRange}
                    onClear={() => setTimeRange(null)}
                />
            </div>

            {/* Equal stat cards */}
            <div className="stat-grid">
                <StatCard label="Total Errors" value={sl ? '…' : s.total} sub="all time" />
                <StatCard label="Resolved" value={sl ? '…' : s.resolved} sub="with ops solution" accentColor="var(--success)" />
                <StatCard label="Pending" value={sl ? '…' : s.unresolved} sub="needs attention" accentColor="var(--warning)" />
            </div>

            {/* Charts */}
            <div className="chart-grid" style={{ gridTemplateColumns: '2fr 1fr' }}>
                <div className="chart-card">
                    <div className="chart-header">
                        <h3>Errors by Application</h3>
                    </div>
                    <AppBarChart data={appData?.data || []} />
                </div>

                <div className="chart-card">
                    <div className="chart-header">
                        <h3>Error Type Split</h3>
                    </div>
                    <DonutChart data={donutData} />
                </div>
            </div>

            {/* ── Application Error Breakdown Table ── */}
            <div className="chart-card" style={{ marginTop: 24 }}>
                <div className="chart-header">
                    <h3>Error Breakdown by Application</h3>
                </div>
                <div style={{ overflowX: 'auto' }}>
                    <table style={{
                        width: '100%',
                        borderCollapse: 'collapse',
                        fontSize: 13,
                        color: 'var(--text-primary)',
                    }}>
                        <thead>
                            <tr style={{ borderBottom: '1px solid var(--border)' }}>
                                {['Application', 'Total', 'Business', 'Platform', 'Technical', 'Unknown'].map(h => (
                                    <th key={h} style={{
                                        padding: '10px 14px',
                                        textAlign: h === 'Application' ? 'left' : 'center',
                                        color: 'var(--text-secondary)',
                                        fontWeight: 600,
                                        textTransform: 'uppercase',
                                        fontSize: 11,
                                        letterSpacing: '0.05em',
                                        whiteSpace: 'nowrap',
                                    }}>{h}</th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {(appData?.data || []).length === 0 ? (
                                <tr>
                                    <td colSpan={6} style={{ padding: 24, textAlign: 'center', color: 'var(--text-secondary)' }}>
                                        No data for selected range
                                    </td>
                                </tr>
                            ) : (appData?.data || []).map((row, i) => (
                                <tr key={row.application_name} style={{
                                    borderBottom: '1px solid var(--border)',
                                    backgroundColor: i % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.02)',
                                    transition: 'background 0.15s',
                                }}>
                                    <td style={{ padding: '10px 14px', fontWeight: 500 }}>{row.application_name}</td>
                                    <td style={{ padding: '10px 14px', textAlign: 'center', fontWeight: 700 }}>{row.total}</td>
                                    <td style={{ padding: '10px 14px', textAlign: 'center', color: '#a78bfa' }}>{row.business ?? 0}</td>
                                    <td style={{ padding: '10px 14px', textAlign: 'center', color: '#f97316' }}>{row.platform ?? 0}</td>
                                    <td style={{ padding: '10px 14px', textAlign: 'center', color: '#38bdf8' }}>{row.technical ?? 0}</td>
                                    <td style={{ padding: '10px 14px', textAlign: 'center', color: 'var(--text-secondary)' }}>{row.unknown ?? 0}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    );
}
