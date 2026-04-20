/**
 * pages/Trends.jsx
 * Error Trend page — line chart of error count vs time.
 */
import { useState, useCallback, useEffect, useRef } from 'react';
import {
    LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
    ResponsiveContainer, ReferenceLine,
} from 'recharts';
import { usePolling } from '../hooks/usePolling';
import { getTrends, getApplications } from '../api/client';
import TimeRangePicker from '../components/TimeRangePicker';

// ── Color palette ─────────────────────────────────────────────────────────────
const LINES = [
    { key: 'total', label: 'Total', color: '#e2e8f0' },
    { key: 'technical', label: 'Technical', color: '#38bdf8' },
    { key: 'business', label: 'Business', color: '#a78bfa' },
    { key: 'platform', label: 'Platform', color: '#f97316' },
    { key: 'unknown', label: 'Unknown', color: '#64748b' },
];

// ── Custom tooltip ────────────────────────────────────────────────────────────
function CustomTooltip({ active, payload, label }) {
    if (!active || !payload?.length) return null;
    return (
        <div style={{
            background: '#1e293b',
            border: '1px solid #334155',
            borderRadius: 8,
            padding: '10px 14px',
            fontSize: 13,
        }}>
            <p style={{ color: '#94a3b8', marginBottom: 6, fontWeight: 600 }}>{label}</p>
            {payload.map(p => (
                <div key={p.dataKey} style={{ display: 'flex', gap: 8, alignItems: 'center', marginBottom: 2 }}>
                    <span style={{ width: 10, height: 10, borderRadius: '50%', background: p.color, flexShrink: 0, display: 'inline-block' }} />
                    <span style={{ color: '#94a3b8' }}>{p.name}:</span>
                    <span style={{ color: p.color, fontWeight: 700 }}>{p.value}</span>
                </div>
            ))}
        </div>
    );
}

// ── Custom App Dropdown ───────────────────────────────────────────────────────
function AppDropdown({ value, options, onChange }) {
    const [open, setOpen] = useState(false);
    const ref = useRef(null);

    useEffect(() => {
        function handler(e) {
            if (ref.current && !ref.current.contains(e.target)) setOpen(false);
        }
        document.addEventListener('mousedown', handler);
        return () => document.removeEventListener('mousedown', handler);
    }, []);

    const allOptions = ['All Applications', ...options];
    const displayLabel = value || 'All Applications';

    return (
        <div ref={ref} style={{ position: 'relative', minWidth: 220 }}>
            <button
                onClick={() => setOpen(o => !o)}
                style={{
                    width: '100%',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    gap: 8,
                    background: 'var(--surface, #1e293b)',
                    border: `1px solid ${open ? '#6366f1' : 'var(--border, #334155)'}`,
                    borderRadius: 8,
                    color: 'var(--text-primary, #f1f5f9)',
                    padding: '8px 14px',
                    fontSize: 13,
                    cursor: 'pointer',
                    transition: 'border-color 0.15s',
                    textAlign: 'left',
                }}
            >
                <span>{displayLabel}</span>
                <span style={{ color: '#64748b', fontSize: 11, flexShrink: 0 }}>▼</span>
            </button>

            {open && (
                <div style={{
                    position: 'absolute',
                    top: 'calc(100% + 4px)',
                    left: 0,
                    right: 0,
                    background: '#1e293b',
                    border: '1px solid #334155',
                    borderRadius: 8,
                    boxShadow: '0 8px 32px rgba(0,0,0,0.5)',
                    zIndex: 200,
                    maxHeight: 280,
                    overflowY: 'auto',
                }}>
                    {allOptions.map(app => {
                        const val = app === 'All Applications' ? '' : app;
                        const isSelected = val === value;
                        return (
                            <div
                                key={app}
                                onClick={() => { onChange(val); setOpen(false); }}
                                style={{
                                    padding: '9px 14px',
                                    fontSize: 13,
                                    cursor: 'pointer',
                                    color: isSelected ? '#6366f1' : '#cbd5e1',
                                    background: isSelected ? 'rgba(99,102,241,0.12)' : 'transparent',
                                    fontWeight: isSelected ? 600 : 400,
                                    transition: 'background 0.1s',
                                }}
                                onMouseEnter={e => { if (!isSelected) e.currentTarget.style.background = 'rgba(255,255,255,0.05)'; }}
                                onMouseLeave={e => { if (!isSelected) e.currentTarget.style.background = 'transparent'; }}
                            >
                                {app}
                            </div>
                        );
                    })}
                </div>
            )}
        </div>
    );
}

// ── Main page ─────────────────────────────────────────────────────────────────
export default function Trends({ onLastUpdated }) {
    const [timeRange, setTimeRange] = useState(null);
    const [selectedApp, setSelectedApp] = useState('');
    const [visibleLines, setVisibleLines] = useState(
        () => Object.fromEntries(LINES.map(l => [l.key, true]))
    );

    const from = timeRange?.from_date || null;
    const to = timeRange?.to_date || null;

    // Fetch application list once
    const [applications, setApplications] = useState([]);
    useEffect(() => {
        getApplications()
            .then(r => setApplications(r.applications || []))
            .catch(() => { });
    }, []);

    // Fetch trend data — re-fetches whenever filters change
    const trendFetch = useCallback(async () => {
        const data = await getTrends({
            ...(from ? { from_date: from } : {}),
            ...(to ? { to_date: to } : {}),
            ...(selectedApp ? { application_name: selectedApp } : {}),
        });
        onLastUpdated?.(new Date());
        return data;
    }, [from, to, selectedApp, onLastUpdated]);

    const { data: trendData, loading } = usePolling(trendFetch, 60_000);

    const rawData = trendData?.data || [];
    // Normalise: old API returns 'day', new returns 'bucket'
    const chartData = rawData.map(row => ({ ...row, bucket: row.bucket ?? row.day ?? '' }));
    const granularity = trendData?.granularity || 'daily';

    // Format X-axis label based on granularity
    const formatBucket = (val) => {
        if (!val) return '';
        try {
            const d = new Date(val);
            if (granularity === 'hourly') {
                return d.toLocaleTimeString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
            }
            return d.toLocaleDateString([], { month: 'short', day: 'numeric' });
        } catch { return val; }
    };

    const peak = chartData.reduce(
        (max, row) => (row.total > (max?.total ?? 0) ? row : max),
        null
    );

    const toggleLine = (key) =>
        setVisibleLines(prev => ({ ...prev, [key]: !prev[key] }));

    return (
        <div className="page">
            {/* Header */}
            <div className="page-header" style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between' }}>
                <div>
                    <h2>Error Trends</h2>
                    <p>Error count over time — filter by application and date range</p>
                </div>
            </div>

            {/* Controls row */}
            <div style={{ display: 'flex', gap: 12, marginBottom: 20, alignItems: 'center', flexWrap: 'wrap' }}>
                {/* Custom app dropdown */}
                <AppDropdown
                    value={selectedApp}
                    options={applications}
                    onChange={setSelectedApp}
                />

                {/* Time range picker next to dropdown */}
                <TimeRangePicker
                    value={timeRange}
                    onChange={setTimeRange}
                    onClear={() => setTimeRange(null)}
                />

                {/* Line toggle buttons */}
                <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginLeft: 'auto' }}>
                    {LINES.map(l => (
                        <button
                            key={l.key}
                            onClick={() => toggleLine(l.key)}
                            style={{
                                display: 'flex', alignItems: 'center', gap: 6,
                                padding: '5px 12px',
                                borderRadius: 20,
                                border: `1px solid ${visibleLines[l.key] ? l.color : '#334155'}`,
                                background: visibleLines[l.key] ? `${l.color}22` : 'transparent',
                                color: visibleLines[l.key] ? l.color : '#64748b',
                                fontSize: 12,
                                fontWeight: 500,
                                cursor: 'pointer',
                                transition: 'all 0.2s',
                            }}
                        >
                            <span style={{
                                width: 8, height: 8, borderRadius: '50%',
                                background: visibleLines[l.key] ? l.color : '#334155',
                                display: 'inline-block',
                            }} />
                            {l.label}
                        </button>
                    ))}
                </div>
            </div>

            {/* Chart card */}
            <div className="chart-card">
                <div className="chart-header">
                    <h3>
                        Error Count vs Time
                        {selectedApp && (
                            <span style={{ color: '#64748b', fontWeight: 400, fontSize: 13 }}>
                                {' '}— {selectedApp}
                            </span>
                        )}
                    </h3>
                    {peak && chartData.length > 0 && (
                        <span style={{ fontSize: 12, color: '#f97316', fontWeight: 600 }}>
                            Peak: {peak.total} errors on {formatBucket(peak.bucket)}
                        </span>
                    )}
                </div>

                {loading && (
                    <div style={{ textAlign: 'center', padding: 80, color: '#64748b' }}>
                        Loading chart data…
                    </div>
                )}

                {!loading && chartData.length === 0 && (
                    <div style={{ textAlign: 'center', padding: 80 }}>
                        <div style={{ fontSize: 32, marginBottom: 12 }}>📭</div>
                        <div style={{ color: '#94a3b8', marginBottom: 4 }}>No error data found for the selected filters.</div>
                        <div style={{ color: '#64748b', fontSize: 12 }}>Try adjusting the date range or selecting a different application.</div>
                    </div>
                )}

                {!loading && chartData.length > 0 && (
                    <ResponsiveContainer width="100%" height={380}>
                        <LineChart data={chartData} margin={{ top: 16, right: 24, left: 0, bottom: 0 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                            <XAxis
                                dataKey="bucket"
                                tickFormatter={formatBucket}
                                tick={{ fill: '#64748b', fontSize: 11 }}
                                stroke="#334155"
                                interval="preserveStartEnd"
                            />
                            <YAxis
                                allowDecimals={false}
                                tick={{ fill: '#64748b', fontSize: 11 }}
                                stroke="#334155"
                            />
                            <Tooltip content={<CustomTooltip />} />

                            {peak && (
                                <ReferenceLine
                                    x={peak.bucket}
                                    stroke="#f97316"
                                    strokeDasharray="4 4"
                                    label={{ value: 'Peak', fill: '#f97316', fontSize: 11, position: 'top' }}
                                />
                            )}

                            {LINES.map(l => visibleLines[l.key] && (
                                <Line
                                    key={l.key}
                                    type="monotone"
                                    dataKey={l.key}
                                    name={l.label}
                                    stroke={l.color}
                                    strokeWidth={l.key === 'total' ? 2.5 : 1.5}
                                    dot={chartData.length <= 20}
                                    activeDot={{ r: 5 }}
                                    connectNulls
                                />
                            ))}
                        </LineChart>
                    </ResponsiveContainer>
                )}
            </div>
        </div>
    );
}
