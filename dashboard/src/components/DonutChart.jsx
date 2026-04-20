/**
 * DonutChart.jsx
 * Redesigned donut chart — matches reference:
 *   Left:  Donut with total count in centre
 *   Right: Category list with count badge, description, progress bar
 */
import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer } from 'recharts';

// Category definitions — add / reorder here as needed
const CATEGORIES = [
    {
        key: 'platform',
        label: 'Platform Errors',
        desc: 'Infrastructure related issues',
        color: '#f97316',
    },
    {
        key: 'technical',
        label: 'Technical Errors',
        desc: 'Technical related issues',
        color: '#38bdf8',
    },
    {
        key: 'business',
        label: 'Business Errors',
        desc: 'Business related issues',
        color: '#a78bfa',
    },
    {
        key: 'unknown',
        label: 'Unknown Errors',
        desc: 'No error type assigned',
        color: '#64748b',
    },
];

export default function DonutChart({ data = [] }) {
    // data shape: [{ name, value, key }]
    // Build a lookup by key so we can keep ordering from CATEGORIES
    const byKey = Object.fromEntries(data.map(d => [d.key ?? d.name?.toLowerCase(), d.value ?? 0]));
    const total = data.reduce((s, d) => s + (d.value ?? 0), 0);

    if (total === 0) {
        return <div className="empty-state"><p>No category data yet</p></div>;
    }

    const pieData = CATEGORIES
        .map(c => ({ name: c.label, value: byKey[c.key] || 0, color: c.color }))
        .filter(d => d.value > 0);

    return (
        <div style={{ display: 'flex', alignItems: 'center', gap: 24, padding: '4px 0' }}>

            {/* ── Donut ── */}
            <div style={{ flexShrink: 0, position: 'relative', width: 140, height: 140 }}>
                <ResponsiveContainer width={140} height={140}>
                    <PieChart>
                        <Pie
                            data={pieData}
                            cx="50%"
                            cy="50%"
                            innerRadius={46}
                            outerRadius={68}
                            paddingAngle={3}
                            dataKey="value"
                            startAngle={90}
                            endAngle={-270}
                            strokeWidth={0}
                        >
                            {pieData.map((d, i) => (
                                <Cell key={i} fill={d.color} />
                            ))}
                        </Pie>
                        <Tooltip
                            contentStyle={{ background: '#1a2236', border: '1px solid rgba(255,255,255,0.07)', borderRadius: 8 }}
                            labelStyle={{ color: '#f1f5f9', fontWeight: 600 }}
                            itemStyle={{ color: '#94a3b8' }}
                        />
                    </PieChart>
                </ResponsiveContainer>
                {/* Centre label */}
                <div style={{
                    position: 'absolute',
                    inset: 0,
                    display: 'flex',
                    flexDirection: 'column',
                    alignItems: 'center',
                    justifyContent: 'center',
                    pointerEvents: 'none',
                }}>
                    <span style={{ fontSize: '1.6rem', fontWeight: 700, color: '#f1f5f9', lineHeight: 1 }}>
                        {total}
                    </span>
                    <span style={{ fontSize: '0.7rem', color: '#94a3b8', marginTop: 2 }}>total</span>
                </div>
            </div>

            {/* ── Legend list ── */}
            <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 12 }}>
                {CATEGORIES.map(cat => {
                    const count = byKey[cat.key] || 0;
                    const pct = total > 0 ? (count / total) * 100 : 0;
                    return (
                        <div key={cat.key}>
                            {/* Row 1: dot + label + count */}
                            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 2 }}>
                                <span style={{
                                    width: 10, height: 10, borderRadius: '50%',
                                    background: cat.color, flexShrink: 0,
                                }} />
                                <span style={{ fontSize: '0.82rem', fontWeight: 600, color: '#f1f5f9' }}>
                                    {cat.label}
                                </span>
                                <span style={{
                                    marginLeft: 'auto',
                                    fontSize: '0.82rem', fontWeight: 700,
                                    color: cat.color,
                                }}>
                                    {count}
                                </span>
                            </div>
                            {/* Row 2: description */}
                            <div style={{ fontSize: '0.7rem', color: '#64748b', paddingLeft: 18, marginBottom: 4 }}>
                                {cat.desc}
                            </div>
                            {/* Row 3: progress bar */}
                            <div style={{
                                height: 3, borderRadius: 999,
                                background: 'rgba(255,255,255,0.06)',
                                overflow: 'hidden',
                            }}>
                                <div style={{
                                    width: `${pct}%`,
                                    height: '100%',
                                    background: cat.color,
                                    borderRadius: 999,
                                    transition: 'width 0.5s ease',
                                }} />
                            </div>
                        </div>
                    );
                })}
            </div>

        </div>
    );
}
