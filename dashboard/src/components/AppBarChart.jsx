/**
 * AppBarChart.jsx
 * Horizontal bar chart — Errors by Application
 * Uses Recharts BarChart
 */
import {
    BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
    Legend, ResponsiveContainer,
} from 'recharts';

export default function AppBarChart({ data = [] }) {
    if (!data.length) {
        return <div className="empty-state"><p>No application data yet</p></div>;
    }

    return (
        <ResponsiveContainer width="100%" height={260}>
            <BarChart data={data} layout="vertical" margin={{ left: 16, right: 16, top: 4, bottom: 4 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" horizontal={false} />
                <XAxis type="number" tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={false} tickLine={false} />
                <YAxis
                    dataKey="application_name"
                    type="category"
                    width={140}
                    tick={{ fill: '#94a3b8', fontSize: 11 }}
                    axisLine={false}
                    tickLine={false}
                />
                <Tooltip
                    contentStyle={{ background: '#1a2236', border: '1px solid rgba(255,255,255,0.07)', borderRadius: 8 }}
                    labelStyle={{ color: '#f1f5f9', fontWeight: 600 }}
                    itemStyle={{ color: '#94a3b8' }}
                    cursor={{ fill: 'rgba(255,255,255,0.04)' }}
                />
                <Legend wrapperStyle={{ paddingTop: 8, fontSize: 12, color: '#94a3b8' }} />
                <Bar dataKey="resolved" name="Resolved" fill="#10b981" radius={[0, 4, 4, 0]} stackId="a" />
                <Bar dataKey="unresolved" name="Unresolved" fill="#6366f1" radius={[0, 4, 4, 0]} stackId="a" />
            </BarChart>
        </ResponsiveContainer>
    );
}
