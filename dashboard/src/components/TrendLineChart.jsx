/**
 * TrendLineChart.jsx
 * Multi-line chart — Daily error trends (Total / Technical / Business)
 * Uses Recharts LineChart
 */
import {
    LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
    Legend, ResponsiveContainer,
} from 'recharts';
import { format, parseISO } from 'date-fns';

export default function TrendLineChart({ data = [] }) {
    if (!data.length) {
        return <div className="empty-state"><p>No trend data for this period</p></div>;
    }

    const formatted = data.map(d => ({
        ...d,
        label: format(parseISO(String(d.day)), 'MMM d'),
    }));

    return (
        <ResponsiveContainer width="100%" height={260}>
            <LineChart data={formatted} margin={{ left: 0, right: 16, top: 4, bottom: 4 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
                <XAxis dataKey="label" tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={false} tickLine={false} />
                <YAxis allowDecimals={false} tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={false} tickLine={false} />
                <Tooltip
                    contentStyle={{ background: '#1a2236', border: '1px solid rgba(255,255,255,0.07)', borderRadius: 8 }}
                    labelStyle={{ color: '#f1f5f9', fontWeight: 600 }}
                    itemStyle={{ color: '#94a3b8' }}
                />
                <Legend wrapperStyle={{ paddingTop: 8, fontSize: 12, color: '#94a3b8' }} />
                <Line type="monotone" dataKey="total" name="Total" stroke="#6366f1" strokeWidth={2} dot={false} activeDot={{ r: 5 }} />
                <Line type="monotone" dataKey="technical" name="Technical" stroke="#38bdf8" strokeWidth={2} dot={false} activeDot={{ r: 5 }} />
                <Line type="monotone" dataKey="business" name="Business" stroke="#a78bfa" strokeWidth={2} dot={false} activeDot={{ r: 5 }} />
            </LineChart>
        </ResponsiveContainer>
    );
}
