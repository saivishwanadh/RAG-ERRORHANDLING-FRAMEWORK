/** StatCard.jsx — Summary number card for the Overview page */
export default function StatCard({ icon, label, value, sub, accentColor }) {
    return (
        <div className="stat-card">
            {icon && (
                <div className="stat-icon" style={{ background: accentColor ? `${accentColor}22` : 'var(--accent-glow)' }}>
                    <span style={{ fontSize: '1.1rem' }}>{icon}</span>
                </div>
            )}
            <div className="stat-label">{label}</div>
            <div className="stat-value" style={accentColor ? { color: accentColor } : {}}>
                {value ?? '—'}
            </div>
            {sub && <div className="stat-sub">{sub}</div>}
        </div>
    );
}
