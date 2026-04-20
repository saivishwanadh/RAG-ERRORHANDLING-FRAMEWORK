/**
 * TimeRangePicker.jsx
 * Dropdown with "Show last:" quick presets (left) + dual-month calendar (right).
 * Outputs { from_date, to_date } as ISO strings via onChange().
 */
import { useState, useRef, useEffect } from 'react';
import {
    format, subMinutes, subHours, subDays,
    startOfDay, endOfDay,
    addMonths, subMonths,
    startOfMonth, endOfMonth, eachDayOfInterval,
    isSameMonth, isSameDay, isWithinInterval, isAfter, isBefore,
    getDay,
} from 'date-fns';

const DB_FMT = "yyyy-MM-dd HH:mm:ss"; // PostgreSQL TIMESTAMP WITHOUT TIME ZONE

// ── Quick preset definitions ────────────────────────────────────────────────
const PRESETS = [
    { label: '5 minutes', fn: () => ({ from: subMinutes(new Date(), 5), to: new Date() }) },
    { label: '15 minutes', fn: () => ({ from: subMinutes(new Date(), 15), to: new Date() }) },
    { label: '30 minutes', fn: () => ({ from: subMinutes(new Date(), 30), to: new Date() }) },
    { label: '1 hour', fn: () => ({ from: subHours(new Date(), 1), to: new Date() }) },
    { label: '6 hours', fn: () => ({ from: subHours(new Date(), 6), to: new Date() }) },
    { label: '12 hours', fn: () => ({ from: subHours(new Date(), 12), to: new Date() }) },
    { label: '24 hours', fn: () => ({ from: subHours(new Date(), 24), to: new Date() }) },
    { label: '3 days', fn: () => ({ from: subDays(new Date(), 3), to: new Date() }) },
    { label: '5 days', fn: () => ({ from: subDays(new Date(), 5), to: new Date() }) },
    { label: '7 days', fn: () => ({ from: subDays(new Date(), 7), to: new Date() }) },
    { label: '15 days', fn: () => ({ from: subDays(new Date(), 15), to: new Date() }) },
    { label: '30 days', fn: () => ({ from: subDays(new Date(), 30), to: new Date() }) },
];

const DAY_LABELS = ['Su', 'Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa'];

// ── Mini Calendar ───────────────────────────────────────────────────────────
function MiniCalendar({ month, selecting, tempStart, tempEnd, onDayClick, onDayHover }) {
    const firstDay = startOfMonth(month);
    const lastDay = endOfMonth(month);
    const days = eachDayOfInterval({ start: firstDay, end: lastDay });
    const offset = getDay(firstDay); // 0=Sun

    function dayStyle(d) {
        const isStart = tempStart && isSameDay(d, tempStart);
        const isEnd = tempEnd && isSameDay(d, tempEnd);
        const inRange = tempStart && tempEnd && isWithinInterval(d, {
            start: isBefore(tempStart, tempEnd) ? tempStart : tempEnd,
            end: isAfter(tempStart, tempEnd) ? tempStart : tempEnd,
        });
        const today = isSameDay(d, new Date());

        let bg = 'transparent', color = '#94a3b8', radius = '50%', fontWeight = 400;

        if (isStart || isEnd) {
            bg = '#6366f1'; color = '#fff'; fontWeight = 700;
        } else if (inRange) {
            bg = 'rgba(99,102,241,0.18)'; color = '#f1f5f9'; radius = 0;
        }
        if (today && !isStart && !isEnd) {
            color = '#818cf8'; fontWeight = 600;
        }

        return { background: bg, color, borderRadius: radius, fontWeight };
    }

    return (
        <div style={{ width: 220 }}>
            {/* Month label */}
            <div style={{ textAlign: 'center', fontWeight: 600, fontSize: '0.82rem', color: '#f1f5f9', marginBottom: 8 }}>
                {format(month, 'MMMM yyyy')}
            </div>
            {/* Day headers */}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(7, 1fr)', marginBottom: 4 }}>
                {DAY_LABELS.map(d => (
                    <div key={d} style={{ textAlign: 'center', fontSize: '0.68rem', color: '#475569', padding: '2px 0' }}>{d}</div>
                ))}
            </div>
            {/* Days grid */}
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(7, 1fr)', gap: 2 }}>
                {Array.from({ length: offset }).map((_, i) => <div key={`e${i}`} />)}
                {days.map(d => {
                    const inThisMonth = isSameMonth(d, month);
                    const ds = dayStyle(d);
                    return (
                        <div
                            key={d.toISOString()}
                            onClick={() => inThisMonth && onDayClick(d)}
                            onMouseEnter={() => selecting && inThisMonth && onDayHover(d)}
                            style={{
                                textAlign: 'center',
                                fontSize: '0.75rem',
                                padding: '5px 2px',
                                cursor: inThisMonth ? 'pointer' : 'default',
                                opacity: inThisMonth ? 1 : 0.25,
                                borderRadius: ds.borderRadius,
                                background: ds.background,
                                color: ds.color,
                                fontWeight: ds.fontWeight,
                                transition: 'background 0.12s',
                            }}
                        >
                            {format(d, 'd')}
                        </div>
                    );
                })}
            </div>
        </div>
    );
}

// ── Main component ──────────────────────────────────────────────────────────
export default function TimeRangePicker({ value, onChange, onClear }) {
    const [open, setOpen] = useState(false);
    const [activePreset, setPreset] = useState(null);   // preset label string or null

    // Calendar state
    const [leftMonth, setLeftMonth] = useState(subMonths(new Date(), 1));
    const [tempStart, setTempStart] = useState(null);
    const [tempEnd, setTempEnd] = useState(null);
    const [hovered, setHovered] = useState(null);
    const [selecting, setSelecting] = useState(false);  // true = awaiting second click

    const [fromTime, setFromTime] = useState('00:00');
    const [toTime, setToTime] = useState('23:59');

    const ref = useRef(null);
    const rightMonth = addMonths(leftMonth, 1);

    // Close on outside click
    useEffect(() => {
        function handler(e) { if (ref.current && !ref.current.contains(e.target)) setOpen(false); }
        document.addEventListener('mousedown', handler);
        return () => document.removeEventListener('mousedown', handler);
    }, []);

    // Display label
    const label = activePreset
        ? activePreset
        : (value?.from_date && value?.to_date)
            ? `${value.from_date.slice(0, 10)} → ${value.to_date.slice(0, 10)}`
            : 'Time range';

    function applyPreset(preset) {
        setPreset(preset.label);
        const { from, to } = preset.fn();
        onChange({
            from_date: format(from, DB_FMT),
            to_date: format(to, DB_FMT),
        });
        setOpen(false);
    }

    function handleDayClick(d) {
        if (!selecting) {
            setTempStart(startOfDay(d));
            setTempEnd(null);
            setSelecting(true);
        } else {
            const end = endOfDay(d);
            const finalStart = isBefore(d, tempStart) ? startOfDay(d) : tempStart;
            const finalEnd = isBefore(d, tempStart) ? endOfDay(tempStart) : end;
            setTempEnd(finalEnd);
            setTempStart(finalStart);
            setSelecting(false);
        }
    }

    function handleConfirm() {
        if (!tempStart) return;
        const end = tempEnd || endOfDay(tempStart);
        // Merge time inputs
        const [fh, fm] = fromTime.split(':').map(Number);
        const [th, tm] = toTime.split(':').map(Number);
        const from = new Date(tempStart); from.setHours(fh, fm, 0, 0);
        const to = new Date(end); to.setHours(th, tm, 59, 999);
        setPreset(null);
        onChange({
            from_date: format(from, DB_FMT),
            to_date: format(to, DB_FMT),
        });
        setOpen(false);
    }

    function handleClear() {
        setPreset(null);
        setTempStart(null); setTempEnd(null);
        onClear?.();
        setOpen(false);
    }

    const effectiveTempEnd = selecting ? hovered : tempEnd;

    return (
        <div ref={ref} style={{ position: 'relative' }}>
            {/* Trigger */}
            <button
                onClick={() => setOpen(o => !o)}
                style={{
                    display: 'flex', alignItems: 'center', gap: 8,
                    background: open ? 'var(--accent-glow)' : 'var(--bg-input)',
                    border: `1px solid ${open ? 'var(--accent)' : 'var(--border)'}`,
                    borderRadius: 'var(--radius-sm)',
                    color: activePreset || (value?.from_date) ? 'var(--accent-light)' : 'var(--text-muted)',
                    padding: '7px 12px',
                    fontSize: '0.82rem',
                    fontWeight: 500,
                    cursor: 'pointer',
                    transition: 'all 0.15s',
                    whiteSpace: 'nowrap',
                }}
            >
                <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <rect x="3" y="4" width="18" height="18" rx="2" /><line x1="16" y1="2" x2="16" y2="6" />
                    <line x1="8" y1="2" x2="8" y2="6" /><line x1="3" y1="10" x2="21" y2="10" />
                </svg>
                {label}
                <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
                    <path d="M6 9l6 6 6-6" />
                </svg>
            </button>

            {/* Dropdown */}
            {open && (
                <div style={{
                    position: 'absolute',
                    top: 'calc(100% + 6px)',
                    right: 0,
                    zIndex: 300,
                    background: '#111827',
                    border: '1px solid var(--border-strong)',
                    borderRadius: 'var(--radius-lg)',
                    boxShadow: '0 20px 60px rgba(0,0,0,0.7)',
                    display: 'flex',
                    minWidth: 580,
                    overflow: 'hidden',
                }}>
                    {/* Left: Presets */}
                    <div style={{
                        padding: '20px 16px',
                        borderRight: '1px solid var(--border)',
                        minWidth: 160,
                        background: '#0f1623',
                    }}>
                        <div style={{ fontSize: '0.72rem', fontWeight: 700, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.6px', marginBottom: 12 }}>
                            Show last:
                        </div>
                        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '6px 16px' }}>
                            {PRESETS.map(p => (
                                <label key={p.label} style={{ display: 'flex', alignItems: 'center', gap: 7, cursor: 'pointer', fontSize: '0.78rem', color: activePreset === p.label ? 'var(--accent-light)' : 'var(--text-secondary)', whiteSpace: 'nowrap' }}>
                                    <input
                                        type="radio"
                                        name="preset"
                                        style={{ accentColor: '#6366f1' }}
                                        checked={activePreset === p.label}
                                        onChange={() => applyPreset(p)}
                                    />
                                    {p.label}
                                </label>
                            ))}
                        </div>
                    </div>

                    {/* Right: Calendar */}
                    <div style={{ padding: '20px', flex: 1 }}>
                        {/* Month navigation */}
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
                            <button onClick={() => setLeftMonth(m => subMonths(m, 1))} style={{ background: 'none', border: 'none', color: 'var(--text-secondary)', cursor: 'pointer', fontSize: '1rem', padding: '2px 8px' }}>‹</button>
                            <div style={{ display: 'flex', gap: 32 }}>
                                <MiniCalendar month={leftMonth} selecting={selecting} tempStart={tempStart} tempEnd={effectiveTempEnd} onDayClick={handleDayClick} onDayHover={setHovered} />
                                <MiniCalendar month={rightMonth} selecting={selecting} tempStart={tempStart} tempEnd={effectiveTempEnd} onDayClick={handleDayClick} onDayHover={setHovered} />
                            </div>
                            <button onClick={() => setLeftMonth(m => addMonths(m, 1))} style={{ background: 'none', border: 'none', color: 'var(--text-secondary)', cursor: 'pointer', fontSize: '1rem', padding: '2px 8px' }}>›</button>
                        </div>

                        {/* Time inputs */}
                        <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 16, padding: '10px 0', borderTop: '1px solid var(--border)' }}>
                            <input type="time" value={fromTime} onChange={e => setFromTime(e.target.value)}
                                style={{ background: 'var(--bg-input)', border: '1px solid var(--border)', borderRadius: 6, color: '#f1f5f9', padding: '6px 10px', fontSize: '0.82rem', flex: 1, colorScheme: 'dark' }} />
                            <span style={{ color: 'var(--text-muted)', fontSize: '0.8rem' }}>to</span>
                            <input type="time" value={toTime} onChange={e => setToTime(e.target.value)}
                                style={{ background: 'var(--bg-input)', border: '1px solid var(--border)', borderRadius: 6, color: '#f1f5f9', padding: '6px 10px', fontSize: '0.82rem', flex: 1, colorScheme: 'dark' }} />
                        </div>

                        {/* Actions */}
                        <div style={{ display: 'flex', gap: 10 }}>
                            <button onClick={handleClear} style={{ flex: 1, padding: '8px', border: '1px solid var(--border)', borderRadius: 6, background: 'transparent', color: 'var(--text-secondary)', cursor: 'pointer', fontSize: '0.82rem' }}>
                                Cancel
                            </button>
                            <button onClick={handleConfirm} disabled={!tempStart} style={{ flex: 1, padding: '8px', border: 'none', borderRadius: 6, background: '#6366f1', color: '#fff', cursor: tempStart ? 'pointer' : 'not-allowed', fontSize: '0.82rem', fontWeight: 600, opacity: tempStart ? 1 : 0.5 }}>
                                Confirm
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
