/**
 * DetailPanel.jsx
 * Slide-in panel: error metadata, LLM solution cards, custom textarea, submit button.
 * Props:
 *   record       — the selected error record object (or null to hide)
 *   onClose      fn() — close the panel
 *   onSubmitted  fn() — called after successful submission so parent can refresh
 */
import { useState, useEffect } from 'react';
import { submitSolution } from '../api/client';
import { format, parseISO } from 'date-fns';

function parseLlmSolutions(llmJson) {
    if (!llmJson) return [];
    try {
        const data = typeof llmJson === 'string' ? JSON.parse(llmJson) : llmJson;
        return [1, 2, 3]
            .map(i => ({ id: String(i), ...data[`solution${i}`] }))
            .filter(s => s.instructions);
    } catch { return []; }
}

/**
 * Splits a flat instructions string into numbered steps.
 * Handles patterns like "1. Step one. 2. Step two" or "1) Step one. 2) Step two".
 * Returns an array of step strings, or a single-item array if no numbers found.
 */
function parseSteps(text) {
    if (!text) return [];
    // Split on patterns like " 1. " " 2. " " 1) " — allow leading space or start-of-string
    const parts = text.split(/(?<=[.?!])\s+(?=\d+[.)\s])/);
    if (parts.length > 1) return parts.map(p => p.trim()).filter(Boolean);
    // Fallback: split on "N. " at the start or after whitespace
    const parts2 = text.split(/(?:\s|^)(?=\d+\.\s)/).map(p => p.trim()).filter(Boolean);
    return parts2.length > 1 ? parts2 : [text.trim()];
}

function FormattedSteps({ text, color }) {
    const steps = parseSteps(text);
    if (steps.length === 1) {
        return <p style={{ fontSize: '0.8rem', color: 'var(--text-secondary)', lineHeight: 1.6 }}>{text}</p>;
    }
    return (
        <ol style={{ paddingLeft: 0, margin: 0, listStyle: 'none', display: 'flex', flexDirection: 'column', gap: 8 }}>
            {steps.map((step, i) => (
                <li key={i} style={{ display: 'flex', gap: 10, alignItems: 'flex-start' }}>
                    <span style={{
                        flexShrink: 0,
                        minWidth: 22, height: 22,
                        borderRadius: '50%',
                        background: color ? `${color}22` : 'var(--accent-glow)',
                        color: color || 'var(--accent-light)',
                        fontSize: '0.68rem',
                        fontWeight: 700,
                        display: 'flex', alignItems: 'center', justifyContent: 'center',
                    }}>
                        {i + 1}
                    </span>
                    <span style={{ fontSize: '0.8rem', color: 'var(--text-secondary)', lineHeight: 1.6 }}>
                        {/* Strip leading number like "1. " or "1) " from the step text */}
                        {step.replace(/^\d+[.)\s]+\s*/, '')}
                    </span>
                </li>
            ))}
        </ol>
    );
}

function formatTs(ts) {
    if (!ts) return '—';
    try { return format(parseISO(String(ts)), 'MMM d, yyyy HH:mm'); }
    catch { return String(ts).slice(0, 16); }
}

export default function DetailPanel({ record, onClose, onSubmitted }) {
    const [selectedId, setSelectedId] = useState(null);
    const [customText, setCustomText] = useState('');
    const [submitting, setSubmitting] = useState(false);
    const [submitError, setSubmitError] = useState(null);
    const [submitOk, setSubmitOk] = useState(false);

    // Reset all state when the user switches to a different record
    useEffect(() => {
        setSelectedId(null);
        setCustomText('');
        setSubmitError(null);
        setSubmitOk(false);
        setSubmitting(false);
    }, [record?.id]);

    if (!record) return null;

    const solutions = parseLlmSolutions(record.llm_solution);
    const alreadyResolved = record.is_resolved;

    async function handleSubmit() {
        if (!selectedId && !customText.trim()) {
            setSubmitError('Please select a solution or write a custom one.');
            return;
        }
        setSubmitting(true);
        setSubmitError(null);
        try {
            await submitSolution({
                errorId: record.id,
                customSolution: customText.trim() || null,
                solutionId: customText.trim() ? null : selectedId,
                solutionTimestamp: new Date().toISOString(),
            });
            setSubmitOk(true);
            onSubmitted?.();
        } catch (err) {
            setSubmitError(err.message);
        } finally {
            setSubmitting(false);
        }
    }

    return (
        <>
            {/* Backdrop */}
            <div className="panel-overlay" onClick={onClose} />

            {/* Panel */}
            <div className="panel">
                {/* Header */}
                <div className="panel-header">
                    <div>
                        <h3>Error Detail</h3>
                        <div style={{ marginTop: 4 }}>
                            {record.error_type && (
                                <span className={`badge ${record.error_type === 'technical' ? 'badge-technical' : 'badge-business'}`}>
                                    {record.error_type}
                                </span>
                            )}
                            {' '}
                            {alreadyResolved
                                ? <span className="badge badge-success">✓ Resolved</span>
                                : <span className="badge badge-warning">⏳ Pending</span>}
                        </div>
                    </div>
                    <button className="panel-close" onClick={onClose}>✕</button>
                </div>

                {/* Body */}
                <div className="panel-body">
                    {/* Meta */}
                    <div className="meta-grid">
                        <div className="meta-item"><label>Application</label><span>{record.application_name || '—'}</span></div>
                        <div className="meta-item"><label>Error Code</label><span className="font-mono">{record.error_code || '—'}</span></div>
                        <div className="meta-item"><label>Timestamp</label><span>{formatTs(record.error_timestamp)}</span></div>
                        <div className="meta-item"><label>Occurrences</label><span>{record.occurrence_count ?? 1}</span></div>
                    </div>

                    {/* Description */}
                    <div className="section-label">Description</div>
                    <div style={{
                        background: 'var(--bg-input)',
                        border: '1px solid var(--border)',
                        borderRadius: 'var(--radius-sm)',
                        padding: '10px 14px',
                        fontSize: '0.8rem',
                        color: 'var(--text-secondary)',
                        lineHeight: 1.6,
                        marginBottom: 20,
                        maxHeight: 110,
                        overflowY: 'auto',
                    }}>
                        {record.error_description || '—'}
                    </div>

                    {/* Already resolved */}
                    {alreadyResolved && (
                        <>
                            <div className="resolved-badge-large">
                                ✓ This error has an approved solution
                            </div>
                            <div className="section-label">Approved Solution</div>
                            <div style={{
                                background: 'var(--success-bg)',
                                border: '1px solid var(--success)',
                                borderRadius: 'var(--radius-sm)',
                                padding: '14px 16px',
                                marginBottom: 20,
                            }}>
                                <FormattedSteps text={record.ops_solution} color="#10b981" />
                            </div>
                        </>
                    )}

                    {/* LLM solutions */}
                    {!alreadyResolved && solutions.length > 0 && (
                        <>
                            <div className="section-label">AI-Suggested Solutions</div>
                            <div className="solution-cards">
                                {solutions.map(s => (
                                    <div
                                        key={s.id}
                                        className={`solution-card ${selectedId === s.id ? 'selected' : ''}`}
                                        onClick={() => { setSelectedId(s.id); setCustomText(''); }}
                                    >
                                        <div className="solution-card-label">Solution {s.id}</div>
                                        <FormattedSteps text={s.instructions} />
                                    </div>
                                ))}
                            </div>
                        </>
                    )}

                    {/* Custom solution */}
                    {!alreadyResolved && (
                        <>
                            <div className="section-label" style={{ marginTop: 16 }}>Custom Solution</div>
                            <textarea
                                className="custom-textarea"
                                placeholder="Write your own solution here (overrides AI selection above)…"
                                value={customText}
                                onChange={e => { setCustomText(e.target.value); setSelectedId(null); }}
                            />
                        </>
                    )}

                    {/* Feedback */}
                    {submitError && <div className="error-banner mt-12">{submitError}</div>}
                    {submitOk && <div className="success-banner mt-12">✓ Solution submitted successfully!</div>}
                </div>

                {/* Footer */}
                {!alreadyResolved && !submitOk && (
                    <div className="panel-footer">
                        <button
                            className="btn btn-primary w-full"
                            onClick={handleSubmit}
                            disabled={submitting}
                        >
                            {submitting ? 'Submitting…' : '✓ Submit Solution'}
                        </button>
                    </div>
                )}
            </div>
        </>
    );
}
