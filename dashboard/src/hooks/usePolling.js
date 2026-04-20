/**
 * hooks/usePolling.js
 * Generic auto-refresh hook used by all dashboard pages.
 *
 * Usage:
 *   const { data, loading, error, refresh } = usePolling(fetchFn, 30_000);
 *
 * - Calls fetchFn() immediately on mount
 * - Re-calls fetchFn() every `intervalMs` milliseconds
 * - Clears the timer on unmount (no memory leaks)
 * - `refresh()` triggers an immediate manual refetch and resets the timer
 */

import { useState, useEffect, useCallback, useRef } from 'react';

export function usePolling(fetchFn, intervalMs = 30_000) {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [lastUpdated, setLastUpdated] = useState(null);
    const timerRef = useRef(null);

    const fetch_ = useCallback(async () => {
        try {
            const result = await fetchFn();
            setData(result);
            setError(null);
            setLastUpdated(new Date());
        } catch (err) {
            setError(err.message || 'Failed to fetch data');
        } finally {
            setLoading(false);
        }
    }, [fetchFn]);

    const refresh = useCallback(() => {
        // Clear existing timer and re-schedule from now
        if (timerRef.current) clearInterval(timerRef.current);
        setLoading(true);
        fetch_();
        timerRef.current = setInterval(fetch_, intervalMs);
    }, [fetch_, intervalMs]);

    useEffect(() => {
        fetch_();
        timerRef.current = setInterval(fetch_, intervalMs);
        return () => clearInterval(timerRef.current);
    }, [fetch_, intervalMs]);

    return { data, loading, error, refresh, lastUpdated };
}
