/**
 * App.jsx
 * Root layout: sidebar navigation + topbar + page routing
 */
import { useState, useCallback } from 'react';
import { BrowserRouter, Routes, Route, NavLink, Navigate } from 'react-router-dom';
import { format } from 'date-fns';
import Overview from './pages/Overview';
import Records from './pages/Records';
import KnowledgeBase from './pages/KnowledgeBase';
import Trends from './pages/Trends';

// ── Icons (inline SVG to avoid extra dependencies) ──────────────────────────
const IconOverview = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <rect x="3" y="3" width="7" height="7" /><rect x="14" y="3" width="7" height="7" />
    <rect x="14" y="14" width="7" height="7" /><rect x="3" y="14" width="7" height="7" />
  </svg>
);
const IconRecords = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <path d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2" />
    <rect x="9" y="3" width="6" height="4" rx="1" />
    <line x1="9" y1="12" x2="15" y2="12" /><line x1="9" y1="16" x2="13" y2="16" />
  </svg>
);
const IconKB = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <path d="M12 2L2 7l10 5 10-5-10-5z" />
    <path d="M2 17l10 5 10-5" /><path d="M2 12l10 5 10-5" />
  </svg>
);
const IconTrend = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <polyline points="22 12 18 12 15 21 9 3 6 12 2 12" />
  </svg>
);

function Sidebar() {
  return (
    <aside className="sidebar">
      <div className="sidebar-logo" style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', textAlign: 'center' }}>
        <img
          src="https://www.prowesssoft.com/wp-content/uploads/2023/06/Prowess_soft_logo1.png"
          alt="Prowess Soft"
          style={{ width: '100%', maxWidth: 80, objectFit: 'contain', marginBottom: 8 }}
        />
        <h1>OpsResolver</h1>
        <span>Error Intelligence Dashboard</span>
      </div>

      <nav className="sidebar-nav">
        <NavLink to="/overview" className={({ isActive }) => `nav-item ${isActive ? 'active' : ''}`}>
          <IconOverview /> Overview
        </NavLink>
        <NavLink to="/records" className={({ isActive }) => `nav-item ${isActive ? 'active' : ''}`}>
          <IconRecords /> Error Records
        </NavLink>
        <NavLink to="/trends" className={({ isActive }) => `nav-item ${isActive ? 'active' : ''}`}>
          <IconTrend /> Error Trends
        </NavLink>
        <NavLink to="/knowledge-base" className={({ isActive }) => `nav-item ${isActive ? 'active' : ''}`}>
          <IconKB /> Knowledge Base
        </NavLink>
      </nav>

      <div className="sidebar-footer">
        RAG Error Pipeline v2.0
      </div>
    </aside>
  );
}

// ── Topbar ───────────────────────────────────────────────────────────────────
function Topbar({ pageTitle, lastUpdated, rightContent }) {
  return (
    <header className="topbar">
      <span className="topbar-title">{pageTitle}</span>
      <div className="topbar-meta">
        {rightContent}
        {lastUpdated && (
          <div className="refresh-badge">
            <span className="refresh-dot" />
            Updated {format(lastUpdated, 'HH:mm:ss')}
          </div>
        )}
      </div>
    </header>
  );
}

// ── App ───────────────────────────────────────────────────────────────────────
export default function App() {
  const [lastUpdated, setLastUpdated] = useState(null);

  const handleLastUpdated = useCallback((date) => setLastUpdated(date), []);

  return (
    <BrowserRouter>
      <div className="app-layout">
        <Sidebar />
        <div className="main-content">
          <Routes>
            <Route
              path="/overview"
              element={
                <>
                  <Topbar lastUpdated={lastUpdated} />
                  <Overview onLastUpdated={handleLastUpdated} />
                </>
              }
            />
            <Route
              path="/records"
              element={
                <>
                  <Topbar lastUpdated={lastUpdated} />
                  <Records onLastUpdated={handleLastUpdated} />
                </>
              }
            />
            <Route
              path="/knowledge-base"
              element={
                <>
                  <Topbar lastUpdated={lastUpdated} />
                  <KnowledgeBase onLastUpdated={handleLastUpdated} />
                </>
              }
            />
            <Route
              path="/trends"
              element={
                <>
                  <Topbar lastUpdated={lastUpdated} />
                  <Trends onLastUpdated={handleLastUpdated} />
                </>
              }
            />
            {/* Default redirect */}
            <Route path="*" element={<Navigate to="/overview" replace />} />
          </Routes>
        </div>
      </div>
    </BrowserRouter>
  );
}
