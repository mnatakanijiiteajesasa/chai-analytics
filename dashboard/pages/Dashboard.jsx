import { useState, useEffect } from 'react'
import { Routes, Route, useNavigate, useLocation } from 'react-router-dom'
import { Leaf, TrendingUp, BarChart2, Cpu, LogOut, Sun, Moon } from 'lucide-react'
import { getStoredMember, logout, fetchFarms } from '../src/lib/api'
import { useTheme } from '../src/App'
import AIInsights       from './AIInsights'
import HistoricalTrends from './HistoricalTrends'
import PricingIntel     from './PricingIntel'
import CurrentSeason    from './CurrentSeason'

const NAV = [
  { path: '/insights',   label: 'AI Insights',        Icon: Cpu },
  { path: '/season',     label: 'Current Season',      Icon: Leaf },
  { path: '/historical', label: 'Historical Trends',   Icon: TrendingUp },
  { path: '/pricing',    label: 'Pricing Intelligence', Icon: BarChart2 },
]

export default function Dashboard() {
  const navigate  = useNavigate()
  const location  = useLocation()
  const { theme, toggleTheme } = useTheme()
  const member    = getStoredMember()

  const [farms,          setFarms]          = useState([])
  const [selectedMember, setSelectedMember] = useState(
    member?.ktda_member_no || ''
  )

  useEffect(() => {
    fetchFarms({ factory: member?.factory_code, per_page: 100 })
      .then(d => setFarms(d.farms || []))
      .catch(() => {})
  }, [])

  // Redirect root to AI Insights
  useEffect(() => {
    if (location.pathname === '/') navigate('/insights', { replace: true })
  }, [location.pathname])

  function handleLogout() {
    logout()
    navigate('/login')
  }

  const activePath = '/' + location.pathname.split('/')[1]

  return (
    <div className="layout">
      {/* ── Sidebar ────────────────────────────────────────────── */}
      <aside className="sidebar">
        <div className="sidebar-logo">
          <div className="sidebar-logo-mark">ChaiMetrics</div>
          <div className="sidebar-logo-sub">KTDA Farm Intelligence</div>
        </div>

        {/* Farm selector */}
        <div className="sidebar-farm-selector">
          <div className="sidebar-farm-label">Active farm</div>
          <select
            className="sidebar-farm-select"
            value={selectedMember}
            onChange={e => setSelectedMember(e.target.value)}
          >
            {farms.map(f => (
              <option key={f.ktda_member_no} value={f.ktda_member_no}>
                {f.ktda_member_no} — {f.collection_centre}
              </option>
            ))}
          </select>
        </div>

        {/* Nav */}
        <nav className="sidebar-nav">
          <div className="sidebar-section-label">Analytics</div>
          {NAV.map(({ path, label, Icon }) => (
            <button
              key={path}
              className={`sidebar-nav-item ${activePath === path ? 'active' : ''}`}
              onClick={() => navigate(path)}
            >
              <Icon size={14} />
              {label}
            </button>
          ))}
        </nav>

        {/* Bottom bar */}
        <div className="sidebar-bottom">
          <div className="sidebar-member">
            <div style={{ color: 'var(--sidebar-text)', fontWeight: 500 }}>
              {member?.name?.split(' ').slice(0,2).join(' ') || member?.ktda_member_no}
            </div>
            <div>{member?.factory_code}</div>
          </div>
          <div style={{ display: 'flex', gap: '0.4rem' }}>
            <button className="theme-toggle" onClick={toggleTheme} title="Toggle theme">
              {theme === 'dark' ? <Sun size={12} /> : <Moon size={12} />}
            </button>
            <button className="theme-toggle" onClick={handleLogout} title="Sign out">
              <LogOut size={12} />
            </button>
          </div>
        </div>
      </aside>

      {/* ── Main content ───────────────────────────────────────── */}
      <main className="main-content">
        <Routes>
          <Route path="/insights"   element={<AIInsights   memberNo={selectedMember} />} />
          <Route path="/season"     element={<CurrentSeason memberNo={selectedMember} />} />
          <Route path="/historical" element={<HistoricalTrends memberNo={selectedMember} />} />
          <Route path="/pricing"    element={<PricingIntel factoryCode={member?.factory_code} />} />
        </Routes>
      </main>
    </div>
  )
}