// HistoricalTrends.jsx
import { useState, useEffect } from 'react'
import { fetchFarm } from '../lib/api'
import {
  AreaChart, Area, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend
} from 'recharts'

const MONTHS = ['Jul','Aug','Sep','Oct','Nov','Dec','Jan','Feb','Mar','Apr','May','Jun']

export function HistoricalTrends({ memberNo }) {
  const [farm,    setFarm]    = useState(null)
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState('')

  useEffect(() => {
    if (!memberNo) return
    setLoading(true)
    fetchFarm(memberNo)
      .then(setFarm)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [memberNo])

  if (loading) return <div style={{ display:'flex',alignItems:'center',gap:'0.75rem',padding:'3rem 0',color:'var(--text-muted)',fontFamily:'var(--font-mono)',fontSize:'0.85rem' }}><span className="loading-spinner" /> Loading history…</div>
  if (error)   return <div className="error-box">{error}</div>
  if (!farm)   return null

  // Annual totals for bar chart
  const annualData = (farm.seasons || []).map(s => ({
    season:   s.season_year,
    total_kg: Math.round(s.total_kg),
    earnings: Math.round(s.total_earnings),
    bonus:    Math.round(s.yearly_bonus || 0),
  }))

  // Monthly profile for the most recent 3 seasons
  const recentSeasons = (farm.seasons || []).slice(-3)
  const monthlyData = MONTHS.map((mo, idx) => {
    const row = { month: mo }
    recentSeasons.forEach(s => {
      row[s.season_year] = s.monthly_kg?.[idx] ? Math.round(s.monthly_kg[idx]) : null
    })
    return row
  })

  const seasonColors = ['var(--stone-400)', 'var(--clay-400)', 'var(--accent)']

  return (
    <div className="fade-in">
      <div className="page-header">
        <h1 className="page-title">Historical Trends</h1>
        <div className="page-subtitle mono">{farm.name} · {farm.ktda_member_no}</div>
      </div>

      {/* Annual totals */}
      <div className="card" style={{ marginBottom: '1.25rem' }}>
        <div className="card-title">Annual yield by season (kg)</div>
        <div className="chart-wrap" style={{ height: 240 }}>
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={annualData} margin={{ top: 8, right: 8, left: -10, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
              <XAxis dataKey="season" tick={{ fontFamily:'var(--font-mono)',fontSize:11,fill:'var(--text-muted)' }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fontFamily:'var(--font-mono)',fontSize:11,fill:'var(--text-muted)' }} axisLine={false} tickLine={false} />
              <Tooltip contentStyle={{ background:'var(--bg-surface)',border:'1px solid var(--border)',borderRadius:6,fontFamily:'var(--font-mono)',fontSize:12 }} formatter={v => [`${v.toLocaleString()} kg`,'']} />
              <Bar dataKey="total_kg" fill="var(--accent)" radius={[3,3,0,0]} name="Total kg" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Monthly profile - last 3 seasons */}
      <div className="card" style={{ marginBottom: '1.25rem' }}>
        <div className="card-title">Monthly yield profile — last 3 seasons</div>
        <div className="chart-wrap" style={{ height: 240 }}>
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={monthlyData} margin={{ top: 8, right: 8, left: -10, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
              <XAxis dataKey="month" tick={{ fontFamily:'var(--font-mono)',fontSize:11,fill:'var(--text-muted)' }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fontFamily:'var(--font-mono)',fontSize:11,fill:'var(--text-muted)' }} axisLine={false} tickLine={false} />
              <Tooltip contentStyle={{ background:'var(--bg-surface)',border:'1px solid var(--border)',borderRadius:6,fontFamily:'var(--font-mono)',fontSize:12 }} formatter={v => v ? [`${v} kg`,''] : ['—','']} />
              <Legend wrapperStyle={{ fontFamily:'var(--font-mono)',fontSize:11 }} />
              {recentSeasons.map((s, i) => (
                <Area key={s.season_year} type="monotone" dataKey={s.season_year}
                  stroke={seasonColors[i]} fill="none" strokeWidth={i === recentSeasons.length-1 ? 2 : 1}
                  strokeDasharray={i < recentSeasons.length-1 ? '4 4' : undefined}
                  connectNulls dot={false} name={`${s.season_year}`} />
              ))}
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Season table */}
      <div className="card">
        <div className="card-title">Season summary table</div>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width:'100%',borderCollapse:'collapse',fontFamily:'var(--font-mono)',fontSize:'0.78rem' }}>
            <thead>
              <tr style={{ borderBottom:'1px solid var(--border)' }}>
                {['Season','Total kg','Total earnings','Annual bonus'].map(h => (
                  <th key={h} style={{ padding:'0.5rem 0.75rem',textAlign:'left',color:'var(--text-muted)',fontWeight:400,letterSpacing:'0.06em',textTransform:'uppercase',fontSize:'0.65rem' }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {annualData.map((s, i) => (
                <tr key={s.season} style={{ borderBottom:'1px solid var(--border)', background: i%2===0 ? 'transparent' : 'var(--bg-raised)' }}>
                  <td style={{ padding:'0.5rem 0.75rem',color:'var(--text-secondary)' }}>{s.season}</td>
                  <td style={{ padding:'0.5rem 0.75rem',color:'var(--text-primary)' }}>{s.total_kg.toLocaleString()}</td>
                  <td style={{ padding:'0.5rem 0.75rem',color:'var(--data-positive)' }}>KES {s.earnings.toLocaleString()}</td>
                  <td style={{ padding:'0.5rem 0.75rem',color:'var(--clay-500)' }}>KES {s.bonus.toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

export default HistoricalTrends