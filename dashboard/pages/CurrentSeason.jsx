// CurrentSeason.jsx
import { useState, useEffect } from 'react'
import { fetchFarm } from '../src/lib/api'
import {
  LineChart, Line, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine
} from 'recharts'

const MONTHS = ['Jul','Aug','Sep','Oct','Nov','Dec','Jan','Feb','Mar','Apr','May','Jun']
const MINIBONUS = new Set([0,1,2,4,5])

export function CurrentSeason({ memberNo }) {
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

  if (loading) return <div style={{display:'flex',alignItems:'center',gap:'0.75rem',padding:'3rem 0',color:'var(--text-muted)',fontFamily:'var(--font-mono)',fontSize:'0.85rem'}}><span className="loading-spinner"/>Loading…</div>
  if (error)   return <div className="error-box">{error}</div>
  if (!farm)   return null

  const seasons = farm.seasons || []
  const current = seasons[seasons.length - 1]
  const prev    = seasons[seasons.length - 2]
  if (!current) return <div className="muted mono">No season data available.</div>

  const chartData = MONTHS.map((mo, idx) => ({
    month:    mo,
    current:  current.monthly_kg?.[idx] ?? null,
    previous: prev?.monthly_kg?.[idx]   ?? null,
    minibonus: MINIBONUS.has(idx),
  }))

  const validKg  = (current.monthly_kg || []).filter(v => v != null)
  const avgKg    = validKg.length ? Math.round(validKg.reduce((a,b)=>a+b,0) / validKg.length) : 0
  const prevAvg  = prev ? Math.round((prev.monthly_kg||[]).filter(v=>v!=null).reduce((a,b)=>a+b,0) / 12) : null

  return (
    <div className="fade-in">
      <div className="page-header">
        <h1 className="page-title">Current Season</h1>
        <div className="page-subtitle mono">{farm.name} · Season {current.season_year}</div>
      </div>

      <div className="stat-grid" style={{ marginBottom:'1.25rem' }}>
        <div className="stat-tile">
          <div className="stat-label">Season total</div>
          <div className="stat-value">{Math.round(current.total_kg).toLocaleString()}<span className="stat-unit">kg</span></div>
        </div>
        <div className="stat-tile">
          <div className="stat-label">Monthly avg</div>
          <div className="stat-value">{avgKg}<span className="stat-unit">kg</span></div>
          {prevAvg && <div className={`stat-delta ${avgKg >= prevAvg ? 'up' : 'down'}`}>{avgKg >= prevAvg ? '↑' : '↓'} vs {prev.season_year}: {prevAvg}kg</div>}
        </div>
        <div className="stat-tile">
          <div className="stat-label">Total earnings</div>
          <div className="stat-value" style={{fontSize:'1.2rem'}}>KES {Math.round(current.total_earnings).toLocaleString()}</div>
        </div>
        <div className="stat-tile">
          <div className="stat-label">Annual bonus</div>
          <div className="stat-value" style={{fontSize:'1.2rem',color:'var(--clay-500)'}}>KES {Math.round(current.yearly_bonus||0).toLocaleString()}</div>
        </div>
      </div>

      <div className="card">
        <div className="card-title">Monthly yield — current vs previous season</div>
        <div className="chart-wrap" style={{height:260}}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData} margin={{top:8,right:8,left:-10,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false}/>
              <XAxis dataKey="month" tick={{fontFamily:'var(--font-mono)',fontSize:11,fill:'var(--text-muted)'}} axisLine={false} tickLine={false}/>
              <YAxis tick={{fontFamily:'var(--font-mono)',fontSize:11,fill:'var(--text-muted)'}} axisLine={false} tickLine={false}/>
              <Tooltip contentStyle={{background:'var(--bg-surface)',border:'1px solid var(--border)',borderRadius:6,fontFamily:'var(--font-mono)',fontSize:12}} formatter={v=>v?[`${v} kg`,'']:['—','']}/>
              {MONTHS.map((_, idx) => MINIBONUS.has(idx) && (
                <ReferenceLine key={idx} x={MONTHS[idx]} stroke="var(--clay-300)" strokeWidth={1} strokeDasharray="3 3" label={{value:'MB',position:'top',fontSize:8,fill:'var(--clay-400)',fontFamily:'var(--font-mono)'}}/>
              ))}
              {prev && <Line type="monotone" dataKey="previous" stroke="var(--stone-400)" strokeWidth={1} strokeDasharray="4 4" dot={false} connectNulls name={`${prev.season_year}`}/>}
              <Line type="monotone" dataKey="current" stroke="var(--accent)" strokeWidth={2} dot={{r:3,fill:'var(--accent)'}} connectNulls name={`${current.season_year}`}/>
            </LineChart>
          </ResponsiveContainer>
        </div>
        <div style={{fontFamily:'var(--font-mono)',fontSize:'0.65rem',color:'var(--text-muted)',marginTop:'0.5rem'}}>
          MB = minibonus month (Jul, Aug, Sep, Nov, Dec)
        </div>
      </div>
    </div>
  )
}

export default CurrentSeason