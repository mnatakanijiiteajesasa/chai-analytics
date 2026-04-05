import { useState, useEffect } from 'react'
import { fetchPricingTrends } from '../lib/api'
import {
  LineChart, Line, AreaChart, Area,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine, Legend
} from 'recharts'

export function PricingIntel({ factoryCode }) {
  const [data,    setData]    = useState(null)
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState('')
  const [factory, setFactory] = useState(factoryCode || 'WRU-01')

  useEffect(() => {
    setLoading(true)
    fetchPricingTrends(factory)
      .then(setData)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [factory])

  if (loading) return <div style={{display:'flex',alignItems:'center',gap:'0.75rem',padding:'3rem 0',color:'var(--text-muted)',fontFamily:'var(--font-mono)',fontSize:'0.85rem'}}><span className="loading-spinner"/>Loading pricing…</div>
  if (error)   return <div className="error-box">{error}</div>

  const historical = data?.historical || []
  const forecasts  = data?.forecasts  || {}
  const summary    = data?.summary    || {}

  // Monthly rate chart data — last 5 seasons
  const rateData = historical.slice(-60).map(h => ({
    period:  h.period || `${h.season_year}-${h.season_month}`,
    rate:    h.monthly_rate,
    minibonus: h.is_minibonus_month ? h.minibonus_rate : null,
  }))

  // Forecast chart
  const fcData = (forecasts.monthly_rate?.forecast || []).map((v, i) => ({
    name:  `+${i+1}mo`,
    rate:  Math.round(v * 100) / 100,
    lower: Math.round((forecasts.monthly_rate?.ci_80_lower?.[i] ?? v*0.95) * 100) / 100,
    upper: Math.round((forecasts.monthly_rate?.ci_80_upper?.[i] ?? v*1.05) * 100) / 100,
  }))

  return (
    <div className="fade-in">
      <div className="page-header">
        <div style={{display:'flex',alignItems:'flex-start',justifyContent:'space-between'}}>
          <div>
            <h1 className="page-title">Pricing Intelligence</h1>
            <div className="page-subtitle mono">KTDA monthly rates · minibonus · annual bonus</div>
          </div>
          <div style={{display:'flex',gap:'0.5rem'}}>
            {['WRU-01','RKR-01'].map(f => (
              <button key={f} className={`btn ${factory===f?'btn-primary':'btn-ghost'}`} onClick={()=>setFactory(f)}>{f}</button>
            ))}
          </div>
        </div>
      </div>

      {/* Summary stats */}
      {summary.rate_latest && (
        <div className="stat-grid" style={{marginBottom:'1.25rem'}}>
          <div className="stat-tile">
            <div className="stat-label">Latest rate</div>
            <div className="stat-value">KES {summary.rate_latest?.toFixed(2)}<span className="stat-unit">/kg</span></div>
          </div>
          <div className="stat-tile">
            <div className="stat-label">Rate range</div>
            <div className="stat-value" style={{fontSize:'1.1rem'}}>
              {summary.rate_min?.toFixed(2)} – {summary.rate_max?.toFixed(2)}
            </div>
            <div className="stat-delta flat mono">{summary.seasons_on_record} seasons on record</div>
          </div>
          <div className="stat-tile">
            <div className="stat-label">Forecast available</div>
            <div style={{display:'flex',gap:'0.4rem',flexWrap:'wrap',marginTop:'0.4rem'}}>
              {(summary.forecast_available||[]).map(f=>(
                <span key={f} className="badge badge-green">{f.replace(/_/g,' ')}</span>
              ))}
              {!(summary.forecast_available||[]).length && <span className="muted mono" style={{fontSize:'0.75rem'}}>none yet</span>}
            </div>
          </div>
        </div>
      )}

      {/* Historical rate chart */}
      <div className="card" style={{marginBottom:'1.25rem'}}>
        <div className="card-title">Monthly rate history (KES/kg)</div>
        <div className="chart-wrap" style={{height:240}}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={rateData} margin={{top:8,right:8,left:-10,bottom:0}}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false}/>
              <XAxis dataKey="period" tick={{fontFamily:'var(--font-mono)',fontSize:10,fill:'var(--text-muted)'}} axisLine={false} tickLine={false} interval={11}/>
              <YAxis tick={{fontFamily:'var(--font-mono)',fontSize:11,fill:'var(--text-muted)'}} axisLine={false} tickLine={false} domain={['auto','auto']}/>
              <Tooltip contentStyle={{background:'var(--bg-surface)',border:'1px solid var(--border)',borderRadius:6,fontFamily:'var(--font-mono)',fontSize:12}} formatter={v=>v?[`KES ${v}/kg`,'']:['—','']}/>
              <Line type="monotone" dataKey="rate" stroke="var(--accent)" strokeWidth={1.5} dot={false} name="Monthly rate"/>
              <Line type="monotone" dataKey="minibonus" stroke="var(--clay-400)" strokeWidth={1} dot={{r:3,fill:'var(--clay-400)'}} connectNulls={false} name="Minibonus rate"/>
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* SARIMA rate forecast */}
      {fcData.length > 0 && (
        <div className="card">
          <div className="card-title">SARIMA rate forecast — next 12 months</div>
          <div className="chart-wrap" style={{height:220}}>
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={fcData} margin={{top:8,right:8,left:-10,bottom:0}}>
                <defs>
                  <linearGradient id="fcGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%"  stopColor="var(--clay-400)" stopOpacity={0.15}/>
                    <stop offset="95%" stopColor="var(--clay-400)" stopOpacity={0.02}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false}/>
                <XAxis dataKey="name" tick={{fontFamily:'var(--font-mono)',fontSize:11,fill:'var(--text-muted)'}} axisLine={false} tickLine={false}/>
                <YAxis tick={{fontFamily:'var(--font-mono)',fontSize:11,fill:'var(--text-muted)'}} axisLine={false} tickLine={false} domain={['auto','auto']}/>
                <Tooltip contentStyle={{background:'var(--bg-surface)',border:'1px solid var(--border)',borderRadius:6,fontFamily:'var(--font-mono)',fontSize:12}} formatter={v=>[`KES ${v}/kg`,'']}/>
                <Area type="monotone" dataKey="upper" stroke="none" fill="url(#fcGrad)" name="80% CI upper"/>
                <Area type="monotone" dataKey="lower" stroke="none" fill="var(--bg-surface)" name="80% CI lower"/>
                <Line type="monotone" dataKey="rate" stroke="var(--clay-400)" strokeWidth={2} dot={{r:3,fill:'var(--clay-400)'}} name="Forecast rate"/>
              </AreaChart>
            </ResponsiveContainer>
          </div>
          <div style={{fontFamily:'var(--font-mono)',fontSize:'0.65rem',color:'var(--text-muted)',marginTop:'0.5rem'}}>
            Shaded band = 80% confidence interval. KTDA rates declared monthly — forecasts are indicative only.
          </div>
        </div>
      )}
    </div>
  )
}

export default PricingIntel