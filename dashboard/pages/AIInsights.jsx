import { useState, useEffect, useCallback } from 'react'
import { RefreshCw, MessageSquare, Zap, TrendingUp, AlertTriangle } from 'lucide-react'
import {
  AreaChart, Area, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine
} from 'recharts'
import { fetchInsights } from '../lib/api'

const MONTH_LABELS = ['Jul','Aug','Sep','Oct','Nov','Dec','Jan','Feb','Mar','Apr','May','Jun']

//  Sub-components 

function ScoreRing({ score }) {
  const r    = 36
  const circ = 2 * Math.PI * r
  const fill = ((score / 100) * circ)
  const color = score >= 70 ? 'var(--score-high)' : score >= 45 ? 'var(--score-mid)' : 'var(--score-low)'

  return (
    <div style={{ position: 'relative', width: 100, height: 100, flexShrink: 0 }}>
      <svg width="100" height="100" style={{ transform: 'rotate(-90deg)' }}>
        <circle cx="50" cy="50" r={r} fill="none" stroke="var(--border)" strokeWidth="6" />
        <circle
          cx="50" cy="50" r={r} fill="none"
          stroke={color} strokeWidth="6"
          strokeDasharray={`${fill} ${circ}`}
          strokeLinecap="round"
          style={{ transition: 'stroke-dasharray 0.8s cubic-bezier(0.4,0,0.2,1)' }}
        />
      </svg>
      <div style={{
        position: 'absolute', inset: 0, display: 'flex',
        flexDirection: 'column', alignItems: 'center', justifyContent: 'center',
      }}>
        <span style={{
          fontFamily: 'var(--font-mono)', fontSize: '1.4rem',
          fontWeight: 500, color, lineHeight: 1,
        }}>{score}</span>
        <span style={{ fontFamily: 'var(--font-mono)', fontSize: '0.55rem',
          color: 'var(--text-muted)', letterSpacing: '0.08em', textTransform: 'uppercase' }}>
          score
        </span>
      </div>
    </div>
  )
}

function ForecastChart({ forecast, ci80Lower, ci80Upper }) {
  if (!forecast?.length) return <div className="muted mono" style={{ fontSize: '0.8rem', padding: '1rem 0' }}>No SARIMA forecast available yet — model may still be fitting.</div>

  const data = forecast.map((kg, i) => ({
    name:  `+${i + 1}mo`,
    kg:    Math.round(kg),
    lower: Math.round(ci80Lower?.[i] ?? kg * 0.85),
    upper: Math.round(ci80Upper?.[i] ?? kg * 1.15),
  }))

  return (
    <div className="chart-wrap">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={data} margin={{ top: 8, right: 8, left: -10, bottom: 0 }}>
          <defs>
            <linearGradient id="ciGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%"  stopColor="var(--accent)" stopOpacity={0.15} />
              <stop offset="95%" stopColor="var(--accent)" stopOpacity={0.02} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
          <XAxis dataKey="name" tick={{ fontFamily: 'var(--font-mono)', fontSize: 11, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false} />
          <YAxis tick={{ fontFamily: 'var(--font-mono)', fontSize: 11, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false} />
          <Tooltip
            contentStyle={{ background: 'var(--bg-surface)', border: '1px solid var(--border)', borderRadius: 6, fontFamily: 'var(--font-mono)', fontSize: 12 }}
            labelStyle={{ color: 'var(--text-secondary)' }}
            itemStyle={{ color: 'var(--accent)' }}
            formatter={(v) => [`${v} kg`, '']}
          />
          <Area type="monotone" dataKey="upper" stroke="none" fill="url(#ciGrad)" name="80% CI upper" />
          <Area type="monotone" dataKey="lower" stroke="none" fill="var(--bg-surface)" name="80% CI lower" />
          <Area type="monotone" dataKey="kg" stroke="var(--accent)" strokeWidth={2} fill="none" dot={{ r: 3, fill: 'var(--accent)' }} name="Forecast" />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  )
}

function FeatureImportance({ features }) {
  if (!features?.length) return null
  const max = Math.max(...features.map(f => f.importance))

  const labels = {
    rainfall_mm:          'Rainfall',
    prior_season_avg:     'Prior season avg',
    rolling_yield_3mo:    'Rolling 3-mo yield',
    fert_kg:              'Fertiliser applied',
    fert_lag1_kg:         'Fertiliser lag (1mo)',
    fert_lag2_kg:         'Fertiliser lag (2mo)',
    is_pruning_month:     'Pruning month',
    months_since_pruning: 'Months since pruning',
    hectares:             'Farm size (ha)',
    altitude_m:           'Altitude',
    season_month_idx:     'Season month',
    rain_3mo_sum:         'Rainfall 3-mo sum',
    rain_deficit_mm:      'Rainfall deficit',
    temp_c:               'Temperature',
    is_minibonus_month:   'Minibonus month',
  }

  return (
    <div className="feat-bar-list">
      {features.slice(0, 8).map(f => (
        <div key={f.feature} className="feat-bar-item">
          <div className="feat-bar-label">
            <span>{labels[f.feature] || f.feature}</span>
            <span>{(f.importance * 100).toFixed(1)}%</span>
          </div>
          <div className="feat-bar-track">
            <div className="feat-bar-fill" style={{ width: `${(f.importance / max) * 100}%` }} />
          </div>
        </div>
      ))}
    </div>
  )
}

function RecCard({ rec }) {
  return (
    <div className={`rec-card priority-${rec.priority}`}>
      <div className="rec-priority">{rec.priority} priority · {rec.category}</div>
      <div className="rec-title">{rec.title}</div>
      <div className="rec-action">→ {rec.action}</div>
    </div>
  )
}

// ── Main page ──────────────────────────────────────────────────────

export default function AIInsights({ memberNo }) {
  const [data,        setData]        = useState(null)
  const [loading,     setLoading]     = useState(false)
  const [narLoading,  setNarLoading]  = useState(false)
  const [error,       setError]       = useState('')

  const load = useCallback(async (refresh = false) => {
    if (!memberNo) return
    setLoading(true)
    setError('')
    try {
      const d = await fetchInsights(memberNo, { refresh })
      setData(d)
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [memberNo])

  async function loadNarrative() {
    if (!memberNo) return
    setNarLoading(true)
    try {
      const d = await fetchInsights(memberNo, { refresh: true, narrative: true })
      setData(d)
    } catch (e) {
      setError(e.message)
    } finally {
      setNarLoading(false)
    }
  }

  useEffect(() => { load() }, [load])

  if (loading) return (
    <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', padding: '3rem 0', color: 'var(--text-muted)', fontFamily: 'var(--font-mono)', fontSize: '0.85rem' }}>
      <span className="loading-spinner" /> Running ML pipeline…
    </div>
  )

  if (error) return <div className="error-box">{error}</div>
  if (!data)  return null

  const farm    = data.farm || {}
  const current = data.current_season || {}
  const xgb     = data.xgb_prediction || {}
  const sarima  = data.sarima_forecast
  const recs    = data.recommendations || []
  const pricing = data.pricing_forecast || {}

  const fairtrade = farm.fairtrade

  return (
    <div className="fade-in">
      {/* ── Header ─────────────────────────────────────────────── */}
      <div className="page-header">
        <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between' }}>
          <div>
            <h1 className="page-title">{farm.name || memberNo}</h1>
            <div className="page-subtitle">
              {farm.owner_name} · {farm.collection_centre} · {farm.factory_code}
              {fairtrade && <span className="badge badge-green" style={{ marginLeft: '0.5rem' }}>Fairtrade</span>}
            </div>
          </div>
          <div style={{ display: 'flex', gap: '0.5rem', flexShrink: 0 }}>
            <button className="btn btn-ghost" onClick={() => load(true)} title="Refresh predictions">
              <RefreshCw size={12} /> Refresh
            </button>
          </div>
        </div>
        {data.from_cache && (
          <div style={{ fontFamily: 'var(--font-mono)', fontSize: '0.65rem', color: 'var(--text-muted)', marginTop: '0.4rem' }}>
            ↑ cached result · computed {data.computed_at?.slice(0,16).replace('T',' ')} UTC
          </div>
        )}
      </div>

      {/* ── Stat tiles ─────────────────────────────────────────── */}
      <div className="stat-grid">
        <div className="stat-tile">
          <div className="stat-label">Season total</div>
          <div className="stat-value">{Math.round(current.total_kg || 0).toLocaleString()}<span className="stat-unit">kg</span></div>
          <div className="stat-delta flat">{current.months_complete || 0} months complete</div>
        </div>
        <div className="stat-tile">
          <div className="stat-label">Monthly average</div>
          <div className="stat-value">{Math.round(current.season_avg_kg || 0)}<span className="stat-unit">kg</span></div>
        </div>
        <div className="stat-tile">
          <div className="stat-label">XGBoost forecast</div>
          <div className="stat-value" style={{ color: 'var(--accent)' }}>{Math.round(xgb.predicted_kg || 0)}<span className="stat-unit">kg</span></div>
          <div className="stat-delta flat">{xgb.month_name || '—'}</div>
        </div>
        <div className="stat-tile">
          <div className="stat-label">Farm size</div>
          <div className="stat-value">{farm.hectares}<span className="stat-unit">ha</span></div>
          <div className="stat-delta flat">{farm.altitude_m}m altitude</div>
        </div>
        <div className="stat-tile">
          <div className="stat-label">Performance</div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginTop: '0.3rem' }}>
            <ScoreRing score={data.performance_score || 0} />
            <div style={{ fontFamily: 'var(--font-mono)', fontSize: '0.72rem', color: 'var(--text-muted)', lineHeight: 1.6 }}>
              {recs.filter(r => r.priority === 'high').length} high<br />
              {recs.filter(r => r.priority === 'medium').length} medium<br />
              {recs.filter(r => r.priority === 'low').length} low
            </div>
          </div>
        </div>
      </div>

      {/* ── Two column: forecast + feature importance ───────────── */}
      <div className="two-col" style={{ marginBottom: '1.25rem' }}>
        <div className="card">
          <div className="card-title" style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
            <TrendingUp size={11} /> SARIMA 6-month yield forecast
          </div>
          <ForecastChart
            forecast={sarima?.forecast_6mo}
            ci80Lower={sarima?.ci_80_lower}
            ci80Upper={sarima?.ci_80_upper}
          />
        </div>

        <div className="card">
          <div className="card-title" style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
            <Zap size={11} /> XGBoost feature importance
          </div>
          <FeatureImportance features={xgb.top_features} />
        </div>
      </div>

      {/* ── Recommendations ────────────────────────────────────── */}
      <div className="card" style={{ marginBottom: '1.25rem' }}>
        <div className="card-title" style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
          <AlertTriangle size={11} /> Recommendations ({recs.length})
        </div>
        <div className="rec-list">
          {recs.map((r, i) => <RecCard key={i} rec={r} />)}
        </div>
      </div>

      {/* ── LLM Narrative ──────────────────────────────────────── */}
      <div className="card">
        <div className="card-title" style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <span style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
            <MessageSquare size={11} /> AI farm narrative
          </span>
          {!data.narrative && (
            <button className="btn btn-ghost" onClick={loadNarrative} disabled={narLoading}
              style={{ padding: '0.3rem 0.7rem' }}>
              {narLoading
                ? <><span className="loading-spinner" style={{ width: 12, height: 12, borderWidth: 1.5 }} /> Generating…</>
                : <><MessageSquare size={11} /> Generate narrative</>
              }
            </button>
          )}
        </div>

        {data.narrative ? (
          <div className="narrative-box">{data.narrative}</div>
        ) : narLoading ? (
          <div className="narrative-loading">
            <span className="loading-spinner" style={{ width: 14, height: 14, borderWidth: 2 }} />
            Calling Mistral — this takes 5–10 seconds…
          </div>
        ) : (
          <div style={{ fontFamily: 'var(--font-mono)', fontSize: '0.78rem', color: 'var(--text-muted)', padding: '0.5rem 0' }}>
            Click "Generate narrative" to produce a plain-language farm summary via the local LLM.
          </div>
        )}
      </div>
    </div>
  )
}