import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { login } from '../lib/api'

export default function Login() {
  const navigate = useNavigate()
  const [memberNo, setMemberNo] = useState('')
  const [password, setPassword] = useState('')
  const [error,    setError]    = useState('')
  const [loading,  setLoading]  = useState(false)

  async function handleSubmit(e) {
    e.preventDefault()
    setError('')
    setLoading(true)
    try {
      await login(memberNo.trim(), password.trim())
      navigate('/')
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={{
      minHeight: '100vh', display: 'flex', alignItems: 'center',
      justifyContent: 'center', background: 'var(--bg-base)',
      padding: '2rem',
    }}>
      <div style={{ width: '100%', maxWidth: '360px' }}>

        {/* Logo */}
        <div style={{ marginBottom: '2.5rem', textAlign: 'center' }}>
          <div style={{
            fontFamily: 'var(--font-display)', fontSize: '2rem',
            fontWeight: 600, color: 'var(--accent)', letterSpacing: '-0.02em',
          }}>ChaiMetrics</div>
          <div style={{
            fontFamily: 'var(--font-mono)', fontSize: '0.65rem',
            letterSpacing: '0.12em', textTransform: 'uppercase',
            color: 'var(--text-muted)', marginTop: '0.3rem',
          }}>KTDA Farm Intelligence Platform</div>
        </div>

        {/* Form */}
        <div className="card">
          <div className="card-title" style={{ marginBottom: '1.25rem' }}>
            Sign in with your KTDA member number
          </div>

          <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
            <div>
              <label style={{
                display: 'block', fontFamily: 'var(--font-mono)',
                fontSize: '0.68rem', letterSpacing: '0.08em',
                textTransform: 'uppercase', color: 'var(--text-muted)',
                marginBottom: '0.4rem',
              }}>Member Number</label>
              <input
                value={memberNo}
                onChange={e => setMemberNo(e.target.value)}
                placeholder="KTD-XXXXX"
                required
                style={{
                  width: '100%', padding: '0.6rem 0.8rem',
                  background: 'var(--bg-raised)', border: '1px solid var(--border)',
                  borderRadius: '4px', color: 'var(--text-primary)',
                  fontFamily: 'var(--font-mono)', fontSize: '0.875rem',
                  outline: 'none',
                }}
              />
            </div>

            <div>
              <label style={{
                display: 'block', fontFamily: 'var(--font-mono)',
                fontSize: '0.68rem', letterSpacing: '0.08em',
                textTransform: 'uppercase', color: 'var(--text-muted)',
                marginBottom: '0.4rem',
              }}>Password</label>
              <input
                type="password"
                value={password}
                onChange={e => setPassword(e.target.value)}
                placeholder="Enter password"
                required
                style={{
                  width: '100%', padding: '0.6rem 0.8rem',
                  background: 'var(--bg-raised)', border: '1px solid var(--border)',
                  borderRadius: '4px', color: 'var(--text-primary)',
                  fontFamily: 'var(--font-mono)', fontSize: '0.875rem',
                  outline: 'none',
                }}
              />
            </div>

            {error && (
              <div className="error-box">{error}</div>
            )}

            <button
              type="submit"
              className="btn btn-primary"
              disabled={loading}
              style={{ width: '100%', justifyContent: 'center', marginTop: '0.25rem' }}
            >
              {loading ? <><span className="loading-spinner" /> Signing in…</> : 'Sign in'}
            </button>
          </form>

          <div style={{
            marginTop: '1rem', fontFamily: 'var(--font-mono)',
            fontSize: '0.68rem', color: 'var(--text-muted)',
            textAlign: 'center',
          }}>
            Demo: use your member number as both fields
          </div>
        </div>
      </div>
    </div>
  )
}