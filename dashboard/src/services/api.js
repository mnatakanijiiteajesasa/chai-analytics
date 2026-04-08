const BASE = '/api'

const handle = async (res) => {
  if (!res.ok) throw new Error(`API error ${res.status}`)
  return res.json()
}

// Auth
export const login = (credentials) =>
  fetch(`${BASE}/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(credentials)
  }).then(handle)

// Farms
export const getFarms = (token) =>
  fetch(`${BASE}/farms`, {
    headers: { Authorization: `Bearer ${token}` }
  }).then(handle)

export const getFarmInsights = (farmId, token) =>
  fetch(`${BASE}/farms/${farmId}/insights`, {
    headers: { Authorization: `Bearer ${token}` }
  }).then(handle)

// Pricing
export const getPricingTrends = (factoryCode, token) =>
  fetch(`${BASE}/pricing/trends/${factoryCode}`, {
    headers: { Authorization: `Bearer ${token}` }
  }).then(handle)