// src/lib/api.js — ChaiMetrics API client

const BASE = ''  // proxied through vite dev server

function getToken() {
  return localStorage.getItem('chai_token')
}

function setToken(token) {
  localStorage.setItem('chai_token', token)
}

function clearToken() {
  localStorage.removeItem('chai_token')
  localStorage.removeItem('chai_member')
}

async function request(path, options = {}) {
  const token = getToken()
  const headers = {
    'Content-Type': 'application/json',
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
    ...options.headers,
  }

  const res = await fetch(`${BASE}${path}`, { ...options, headers })

  if (res.status === 401) {
    clearToken()
    window.location.href = '/login'
    return
  }

  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }))
    throw new Error(err.error || `HTTP ${res.status}`)
  }

  return res.json()
}

//  Auth 
export async function login(memberNo, password) {
  const data = await request('/auth/login', {
    method: 'POST',
    body: JSON.stringify({ ktda_member_no: memberNo, password }),
  })
  setToken(data.access_token)
  localStorage.setItem('chai_member', JSON.stringify({
    ktda_member_no: data.ktda_member_no,
    name:           data.name,
    factory_code:   data.factory_code,
  }))
  return data
}

export function logout() {
  clearToken()
}

export function getStoredMember() {
  try {
    return JSON.parse(localStorage.getItem('chai_member'))
  } catch {
    return null
  }
}

export function isAuthenticated() {
  return !!getToken()
}

// Farms 
export function fetchFarms(params = {}) {
  const qs = new URLSearchParams(params).toString()
  return request(`/farms${qs ? '?' + qs : ''}`)
}

export function fetchFarm(memberNo) {
  return request(`/farms/${memberNo}`)
}

export function postDaily(memberNo, record) {
  return request(`/farms/${memberNo}/daily`, {
    method: 'POST',
    body: JSON.stringify(record),
  })
}

// Insights 
export function fetchInsights(memberNo, { refresh = false, narrative = false } = {}) {
  const qs = new URLSearchParams({
    ...(refresh   ? { refresh: 'true' }   : {}),
    ...(narrative ? { narrative: 'true' } : {}),
  }).toString()
  return request(`/farms/${memberNo}/insights${qs ? '?' + qs : ''}`)
}

//  Pricing 
export function fetchPricingTrends(factoryCode) {
  return request(`/pricing/trends/${factoryCode}`)
}

export function fetchCentres() {
  return request('/pricing/centres')
}