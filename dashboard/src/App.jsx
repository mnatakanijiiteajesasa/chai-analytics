import { useState, useEffect, createContext, useContext } from 'react'
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import './index.css'
import { isAuthenticated } from './lib/api'
import Login        from '../pages/Login'
import Dashboard    from '../pages/Dashboard'

//  Theme context 
export const ThemeContext = createContext(null)

export function useTheme() {
  return useContext(ThemeContext)
}

function AuthGuard({ children }) {
  return isAuthenticated() ? children : <Navigate to="/login" replace />
}

export default function App() {
  const [theme, setTheme] = useState(
    () => localStorage.getItem('chai_theme') || 'dark'
  )

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme)
    localStorage.setItem('chai_theme', theme)
  }, [theme])

  const toggleTheme = () => setTheme(t => t === 'dark' ? 'light' : 'dark')

  return (
    <ThemeContext.Provider value={{ theme, toggleTheme }}>
      <BrowserRouter>
        <Routes>
          <Route path="/login" element={<Login />} />
          <Route path="/*" element={
            <AuthGuard>
              <Dashboard />
            </AuthGuard>
          } />
        </Routes>
      </BrowserRouter>
    </ThemeContext.Provider>
  )
}