import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    proxy: {
      '/auth':    { target: 'http://api:5000', changeOrigin: true },
      '/farms':   { target: 'http://api:5000', changeOrigin: true },
      '/pricing': { target: 'http://api:5000', changeOrigin: true },
      }
    }
  })

  