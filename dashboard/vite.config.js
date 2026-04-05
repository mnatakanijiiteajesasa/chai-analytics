import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      '/farms':   { target: 'http://localhost:5000', changeOrigin: true },
      '/auth':    { target: 'http://localhost:5000', changeOrigin: true },
      '/pricing': { target: 'http://localhost:5000', changeOrigin: true },
      '/health':  { target: 'http://localhost:5000', changeOrigin: true },
    }
  }
})