import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    fs: {
      allow: ['..']
    },
    headers: {
      'Cache-Control': 'no-cache',
      'Accept-Ranges': 'bytes'
    }
  },
  assetsInclude: ['**/*.wasm', '**/*.pmtiles'],
  optimizeDeps: {
    exclude: ['@ngageoint/geopackage']
  }
})
