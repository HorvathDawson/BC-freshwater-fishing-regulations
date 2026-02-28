import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { resolve } from 'path'
import { readdirSync, statSync, mkdirSync, copyFileSync } from 'fs'

/**
 * Custom plugin: copies public/ to dist/ but skips .pmtiles files.
 * In production, tiles are served from Cloudflare R2.
 */
function copyPublicWithoutPmtiles() {
  return {
    name: 'copy-public-without-pmtiles',
    apply: 'build' as const,
    closeBundle() {
      const publicDir = resolve(__dirname, 'public')
      const outDir = resolve(__dirname, 'dist')

      function copyDir(src: string, dest: string) {
        mkdirSync(dest, { recursive: true })
        for (const entry of readdirSync(src)) {
          const srcPath = resolve(src, entry)
          const destPath = resolve(dest, entry)
          if (statSync(srcPath).isDirectory()) {
            copyDir(srcPath, destPath)
          } else if (!entry.endsWith('.pmtiles') && entry !== 'search_index.json') {
            copyFileSync(srcPath, destPath)
          }
        }
      }
      copyDir(publicDir, outDir)
    }
  }
}

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), copyPublicWithoutPmtiles()],
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
  },
  build: {
    // Disable default public dir copy — our plugin handles it (minus .pmtiles)
    copyPublicDir: false,
  }
})
