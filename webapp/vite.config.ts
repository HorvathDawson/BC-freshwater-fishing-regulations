import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { resolve } from 'path'
import { readdirSync, statSync, mkdirSync, copyFileSync, existsSync } from 'fs'

/** Files served from R2 — skip copying to dist/ */
const R2_ONLY_FILES = new Set(['waterbody_data.json'])
const R2_ONLY_EXTENSIONS = ['.pmtiles']

/**
 * Custom plugin: copies public/ to dist/ but skips large files
 * that are served from Cloudflare R2 in production.
 * Gracefully handles missing public/ (e.g. in CI where data files are gitignored).
 */
function copyPublicWithoutR2Files() {
  return {
    name: 'copy-public-without-r2-files',
    apply: 'build' as const,
    closeBundle() {
      const publicDir = resolve(__dirname, 'public')
      const outDir = resolve(__dirname, 'dist')

      if (!existsSync(publicDir)) {
        console.log('[copy-public] public/ not found (CI?), skipping copy.')
        return
      }

      function copyDir(src: string, dest: string) {
        mkdirSync(dest, { recursive: true })
        for (const entry of readdirSync(src)) {
          const srcPath = resolve(src, entry)
          const destPath = resolve(dest, entry)
          if (statSync(srcPath).isDirectory()) {
            copyDir(srcPath, destPath)
          } else if (!R2_ONLY_FILES.has(entry) && !R2_ONLY_EXTENSIONS.some(ext => entry.endsWith(ext))) {
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
  plugins: [react(), copyPublicWithoutR2Files()],
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
