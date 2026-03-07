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

/**
 * Inject preconnect + early-fetch hints for the R2 data origin at build time.
 * In dev the data comes from local /data/, so nothing is injected.
 */
function injectPreloadHints() {
  return {
    name: 'inject-preload-hints',
    transformIndexHtml() {
      const r2Base = process.env.VITE_TILE_BASE_URL
      if (!r2Base) return []   // dev — no transforms

      // Extract the origin (e.g. https://bc-fishing-r2.horvath-dawson.workers.dev)
      let origin: string
      try { origin = new URL(r2Base).origin } catch { origin = r2Base }

      return [
        // Preconnect to R2 — saves ~120ms TLS handshake
        {
          tag: 'link',
          attrs: { rel: 'preconnect', href: origin, crossorigin: true },
          injectTo: 'head' as const,
        },
        // Start waterbody JSON fetch before the JS bundle loads.
        // The waterbodyDataService picks up the in-flight promise.
        {
          tag: 'script',
          children: `(function(){var u="${r2Base}/waterbody_data.json";window.__earlyFetch=fetch(u).then(function(r){return r.ok?r:null}).catch(function(){return null})})()`,
          injectTo: 'head' as const,
        },
      ]
    },
  }
}

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), copyPublicWithoutR2Files(), injectPreloadHints()],
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
    sourcemap: true,
  }
})
