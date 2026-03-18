import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

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
        // Start tier0.json fetch before the JS bundle loads.
        // The waterbodyDataService picks up the in-flight promise.
        {
          tag: 'script',
          children: `(function(){var u="${r2Base}/tier0.json";window.__earlyFetch=fetch(u).then(function(r){return r.ok?r:null}).catch(function(){return null})})()`,
          injectTo: 'head' as const,
        },
      ]
    },
  }
}

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), injectPreloadHints()],
  server: {
    // Proxy /data/* and /api/* to the R2 Worker (wrangler dev) for true
    // dev/prod parity. The worker handles file serving, Range requests
    // (PMTiles), shard resolution, and cache headers — same code path as prod.
    proxy: {
      '/data': {
        target: 'http://localhost:8787',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/data(?=\/|$)/, ''),
      },
      '/api': {
        target: 'http://localhost:8787',
        changeOrigin: true,
      },
    },
  },
  assetsInclude: ['**/*.wasm', '**/*.pmtiles'],
  optimizeDeps: {
    exclude: ['@ngageoint/geopackage']
  },
  build: {
    sourcemap: true,
  }
})
