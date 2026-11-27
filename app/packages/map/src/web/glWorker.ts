/**
 * (T1252) WHERE GL'S WORKER IS. GL 6 is ES modules only and starts its worker from a separate file it finds by its own
 * `import.meta.url`; inside Metro's bundle that names the bundle's folder, the worker 404s, and the map fetches each
 * archive's header and never a tile (the "pmtiles fails on 6" of deps.md: the protocol was never the cause). The file
 * is copied from the installed package into the web's `public/` when Metro starts (`apps/mobile/gl-worker.js`), in a
 * folder named by GL's version, so the page and the worker are always the same release and a cached old worker is never
 * paired with a new page. A missing file is GL's own "Worker failed to load" error on the map's `error` event: shown and
 * reported.
 */
export const glWorkerUrl = (version: string) => `/maplibre-gl-${version}/maplibre-gl-worker.mjs`;
