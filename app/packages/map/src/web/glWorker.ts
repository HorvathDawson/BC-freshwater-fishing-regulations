/**
 * (T1252) WHERE GL'S WORKER IS. GL 6 is ES modules only and starts its worker from a separate file it finds by its own
 * `import.meta.url`; inside Metro's bundle that names the bundle's folder, the worker 404s, and the map fetches each
 * archive's header and never a tile (the "pmtiles fails on 6" of deps.md: the protocol was never the cause). The file
 * is copied from the installed package into the web's `public/` when Metro starts (`apps/mobile/gl-worker.js`), in a
 * folder named by GL's version, so the page and the worker are always the same release and a cached old worker is never
 * paired with a new page. A missing file is GL's own "Worker failed to load" on the map's `error` event; the map then
 * draws no tile, so `useMapInstance` SAYS so on every map (`@app/core` `MAP_WORKER_FAILED_WORDS`) and REPORTS it once a
 * page as `MapWorkerMissing` (through the first map's `onError`, or `reportFailure` without one); a web export without
 * the file fails its build (`tools/web-shell.mjs`). GL uses the same words for any uncaught error INSIDE its worker, so a
 * worker crash is filed as `MapWorkerMissing` too.
 */
export const glWorkerUrl = (version: string) => `/maplibre-gl-${version}/maplibre-gl-worker.mjs`;

/** GL's words when its worker file did not load (maplibre-gl `src/util/actor.ts` `createActors`). */
export const isGlWorkerMissing = (e: Error) => /^Worker failed to load\b/.test(e.message);

/** The failure reported for it: one stable name and message per GL version, GL's error kept as the cause. */
export function glWorkerMissingError(cause: Error, version: string): Error {
  const e = new Error(`GL's worker failed to load (${glWorkerUrl(version)})`, { cause });
  e.name = "MapWorkerMissing";
  return e;
}
