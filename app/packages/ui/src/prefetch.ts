/**
 * (T186, Q164) THE WEB PREFETCH AS A HOOK: the map reports the region under its centre each time it settles
 * (`MapProps.onRegion`); the first report is first paint. From then on the region's regs file is fetched ahead, and —
 * once the browser is idle, unless the reader asked to save data or the connection is slow — `core` with the sql.js
 * engine. The decision is `@app/data/release` `prefetchPlan`; this hook only gathers its signals and carries it out.
 *
 * Pass `null` to turn it off (a phone: its release is installed, there is nothing to fetch).
 */
import { useCallback, useEffect, useRef } from "react";
import { prefetchPlan, regionOfTile, runPrefetch, type PrefetchSignals } from "@app/data/release";
import type { ReleaseLoader } from "@app/data/release";

interface Connection { saveData?: boolean; effectiveType?: string }
type IdleHost = { requestIdleCallback?: (cb: () => void) => number; cancelIdleCallback?: (h: number) => void };

/** What the browser says about the connection (nothing said: not Save-Data, speed unknown). */
export function connectionSignals(nav: { connection?: Connection } | undefined):
    Pick<PrefetchSignals, "saveData" | "effectiveType"> {
  const c = nav?.connection;
  return { saveData: c?.saveData === true, effectiveType: typeof c?.effectiveType === "string" ? c.effectiveType : null };
}

/** How long a browser with no `requestIdleCallback` (Safari) waits after first paint before calling itself idle. */
const IDLE_FALLBACK_MS = 2000;

export function useReleasePrefetch(loader: ReleaseLoader | null): ((regionId: unknown) => void) | undefined {
  const s = useRef<{ painted: boolean; idle: boolean; region: PrefetchSignals["region"] }>(
    { painted: false, idle: false, region: null });
  const cancel = useRef<(() => void) | null>(null);
  useEffect(() => () => { cancel.current?.(); }, []);

  const run = useCallback(() => {
    if (!loader) return;
    const g = globalThis as typeof globalThis & IdleHost & { navigator?: { connection?: Connection } };
    const plan = prefetchPlan({ region: s.current.region, idle: s.current.idle, ...connectionSignals(g.navigator) });
    runPrefetch(loader, plan, (name) => { try { g.performance?.mark?.(name); } catch { /* no marks: nothing lost */ } });
  }, [loader]);

  const onRegion = useCallback((regionId: unknown) => {
    if (!loader) return;
    const r = regionOfTile(regionId);
    // a settle over no region (the sea, outside B.C., a view that hides the region layer) forgets nothing
    if (r) s.current.region = r;
    if (!s.current.painted) {
      s.current.painted = true;
      const g = globalThis as typeof globalThis & IdleHost;
      const idle = () => { s.current.idle = true; cancel.current = null; run(); };
      if (typeof g.requestIdleCallback === "function") {
        const h = g.requestIdleCallback(idle);
        cancel.current = () => g.cancelIdleCallback?.(h);
      } else {
        const t = setTimeout(idle, IDLE_FALLBACK_MS);
        cancel.current = () => clearTimeout(t);
      }
    }
    run();
  }, [loader, run]);

  return loader ? onRegion : undefined;
}
