import { useEffect } from "react";
import type { RunTimingSource } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import { prewarmRegs } from "@app/ui";

/** (T286) How long after the release is up the regulations are made ready (the map draws first). */
const PREWARM_MS = 1500;

/**
 * (T286) the regulations made ready while the reader looks at the map (idle, a moment after the release is up): the
 * first tap of a session then costs what every later one does. A PHONE ONLY: it holds every file, so this reads nothing
 * from the network (the web twin, prewarm.web.ts, does nothing: there the first tap opens the store).
 */
export function usePrewarmRegs(loader: ReleaseLoader | null): void {
  useEffect(() => {
    if (!loader) return;
    // (T250) RN 0.88 removed InteractionManager; requestIdleCallback is its named successor (same "after the frame" wait)
    let idle: number | null = null;
    const t = setTimeout(() => {
      idle = requestIdleCallback(() => {
        void prewarmRegs(loader, { files: true }).catch(() => {});
      });
    }, PREWARM_MS);
    return () => { clearTimeout(t); if (idle !== null) cancelIdleCallback(idle); };
  }, [loader]);
}

/** (T1013) How long after the map is paired the Runs feed is read (the map's own first paint goes first). */
const PREWARM_RUNS_MS = 4000;

/**
 * (T1013, staging report 50e342ef056ab300: "When you switch to runs it takes a very long time to load the first time")
 * THE RUNS FEED READ AHEAD, on a phone: idle, a few seconds after the map is paired. It is the source's own `latest()`,
 * the one the Runs tab asks (held for the hour, `httpRunTiming`): ONE load path, whichever asks first; the tab's first
 * open then waits for nothing that is already read. Its failures are the source's (reported there; it never rejects).
 */
export function usePrewarmRuns(source: RunTimingSource | null | undefined, paired: boolean): void {
  useEffect(() => {
    if (!source || !paired) return;
    let idle: number | null = null;
    const t = setTimeout(() => { idle = requestIdleCallback(() => { void source.latest(); }); }, PREWARM_RUNS_MS);
    return () => { clearTimeout(t); if (idle !== null) cancelIdleCallback(idle); };
  }, [source, paired]);
}
