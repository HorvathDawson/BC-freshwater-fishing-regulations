import { useEffect } from "react";
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
