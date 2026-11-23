import type { RunTimingSource } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";

/**
 * (T286) Nothing on the web: it fetches its files (the water file, region files) and must fetch nothing unasked (T254:
 * low-bandwidth mode, Save-Data; T256 routes it by region), so the first tap opens the store. The phone twin is
 * prewarm.native.ts.
 */
export function usePrewarmRegs(_loader: ReleaseLoader | null): void {}

/** (T1013) Nothing on the web: the Runs feed is read when the tab is opened (nothing is fetched unasked). */
export function usePrewarmRuns(_source: RunTimingSource | null | undefined, _paired: boolean): void {}
