import type { RunTimingSource } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";

/**
 * (T286) The regulations made ready while the reader looks at the map. A phone (`prewarm.native.ts`): idle, a moment
 * after the release is up. The web (`prewarm.web.ts`): nothing — it must fetch nothing unasked.
 */
export declare function usePrewarmRegs(loader: ReleaseLoader | null): void;

/**
 * (T1013) The Runs tab's feed read while the reader looks at the map. A phone (`prewarm.native.ts`): idle, a few seconds
 * after the map is paired — the SAME `latest()` the tab asks (the source holds it), so the first switch to Runs finds it
 * read instead of starting a download, a verify and a parse of ~6 MB. The web (`prewarm.web.ts`): nothing (it fetches
 * nothing unasked).
 */
export declare function usePrewarmRuns(source: RunTimingSource | null | undefined, paired: boolean): void;
