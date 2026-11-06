import type { ReleaseLoader } from "@app/data/release";

/**
 * (T286) The regulations made ready while the reader looks at the map. A phone (`prewarm.native.ts`): idle, a moment
 * after the release is up. The web (`prewarm.web.ts`): nothing — it must fetch nothing unasked.
 */
export declare function usePrewarmRegs(loader: ReleaseLoader | null): void;
