import type { ReleaseLoader } from "@app/data/release";

/**
 * (T360) The OS asking for memory back: `memoryTrim.native.ts` listens (Android's `onTrimMemory` through
 * modules/memory-trim, iOS's `memoryWarning`) and shrinks the release's databases; `memoryTrim.web.ts` is a no-op.
 * Bundlers pick by platform extension; TypeScript does not, so both are checked against this one signature.
 */
export declare function trimOnPressure(loader: ReleaseLoader, failed: (e: unknown) => void): () => void;
