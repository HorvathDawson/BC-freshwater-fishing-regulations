/**
 * (T199) The shared declaration for `./runTimingStore`: the last captured run timing, as whole text. `.web.ts` keeps it
 * in the Cache API (a few MB is past what localStorage holds), `.native.ts` in a file. Bundlers pick by platform
 * extension, TypeScript does not — so both are checked against this one signature.
 */
import type { RunTimingStore } from "../feed/runtiming";

export declare function openRunTimingStore(): RunTimingStore;
