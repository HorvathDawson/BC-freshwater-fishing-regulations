/**
 * The shared declaration for `./connectivity`: `connectivity.web.ts` reads the browser's `navigator.onLine` and its
 * online/offline events; `connectivity.native.ts` has no source and never says (no NetInfo dependency). Bundlers pick
 * by platform extension; TypeScript does not, so both are checked against this one signature.
 */
import type { Connectivity } from "@app/ui";

export declare const connectivity: Connectivity;
