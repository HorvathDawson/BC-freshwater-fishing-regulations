/**
 * The shared declaration for `./connectivity`: `connectivity.web.ts` reads the browser's `navigator.onLine` and its
 * online/offline events; `connectivity.native.ts` reads NetInfo (`netInfoConnectivity.ts`). Bundlers pick
 * by platform extension; TypeScript does not, so both are checked against this one signature.
 */
import type { Connectivity } from "@app/ui";

export declare const connectivity: Connectivity;
