/**
 * The shared declaration for `./store`.
 *
 * `store.native.ts` writes a `.spots` file; `store.web.ts` uses browser storage. Bundlers
 * pick by platform extension, TypeScript does not — so both are checked against one
 * signature. A store that exports something the other does not is how a feature ships on
 * one platform only.
 */
import type { SpotStore } from "./model";

export declare function openSpots(): SpotStore;
