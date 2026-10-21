/**
 * The shared declaration for `./photoStore` (S1), as `store.d.ts` is for the spots store.
 *
 * `photoStore.native.ts` keeps files in `documentDirectory/spots/photos/`; `photoStore.web.ts` keeps blobs in
 * IndexedDB. Bundlers pick by platform extension, TypeScript does not — so both are checked against one signature.
 */
import type { PhotoStore } from "./photos";

export declare function openPhotos(): PhotoStore;
