/**
 * The shared declaration for `./photoStore` (S1), as `store.d.ts` is for the spots store.
 *
 * `photoStore.native.ts` keeps files in `documentDirectory/spots/photos/`; `photoStore.web.ts` keeps blobs in
 * IndexedDB. Bundlers pick by platform extension, TypeScript does not — so both are checked against one signature.
 */
import type { PhotoFiles, PhotoStore } from "./photos";

/** The platform's file half (the web's takes an IndexedDB factory, for a test; the phone's takes nothing). */
export declare function photoFiles(idb?: IDBFactory): PhotoFiles;
export declare function openPhotos(stamp?: () => string): PhotoStore;
