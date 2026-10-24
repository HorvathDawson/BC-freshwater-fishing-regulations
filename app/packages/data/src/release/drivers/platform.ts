/**
 * THE RELEASE'S PLATFORM HALF (P11), whichever platform this is: the app's one import of it (`@app/data/release/platform`).
 *
 * The split is on the RELATIVE imports below, never on this file's name — a package `exports` target is not subject to
 * platform-extension resolution: an exports map naming one file once handed the web the native driver, and the web app
 * loaded expo-sqlite's OPFS worker and hung on a spinner with no error. Metro hands the web
 * `*.web.ts` and the phone `*.native.ts`; TypeScript checks both against the `*.d.ts` beside them.
 */
export { releaseOpener, type OpenAt } from "./open";
export { releaseCache, type ByteCache } from "./cache";
export { releaseSource, type SourceAt } from "./source";
export { device, type Device } from "./install";
