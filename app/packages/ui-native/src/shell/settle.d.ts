/**
 * The shared declaration for `./settle` (T910): when the web's unasked downloads may start (`@app/ui` `useReleasePrefetch`'s
 * `settle`). `settle.web.ts`: after the page's load event and then `QUIET_MS` with no long task on its main thread (at the
 * latest `QUIET_CAP_MS` after load). `settle.native.ts`: at once (a phone fetches nothing ahead: its release is
 * installed, and the hook is off there). Returns the cancel. Bundlers pick by platform extension, TypeScript does not.
 */
export declare const QUIET_MS: number;
export declare function afterLoadQuiet(go: () => void): () => void;
