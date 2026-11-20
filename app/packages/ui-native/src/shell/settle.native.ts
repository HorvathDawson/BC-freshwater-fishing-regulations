/** (T910) A phone has nothing to wait for (its release is installed; the prefetch hook is off there). Web twin: settle.web.ts. */
export const QUIET_MS = 0;
export function afterLoadQuiet(go: () => void): () => void { go(); return () => {}; }
