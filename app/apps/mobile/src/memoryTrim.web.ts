import type { ReleaseLoader } from "@app/data/release";

/** (T360) Nothing on the web: the browser does not ask for memory back (the phone twin listens, memoryTrim.native.ts). */
export function trimOnPressure(_loader: ReleaseLoader, _failed: (e: unknown) => void): () => void { return () => {}; }
