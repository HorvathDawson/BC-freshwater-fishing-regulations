/**
 * (T360) THE OS ASKING FOR MEMORY BACK. Android: `onTrimMemory`, from modules/memory-trim (Kotlin), which has already told
 * every MapLibre map to drop what it is not drawing. iOS: React Native's `memoryWarning` (MapLibre's iOS map listens for
 * the same warning itself). Either way the release's open databases give back their page caches (`ReleaseLoader.trim`).
 * Nothing on the web (memoryTrim.web.ts).
 */
import { AppState, Platform } from "react-native";
import { requireOptionalNativeModule } from "expo";
import type { ReleaseLoader } from "@app/data/release";

interface TrimEvent { level: number; maps: number; purged: number }
const native = requireOptionalNativeModule<{ addListener(event: "trim", f: (e: TrimEvent) => void): { remove(): void } }>("MemoryTrim");

/** A database not read for this long is closed on a trim. */
const IDLE_MS = 60_000;

/** Shrink `loader` on every trim; the returned function stops listening. A shrink that fails is handed to `failed`. */
export function trimOnPressure(loader: ReleaseLoader, failed: (e: unknown) => void): () => void {
  // (T360) then the connections no screen has read for a minute are closed (the next read opens one again)
  const trim = (what: string) =>
    loader.trim().then(async (n) => {
      const closed = await loader.closeIdle?.(IDLE_MS) ?? 0;
      console.info(`[trim] ${what}, ${n} database(s) shrunk, ${closed} idle closed`);
    }).catch(failed);
  const sub = native ? native.addListener("trim", (e) => {
      // the allocator's purge (memorytrim.cpp): -1 is the native library missing from the build, said and reported
      if (e.purged < 0) failed(new Error("memory-trim: libmemorytrim is not in this build (no purge on a trim)"));
      void trim(`level ${e.level}: ${e.maps} map(s), purge ${e.purged}`);
    })
    : Platform.OS === "ios" ? AppState.addEventListener("memoryWarning", () => trim("iOS memory warning"))
    : null;
  return () => sub?.remove();
}
