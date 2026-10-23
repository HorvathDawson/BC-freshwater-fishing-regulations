/**
 * Spots on a phone: ONE FILE, and not the bundle's.
 *
 * `pins.ts` states the rule this exists to obey — the regulation bundle is replaced
 * wholesale on every update (staging, verify, atomic swap), and anything stored inside it
 * is destroyed by an update. A person's notes and photographs are the one thing in this
 * app that cannot be re-downloaded, so they get a file of their own that no update touches.
 *
 * Written whole rather than appended: the list is small (a keen angler might reach a few
 * hundred), and a whole-file write to a temp path plus a rename cannot leave a half-written
 * record behind. An append-log would be faster and would eventually lose the last entry to
 * a crash, which is the entry someone just made.
 */
import { Directory, File, Paths } from "expo-file-system";
import { openSpotsWith, type SpotsFs, type SpotsStore } from "./core";
import { openPhotos } from "./photoStore.native";

const NAME = "spots.spots";

/** The file rules (atomic save, recovery, a corrupt file kept aside, one write at a time) are `core.ts`'s; this only
 *  says how to touch a file on the phone. */
export function openSpots(): SpotsStore {
  const dir = new Directory(Paths.document, "spots");
  const at = (n: string) => new File(dir, n);
  const fs: SpotsFs = {
    exists: (n) => at(n).exists,
    read: (n) => at(n).textSync(),
    write: (n, t) => { if (!dir.exists) dir.create({ intermediates: true }); at(n).write(t); },
    // expo-file-system's move refuses an existing destination: replace it explicitly
    rename: (a, b) => { const to = at(b); if (to.exists) to.delete(); at(a).move(to); },
    remove: (n) => { const f = at(n); if (f.exists) f.delete(); },
  };
  // a spot deleted takes its photos with it (S1)
  return openSpotsWith(fs, NAME, Date.now, openPhotos());
}

/**
 * THE LAYERS SHEET'S CHOICE (owner 2026-10-22: the Spots switch, and every other, kept across a launch): a small file
 * of its own beside the spots file, never inside it. Unreadable reads as nothing stored; a failed write throws for
 * `@app/ui` `useLayerChoice` to shrug off — it is a preference, not a record.
 */
export function openLayerStore(): { read(): string | null; write(text: string): void } {
  return fileStore("layers.json");
}

/** T51: the last fetched "Like your visit?" forecasts, a small file of their own — a cache, never inside the spots. */
export function openLikeStore(): { read(): string | null; write(text: string): void } {
  return fileStore("like-visit.json");
}

function fileStore(name: string): { read(): string | null; write(text: string): void } {
  const dir = new Directory(Paths.document, "spots");
  const file = () => new File(dir, name);
  return {
    read: () => { try { const f = file(); return f.exists ? f.textSync() : null; } catch { return null; } },
    write: (t) => { if (!dir.exists) dir.create({ intermediates: true }); file().write(t); },
  };
}
