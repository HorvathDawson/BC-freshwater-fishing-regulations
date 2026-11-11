/**
 * Spots on a phone: ONE FILE, and not the bundle's.
 *
 * `model.ts` states the rule this exists to obey — the regulation bundle is replaced
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
import { reportFailure } from "@app/core";
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
    write: (n, t) => { if (!dir.exists) dir.create({ intermediates: true }); at(n).writeSync(t); },
    // expo-file-system's move refuses an existing destination: replace it explicitly. (T446) The SYNC calls: since SDK 58
    // `write`/`move` return a promise, and `core.ts`'s save (write, then rename) moved the file before its bytes were in it
    rename: (a, b) => { const to = at(b); if (to.exists) to.delete(); at(a).moveSync(to); },
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

/** (T211) The last captured DFO salmon feed (owner 2026-10-27: offline, the last feed captured) — a file of its own. */
export function openSalmonFeedStore(): { read(): string | null; write(text: string): void } {
  return fileStore("dfo-feed.json");
}

/** (T315) The gauges' last week, kept for offline (`@app/data` `gaugeHistory`) — a cache file of its own. */
export function openGaugeHistoryStore(): { read(): string | null; write(text: string): void } | null {
  return fileStore("gauge-history.json");
}

/** (T230) The last captured in-season feed + links, and the in-season ids this device has been shown — files of their own. */
export function openInSeasonStore(): { read(): string | null; write(text: string): void } {
  return fileStore("inseason-feed.json");
}
export function openInSeasonSeenStore(): { read(): string | null; write(text: string): void } {
  return fileStore("inseason-seen.json");
}


/** (T231) The update service's small state (`@app/data` `UpdateStore`): validators, times, the metered setting. */
export function openUpdateStore(): { read(): string | null; write(text: string): void } {
  return fileStore("updates.json");
}

/** (T251) The Updates log's ring buffer (development and staging builds only), as whole text. */
export function openUpdateLogStore(): { read(): string | null; write(text: string): void } {
  return fileStore("update-log.json");
}

/** (T237) Problem reports waiting to be sent (offline), as whole text (a JSON list). */
export function openReportOutboxStore(): { read(): string | null; write(text: string): void } {
  return fileStore("report-outbox.json");
}

/** (T242) The "Send crash reports" switch ("on" | "off"; nothing stored = on), a small file of its own. */
export function openCrashSettingStore(): { read(): string | null; write(text: string): void } {
  return fileStore("crash-reports.txt");
}

/** (T242) The newest Android exit record already looked at (epoch ms as text), a small file of its own. */
export function openCrashExitStore(): { read(): string | null; write(text: string): void } {
  return fileStore("crash-exit-seen.txt");
}

/** (T242) A fatal error kept for the next start (one JSON payload, or empty), a small file of its own. */
export function openCrashPendingStore(): { read(): string | null; write(text: string): void } {
  return fileStore("crash-pending.json");
}

/** (T352) The random install id crash and problem reports carry (16 hex characters, made once), one key of its own. */
export function openInstallIdStore(): { read(): string | null; write(text: string): void } {
  return fileStore("install-id.txt");
}

/** (T236) The first-run notice's accepted version (`FIRST_RUN_VERSION`), a small file of its own. */
export function openFirstRunStore(): { read(): string | null; write(text: string): void } {
  return fileStore("first-run.txt");
}

/** (T248) The first-run walkthrough's seen version (`WALKTHROUGH_VERSION`), a small file of its own. */
export function openWalkthroughStore(): { read(): string | null; write(text: string): void } {
  return fileStore("walkthrough.txt");
}

/** (T384) Whether the first open has asked for the reader's location ("asked"), a small file of its own: asked once. */
export function openLocationAskStore(): { read(): string | null; write(text: string): void } {
  return fileStore("location-ask.txt");
}

/** (T390) The once-a-day newer-app check: the device day it was asked and the answer (`@app/core` `AppUpdateSaved`). */
export function openAppUpdateStore(): { read(): string | null; write(text: string): void } {
  return fileStore("app-update.json");
}

/** (T247) The recent searches (a JSON list), a small file of its own; device-only. */
export function openRecentSearchStore(): { read(): string | null; write(text: string): void } {
  return fileStore("recent-searches.json");
}

/** (T261) The species the Runs tab paints (a JSON list of names), kept between visits; device-only. */
export function openRunSpeciesStore(): { read(): string | null; write(text: string): void } {
  return fileStore("run-species.json");
}

/** (T234) The notification settings, alert rules and background-check log (`@app/data` `NotifySettings`), as whole text. */
export function openNotifyStore(): { read(): string | null; write(text: string): void; keepAside(text: string): void } {
  const dir = new Directory(Paths.document, "spots");
  return {
    ...fileStore("notify.json"),
    // (T554 item 2) an unreadable notify.json is copied aside before anything writes over it
    keepAside: (t) => { if (!dir.exists) dir.create({ intermediates: true }); new File(dir, `notify.json.corrupt-${Date.now()}`).writeSync(t); },
  };
}

/** (T254) The Data setting for low-bandwidth mode ("auto" | "on" | "off"), a small file of its own. */
export function openDataModeStore(): { read(): string | null; write(text: string): void } {
  return fileStore("low-data.txt");
}

/**
 * A small file of its own. (T554 item 2) THE WRITE IS ATOMIC: the text goes to `<name>.part`, which is then moved over
 * the file (as the spots file's save, core.ts), so a write cut short leaves the old file whole, never a torn one. A read
 * that fails is REPORTED (it once was swallowed into "nothing stored") and answers null.
 */
function fileStore(name: string): { read(): string | null; write(text: string): void } {
  const dir = new Directory(Paths.document, "spots");
  const file = () => new File(dir, name);
  return {
    read: () => {
      try { const f = file(); return f.exists ? f.textSync() : null; }
      catch (e) { reportFailure(new Error(`${name} could not be read: ${e instanceof Error ? e.message : String(e)}`)); return null; }
    },
    write: (t) => {
      if (!dir.exists) dir.create({ intermediates: true });
      const part = new File(dir, `${name}.part`);
      part.writeSync(t);
      const to = file();
      if (to.exists) to.delete();
      part.moveSync(to);
    },
  };
}
