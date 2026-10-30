/**
 * (T298) THE FIRST INSTALL GOES ON WITH THE APP CLOSED: the big files (tiles, mask, basemap, rule book — ~95 % of the
 * bytes) are handed to the platform's own downloader, which runs in its own process with its own progress
 * notification. On Android that is the system's DownloadManager (`apps/mobile/modules/bg-download`, Kotlin); this file
 * is the platform-free half — what is queued, how a job is waited on, and how its file becomes the `.part` that
 * `partDownload` verifies (sha256) and renames. Tested with a fake downloader (`bgDownload.test.ts`).
 *
 *   queue     before the install starts, every big file the set needs is enqueued at once (closing the app a second
 *             later still downloads all of them). A job already there is kept — unless it was queued under the other
 *             mobile-data choice (it is queued again: the system cannot change a job's network) or it failed.
 *   fill      `PartFill` over a job: polls it (progress), and once it is done claims its file onto the `.part`. A job
 *             that failed is queued again (a few times); one waiting for Wi-Fi is waited on. A `.part` an older app
 *             left with no job is resumed the old way (`fallback`).
 */
import { DOWNLOAD_NOTE_WORDS } from "@app/core";
import type { PartFill, PartFs } from "./install";

/** One of this app's downloads, as the platform's downloader reports it. `total` is -1 until the server said. */
export interface BgJob {
  id: number;
  url: string;
  status: "pending" | "running" | "paused" | "done" | "failed";
  /** the platform's reason code (why paused / why failed) */
  reason: number;
  bytes: number;
  total: number;
  /** queued to use mobile data (false: waits for an unmetered network) */
  metered: boolean;
  /**
   * its file is on disk. A "done" job without one is stale: the app's data was cleared (the file with it) while the
   * system kept its record of the job — it is queued again, never claimed.
   */
  present?: boolean;
}

/** A finished job whose file is gone (see `BgJob.present`). */
const stale = (j: BgJob) => j.status === "done" && j.present === false;

/** The platform's downloader (Android: `BgDownload`, the system's DownloadManager). */
export interface BackgroundNative {
  jobs(): Promise<BgJob[]>;
  /** `name`: its file in the downloader's folder; `title`/`text`: the notification's two lines. */
  enqueue(url: string, name: string, title: string, text: string, metered: boolean): Promise<number>;
  remove(id: number): Promise<void>;
  /** Move a finished job's file onto `toUri` (replacing it) and forget the job. */
  claim(id: number, toUri: string): Promise<void>;
  /** The sha256 (hex) of a file, natively. */
  sha256(uri: string): Promise<string>;
  /** (Android 13+) may the app notify — the system's download notification shows only then */
  notificationsAllowed?(): Promise<boolean>;
  /** Ask for it (the system's dialog); resolves whether it was granted. */
  askNotifications?(): Promise<boolean>;
}

/** A big file to queue: its absolute url, what it is (the notification says), its size and where it is installed. */
export interface BgItem { url: string; file: string; bytes: number; path: string }

/** The notification's second line for a file ("Map of the waters · 185 MB"). */
function noteFor(file: string | undefined, bytes: number): { title: string; text: string } {
  const W = DOWNLOAD_NOTE_WORDS;
  const what = (file && (W.files as Record<string, string>)[file]) || W.other;
  return { title: W.title, text: W.text(what, bytes) };
}

/** The downloader's file name for a url: its last segment and size (two releases never share one). */
const nameFor = (url: string, bytes: number) =>
  `${(url.split("?")[0]!.split("/").pop() || "file").replace(/[^A-Za-z0-9._-]/g, "_")}-${bytes}`;

const wait = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export interface Background {
  /** Enqueue every big file not yet in place; `metered`: may they use mobile data (false: they wait for Wi-Fi). */
  queue(items: BgItem[], metered: boolean): Promise<void>;
  /** The `PartFill` that waits on (or starts) a file's job and claims it onto the `.part`. */
  fill: PartFill;
}

export function background(bg: BackgroundNative, fs: PartFs & { uri(path: string): string }, fallback: PartFill,
                           o: { pollMs?: number; tries?: number } = {}): Background {
  const pollMs = o.pollMs ?? 500, tries = o.tries ?? 4;
  /** what each url is (for a job started by `fill` itself: an update's file, or one whose job vanished) */
  const kinds = new Map<string, string>();
  /** the last mobile-data choice (`queue`'s); an update's job started by `fill` uses it too */
  let metered = true;

  const enqueue = (url: string, bytes: number) => {
    const n = noteFor(kinds.get(url), bytes);
    return bg.enqueue(url, nameFor(url, bytes), n.title, n.text, metered);
  };

  return {
    async queue(items, allowMetered) {
      metered = allowMetered;
      const jobs = await bg.jobs();
      for (const it of items) {
        kinds.set(it.url, it.file);
        const job = jobs.find((j) => j.url === it.url) ?? null;
        if (job?.status === "done" && !stale(job)) continue;                      // claimed when the install reaches it
        if (await fs.size(it.path) === it.bytes) continue;                        // in place (verified by the install)
        if (!job && await fs.size(`${it.path}.part`) > 0) continue;               // an older app's .part: resumed as before
        if (job && job.status !== "failed" && !stale(job) && job.metered === allowMetered) continue;
        if (job) await bg.remove(job.id);
        await enqueue(it.url, it.bytes);
      }
    },

    fill: async (url, part, have, total, onBytes, onRewrite) => {
      const first = (await bg.jobs()).find((j) => j.url === url) ?? null;
      if (!first && have > 0) return fallback(url, part, have, total, onBytes, onRewrite);
      onRewrite?.();                                         // the job's file replaces whatever is in the .part
      let id = first?.id ?? await enqueue(url, total);
      let failures = 0, said = "";
      for (;;) {
        const job = (await bg.jobs()).find((j) => j.id === id) ?? null;
        const bad = !job ? "is gone" : job.status === "failed" ? `failed (reason ${job.reason})`
          : job.total > 0 && job.total !== total ? `is ${job.total} bytes, the manifest says ${total}`
          : job.status === "done" && job.bytes !== total ? `ended at ${job.bytes} of ${total} bytes`
          : stale(job) ? "finished, but its file is gone (the app's data was cleared)" : null;
        if (bad) {
          // failed, the wrong size, or cancelled from the system's Downloads: queued again, a few times
          if (++failures > tries) throw new Error(`${url}: the download ${bad}`);
          console.warn(`[download] ${url}: the download ${bad}; queued again (${failures})`);
          if (job) await bg.remove(job.id);
          await wait(1500 * 2 ** (failures - 1));
          id = await enqueue(url, total);
          continue;
        }
        if (job!.status === "done") {
          await bg.claim(id, fs.uri(part));
          onBytes?.(total);
          console.info(`[download] ${url}: done (system download manager), ${total} bytes`);
          return;
        }
        onBytes?.(Math.max(0, job!.bytes));
        const now = job!.status === "paused" ? `paused (reason ${job!.reason})` : job!.status;
        if (now !== said) { console.info(`[download] ${url}: ${now}, ${job!.bytes} of ${total} bytes`); said = now; }
        await wait(pollMs);
      }
    },
  };
}
