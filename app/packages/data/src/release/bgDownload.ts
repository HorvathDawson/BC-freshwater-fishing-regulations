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
 *             (T756) One exception: a Wi-Fi-only job that is RUNNING stays when mobile data is now allowed. It is on
 *             an unmetered network, which both choices permit; removing it threw its bytes away (a reopen on Wi-Fi
 *             after "Wait for Wi-Fi" restarted every big file from zero).
 *   fill      `PartFill` over a job: polls it (progress), and once it is done claims its file onto the `.part`. A job
 *             that failed is queued again (a few times); one waiting for Wi-Fi is waited on. A `.part` an older app
 *             left with no job is resumed the old way (`fallback`). (T756) A Wi-Fi-only job that was kept and then
 *             sits paused for `wifiWaitMs` while mobile data is allowed is queued again under the allowed rule, and the
 *             log says so: it would otherwise wait for a Wi-Fi the reader no longer asked it to wait for.
 *             (T760) A job pending, or paused for anything but a wait for a network, with no new byte for `stuckMs` is
 *             queued again (said in the log, its bytes lost); after `stuckTries` the install fails with words.
 *   fetch     (T912) AN UPDATE'S DATA FILES go to the system too (`BgItem.whole`): an update that changes only the
 *             regulations is nothing but data files, and an update that also moves the map fetched them first, in the app,
 *             before any big file was queued — so a reader who closed the app while an update downloaded stopped all of
 *             it. `fetch` waits on such a file's job, claims it beside its install path, and verifies it (gzip or not:
 *             the data host decodes a stored gzip for a client that asks for the bytes as they are) before the install
 *             places it. A file with no job, or whose job failed, vanished, stopped moving or brought other bytes, is
 *             downloaded in the app instead, and the log says why (W46).
 */
import { DOWNLOAD_NOTE_WORDS, INSTALL_WORDS, now as clockNow, reportFailure } from "@app/core";
import { verifyAsync, VerifyError } from "../fetchVerified";
import type { Download, PartFill, PartFs } from "./install";

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

/**
 * A file to queue: its absolute url, what it is (the notification says), its size and where it is installed. `whole`:
 * (T912) a data file — fetched whole by `fetch` (its body may come gzip or decoded, so its length is not known ahead).
 */
export interface BgItem { url: string; file: string; bytes: number; path: string; whole?: boolean }

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

/**
 * (T756) Does a job queued under one mobile-data rule serve the rule now asked? The same rule does. So does a Wi-Fi-only
 * job that is running when mobile data is allowed (it is downloading on an unmetered network), or that already has
 * bytes (a pause between tries must not throw them away: `fill` queues it again only if it stays paused, T756). A job
 * that allows mobile data never serves "Wi-Fi only", and a Wi-Fi-only job with nothing down yet does not serve "use
 * mobile data" (nothing is lost by queueing it again).
 */
const serves = (job: BgJob, allowMetered: boolean) =>
  job.metered === allowMetered || (allowMetered && !job.metered && (job.status === "running" || job.bytes > 0));

export interface Background {
  /** Enqueue every file not yet in place; `metered`: may they use mobile data (false: they wait for Wi-Fi). */
  queue(items: BgItem[], metered: boolean): Promise<void>;
  /** The `PartFill` that waits on (or starts) a file's job and claims it onto the `.part`. */
  fill: PartFill;
  /** (T912) A data file's verified bytes: from its queued job when there is one, else (or when the job fails) `inApp`. */
  fetch(url: string, expect: { bytes: number; sha256: string }, inApp: Download): Promise<Uint8Array>;
}

/**
 * (T760) Android's DownloadManager reasons for a PAUSED job that are honest waits for a network
 * (`DownloadManager.PAUSED_WAITING_FOR_NETWORK` = 2, `PAUSED_QUEUED_FOR_WIFI` = 3; the module passes `COLUMN_REASON`
 * through as is). Such a job is never "stuck": it starts by itself when the network it needs is up.
 * `PAUSED_WAITING_TO_RETRY` (1) and `PAUSED_UNKNOWN` (4) are not.
 */
const NETWORK_WAITS: ReadonlySet<number> = new Set([2, 3]);

/** (T760) How long a job may sit pending, or paused not waiting for a network, with no new byte before it is queued again. */
const STUCK_MS = 10 * 60_000;
/** (T760) How many times a file that does not move is queued again before the install fails (`INSTALL_WORDS.stuck`). */
const STUCK_TRIES = 2;

interface BackgroundOptions {
  /** how often a job is polled */
  pollMs?: number;
  /** how often a FAILED job (or one of the wrong size, or gone) is queued again before the install fails */
  tries?: number;
  /** (T756) how long a kept Wi-Fi-only job may sit paused while mobile data is allowed before it is queued to use it */
  wifiWaitMs?: number;
  /**
   * (T760) how long a job may sit pending, or paused for a reason that is not a wait for a network, with no new byte,
   * before it is removed and queued again. Ten minutes: the downloader's own back-off after read errors is believed to
   * wait up to ~8 minutes before a try, and a removed job's bytes are lost (it cannot be resumed).
   */
  stuckMs?: number;
  /** (T760) how many such re-queues of one file before the install fails */
  stuckTries?: number;
}

export function background(bg: BackgroundNative, fs: PartFs & { uri(path: string): string; read(path: string): Promise<Uint8Array> },
                           fallback: PartFill,
                           o: BackgroundOptions = {}): Background {
  const pollMs = o.pollMs ?? 500, tries = o.tries ?? 4, wifiWaitMs = o.wifiWaitMs ?? 20_000;
  const stuckMs = o.stuckMs ?? STUCK_MS, stuckTries = o.stuckTries ?? STUCK_TRIES;
  /** what each url is (for a job started by `fill` itself: an update's file, or one whose job vanished) */
  const kinds = new Map<string, string>();
  /** (T912) the data files queued (`whole`), by url: where each is installed (its job is claimed beside it) */
  const wholes = new Map<string, string>();
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
        if (it.whole) wholes.set(it.url, it.path);
        const job = jobs.find((j) => j.url === it.url) ?? null;
        if (job?.status === "done" && !stale(job)) continue;                      // claimed when the install reaches it
        if (await fs.size(it.path) === it.bytes) continue;                        // in place (verified by the install)
        if (!it.whole && !job && await fs.size(`${it.path}.part`) > 0) continue;  // an older app's .part: resumed as before
        if (job && job.status !== "failed" && !stale(job) && serves(job, allowMetered)) continue;
        if (job) {
          // (T756 / T760, W46) never a silent restart: what is thrown away, and why (the bench reads this line)
          const why = job.status === "failed" ? `it failed (reason ${job.reason})`
            : stale(job) ? "it finished, but its file is gone (the app's data was cleared)"
            : `it was queued ${job.metered ? "to use mobile data" : "for Wi-Fi only"} and ${allowMetered ? "mobile data is now allowed" : "Wi-Fi only is now asked"}`;
          console.info(`[download] ${it.url}: queued again from zero (was ${job.status}, ${Math.max(0, job.bytes)} of ${it.bytes} bytes): ${why}`);
          await bg.remove(job.id);
        }
        await enqueue(it.url, it.bytes);
      }
    },

    fill: async (url, part, have, total, onBytes, onRewrite) => {
      const first = (await bg.jobs()).find((j) => j.url === url) ?? null;
      if (!first && have > 0) return fallback(url, part, have, total, onBytes, onRewrite);
      onRewrite?.();                                         // the job's file replaces whatever is in the .part
      let id = first?.id ?? await enqueue(url, total);
      let failures = 0, said = "", pausedAt = 0;
      // (T760) when the job's byte count last changed (or it was queued, or stopped honestly waiting), and how often a
      // job of this file that did not move was queued again (never reset: a file is never queued again for ever)
      let movedAt = clockNow(), lastBytes = -1, requeued = 0;
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
        // (T756) a Wi-Fi-only job (kept by `queue` because it was running) that now waits, while mobile data is allowed
        if (job!.status === "paused" && !job!.metered && metered) {
          pausedAt ||= clockNow();
          if (clockNow() - pausedAt >= wifiWaitMs) {
            console.info(`[download] ${url}: a Wi-Fi-only download paused (reason ${job!.reason}) at ${job!.bytes} of ${total} bytes while mobile data is allowed; queued again to use it`);
            await bg.remove(job!.id);
            id = await enqueue(url, total);
            pausedAt = 0; said = ""; movedAt = clockNow(); lastBytes = -1;
            continue;
          }
        } else pausedAt = 0;
        // (T760) a job the system has parked and does not run again (a stale row a cleared app adopted, a deferred job,
        // the downloader's own back-off gone quiet): pending, or paused for a reason that is not a wait for a network,
        // with no new byte for `stuckMs`: removed and queued again (its bytes are lost: a removed job cannot be resumed);
        // `stuckTries` times, then the install fails with the reader's words, never an endless wait (W46)
        const parked = job!.status === "pending" || (job!.status === "paused" && !NETWORK_WAITS.has(job!.reason));
        if (!parked || job!.bytes !== lastBytes) { lastBytes = job!.bytes; movedAt = clockNow(); }
        else if (clockNow() - movedAt >= stuckMs) {
          const still = `not moved for ${Math.round((clockNow() - movedAt) / 1000)} s (${job!.status}, reason ${job!.reason}, ${Math.max(0, job!.bytes)} of ${total} bytes)`;
          if (++requeued > stuckTries) {
            console.warn(`[download] ${url}: ${still} after ${stuckTries} times queued again; the install fails`);
            throw new Error(INSTALL_WORDS.stuck);
          }
          console.info(`[download] ${url}: ${still}; removed and queued again from zero (${requeued} of ${stuckTries}): the system cannot resume a removed download, those bytes are lost`);
          await bg.remove(job!.id);
          id = await enqueue(url, total);
          said = ""; movedAt = clockNow(); lastBytes = -1;
          continue;
        }
        const now = job!.status === "paused" ? `paused (reason ${job!.reason})` : job!.status;
        if (now !== said) { console.info(`[download] ${url}: ${now}, ${job!.bytes} of ${total} bytes`); said = now; }
        await wait(pollMs);
      }
    },

    async fetch(url, expect, inApp) {
      const to = wholes.get(url);
      let job = to ? (await bg.jobs()).find((j) => j.url === url) ?? null : null;
      if (!to || !job) return inApp(url, expect);                 // not queued (a first install's data file): as before
      // (W46) never a silent switch: why the system's download was given up, then the app's own download
      const instead = async (why: string) => {
        console.warn(`[download] ${url}: ${why}; downloaded in the app instead`);
        if (job) await bg.remove(job.id);
        job = null;
        return inApp(url, expect);
      };
      let movedAt = clockNow(), lastBytes = -1, pausedAt = 0, said = "";
      for (;;) {
        const id: number = job!.id;
        job = (await bg.jobs()).find((j) => j.id === id) ?? null;
        if (!job) return instead("its system download is gone");
        if (job.status === "failed") return instead(`its system download failed (reason ${job.reason})`);
        if (stale(job)) return instead("its system download finished, but its file is gone (the app's data was cleared)");
        if (job.status === "done") {
          const at = `${to}.dl`;
          await bg.claim(job.id, fs.uri(at));
          job = null;                                             // claimed: the system has forgotten it
          let body: Uint8Array;
          try { body = await fs.read(at); } finally { await fs.remove(at); }
          let raw: Uint8Array;
          try { raw = await verifyAsync(url, body, expect); }
          catch (e) {
            // not the manifest's bytes (or a gzip body cut short): reported (the system's downloader brought a file the
            // host did not serve as it should), and the app's own download decides
            reportFailure(e);
            const why = e instanceof VerifyError ? e.message : `it would not unpack: ${e instanceof Error ? e.message : String(e)}`;
            return instead(`its system download brought other bytes (${why})`);
          }
          console.info(`[download] ${url}: done (system download manager), ${expect.bytes} bytes`);
          return raw;
        }
        // the same waits as `fill`: a Wi-Fi-only job paused while mobile data is allowed, or one parked with no new byte
        if (job.status === "paused" && !job.metered && metered) {
          pausedAt ||= clockNow();
          if (clockNow() - pausedAt >= wifiWaitMs) return instead(`a Wi-Fi-only download paused (reason ${job.reason}) while mobile data is allowed`);
        } else pausedAt = 0;
        const parked = job.status === "pending" || (job.status === "paused" && !NETWORK_WAITS.has(job.reason));
        if (!parked || job.bytes !== lastBytes) { lastBytes = job.bytes; movedAt = clockNow(); }
        else if (clockNow() - movedAt >= stuckMs)
          return instead(`its system download has not moved for ${Math.round((clockNow() - movedAt) / 1000)} s (${job.status}, reason ${job.reason})`);
        const now = job.status === "paused" ? `paused (reason ${job.reason})` : job.status;
        if (now !== said) { console.info(`[download] ${url}: ${now}, ${Math.max(0, job.bytes)} bytes (system download manager)`); said = now; }
        await wait(pollMs);
      }
    },
  };
}
