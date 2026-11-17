/**
 * (T738) THE FIRST INSTALL'S "N of M MB downloaded": the bytes already had, summed over EVERY file of the download —
 * the data files fetched, and each big file's bytes so far (its own transfer's count once its turn comes, and before
 * that what the device holds of it: the system downloader's job, its `.part`, the finished file). The system downloads
 * the four big files at once from before the install starts (T298, `bgDownload.ts`), so counting a big file only from
 * its turn read low by everything the others had (T722: 38.8 MB shown with 116.3 MB on disk, after a relaunch).
 *
 * Platform-free: the platforms only feed it (`install.native.ts` `look`: the jobs and the file sizes; the transfers'
 * `onBytes`). One computation for Android and iOS.
 */
import { reportOnce } from "@app/core";
import type { BgJob } from "./bgDownload";

/** A byte count as a platform hands it: a number, or (iOS, an Int64 through expo) a BigInt. Below 0: not known yet. */
export type Bytes = number | bigint;

/**
 * One file's bytes had, 0..`size`. A count below 0 is the platforms' "not known yet" (-1 before the server said): none
 * had yet. A count that is not a number at all is a defect of the feeder and is thrown, never shown as a figure.
 */
export function had(n: Bytes, size: number): number {
  const x = typeof n === "bigint" ? Number(n) : n;
  if (typeof x !== "number" || Number.isNaN(x)) throw new TypeError(`download progress: ${String(n)} is not a byte count`);
  return Math.min(size, Math.max(0, x));
}

/** THE SUM: every file's bytes had, each held to its size, the whole to `of`. */
export function bytesHad(files: readonly { n: Bytes; size: number }[], of: number): number {
  return Math.min(of, files.reduce((s, f) => s + had(f.n, f.size), 0));
}

/** What the device holds of a big file before its turn: the finished file's size, its `.part`, its system download. */
export interface Look { file: number; part: number; job: BgJob | null }

/**
 * What of a big file of `size` bytes a `Look` says is down. The finished file in place (`size` bytes) is all of it; a
 * live system job is its own count (the downloader writes elsewhere, and its file replaces the `.part` when claimed);
 * a job that failed or whose file is gone is fetched again from nothing (`bgDownload` queues it again); no job: the
 * `.part` the app's own transfer resumes from.
 */
export function lookHad(l: Look, size: number): Bytes {
  if (l.file === size) return size;
  const j = l.job;
  if (j) return j.status === "failed" || (j.status === "done" && j.present === false) ? 0 : j.bytes;
  return l.part;
}

/**
 * One install's running figure. `set` a file's latest count (it replaces the last one); `real` is the sum as it is
 * (what decides the last byte is down); `shown` is what the bar says: never above `of`, and never back within the run
 * (a transfer that starts a file again holds the figure until it has caught up).
 */
export function downloadTally(of: number) {
  const files = new Map<string, { n: Bytes; size: number }>();
  let high = 0;
  const real = () => bytesHad([...files.values()], of);
  return {
    set(key: string, n: Bytes, size: number): void {
      // a count that is not one is reported (once: a feeder that sends one sends many) and the file keeps its last
      // count; thrown here it would land in a native progress callback, never on the screen that could show it
      try { had(n, size); } catch (e) { reportOnce("download-count", e); return; }
      files.set(key, { n, size });
    },
    real,
    shown(): number { high = Math.max(high, real()); return high; },
  };
}
