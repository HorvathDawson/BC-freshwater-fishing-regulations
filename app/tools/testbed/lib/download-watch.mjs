// (T756) WHAT THE FIRST-INSTALL BENCH READS FROM THE SYSTEM'S DOWNLOADER, as plain functions so they are tested without
// a device (node --test app/tools/testbed/lib/download-watch.test.mjs).
//
//   cancelledByApp(lines)   the RUNNING jobs the app itself stopped ("onStopJob id=N, reason=cancel() called by app").
//                           In "Wait for Wi-Fi, then reopened on Wi-Fi" there must be none: a stopped job's bytes are
//                           deleted and the file starts again from zero. A finished file's row being deleted ("Deleting
//                           … via provider delete" with no onStopJob) is the app tidying up, not a cancel.
//   diskBytes(ls)           the bytes the downloader has on disk for us (`ls -l` of its folder).
//   stallWatch(stallMs)     see(value, now) → ms since `value` last changed; `stalled` once that passes stallMs. The
//                           step waits for as long as bytes move, and fails on a stall, whatever the line's speed.

const CANCEL = /DownloadManager: onStopJob id=(\d+), reason=cancel\(\) called by app/;

export function cancelledByApp(lines) {
  const seen = new Map();
  for (const l of lines) {
    const m = CANCEL.exec(l);
    if (m && !seen.has(m[1])) seen.set(m[1], l.trim());
  }
  return [...seen].map(([id, line]) => ({ id: Number(id), line }));
}

export function diskBytes(ls) {
  let n = 0;
  for (const l of String(ls).split('\n')) {
    const c = l.trim().split(/\s+/);
    // -rw-rw---- 1 u0_a94 ext_data_rw 197196706 2026-11-15 11:02 name
    if (c.length >= 8 && /^-/.test(c[0]) && /^\d+$/.test(c[4])) n += Number(c[4]);
  }
  return n;
}

export function stallWatch(stallMs) {
  let last, at;
  return {
    see(value, now = Date.now()) {
      if (at === undefined || value !== last) { last = value; at = now; }
      return now - at;
    },
    stalled(now = Date.now()) { return at !== undefined && now - at >= stallMs; },
  };
}
