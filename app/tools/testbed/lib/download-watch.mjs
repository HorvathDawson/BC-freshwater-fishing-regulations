// (T756) WHAT THE FIRST-INSTALL BENCH READS FROM THE SYSTEM'S DOWNLOADER, as plain functions so they are tested without
// a device (node --test app/tools/testbed/lib/download-watch.test.mjs).
//
//   thrownAway(jsLines)    the downloads the APP queued again from zero while they had bytes down: its own line
//                           "[download] <url>: queued again from zero (was <status>, N of M bytes): <why>"
//                           (`bgDownload.ts` `queue`). In "Wait for Wi-Fi, then reopened on Wi-Fi" there must be none.
//                           NOT read from the system downloader's log: its "onStopJob … cancel() called by app" is
//                           the downloader's own call (its uid) and also follows every "WAITING_TO_RETRY … rescheduling",
//                           so it fires on any network retry (seen 2026-11-15, T760).
//   diskBytes(ls)           the bytes the downloader has on disk for us (`ls -l` of its folder).
//   stallWatch(stallMs)     see(value, now) → ms since `value` last changed; `stalled` once that passes stallMs. The
//                           step waits for as long as bytes move, and fails on a stall, whatever the line's speed.

const AGAIN = /\[download\] (\S+): queued again from zero \(was (\w+), (\d+) of (\d+) bytes\): (.*)$/;

export function thrownAway(jsLines) {
  const out = [];
  for (const l of jsLines) {
    const m = AGAIN.exec(l);
    // a FAILED job (or a finished one whose file is gone) cannot be resumed: queueing it again loses nothing the app could keep
    if (m && Number(m[3]) > 0 && m[2] !== 'failed' && m[2] !== 'done') out.push({ url: m[1], was: m[2], bytes: Number(m[3]), total: Number(m[4]), why: m[5].trim() });
  }
  return out;
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
