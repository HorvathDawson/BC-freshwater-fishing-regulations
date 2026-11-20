// (T760) THE APP'S LOG ACROSS MAESTRO FLOWS. Maestro clears the device's log at the start of every flow (2.12.0's
// AndroidDriver runs `logcat -c`, then captures its own copy into the flow's `device-logcat.txt`), so a `logcat -d` taken
// after a flow holds only what came since that flow began. That is why the bench's app-log capture (a `logcat -d` after
// the run) came back empty for every run whose last steps used a flow (2026-11-15, round 2: four runs, 0 bytes), and why
// a scenario's own reads lost the lines from before its last tap. The lines that matter are kept before each flow
// (`device.keepLog`, called by run.mjs's `ctx.flow`) and read back with what the log holds now. Plain functions, tested
// without a device (node --test app/tools/testbed/lib/kept-log.test.mjs).

/** The tags kept: the app's JS (`[download]`, `[boot]`, `[setup]` …) and the system's downloader. */
export const KEPT_TAGS = ['ReactNativeJS', 'DownloadManager'];

const tagged = (tag) => new RegExp(`\\s${tag}\\s*:`);

export function keptLog(tags = KEPT_TAGS) {
  let kept = [];
  const any = new RegExp(`\\s(?:${tags.join('|')})\\s*:`);
  return {
    /** Keep the tagged lines of a `logcat -d` (threadtime) text, before a flow clears it. */
    keep(text) { for (const l of String(text).replace(/\r/g, '').split('\n')) if (any.test(l)) kept.push(l); },
    /** A scenario's own clear (`logcatClear`): what came before is not this step's. */
    clear() { kept = []; },
    /** The `tag` lines: the kept ones, then the ones of `current` (the log now) not already kept, in that order. */
    lines(tag, current = '') {
      const re = tagged(tag);
      const mine = kept.filter((l) => re.test(l));
      const seen = new Set(mine);
      return [...mine, ...String(current).replace(/\r/g, '').split('\n').filter((l) => re.test(l) && !seen.has(l))];
    },
  };
}
