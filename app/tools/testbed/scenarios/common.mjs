// (T255) Steps several scenarios share.

/**
 * The app in the foreground, the map drawn (the tab bar there, no boot screen, no failed-boot words). A dead app is
 * relaunched while waiting, as a reader would (T262: the first install can die of OutOfMemoryError and resumes); the
 * crash itself is still the step's failure (the runner reads the crash buffer after every step).
 */
export async function appUp(ctx, { launch = true, ms = 120_000 } = {}) {
  if (launch) ctx.device.launch();
  const end = Date.now() + ms;
  let w = [];
  while (Date.now() < end) {
    w = ctx.device.screenText();
    if (w.includes('More') || w.includes('MORE')) break;
    // (T298) a first install asks to notify (Android 13+): allowed, as a reader would
    if (w.some((x) => /send you notifications/i.test(x))) ctx.flow('tap-text', { TEXT: 'Allow' });
    // a cleared app shows the first-run notice and the tour over the map: closed as a reader would
    else if (w.some((x) => /^(I understand|Skip|Before you fish|Welcome|Got it)$/.test(x))) ctx.flow('dismiss-first-run');
    // (T706) the notification inbox's sheet, which opens by itself once at a cold start for an unread in-season notice not
    // on the map (it replaced T360's "In-season changes" sheet): detected by its testID, closed as a reader would
    else if (w.includes('Done') && ctx.hasId('inseason-notice')) await closeInbox(ctx);
    else if (!ctx.device.pidOf()) ctx.device.launch();
    await ctx.device.sleep(3000);
  }
  // (T440) one last look: `dismiss-first-run` (Maestro, six optional taps) can outlast the whole wait, and the words
  // read BEFORE it ran ("Before you fish …") failed a step whose app was up by then
  if (!(w.includes('More') || w.includes('MORE'))) w = ctx.device.screenText();
  if (!(w.includes('More') || w.includes('MORE'))) throw new Error(`the app is not up in ${ms / 1000} s (saw: ${w.slice(0, 8).join(' | ').slice(0, 300)})`);
  const bad = w.find((x) => /could not be loaded|Something went wrong|Unable to load/i.test(x));
  if (bad) throw new Error(`the app says: ${bad}`);
}

/**
 * (T706) The notification inbox's sheet (testID `inseason-notice`, closed by its "Done"): the app opens it by itself once at
 * a cold start for an unread in-season notice that is not on the map, and it covers the map. Shown: recorded as a finding
 * in the scenario's report and closed by "Done" as a reader would; a sheet that is still up afterwards is a failed step.
 * Not shown: nothing happens. Returns whether it was shown.
 */
export async function closeInbox(ctx) {
  if (!ctx.hasId('inseason-notice')) return false;
  ctx.finding('the notification inbox sheet (inseason-notice) was showing over the map: closed with Done');
  ctx.flow('tap-text', { TEXT: 'Done' });
  await ctx.until(() => !ctx.hasId('inseason-notice'), { ms: 10_000, what: 'the notification inbox sheet (inseason-notice) closing after Done' });
  return true;
}

/** The app put in the background (Home) and gone from the screen: its tab bar is no longer there. */
export async function toBackground(ctx) {
  ctx.device.background();
  await ctx.until(() => !ctx.device.screenText().some((w) => w === 'More' || w === 'MORE'), { ms: 10_000, what: 'the app leaving the screen' });
}

/** Cold start: force-stop, then launch. */
export async function coldStart(ctx, opts) {
  ctx.device.stop();
  await ctx.device.sleep(800);
  return appUp(ctx, opts);
}

/** The Updates log page, open (Map → long-press © → Developer card → Updates log, T476). */
export function openLog(ctx) {
  const w = ctx.device.screenText();
  if (w.includes('Updates log') && w.includes('Check now')) return;         // already there
  if (!w.includes('MORE')) ctx.device.sh('input keyevent 4');              // a menu page: back to the tabs first
  ctx.flow('open-updates-log');
}

/** The log page's status lines ("Gauges: ok", "Release: ok · waiting-wifi r-…"), host side. */
export function logStatus(ctx) {
  return ctx.device.screenText().find((w) => /Gauges: /.test(w) && /Release: /.test(w)) ?? '';
}

/** The sync icon's spoken state ("Refresh data: up to date"). */
export function syncLabel(ctx) {
  return ctx.device.screenText().find((w) => /^Refresh data: /.test(w)) ?? null;
}

/** Press "Check now" on the log page and wait for the check to finish (a new row whose words match `re`). */
export async function checkNow(ctx, re, ms = 60_000) {
  ctx.flow('updates-log-action', { ACTION: 'check' });
  return ctx.waitText(re, ms);
}

/** Back to the map tab from wherever: system back until the tab bar shows (never past it), then the Map tab. */
export async function toMap(ctx) {
  for (let i = 0; i < 5; i++) {
    const w = ctx.device.screenText();
    if (w.includes('MORE') && w.includes('MAP')) break;
    if (w.some((x) => /^(At a glance|Google search)$/.test(x))) {
      await appUp(ctx);                                        // left the app: back in
      break;
    }
    ctx.device.sh('input keyevent 4');
    await ctx.device.sleep(800);
  }
  const w = ctx.device.screenText();
  if (!(w.includes('MORE') && w.includes('MAP'))) await coldStart(ctx);       // lost: start over (data stays)
  ctx.flow('tab', { TAB: 'Map' });
}

/** (T360/T345) A water left open (its screen hides the tab bar) closed by its own ✕, up to three deep. */
export async function closeWaters(ctx) {
  for (let i = 0; i < 3; i++) {
    ctx.device.sh('uiautomator dump /sdcard/tb-cw.xml >/dev/null 2>&1');
    if (!ctx.device.sh('cat /sdcard/tb-cw.xml').includes('resource-id="water-close"')) return;
    ctx.flow('tap-id', { ID: 'water-close' }); await ctx.device.sleep(1500);
  }
}

/**
 * (T603) A water opened from search on one of its views: the Map tab, the query, its result row, "Open <water>", then the
 * view's tab (`water-view-regs` | `water-view-conditions` | `water-view-map`). Returns when the view strip is up.
 */
export async function openWater(ctx, { q, row, view = 'regs' }) {
  await closeWaters(ctx);
  await toMap(ctx);
  ctx.flow('search', { Q: q });
  ctx.flow('open-result', { LINE: row });
  await ctx.until(() => ctx.hasId('water-view-regs'), { ms: 20_000, what: `the water screen of ${q} (water-view-regs)` });
  if (view !== 'regs') ctx.flow('tap-id', { ID: `water-view-${view}` });
}
