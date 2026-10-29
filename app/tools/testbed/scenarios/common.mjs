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
    // a cleared app shows the first-run notice and the tour over the map: closed as a reader would
    if (w.some((x) => /^(I understand|Skip|Before you fish|Welcome)$/.test(x))) ctx.flow('dismiss-first-run');
    else if (!ctx.device.pidOf()) ctx.device.launch();
    await ctx.device.sleep(3000);
  }
  if (!(w.includes('More') || w.includes('MORE'))) throw new Error(`the app is not up in ${ms / 1000} s (saw: ${w.slice(0, 8).join(' | ').slice(0, 300)})`);
  const bad = w.find((x) => /could not be loaded|Something went wrong|Unable to load/i.test(x));
  if (bad) throw new Error(`the app says: ${bad}`);
}

/** Cold start: force-stop, then launch. */
export async function coldStart(ctx, opts) {
  ctx.device.stop();
  await ctx.device.sleep(800);
  return appUp(ctx, opts);
}

/** The Updates log page, open (More → About → Updates log). */
export function openLog(ctx) {
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
