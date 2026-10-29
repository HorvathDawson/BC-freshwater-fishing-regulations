// (T255) Steps several scenarios share.

/** The app in the foreground, the map drawn (the tab bar there, no boot screen, no failed-boot words). */
export async function appUp(ctx, { launch = true, ms = 120_000 } = {}) {
  if (launch) ctx.device.launch();
  await ctx.waitText(/^More$/, ms);
  const words = ctx.device.screenText();
  const bad = words.find((w) => /could not be loaded|Something went wrong|Unable to load/i.test(w));
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
  ctx.flow('tab', { TAB: 'Map' });
}
