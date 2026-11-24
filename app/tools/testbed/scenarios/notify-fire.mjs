// (T1027) A NOTIFICATION RAISED BY THE BACKGROUND CHECK AND TAPPED, END TO END: the walk for expo-notifications,
// expo-task-manager and expo-background-task, whose own keep rules R8 skips since T1027 (plugins/r8-keep-rules.pro
// rules 8 and 9). A gauge alert is saved through the UI (the permission ask and the channels: expo-notifications; the
// periodic task registered: expo-background-task / expo-task-manager), the check is run by WorkManager's own job
// (`cmd jobscheduler run -f`, the app in the background), and the phone's clock is moved on so the gauge's newest
// reading is older than ALERT_STALE_MS and then ALERT_SILENT_TELL_MS (core alerts.ts): the check raises the "has not
// reported since" notice through `scheduleNotificationAsync` (the Parcelable request in the notification's
// PendingIntent). A tap on it in the shade must open that water's Conditions (the response read back: onTap).
// Read-only on staging (no test-channel write: runs under `run.mjs --no-r2`). The clock is moved as root (`su 0 date`)
// with automatic time off, and put back (automatic time on, the host's time) whatever happens.
//
//   node app/tools/testbed/run.mjs notify-fire        ALERT_Q / ALERT_ROW: the gauged water (as saved-spots)
import { coldStart, openWater, toBackground } from './common.mjs';
import { look, scrollTo, tap } from '../lib/screen.mjs';
import { APP_ID } from '../lib/device.mjs';

const ALERT_Q = process.env.ALERT_Q ?? 'Coquitlam River';
const ALERT_ROW = process.env.ALERT_ROW ?? '.* · Region 2 · MU 2-8.*';
const PANE_BAND = { top: 720, bottom: 2300 };
const HOUR = 3_600_000;
const WORK = 'androidx.work.impl.background.systemjob.SystemJobService';

/** toybox `date -u MMDDhhmmYYYY.ss` for an epoch ms */
const stamp = (ms) => {
  const t = new Date(ms); const p = (n) => String(n).padStart(2, '0');
  return `${p(t.getUTCMonth() + 1)}${p(t.getUTCDate())}${p(t.getUTCHours())}${p(t.getUTCMinutes())}${t.getUTCFullYear()}.${p(t.getUTCSeconds())}`;
};
const deviceMs = (d) => Number(d.sh('date +%s').trim()) * 1000;

/** WorkManager's job(s) of the app: the "Registered jobs" headers of `dumpsys jobscheduler`
 *  (`JOB #u0aN/<id>: <hash> <app>/…SystemJobService`), their JobScheduler ids. WorkManager schedules a periodic work as
 *  a one-off job with a minimum latency, so no "periodic" flag is read; the app's only WorkManager work is the
 *  background check. */
function workJobs(d) {
  const re = new RegExp(`JOB #u\\d+a\\d+/(\\d+): [0-9a-f]+ ${APP_ID.replace(/\./g, '\\.')}/${WORK.replace(/\./g, '\\.')}$`);
  return [...new Set(d.sh('dumpsys jobscheduler').split('\n').map((l) => re.exec(l)?.[1]).filter(Boolean))];
}

/**
 * One run of the background check through WorkManager's job, the app in the background. Resolves once the worker
 * started (expo-background-task's BackgroundTaskWork logs "doWork: Running worker") and WorkManager logged its result,
 * or, when WorkManager's own log level hides that line, once the app's own run list (More → Notifications) grew.
 * The job ids are read again after a clock change: WorkManager reschedules its jobs (new ids) on one.
 */
async function runCheck(ctx) {
  const d = ctx.device;
  let ids = [];
  await ctx.until(() => (ids = workJobs(d)).length > 0, { ms: 30_000, what: `a WorkManager job of ${APP_ID} (dumpsys jobscheduler): the background task registered` });
  d.adb(['logcat', '-c']);
  let ran = [];
  for (let i = 0; i < 3 && !ran.length; i++) {
    for (const id of ids) {
      const out = d.sh(`cmd jobscheduler run -f ${APP_ID} ${id} 2>&1 || true`);
      if (!/Could not find job/.test(out)) ran.push(id);
    }
    if (!ran.length) { await d.sleep(3000); ids = workJobs(d); }
  }
  if (!ran.length) throw new Error(`WorkManager's job(s) ${ids.join(',')} could not be run (cmd jobscheduler run -f)`);
  const log = () => d.adb(['logcat', '-d', '-s', 'BackgroundTaskWork:*', 'WM-WorkerWrapper:*', 'TaskService:*', 'ReactNativeJS:*']).toString();
  await ctx.until(() => /doWork: Running worker/.test(log()), { ms: 60_000, what: 'BackgroundTaskWork starting ("doWork: Running worker")' });
  let end = '';
  await ctx.until(() => {
    end = log().split('\n').find((l) => /Worker result (SUCCESS|FAILURE|RETRY)|doWork: Worker was cancelled/.test(l)) ?? '';
    return !!end;
  }, { ms: 90_000, what: 'WorkManager\'s result for the background check', soft: true });
  if (end && !/SUCCESS/.test(end)) throw new Error(`the background check did not succeed: ${end.trim()}`);
  if (!end) await d.sleep(20_000);   // no result line in this log level: the JS task given its time
  return `job ${ran.join(',')} ran · ${end ? end.replace(/^.*(Worker result)/, '$1').trim().slice(0, 100) : 'worker started (no result line logged)'}`;
}

export const notifyFire = {
  id: 'notify-fire',
  title: 'Notifications (T1027): a gauge alert saved, the background check run by WorkManager, its notice raised and tapped',
  async run(ctx) {
    const d = ctx.device;
    let saved = false;
    let moved = false;
    const putBack = () => {
      d.sh('settings put global auto_time 1');
      d.sh(`su 0 date -u ${stamp(Date.now())} >/dev/null 2>&1 || true`);   // the host's clock; automatic time corrects it too
    };

    await ctx.step('setup: Wi-Fi, a cold start', async () => {
      await d.net('wifi'); d.link('full', 'none');
      await coldStart(ctx);
    }, { shot: false, stop: true });

    await ctx.step(`${ALERT_Q} → Conditions → "Alert me" → Save (the permission allowed)`, async () => {
      await openWater(ctx, { q: ALERT_Q, row: ALERT_ROW, view: 'conditions' });
      await ctx.until(() => ctx.hasId('gauge-card'), { ms: 30_000, what: 'the Conditions view (gauge-card)' });
      tap(d, await scrollTo(d, 'resource-id="alert-me-open"', { ...PANE_BAND, bottom: 1500 }));
      await ctx.until(() => ctx.hasId('alert-form'), { ms: 8000, what: 'the alert editor (alert-form)' });
      tap(d, await scrollTo(d, 'resource-id="alert-form-save"', PANE_BAND));
      let asked = false;
      await ctx.until(() => {
        if (d.screenText().some((x) => /send you notifications/i.test(x))) { asked = true; ctx.flow('tap-text', { TEXT: 'Allow' }); }
        return !ctx.hasId('alert-form');
      }, { ms: 15_000, what: 'the editor closing after Save' });
      await ctx.until(() => ctx.hasId('alert-me-saved'), { ms: 8000, what: '"Saved." on the gauge card (alert-me-saved)' });
      if (!/POST_NOTIFICATIONS: granted=true/.test(d.sh(`dumpsys package ${APP_ID} | grep POST_NOTIFICATIONS`))) {
        throw new Error('POST_NOTIFICATIONS is not granted after the save');
      }
      saved = true;
      return `saved · permission ${asked ? 'asked and allowed' : 'already allowed'}`;
    });

    try {
      await ctx.step('the clock 7 h on (the newest reading stale), the check run in the background: no notice yet', async () => {
        if (!saved) ctx.skip('no alert was saved');
        await toBackground(ctx);
        d.sh('settings put global auto_time 0');
        moved = true;
        d.sh(`su 0 date -u ${stamp(deviceMs(d) + 7 * HOUR)}`);
        await d.sleep(5000);   // WorkManager reschedules on a time change
        const r = await runCheck(ctx);
        const n = d.sh(`dumpsys notification --noredact | grep -c "pkg=${APP_ID}" || true`).trim();
        return `${r} · our notifications in the shade: ${n}`;
      });

      let title = '';
      await ctx.step('31 h more (silent for over a day), the check run again: the "has not reported since" notice is raised', async () => {
        if (!saved) ctx.skip('no alert was saved');
        d.sh(`su 0 date -u ${stamp(deviceMs(d) + 31 * HOUR)}`);
        await d.sleep(5000);
        const r = await runCheck(ctx);
        d.sh('cmd statusbar expand-notifications');
        await ctx.until(() => d.screenText().some((w) => /has not reported since/.test(w)),
          { ms: 20_000, what: 'the silent-gauge notice in the shade ("has not reported since")' });
        const w = d.screenText();
        title = w.find((x) => /: (level|flow|discharge)? ?alert$/i.test(x)) ?? w.find((x) => /alert$/i.test(x)) ?? '';
        if (!title) throw new Error(`the notice has no "<water>: … alert" title (shade: ${w.slice(0, 12).join(' | ').slice(0, 300)})`);
        return `${r} · "${title}"`;
      });

      await ctx.step('a tap on the notice opens that water\'s Conditions', async () => {
        if (!title) ctx.skip('no notice was raised');
        const node = (await look(d)).find((n) => n.text === title);
        if (!node) throw new Error(`the notice "${title}" is no longer in the shade`);
        tap(d, node);
        await ctx.until(() => ctx.hasId('gauge-card'), { ms: 30_000, what: 'the water\'s Conditions (gauge-card) after the tap' });
        return 'Conditions open on the gauge card';
      });
    } finally {
      if (moved) putBack();
    }
  },
};
