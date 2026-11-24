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

/** WorkManager's periodic job(s) of the app (the background check every 15 min): their JobScheduler ids */
function periodicJobs(d) {
  const blocks = d.sh('dumpsys jobscheduler').split(/\n\s*JOB #/).filter((b) => b.includes(`${APP_ID}/${WORK}`));
  return [...new Set(blocks.filter((b) => /Periodic|interval=\+15m/i.test(b)).map((b) => /^u\d+a\d+\/(\d+):/.exec(b)?.[1]).filter(Boolean))];
}

/** one run of the background check through WorkManager's job, the app in the background; resolves on its result line */
async function runCheck(ctx) {
  const d = ctx.device;
  const ids = periodicJobs(d);
  if (!ids.length) throw new Error(`no periodic WorkManager job of ${APP_ID} (dumpsys jobscheduler): the background task is not registered`);
  d.adb(['logcat', '-c']);
  for (const id of ids) d.sh(`cmd jobscheduler run -f ${APP_ID} ${id}`);
  let line = '';
  await ctx.until(() => {
    const log = d.adb(['logcat', '-d', '-s', 'WM-WorkerWrapper:*', 'BackgroundTaskWork:*']).toString();
    line = log.split('\n').find((l) => /Worker result (SUCCESS|FAILURE|RETRY)/.test(l)) ?? '';
    return !!line;
  }, { ms: 120_000, what: 'WorkManager\'s result line for the background check (WM-WorkerWrapper)' });
  if (!/SUCCESS/.test(line)) throw new Error(`the background check did not succeed: ${line.trim()}`);
  return `job ${ids.join(',')} · ${line.replace(/^.*(Worker result)/, '$1').trim().slice(0, 140)}`;
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
      return `saved · permission ${asked ? 'asked and allowed' : 'already allowed'} · channels: ${
        (d.sh(`dumpsys notification | grep -o "mId='[a-z]*'" | sort -u`).match(/mId='(gauges|updates|app)'/g) ?? []).length} of ours`;
    });

    try {
      await ctx.step('the clock 7 h on (the newest reading stale), the check run in the background: no notice yet', async () => {
        if (!saved) ctx.skip('no alert was saved');
        await toBackground(ctx);
        d.sh('settings put global auto_time 0');
        moved = true;
        d.sh(`su 0 date -u ${stamp(deviceMs(d) + 7 * HOUR)}`);
        const r = await runCheck(ctx);
        const n = d.sh(`dumpsys notification --noredact | grep -c "pkg=${APP_ID}" || true`).trim();
        return `${r} · our notifications in the shade: ${n}`;
      });

      let title = '';
      await ctx.step('31 h more (silent for over a day), the check run again: the "has not reported since" notice is raised', async () => {
        if (!saved) ctx.skip('no alert was saved');
        d.sh(`su 0 date -u ${stamp(deviceMs(d) + 31 * HOUR)}`);
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
