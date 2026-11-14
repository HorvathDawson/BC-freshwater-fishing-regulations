// (T286) A MAP TAP TO ITS REGULATIONS, TIMED: for each case (a small stream, the Fraser, a lake) search the water, let
// the map frame it (the result row's preview), close the search, then TAP THE WATER ON THE MAP at the point the app
// itself says the water answers a tap (`[target]`, logged by the test bed's build once the camera rests: Map.native.tsx
// `findTarget`) and wait for the page's words. Never a colour hunted on screen (the "has its own regulations" lines
// share the highlight's orange: it tapped the wrong water), and never Back at the map root (that leaves the app). The app's open probe (`@app/data`
// openProbe.ts, on in the test-bed build) logs one `[open] {…}` line per tap: each stage's arrival (ms since the press,
// SQLite calls by then) — hit test, route, water sheet, store, target, inputs, page, commit, first frame — and the
// SQLite calls' queue wait and run time. Each case is tapped OPEN_TAPS times (first = cold for that water, then warm).
// A benchmark: the lines go into the step note and `T286-open.txt` beside the report; a spinner seen fails the step.
//
//   node app/tools/testbed/run.mjs regs-open          (OPEN_TAPS=n, default 3; OPEN_CASES=small,unnamed,mid,fraser,skeena,lake)
//
// (T716) Each case starts from a COLD START (OPEN_COLD=0: one cold start for all), so tap 1 is the first map tap after
// launch (the water was opened once from search to frame the map; `unnamed` — the nearest other stream the probe finds
// beside Norrish Creek, `[target-other]` — was never opened). Every tap also reads the DEVICE clock just after the
// injected tap, so `press` = how long the tap took to reach the map's JS handler (MapLibre's single-tap confirmation
// included); the trace's `shown` is the commit in which the pane is on screen, `cue` the first feedback drawn.
import fs from 'node:fs';
import path from 'node:path';
import { coldStart, toMap } from './common.mjs';
import { recordOpen } from './pane.mjs';

const TAPS = Number(process.env.OPEN_TAPS ?? 3);
const COLD = process.env.OPEN_COLD !== '0';
/** (T716) the latest the tap's cue may reach the screen, ms after the injected tap (owner: "no feedback"; aim ~100) */
const CUE_MAX = Number(process.env.OPEN_CUE_MAX ?? 150);
/** (T716) OPEN_RECORD=1: each case's first tap is also recorded and cut into the screens shown (`pane-frames.py open`) */
const RECORD = process.env.OPEN_RECORD === '1';
const CASES = [
  { id: 'small', q: 'Norrish Creek', row: 'Stream · Region 2 · MU 2-8 · near Dewdney.*', title: /Norrish Creek/ },
  // (T716) a never-opened neighbour of Norrish Creek (most often an unnamed tributary): its page names it
  { id: 'unnamed', q: 'Norrish Creek', row: 'Stream · Region 2 · MU 2-8 · near Dewdney.*', title: /Unnamed|Tributary|Creek|River/, other: true },
  { id: 'mid', q: 'Coquitlam River', row: 'Stream · .*', title: /Coquitlam River/ },
  { id: 'fraser', q: 'Fraser River', row: 'Stream · .*', title: /Fraser River/ },
  { id: 'skeena', q: 'Skeena River', row: 'Stream · .*', title: /Skeena River/ },
  { id: 'lake', q: 'Cultus', row: 'Lake · .*Lindell Beach.*', title: /Cultus Lake/ },
].filter((c) => !process.env.OPEN_CASES || process.env.OPEN_CASES.split(',').includes(c.id));
/** The screen's density (px per dp). */
function density(ctx) {
  const m = /(\d+)\s*$/.exec(ctx.device.sh('wm density').trim().split('\n').pop() ?? '');
  return m ? Number(m[1]) / 160 : 2.625;
}

/** The last `[target]` the app logged, as a screen pixel (null: none yet, or the water answered nowhere). */
const targets = (ctx, tag = '[target] ') => ctx.device.logcatJs().split('\n').filter((l) => l.includes(tag))
  .map((l) => { try { return JSON.parse(l.slice(l.indexOf(tag) + tag.length)); } catch { return null; } }).filter(Boolean);

/** Wait until the camera has rested (no new `[target]` for `quietMs`), then the last one as a screen pixel. */
export async function settledTarget(ctx, ms = 30_000, quietMs = 5000, tag = '[target] ') {
  const end = Date.now() + ms;
  let n = 0, since = Date.now();
  while (Date.now() < end) {
    await ctx.device.sleep(1000);
    const now = targets(ctx).length;
    if (now !== n) { n = now; since = Date.now(); continue; }
    if (n > 0 && Date.now() - since >= quietMs) return targetPoint(ctx, tag);
  }
  return null;
}

function targetPoint(ctx, tag = '[target] ') {
  // (0.5.0) more than one map logs a probe (the browse map and a water's small map, `win` 388 dp tall): the tap is for
  // the tallest one on screen, its latest line
  const all = targets(ctx, tag);
  const tall = Math.max(0, ...all.map((x) => x.win?.h ?? 0));
  const t = all.filter((x) => (x.win?.h ?? 0) === tall).at(-1);
  if (!t || t.none) return null;
  // the map view's top-left on screen, px: the app's own measure of THIS map (`win`, dp) first; the accessibility tree's
  // first "Showing a Map" only without it (0.5.0: that matched another map view mounted off screen, y −428 px, and the
  // tap opened a neighbouring stream)
  const k = density(ctx);
  let org = t.win ? { x: t.win.x * k, y: t.win.y * k } : null;
  if (!org) {
    try {
      ctx.device.sh('uiautomator dump /sdcard/tb-map.xml');
      const m = /content-desc="Showing a Map[^"]*"[^>]*bounds="\[(\d+),(\d+)\]/.exec(ctx.device.sh('cat /sdcard/tb-map.xml'));
      if (m) org = { x: Number(m[1]), y: Number(m[2]) };
    } catch { /* no dump */ }
  }
  org ??= { x: 0, y: 0 };
  return { x: Math.round(org.x + t.x * k), y: Math.round(org.y + t.y * k) };
}

const opens = (ctx) => ctx.device.logcatJs().split('\n').filter((l) => l.includes('[open] '))
  .map((l) => { try { return JSON.parse(l.slice(l.indexOf('[open] ') + 7)); } catch { return null; } }).filter(Boolean);

/** One trace as a line: total to the first frame, then each stage. */
function said(o) {
  const pre = (o.cueMs !== undefined ? `cue ${o.cueMs} ms · ` : '') + (o.pressMs !== undefined ? `press ${o.pressMs} ms + ` : '');
  const st = Object.entries(o.marks).sort((a, b) => a[1][0] - b[1][0]).map(([k, [ms, n]]) => `${k} ${ms}${n ? `/${n}q` : ''}`);
  return `${pre}${o.marks.shownFrame?.[0] ?? o.marks.frame?.[0] ?? o.marks.timeout?.[0] ?? '?'} ms · ${st.join(', ')} · sql ${o.sql} (wait ${o.sqlWait}, run ${o.sqlRun}, max ${o.sqlMax})`
    + (Object.keys(o.spans).length ? ` · ${Object.entries(o.spans).map(([k, v]) => `${k} ${v} ms`).join(', ')}` : '')
    + (o.work?.length ? ` · ${o.work.join('; ')}` : '');
}

export const regsOpen = {
  id: 'regs-open',
  title: `A map tap to its regulations, timed (${CASES.map((c) => c.id).join(', ')} × ${TAPS})`,
  async run(ctx) {
    const lines = [];
    await ctx.step('the map up (cold start, Wi-Fi)', async () => {
      await ctx.device.net('wifi'); ctx.device.link('full', 'none');
      await coldStart(ctx, { ms: 90_000 });
      await ctx.device.sleep(4000);
    });
    for (const c of CASES) {
      let at = null;
      await ctx.step(`${c.id}: ${COLD ? 'cold start, ' : ''}search "${c.q}", the map framed on it, the search closed`, async () => {
        if (COLD) { await coldStart(ctx, { ms: 90_000 }); await ctx.device.sleep(3000); }
        await toMap(ctx);
        ctx.device.logcatClear();                         // a [target] from the water before is not this one's
        // (T340) the water OPENED from its result, then Back: a water opened from a result consumes search and the map
        // comes back FRAMED ON IT (useShellSearch `arrive`). Closing search instead puts the camera back where it was
        // before search (T48) — the old step did that, then tapped where the water had been framed: the stream taps
        // that failed since 0.3.0. Browse mode, the waters on screen read ahead while it is still (T286 prefetch).
        ctx.flow('search', { Q: c.q });
        ctx.flow('open-result', { LINE: c.row });
        await ctx.device.sleep(3500);
        ctx.device.sh('input keyevent 4'); await ctx.device.sleep(3000);
        // the camera at rest: the app's own word on where the water answers a tap
        at = await settledTarget(ctx, 30_000, 5000, c.other ? '[target-other] ' : '[target] ');
        // (T689) two different failures: the probe never ran (nothing lit, no map size) or it searched and found no point
        if (!at) throw new Error(targets(ctx, c.other ? '[target-other] ' : '[target] ').length
          ? 'the app searched and found no tap target for the water ([target] {"none":true})'
          : 'the app named no tap target for the water (no [target] line: nothing lit, or the map not drawn)');
        return `target at ${at.x},${at.y} px`;
      });
      if (!at) continue;
      for (let i = 1; i <= TAPS; i++) {
        let opened = false;
        await ctx.step(`${c.id}: tap ${i}/${TAPS} on the map → its regulations`, async () => {
          // what the settle read ahead (`[prefetch]`, the waters on screen), before this tap
          const pre = ctx.device.logcatJs().split('\n').filter((l) => l.includes('[prefetch] ')).map((l) => l.slice(l.indexOf('[prefetch] ') + 11)).at(-1);
          ctx.device.logcatClear();
          // (T716) the device clock just after the injected tap (DOWN and UP sent, `input` returned)
          const tap = () => Number(ctx.device.sh(`input tap ${at.x} ${at.y}; date +%s%3N`).trim().split('\n').pop());
          let tappedAt = NaN, screens = null;
          if (RECORD && i === 1) screens = await recordOpen(ctx, `T716-${c.id}`, async () => { tappedAt = tap(); }, 4000);
          else tappedAt = tap();
          const end = Date.now() + 20_000;
          let o = null;
          while (Date.now() < end && !o) { await ctx.device.sleep(700); o = opens(ctx).at(-1) ?? null; }
          if (o && o.wall && Number.isFinite(tappedAt)) o.pressMs = o.wall - tappedAt;
          // (T716) what else the open logged: decodes that held the JS thread (`[work]`), how the pane's hold ended
          // (`[hold]`), and the tap's cue — the first lit set to reach the screen after the tap, ms after it (`cueMs`)
          if (o) {
            const log = ctx.device.logcatJs().split('\n');
            o.work = log.filter((l) => l.includes('[work] ') || l.includes('[hold] ')).map((l) => l.slice(l.indexOf('[')));
            const cue = log.filter((l) => l.includes('[cue] ')).map((l) => { try { return JSON.parse(l.slice(l.indexOf('[cue] ') + 6)); } catch { return null; } })
              .find((c) => c && c.n > 0 && Number.isFinite(tappedAt) && c.wall >= tappedAt);
            if (cue) o.cueMs = cue.wall - tappedAt;
          }
          const w = ctx.device.screenText();
          if (!o) throw new Error(`no [open] line in 20 s (saw: ${w.slice(0, 10).join(' | ').slice(0, 300)})`);
          opened = true;
          lines.push(`${c.id} #${i} ${JSON.stringify(o)}`);
          if (screens) lines.push(`${c.id} #${i} screens: ${screens.replace(/\n/g, ' | ')}`);
          fs.writeFileSync(path.join(ctx.dir, 'T286-open.txt'), `${lines.join('\n')}\n`);
          const named = w.some((x) => c.title.test(x));
          const note = `${pre ? `(prefetch: ${pre}) ` : ''}${said(o)}${named ? '' : ` · (title ${c.title} not on screen: ${w.slice(0, 6).join(' | ').slice(0, 160)})`}`;
          if (o.marks.spinner) throw new Error(`a spinner was shown at ${o.marks.spinner[0]} ms — ${note}`);
          // (T716) the tap answered on screen promptly: the tapped section lit (the cue) within CUE_MAX ms of the tap
          if (o.cueMs === undefined) throw new Error(`no cue: nothing was lit on the map after the tap — ${note}`);
          if (o.cueMs > CUE_MAX) throw new Error(`the cue came ${o.cueMs} ms after the tap (limit ${CUE_MAX}) — ${note}`);
          return note;
        });
        // back to the map at the same framing (the water's frame closed) — only when the tap opened one: Back at the map
        // root leaves the app (the search bar keeps the water's name, so the screen's words cannot tell)
        if (opened) { ctx.device.sh('input keyevent 4'); await ctx.device.sleep(2000); }
      }
    }
  },
};
