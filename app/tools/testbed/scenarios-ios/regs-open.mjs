// (T719, IOS-2) THE iOS TWIN OF scenarios/regs-open.mjs (T286, T716): a map tap to its page, timed, on the simulator.
// Same cases, same probe lines (`[target]`, `[open]`, `[cue]`, `[shown]`, `[work]`, `[hold]`), same limits; what
// differs is how the tap is made and how its moment is read:
//
//   * THE TAP: Maestro (`flows/ios-tap-at.yaml`, an absolute point in iOS points = the app's dp: no density) — the
//     simulator has no `input tap`. Maestro's tap is a finger down and up ~115 ms apart (ios-t313g: 116-133 ms).
//   * ITS MOMENT: not a host clock read around the Maestro call (a JVM start, seconds wide) but UIKit's own log line for
//     every touches event the app received (`touchLog`: `[com.apple.UIKit:EventDispatch] Evaluating dispatch of
//     UIEvent …; type: 0;`), on the Mac's clock, which is also the simulator app's `Date.now()`. Android's `press` is from
//     the device clock just after `input tap` returned (down and up sent back to back, i.e. the lift), so the iOS
//     numbers are taken FROM THE LIFT (the last touches event before the press) to compare; `pressFromDown` is beside it.
//   * WHY iOS MAY WAIT: the binding's single tap (`MLRNMapView.m` init: a UITapGestureRecognizer → `didTapMap:`)
//     `requireGestureRecognizerToFail:` every UITapGestureRecognizer MLNMapView already has, which includes MapLibre's
//     double-tap zoom recogniser: so the click can only fire once UIKit has given up on a second tap. The T716 Android
//     `onTapUp` has no iOS half (the patch's Obj-C part has no tap code), so on iOS the cue comes with the click.
//     `gestureMs` lists UIKit's "send gesture actions" lines after the lift: the recogniser firing, independent of JS.
//
//   node app/tools/testbed/run-ios.mjs regs-open    (OPEN_TAPS, OPEN_CASES, OPEN_COLD, OPEN_CUE_MAX as Android's)
//
// Results: `T719-open.txt` beside the report (one JSON line per tap) and the step notes. Unify with the Android scenario
// once its tap/back work lands (the device-specific parts are the tap, its moment and Back).
import fs from 'node:fs';
import path from 'node:path';
import { changed, readPng } from '../lib/png.mjs';

const TAPS = Number(process.env.OPEN_TAPS ?? 3);
const COLD = process.env.OPEN_COLD !== '0';
/** (T716) the latest the tap's cue may reach the screen, ms after the tap (Android: after the injected tap ≈ its lift) */
const CUE_MAX = Number(process.env.OPEN_CUE_MAX ?? 150);
/** touches events of one tap come ~17 ms apart (began / moved / ended); a longer gap starts another tap */
const TAP_GAP = 45;
// the same waters as Android's bench (scenarios/regs-open.mjs CASES): read there, written here (that file is another
// lane's while the Android tap/back work is open)
// (T719) the same waters; the ROW is iOS's own: a result is one element whose accessibility text is "Show <name>, Region 2,
// MU 2-8, near Dewdney, 1 km on the map. …" (Maestro's saved tree, ios-open 14:13), not Android's "Stream · Region 2 · …"
const CASES = [
  { id: 'small', q: 'Norrish Creek', row: 'Show Norrish Creek, Region 2, MU 2-8, near Dewdney.*', title: /Norrish Creek/ },
  { id: 'unnamed', q: 'Norrish Creek', row: 'Show Norrish Creek, Region 2, MU 2-8, near Dewdney.*', title: /Unnamed|Tributary|Creek|River/, other: true },
  { id: 'mid', q: 'Coquitlam River', row: 'Show Coquitlam River,.*', title: /Coquitlam River/ },
  { id: 'fraser', q: 'Fraser River', row: 'Show Fraser River,.*', title: /Fraser River/ },
  { id: 'skeena', q: 'Skeena River', row: 'Show Skeena River,.*', title: /Skeena River/ },
  { id: 'lake', q: 'Cultus', row: 'Show Cultus Lake,.*Lindell Beach.*', title: /Cultus Lake/ },
].filter((c) => !process.env.OPEN_CASES || process.env.OPEN_CASES.split(',').includes(c.id));

/** `[tag] {json}` lines in the app's JS log since the mark, parsed. */
function tagged(ctx, tag) {
  const k = `[${tag}] `;
  return ctx.device.logcatJs().split('\n').filter((l) => l.includes(k))
    .map((l) => { try { return JSON.parse(l.slice(l.indexOf(k) + k.length)); } catch { return null; } }).filter(Boolean);
}

/** Wait until the camera has rested (no new `[target]` for `quietMs`), then the last one as a screen point. */
async function settledTarget(ctx, tag, ms = 30_000, quietMs = 5000) {
  const end = Date.now() + ms;
  let n = 0, since = Date.now();
  while (Date.now() < end) {
    await ctx.device.sleep(1000);
    const now = tagged(ctx, tag).length;
    if (now !== n) { n = now; since = Date.now(); continue; }
    if (n > 0 && Date.now() - since >= quietMs) break;
  }
  const all = tagged(ctx, tag);
  if (!all.length) return { none: 'no-line' };
  // the tallest map on screen, its latest line (Android's rule: a water's small map logs too)
  const tall = Math.max(0, ...all.map((x) => x.win?.h ?? 0));
  const t = all.filter((x) => (x.win?.h ?? 0) === tall).at(-1);
  if (!t || t.none) return { none: 'searched' };
  // iOS points are the app's dp: the map's own window rect (`win`) plus the point in it. No `win`: said, never guessed
  if (!t.win) return { none: 'no-win' };
  return { x: Math.round(t.win.x + t.x), y: Math.round(t.win.y + t.y) };
}

/** The touches events since the mark, grouped into taps: [{ down, up, n }] (Mac clock, ms). */
function tapsSeen(ctx) {
  const { touches, actions } = ctx.device.touchLog();
  const taps = [];
  for (const t of touches) {
    const last = taps.at(-1);
    if (last && t - last.up <= TAP_GAP) { last.up = t; last.n++; } else taps.push({ down: t, up: t, n: 1 });
  }
  return { taps, actions };
}

/** One trace as a line: the iOS times first, then each stage (as Android's `said`). */
function said(o) {
  const pre = (o.cueMs !== undefined ? `cue ${o.cueMs} ms · ` : '') + (o.shownMs !== undefined ? `page ${o.shownMs} ms · ` : '')
    + (o.pressMs !== undefined ? `press ${o.pressMs} ms (from down ${o.pressFromDownMs}, hold ${o.holdMs}) + ` : '');
  const st = Object.entries(o.marks).sort((a, b) => a[1][0] - b[1][0]).map(([k, [ms, n]]) => `${k} ${ms}${n ? `/${n}q` : ''}`);
  return `${pre}${o.marks.shownFrame?.[0] ?? o.marks.frame?.[0] ?? o.marks.timeout?.[0] ?? '?'} ms · ${st.join(', ')} · sql ${o.sql} (wait ${o.sqlWait}, run ${o.sqlRun}, max ${o.sqlMax})`
    + (o.gestureMs?.length ? ` · UIKit gesture actions at ${o.gestureMs.join(', ')} ms after the lift` : '')
    + (o.work?.length ? ` · ${o.work.join('; ')}` : '');
}

export const regsOpenIos = {
  id: 'regs-open',
  inAll: false,                                    // a bench, run by name: not part of the T718 baseline's `all`
  title: `A map tap to its regulations, timed (iOS; ${CASES.map((c) => c.id).join(', ')} × ${TAPS}; double tap on small)`,
  async run(ctx) {
    const { coldStart, toMap, pageBack, hasTabs } = ctx.ios;
    const lines = [];
    const keep = () => fs.writeFileSync(path.join(ctx.dir, 'T719-open.txt'), `${lines.join('\n')}\n`);
    await ctx.step('the map up (cold start)', async () => { await coldStart(ctx); await ctx.device.sleep(4000); });
    for (const c of CASES) {
      let at = null;
      const tag = c.other ? 'target-other' : 'target';
      await ctx.step(`${c.id}: ${COLD ? 'cold start, ' : ''}search "${c.q}", opened, back: the map framed on it`, async () => {
        if (COLD) { await coldStart(ctx); await ctx.device.sleep(3000); }
        await toMap(ctx);
        ctx.device.logcatClear();                     // a [target] from the water before is not this one's
        // (T340, as Android) the water OPENED from its result, then its page's back: the map comes back framed on it
        ctx.flow('search', { Q: c.q });
        ctx.flow('open-result', { LINE: c.row });
        await ctx.device.sleep(3500);
        await pageBack(ctx);
        await ctx.device.sleep(3000);
        const t = await settledTarget(ctx, tag);
        if (t.none === 'no-line') throw new Error(`the app named no tap target for the water (no [${tag}] line: nothing lit, the map not drawn, or not a probe build)`);
        if (t.none === 'searched') throw new Error(`the app searched and found no tap target for the water ([${tag}] {"none":true})`);
        if (t.none === 'no-win') throw new Error(`the [${tag}] line has no map rect (win): the point on screen is not known`);
        at = t;
        return `target at ${at.x},${at.y} pt`;
      });
      if (!at) continue;
      for (let i = 1; i <= TAPS; i++) {
        let opened = false;
        await ctx.step(`${c.id}: tap ${i}/${TAPS} on the map → its regulations`, async () => {
          const pre = ctx.device.logcatJs().split('\n').filter((l) => l.includes('[prefetch] ')).map((l) => l.slice(l.indexOf('[prefetch] ') + 11)).at(-1);
          ctx.device.logcatClear();
          ctx.flow('ios-tap-at', { POINT: `${at.x},${at.y}` });
          const end = Date.now() + 20_000;
          let o = null;
          while (Date.now() < end && !o) { await ctx.device.sleep(700); o = tagged(ctx, 'open').at(-1) ?? null; }
          const { taps, actions } = tapsSeen(ctx);
          // the tap that opened: the last one that went down before the press (only one was sent)
          const tap = o?.wall ? taps.filter((t) => t.down <= o.wall).at(-1) : taps.at(-1);
          const lift = tap?.up;
          const cue = tagged(ctx, 'cue').find((x) => x.n > 0 && lift && x.wall >= lift), shown = tagged(ctx, 'shown').find((x) => lift && x.wall >= lift);
          const seen = { cueMs: cue ? cue.wall - lift : undefined, shownMs: shown ? shown.wall - lift : undefined };
          const w = ctx.device.screenText();
          if (!o) throw new Error(`no [open] line in 20 s (touches seen: ${taps.length}; cue ${seen.cueMs ?? '-'} ms, page shown ${seen.shownMs ?? '-'} ms; saw: ${w.slice(0, 10).join(' | ').slice(0, 300)})`);
          opened = true;
          const opens = tagged(ctx, 'open').length;
          const log = ctx.device.logcatJs().split('\n');
          o.work = log.filter((l) => l.includes('[work] ') || l.includes('[hold] ')).map((l) => l.slice(l.indexOf('[')));
          Object.assign(o, seen);
          if (tap && o.wall) {
            o.pressMs = o.wall - tap.up; o.pressFromDownMs = o.wall - tap.down; o.holdMs = tap.up - tap.down; o.touches = tap.n;
            o.gestureMs = actions.filter((a) => a >= tap.up - 5 && a <= o.wall + 5).map((a) => a - tap.up);
          }
          lines.push(`${c.id} #${i} ${JSON.stringify(o)}`); keep();
          const named = w.some((x) => c.title.test(x));
          const note = `${pre ? `(prefetch: ${pre}) ` : ''}${said(o)}${named ? '' : ` · (title ${c.title} not on screen: ${w.slice(0, 6).join(' | ').slice(0, 160)})`}`;
          if (!tap) throw new Error(`no UIKit touch line in the app log: the press cannot be timed — ${note}`);
          // the app's clock against the log's ([heap] every 2 s carries Date.now()): the times above compare the two
          const skew = ctx.device.clockSkew();
          if (skew === null) ctx.finding(`${c.id} #${i}: no [heap] / [stall] line to check the log's clock against the app's`);
          else if (Math.abs(skew) > 50) throw new Error(`the app's clock and the log's differ by ${skew} ms: the times are not comparable — ${note}`);
          if (!o.wall) throw new Error(`the [open] line has no wall (an older build): the press cannot be timed — ${note}`);
          if (taps.length !== 1) ctx.finding(`${c.id} #${i}: ${taps.length} taps seen in UIKit's log for one Maestro tap`);
          if (opens > 1) throw new Error(`one tap opened ${opens} times — ${note}`);
          if (o.marks.spinner) throw new Error(`a spinner was shown at ${o.marks.spinner[0]} ms — ${note}`);
          if (o.cueMs === undefined) throw new Error(`no cue: nothing was lit on the map after the tap — ${note}`);
          if (o.cueMs > CUE_MAX) throw new Error(`the cue came ${o.cueMs} ms after the lift (limit ${CUE_MAX}) — ${note}`);
          return note;
        });
        // back to the map at the same framing — only when the tap opened one (as Android)
        if (opened) { await pageBack(ctx); await ctx.device.sleep(2000); }
      }
      // (T716) a DOUBLE TAP on the same point: MapLibre's zoom; no page, nothing reported, no open
      if (c.id === 'small' && at) await ctx.step(`${c.id}: a double tap on the water zooms, shows no page, reports nothing`, async () => {
        const d = ctx.device;
        const before = path.join(ctx.dir, 'T719-dbl-before.png'), after = path.join(ctx.dir, 'T719-dbl-after.png');
        d.screenshot(before);
        d.logcatClear();
        ctx.flow('ios-double-tap-at', { POINT: `${at.x},${at.y}` });
        await d.sleep(2500);
        d.screenshot(after);
        const { taps, actions } = tapsSeen(ctx);
        const gaps = taps.slice(1).map((t, k) => t.down - taps[k].up);
        const log = d.logcatJs().split('\n');
        const lit = tagged(ctx, 'cue').map((x) => x.n);
        const bad = log.filter((l) => /\[(shown|failure|open)\] |Unhandled promise rejection/.test(l)).map((l) => l.slice(l.indexOf('[')));
        const words = d.screenText();
        const a = readPng(before), b = readPng(after);
        const moved = changed(a, b, { x: 0, y: Math.round(a.h * 0.2), w: a.w, h: Math.round(a.h * 0.55) });
        const note = `taps seen ${taps.length} (gap ${gaps.join(', ') || '-'} ms; UIKit gesture actions ${actions.length}); cue lit/out: ${lit.join(',') || 'none'}; map changed ${(moved * 100).toFixed(0)}%; on the map: ${hasTabs(words)}`;
        if (taps.length !== 2) throw new Error(`UIKit saw ${taps.length} taps, not 2: not a double tap — ${note}`);
        if (bad.length) throw new Error(`the double tap showed or reported: ${bad.join(' | ').slice(0, 400)} — ${note}`);
        if (!hasTabs(words)) throw new Error(`not on the map after the double tap (saw: ${words.slice(0, 8).join(' | ')}) — ${note}`);
        if (lit.some((n) => n > 0) && lit.at(-1) !== 0) throw new Error(`the cue was lit and left on — ${note}`);
        if (moved < 0.2) throw new Error(`the map did not zoom (${(moved * 100).toFixed(0)}% of the map changed) — ${note}`);
        return note;
      });
    }
  },
};
