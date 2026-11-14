// (T717) BACK FROM A WATER'S PAGE SHOWS THE MAP AS IT WAS LEFT. Owner 2026-11-14 (Android, 0.7.8/0.7.9): "when we go
// back from a regulation tab it flashes very hard while the map is loading."
//
// Each case: the map framed on Norrish Creek (search → Open → Back), the water TAPPED on the map at the app's own
// `[target]`, the page left up for `stay` s, then Back (the system key, or the page's ‹ `water-back`). The whole thing is
// recorded (`screenrecord` writes a frame only when the screen changes) and cut by `pane-frames.py back`: every frame
// after Back is the page, the map as left (within BACK_TOL, default 8, of the last frame before the tap) or a FLASH —
// a blank, grey, half-drawn or wrong-palette screen. A case FAILS on any flash frame, or when the screen does not
// settle on the map as left. Cases vary one thing each from `base` (light, standard ground, Map tab, 2.5 s, the key):
//
//   node app/tools/testbed/run.mjs back-flash      (BACK_CASES=base,button,dark,satellite,conditions,long; BACK_TOL=n)
import { execFileSync } from 'node:child_process';
const ADB = process.env.ADB ?? 'adb';
/** the app's death in the whole log since the last clear: the lines that say why (a native crash, a JS fatal) */
const deathLines = (d) => execFileSync(ADB, ['-s', d.serial(), 'logcat', '-d', '-b', 'main,system,crash']).toString().split('\n')
  .filter((l) => /Fatal signal|SIGSEGV|SIGABRT|Abort message|backtrace:|#\d\d pc |FATAL EXCEPTION|AndroidRuntime|ReactNativeJS.*(Error|fatal)|lowmemorykiller|Process .* has died/.test(l)).slice(0, 60);
import fs from 'node:fs';
import path from 'node:path';
import { coldStart, toMap } from './common.mjs';
import { record } from './pane.mjs';
import { settledTarget, targets } from './regs-open.mjs';
import { find, look, tap } from '../lib/screen.mjs';

const FRAMES = path.join(path.dirname(new URL(import.meta.url).pathname), '..', 'pane-frames.py');
const WATER = { q: 'Norrish Creek', row: 'Stream · Region 2 · MU 2-8 · near Dewdney.*' };
const BASE = { dark: false, satellite: false, tab: 'Map', stay: 2.5, via: 'key' };
const CASES = [
  { id: 'base' },
  { id: 'button', via: 'button' },
  { id: 'dark', dark: true },
  { id: 'satellite', satellite: true },
  { id: 'conditions', tab: 'Conditions' },
  { id: 'long', stay: 30 },
].map((c) => ({ ...BASE, ...c }))
  .filter((c) => !process.env.BACK_CASES || process.env.BACK_CASES.split(',').includes(c.id))
  // BACK_REPEAT=n: each case n times (a rare failure, e.g. a native crash, needs several tries)
  .flatMap((c) => Array.from({ length: Number(process.env.BACK_REPEAT ?? 1) }, (_, k) => (k ? { ...c, id: `${c.id}${k + 1}` } : c)));
/** The recording's boundaries are put this far BEFORE the tap and the Back (the recorder starts late by up to a few
 *  hundred ms, so a boundary at the host's clock could miss the first frames after Back); the screen is still then. */
const MARGIN = 0.8;

export const backFlash = {
  id: 'back-flash',
  title: `Back from a water's page shows the map as left, no flash (${CASES.map((c) => c.id).join(', ')}) (T717)`,
  async run(ctx) {
    const d = ctx.device;
    const night = (on) => d.sh(`cmd uimode night ${on ? 'yes' : 'no'}`);
    let dark = null, satellite = false;
    // (T717) the last case's target, while the app was not restarted: the map is kept under a page now, so a second
    // search → Open → Back of the same water may not move its camera, and the probe logs `[target]` only at a camera's rest
    let lastAt = null;
    const satelliteTo = async (on) => {
      if (satellite === on) return;
      if (on) ctx.flow('layers-satellite');
      else { ctx.flow('tap-text', { TEXT: 'Choose layers.*' }); await d.sleep(1200); ctx.flow('tap-id', { ID: 'option-ground\\.map' }); }
      await d.sleep(800);
      ctx.flow('tap-text', { TEXT: 'Done' });
      satellite = on;
      await d.sleep(6000);                                 // the imagery tiles
    };
    try {
      for (const c of CASES) {
        let at = null;
        await ctx.step(`${c.id}: ${c.dark ? 'dark' : 'light'}${c.satellite ? ', satellite' : ''}, ${c.tab} tab — the map framed on ${WATER.q}`, async () => {
          await d.net('wifi'); d.link('full', 'none');
          if (dark !== c.dark) { night(c.dark); dark = c.dark; lastAt = null; await coldStart(ctx, { ms: 90_000 }); await d.sleep(3000); }
          await toMap(ctx);
          await satelliteTo(c.satellite);
          d.logcatClear();
          ctx.flow('search', { Q: WATER.q });
          ctx.flow('open-result', { LINE: WATER.row });
          await d.sleep(3500);
          d.sh('input keyevent 4'); await d.sleep(3000);
          at = await settledTarget(ctx);
          const kept = !at && lastAt && !targets(ctx).length;
          if (kept) at = lastAt;                          // the camera never moved: the same water, the same point
          if (!at) throw new Error('the app named no tap target for the water (no [target] line)');
          lastAt = at;
          if (c.tab !== 'Map') {
            ctx.flow('tab', { TAB: c.tab });
            await d.sleep(4000);
            for (let k = 0; k < 3; k++) {                  // a first visit's coach marks, closed as a reader would
              const w = d.screenText(), btn = ['Skip', 'Got it'].find((x) => w.includes(x));
              if (!btn) break;
              ctx.flow('tap-text', { TEXT: btn }); await d.sleep(1200);
            }
            await d.sleep(4000);
          }
          return `target at ${at.x},${at.y} px${kept ? ' (the last case\'s: the camera did not move)' : ''}`;
        });
        if (!at) continue;
        await ctx.step(`${c.id}: tap → page (${c.stay} s) → Back (${c.via === 'key' ? 'system back' : 'the page\'s ‹'}), recorded`, async () => {
          const name = `T717-back-${c.id}`;
          const mp4 = path.join(ctx.dir, `${name}.mp4`);
          d.logcatClear();
          const pid = d.pidOf();
          const r = record(d, mp4, Math.ceil(c.stay + 14));
          await d.sleep(1500);
          const tapAt = (Date.now() - r.t0) / 1000;
          d.sh(`input tap ${at.x} ${at.y}`);
          await d.sleep(c.stay * 1000);
          // the page must be up (else Back would leave the app from the map)
          const ns = await look(d);
          const btn = find(ns, 'resource-id="water-back"');
          if (!btn) {
            await r.stop();
            const dead = d.pidOf() !== pid ? deathLines(d) : [];
            if (dead.length) fs.writeFileSync(path.join(ctx.dir, `${name}-death.txt`), `${dead.join('\n')}\n`);
            throw new Error(`no page opened (no water-back on screen)${d.pidOf() !== pid ? `; THE APP DIED (pid ${pid} → ${d.pidOf() ?? 'none'}): ${dead.slice(0, 3).join(' / ').slice(0, 300)}` : ''}: ${d.screenText().slice(0, 8).join(' | ')}`);
          }
          // (T717) the app's memory with the page up (the browse map kept or not under it), said in the note
          const mem = d.heap();
          await d.sleep(1000);                               // the page still for MARGIN before Back
          const backAt = (Date.now() - r.t0) / 1000;
          if (c.via === 'key') d.sh('input keyevent 4'); else tap(d, btn);
          await d.sleep(5000);
          await r.stop();
          if (d.pidOf() !== pid) throw new Error(`the app died (pid ${pid} → ${d.pidOf() ?? 'none'})`);
          const out = execFileSync('python3', ['-I', FRAMES, 'back', mp4, String(tapAt - MARGIN), String(backAt - MARGIN)],
            { env: { ...process.env } }).toString().trim();
          const o = JSON.parse(out.split('\n').pop());
          fs.appendFileSync(path.join(ctx.dir, 'T717-back.txt'), `${c.id} ${JSON.stringify({ ...o, cut: [tapAt - MARGIN, backAt - MARGIN] })}\n`);
          if (o.error) throw new Error(`the recording could not be read: ${o.error}`);
          const fl = o.flashes.map((f) => `+${f.at_ms} ms ${f.held_ms} ms rgb(${f.rgb.join(',')}) flat ${f.flat}`).join('; ');
          const note = `${o.frames_after_back} frames after Back; map as left at +${o.first_map_ms ?? '-'} ms; flash ${o.flash_frames} frame(s), ${o.flash_ms} ms${fl ? `: ${fl}` : ''}; settled: ${o.settled}; memory with the page up ${mem ? `${Math.round(mem.total / 1024)} MB (native ${Math.round(mem.native / 1024)})` : '-'} (strip ${name}-back-strip.png)`;
          if (o.flash_frames > 0) throw new Error(`Back flashed — ${note}`);
          if (o.settled !== 'map') throw new Error(`Back did not settle on the map as left — ${note}`);
          return note;
        });
      }
    } finally {
      if (satellite) await satelliteTo(false);
      if (dark) { night(false); }
    }
  },
};
