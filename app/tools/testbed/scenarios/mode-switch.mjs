// (T903, owner 2026-11-19: "i feel like salmon painting is still slow") THE MODE SWITCH, TIMED TO THE PAINTED FRAME: at one
// place, Trout → Salmon (the rail) → Conditions → Runs (the tabs) → Map (back on Salmon) → Trout, each switch timed from the
// tap to the first FULL frame after the new style's labels landed (the pan probe's `[swap]` line, `paintMs`: hand-over →
// painted, `native/panProbe.ts`), then four flings in that mode with the map surface's frame times (SurfaceFlinger, as
// `pan`). Arms are map-lab settings, each a cold start, so before / after run on the SAME build. A measurement, no budget.
//
//   node app/tools/testbed/run.mjs mode-switch
//     MS_ARMS=a;b       arms: `base`, or `lab:<k=v,…>` (default base;lab:labelMatch=false;lab:paintDefault=false: today, then each T903 switch off)
//     MS_ROUNDS=n       rounds per arm (default 3)
//     MS_AT=lon,lat,z   (default -121.95,49.17,11: the Fraser Valley)
//
// (T903 rerun) Read the pans from `raw` within each fling's window (`marks`, the device's monotonic clock), not from the
// summary: `presents` hands back the layer's last ~128 frames, so the first fling's read also holds the SWITCH's own
// repaint frames (over half of them on the stressed emulator), and `mapFrames` drops every gap over 250 ms as "rest".
import fs from 'node:fs';
import path from 'node:path';
import { coldStart, toMap } from './common.mjs';
import { cameraTo } from './perf.mjs';
import { mapFrames, mapLayer, presents } from './pan.mjs';
import { applyLab, parseLab } from '../lib/lab.mjs';

const ARMS = (process.env.MS_ARMS ?? 'base;lab:labelMatch=false;lab:paintDefault=false')
  .split(';').map((s) => s.trim()).filter(Boolean);
const ROUNDS = Number(process.env.MS_ROUNDS ?? 3);
const AT = (process.env.MS_AT ?? '-121.95,49.17,11').split(',').map(Number);
const WAIT_MS = 5000;
/** each leg: the control pressed, and what the map shows after it */
const LEGS = [['Salmon', 'salmon'], ['Conditions', 'conditions'], ['Runs', 'runs'], ['Map', 'salmon'], ['Trout', 'trout']];
/** what each control says aloud (the rail's radios: `SALMON_WORDS.toggle.said`; the tabs: their label) */
const SAID = { Trout: "Colour by trout and other species' rules", Salmon: 'Colour by salmon openings', Map: 'MAP',
               Conditions: 'CONDITIONS', Runs: 'RUNS' };
const TABS = ['Map', 'Conditions', 'Runs'];

/** Each control's centre (px), from the screen's own layout. */
function controls(d) {
  d.sh('uiautomator dump /sdcard/tb-ms.xml >/dev/null 2>&1');
  const x = d.sh('cat /sdcard/tb-ms.xml');
  const out = {};
  for (const m of x.matchAll(/<node [^>]*?(?:text|content-desc)="([^"]+)"[^>]*?bounds="\[(\d+),(\d+)\]\[(\d+),(\d+)\]"/g)) {
    const label = m[1].replace(/&apos;|&#39;/g, "'").replace(/&amp;/g, '&').trim(), [x0, y0, x1, y1] = m.slice(2).map(Number);
    for (const [k, said] of Object.entries(SAID)) {
      if (out[k] || label.toUpperCase() !== said.toUpperCase()) continue;
      if (TABS.includes(k) && y0 < 1200) continue;   // the bottom bar's tabs, not a heading
      out[k] = [(x0 + x1) >> 1, (y0 + y1) >> 1];
    }
  }
  return out;
}

/** `[swap]` lines from the JS log. */
const swapLines = (js) => js.split('\n').filter((l) => l.includes('[swap] '))
  .map((l) => { try { return JSON.parse(l.slice(l.indexOf('[swap] ') + 7)); } catch { return null; } }).filter(Boolean);

const q = (a, p) => { const s = a.filter((x) => x !== null && Number.isFinite(x)).sort((x, y) => x - y);
  return s.length ? s[Math.min(s.length - 1, Math.floor(p * s.length))] : null; };

async function closeCoach(ctx, d) {
  for (let i = 0; i < 3; i++) {
    const w = d.screenText(), b = ['Skip', 'Got it'].find((s) => w.includes(s));
    if (!b) break;
    ctx.flow('tap-text', { TEXT: b }); await d.sleep(1500);
  }
}

export const modeSwitch = {
  id: 'mode-switch',
  title: `T903: the mode switch Trout → Salmon → Conditions → Runs → Map → Trout, tap → painted, + pan frames per mode (arms ${ARMS.join(' / ')})`,
  async run(ctx) {
    const d = ctx.device;
    const [w, h] = (/(\d+)x(\d+)/.exec(d.sh('wm size'))?.slice(1) ?? ['1080', '2400']).map(Number);
    const cx = Math.round(w / 2), cy = Math.round(h * 0.45), dx = Math.round(w * 0.3), dy = Math.round(h * 0.15);
    const flings = [[-dx, 0], [dx, 0], [0, dy], [0, -dy]];
    const out = { at: AT, rounds: ROUNDS, arms: [] };
    const save = () => fs.writeFileSync(path.join(ctx.dir, 'T903-mode-switch.json'), `${JSON.stringify(out, null, 1)}\n`);
    for (const arm of ARMS) {
      const lab = arm.startsWith('lab:') ? parseLab(arm.slice(4)) : {};
      const row = { arm, legs: [], pans: [] };
      out.arms.push(row);
      let at = null;
      await ctx.step(`${arm}: the map up (cold start), every control visited once${Object.keys(lab).length ? `, lab ${arm.slice(4)}` : ''}`, async () => {
        await d.net('wifi'); d.link('full', 'none');
        await coldStart(ctx, { ms: 90_000 });
        await toMap(ctx);
        cameraTo(ctx, { lon: AT[0], lat: AT[1], zoom: AT[2] });
        await d.sleep(6000);
        at = controls(d);
        for (const k of Object.keys(SAID)) if (!at[k]) throw new Error(`no "${k}" control on screen (${JSON.stringify(at)})`);
        // each visited once: the salmon slices, the Conditions files and the run-timing feed in, coach marks closed
        for (const k of ['Salmon', 'Conditions', 'Runs', 'Map', 'Trout']) {
          d.sh(`input tap ${at[k][0]} ${at[k][1]}`);
          await d.sleep(4000);
          await closeCoach(ctx, d);
        }
        const foot = Object.keys(lab).length ? await applyLab(lab, d) : '';
        cameraTo(ctx, { lon: AT[0], lat: AT[1], zoom: AT[2] });
        await d.sleep(5000);
        return `controls ${JSON.stringify(at)}${foot ? ` · lab: ${foot}` : ''}`;
      }, { stop: true });
      await ctx.step(`${arm}: ${ROUNDS} rounds of Trout → ${LEGS.map((l) => l[0]).join(' → ')}, tap → painted; 4 flings per mode`, async () => {
        const layer = mapLayer(ctx);
        for (let r = 0; r < ROUNDS; r++) {
          cameraTo(ctx, { lon: AT[0], lat: AT[1], zoom: AT[2] });
          await d.sleep(4000);
          for (const [k, mode] of LEGS) {
            d.logcatClear();
            // the device's wall clock at the tap: a log line written in the same shell as the tap, read back with its epoch
            // time (the app's `[swap]` times are its Date.now(), the same clock)
            d.sh(`log -p i -t T903tap ${k}; input tap ${at[k][0]} ${at[k][1]}`);
            await d.sleep(WAIT_MS);
            const tapAt = Math.round(Number(/^\s*(\d+\.\d+)/m.exec(d.adb(['logcat', '-d', '-v', 'epoch', '-s', 'T903tap:I']).toString().trim().split('\n').at(-1))?.[1]) * 1000);
            const swaps = swapLines(d.logcatJs()).filter((s) => s.t >= tapAt - 50);
            const last = swaps.at(-1) ?? null;
            const painted = last && last.paintMs !== null && last.loadMs !== null ? last.t - last.loadMs + last.paintMs : null;
            row.legs.push({ round: r, to: k, mode, loads: swaps.length, tapToPaint: painted === null ? null : painted - tapAt,
                            paintMs: last?.paintMs ?? null, loadMs: last?.loadMs ?? null, bareMs: last?.bareMs ?? null, bare: last?.bare ?? null });
            // four flings in this mode, the map surface's frame times
            // (T903 rerun) each fling marked on the device's monotonic clock (logcat -v monotonic, SurfaceFlinger's clock)
            // and the raw present times kept: under stress a real frame can take > 250 ms, which mapFrames drops as "rest"
            const times = [];
            for (const [mx, my] of flings) {
              d.sh(`log -p i -t T903pan s; input swipe ${cx} ${cy} ${cx + mx} ${cy + my} 120`);
              await d.sleep(900);
              if (layer) times.push(...presents(ctx, layer));
            }
            await d.sleep(800);
            const marks = d.adb(['logcat', '-d', '-v', 'monotonic', '-s', 'T903pan:I']).toString().split('\n')
              .map((l) => Number(/^\s*(\d+\.\d+)/.exec(l)?.[1])).filter(Number.isFinite).map((t) => Math.round(t * 1e9));
            const js2 = d.logcatJs();
            // style swaps that came DURING the flings (after the switch's own): a swap relabels every held tile
            const panSwaps = swapLines(js2).filter((s) => s.t >= tapAt - 50).slice(swaps.length).map((s) => ({ t: s.t - tapAt, why: s.why, loadMs: s.loadMs }));
            const tagged = (tag) => js2.split('\n').filter((l) => l.includes(`[${tag}] `))
              .map((l) => { try { return JSON.parse(l.slice(l.indexOf(`[${tag}] `) + tag.length + 3)); } catch { return null; } }).filter(Boolean);
            // the JS thread's stalls (ms, from the tap) and the probe's [pan] windows (style hand-overs counted in src)
            const stalls = tagged('stall').map((x) => [x.at - tapAt, x.ms]);
            const panLines = tagged('pan').map((x) => ({ t: x.t, tiles: x.tiles, labels: x.labels, frames: x.frames, full: x.full, src: x.src, lagMax: x.lagMax }));
            const drawn = js2.split('\n').filter((l) => l.includes('[drawn] '))
              .map((l) => { try { return JSON.parse(l.slice(l.indexOf('[drawn] ') + 8)); } catch { return null; } }).filter(Boolean);
            row.pans.push({ round: r, mode, ...mapFrames(times), marks, raw: [...new Set(times)].sort((a, b) => a - b), drawn, panSwaps, stalls, panLines });
            save();
          }
        }
        const by = (m) => row.legs.filter((l) => l.to === m);
        return LEGS.map(([k]) => `${k}: tap→painted p50 ${q(by(k).map((l) => l.tapToPaint), 0.5)} ms (${by(k).map((l) => l.tapToPaint).join('/')}), ` +
            `hand-over→painted ${by(k).map((l) => l.paintMs).join('/')}`).join(' | ') + ' || pans: ' +
          ['trout', 'salmon', 'conditions', 'runs'].map((m) => { const p = row.pans.filter((x) => x.mode === m);
            return `${m} >50 ms ${p.reduce((a, x) => a + x.over50, 0)}/${p.reduce((a, x) => a + x.frames, 0)} p95 ${q(p.map((x) => x.p95), 0.5)}`; }).join(', ');
      });
    }
  },
};
