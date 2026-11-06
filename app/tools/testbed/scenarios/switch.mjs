// (T451) THE TAB SWITCH, MEASURED: the browse map switched Map / Conditions / Runs over and over at one place, with
//   - the pan probe's `[swap]` lines (`@app/map` native/panProbe.ts): per style load, how many styles the switch handed
//     the binding (2+: it swapped twice), the frames drawn between the load and the labels landing (drawn without the
//     engine's colours), and whether feature-state survived the swap;
//   - a screen recording of the switches, cut to frames: a frame that differs from BOTH the view before the switch and
//     the view after it (a "detour") is a flicker — a grey or blank frame between two coloured ones.
// A benchmark, not a pass/fail: numbers in the step lines, `T451-switch.json` and the frames beside the report.
//
//   node app/tools/testbed/run.mjs switch      (SWITCH_AT=lon,lat,zoom; SWITCH_ROUNDS=n, default 4)
import { execFileSync, spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { coldStart, toMap } from './common.mjs';
import { cameraTo } from './perf.mjs';

const AT = (process.env.SWITCH_AT ?? '-122.05,49.25,12.5').split(',').map(Number);
const ROUNDS = Number(process.env.SWITCH_ROUNDS ?? 4);
/** each round: to Conditions, to Map, to Runs, to Map, Conditions → Runs, Runs → Conditions, back to Map */
const ORDER = ['Conditions', 'Map', 'Runs', 'Map', 'Conditions', 'Runs', 'Conditions', 'Map'];
const WAIT_MS = 2600;

/** The bottom bar's tabs: label → centre (px), from the screen's own layout. */
function tabCentres(d) {
  d.sh('uiautomator dump /sdcard/tb-tabs.xml >/dev/null 2>&1');
  const x = d.sh('cat /sdcard/tb-tabs.xml');
  const out = {};
  for (const m of x.matchAll(/<node [^>]*?(?:text|content-desc)="([^"]+)"[^>]*?bounds="\[(\d+),(\d+)\]\[(\d+),(\d+)\]"/g)) {
    const label = m[1].trim(), [x0, y0, x1, y1] = m.slice(2).map(Number);
    for (const t of new Set(ORDER)) if (label.toUpperCase() === t.toUpperCase() && y0 > 1200 && !out[t]) out[t] = [(x0 + x1) >> 1, (y0 + y1) >> 1];
  }
  return out;
}

/** `[swap]` lines from the JS log. */
const swapLines = (js) => js.split('\n').filter((l) => l.includes('[swap] '))
  .map((l) => { try { return JSON.parse(l.slice(l.indexOf('[swap] ') + 7)); } catch { return null; } }).filter(Boolean);

export const tabSwitch = {
  id: 'switch',
  title: `The tab switch: Map / Conditions / Runs × ${ROUNDS} rounds at ${AT.join(',')} — style swaps, frames drawn uncoloured, a recording's detour frames (T451)`,
  async run(ctx) {
    const d = ctx.device;
    let tabs = null;
    await ctx.step(`the map up (cold start, Wi-Fi), the camera at ${AT.join(',')} (camera link)`, async () => {
      await d.net('wifi'); d.link('full', 'none');
      await coldStart(ctx, { ms: 90_000 });
      await toMap(ctx);
      cameraTo(ctx, { lon: AT[0], lat: AT[1], zoom: AT[2] });
      await d.sleep(6000);
      tabs = tabCentres(d);
      for (const t of new Set(ORDER)) if (!tabs[t]) throw new Error(`no "${t}" tab on screen (${JSON.stringify(tabs)})`);
      // every tab visited once, so the run-timing feed and the Conditions files are in before anything is timed
      // (a first visit's coach marks, "Skip" / "Got it", are closed as a reader would: it sits over the tab bar)
      for (const t of ['Runs', 'Conditions', 'Map', 'Runs', 'Map']) {
        d.sh(`input tap ${tabs[t][0]} ${tabs[t][1]}`);
        await d.sleep(4000);
        for (let k = 0; k < 3; k++) {
          const w = d.screenText(), b = ['Skip', 'Got it'].find((x) => w.includes(x));
          if (!b) break;
          ctx.flow('tap-text', { TEXT: b }); await d.sleep(1500);
        }
      }
      return `tabs ${JSON.stringify(tabs)}`;
    }, { stop: true });

    const out = { at: AT, switches: [], swaps: [] };
    await ctx.step(`${ROUNDS} rounds of ${ORDER.join(' → ')} (${WAIT_MS} ms each), recorded`, async () => {
      d.logcatClear();
      const secs = Math.min(170, Math.ceil((ROUNDS * ORDER.length * WAIT_MS) / 1000) + 4);
      const rec = spawn(process.env.ADB ?? 'adb', ['-s', d.serial(), 'shell', 'screenrecord', '--bit-rate', '20000000', '--time-limit', String(secs), '/sdcard/t451.mp4'], { stdio: 'ignore' });
      await d.sleep(1500);
      const t0 = Date.now();
      let from = 'Map';
      for (let r = 0; r < ROUNDS; r++) for (const t of ORDER) {
        d.sh(`input tap ${tabs[t][0]} ${tabs[t][1]}`);
        out.switches.push({ from, to: t, ms: Date.now() - t0 });
        from = t;
        await d.sleep(WAIT_MS);
      }
      // the recorder (on the phone) stopped by its own pid, so the file is finished
      await new Promise((res) => { rec.on('exit', res); setTimeout(() => { try { d.sh('kill -2 $(pidof screenrecord)'); } catch { /* done */ } }, 2000); });
      await d.sleep(1500);
      const js = d.logcatJs();
      fs.writeFileSync(path.join(ctx.dir, 'T451-js.log'), js);
      out.swaps = swapLines(js);
      const mp4 = path.join(ctx.dir, 'T451-switch.mp4');
      execFileSync(process.env.ADB ?? 'adb', ['-s', d.serial(), 'pull', '/sdcard/t451.mp4', mp4], { stdio: 'ignore' });
      fs.writeFileSync(path.join(ctx.dir, 'T451-switch.json'), `${JSON.stringify(out, null, 1)}\n`);
      const s = out.swaps;
      const twice = s.filter((x) => x.handed > 1).length, bare = s.filter((x) => x.bare > 0);
      const kept = s.filter((x) => x.kept === true).length, lost = s.filter((x) => x.kept === false).length;
      const q = (a, p) => (a.length ? a[Math.min(a.length - 1, Math.floor(p * a.length))] : null);
      const bareN = s.map((x) => x.bare).sort((a, b) => a - b), bareMs = s.map((x) => x.bareMs).filter((x) => x !== null).sort((a, b) => a - b);
      const whys = {};
      for (const x of s) for (const w of x.why) whys[w] = (whys[w] ?? 0) + 1;
      return `${out.switches.length} switches → ${s.length} style loads; ${twice} loads followed 2+ hand-overs; ` +
        `frames drawn before the labels landed: ${bare.length}/${s.length} loads, p50 ${q(bareN, 0.5)} / max ${bareN.at(-1)} frames, ` +
        `load → labels p50 ${q(bareMs, 0.5)} / max ${bareMs.at(-1)} ms; feature-state kept over a swap ${kept}, lost ${lost}; why ${JSON.stringify(whys)}`;
    });
  },
};
