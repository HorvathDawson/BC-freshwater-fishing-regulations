// (T566) THE TROUT / SALMON / RUNS SWITCH, MEASURED (owner, Pixel 9, build 24: "a blank or grey map, lag, and layers stay
// wrong for half a second or so"). The browse map at one place, the rail's Trout / Salmon buttons and the Runs tab pressed
// over and over in two orders, with
//   - a screen recording, cut by `regs-switch-frames.py`: per switch, when the KEY changed, when the MAP changed and
//     settled, how long the map showed the previous colours under the new key, and any frame whose streams went grey or
//     blank (far fewer coloured pixels than both the view before and the view after);
//   - the binding's held picture over each style swap (`CifSwap`: copy ms, hold ms, why it ended — `cap` is a hold that
//     ran out before the new style drew: what follows it may be a bare map).
// A benchmark, not a pass/fail: the numbers in the step line, `T566-switch.json`, `T566-frames.json` beside the report.
//
//   node app/tools/testbed/run.mjs regs-switch      (REGS_SWITCH_AT=lon,lat,zoom; REGS_SWITCH_ROUNDS=n, default 6)
import { execFileSync, spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { coldStart, toMap } from './common.mjs';
import { cameraTo } from './perf.mjs';

const AT = (process.env.REGS_SWITCH_AT ?? '-122.05,49.25,12.5').split(',').map(Number);
const ROUNDS = Number(process.env.REGS_SWITCH_ROUNDS ?? 6);
/** the two orders, alternated: Trout → Salmon → Runs → Map(salmon) → Trout, and Trout → Runs → Map(trout) → Salmon → Trout */
const FORWARD = ['Salmon', 'Runs', 'Map', 'Trout'];
const BACKWARD = ['Runs', 'Map', 'Salmon', 'Trout'];
const WAIT_MS = 2200;
/** what each control says aloud (the rail's radios: `SALMON_WORDS.toggle.said`; the tabs: their label) */
const SAID = { Trout: "Colour by trout and other species' rules", Salmon: 'Colour by salmon openings', Map: 'MAP', Runs: 'RUNS' };

/** Each control's centre (px), from the screen's own layout. */
function controls(d) {
  d.sh('uiautomator dump /sdcard/tb-rs.xml >/dev/null 2>&1');
  const x = d.sh('cat /sdcard/tb-rs.xml');
  const out = {};
  for (const m of x.matchAll(/<node [^>]*?(?:text|content-desc)="([^"]+)"[^>]*?bounds="\[(\d+),(\d+)\]\[(\d+),(\d+)\]"/g)) {
    const label = m[1].replace(/&apos;|&#39;/g, "'").replace(/&amp;/g, '&').trim(), [x0, y0, x1, y1] = m.slice(2).map(Number);
    for (const [k, said] of Object.entries(SAID)) {
      if (out[k] || label.toUpperCase() !== said.toUpperCase()) continue;
      if ((k === 'Map' || k === 'Runs') && y0 < 1200) continue;   // the bottom bar's tabs, not a heading
      out[k] = [(x0 + x1) >> 1, (y0 + y1) >> 1];
    }
  }
  return out;
}

export const regsSwitch = {
  id: 'regs-switch',
  title: `The Trout / Salmon / Runs switch × ${ROUNDS} rounds of two orders at ${AT.join(',')} — key vs map timing, grey frames, held swaps (T566)`,
  async run(ctx) {
    const d = ctx.device;
    let at = null;
    await ctx.step(`the map up (cold start, Wi-Fi), the camera at ${AT.join(',')}, every control found and visited once`, async () => {
      await d.net('wifi'); d.link('full', 'none');
      await coldStart(ctx, { ms: 90_000 });
      await toMap(ctx);
      cameraTo(ctx, { lon: AT[0], lat: AT[1], zoom: AT[2] });
      await d.sleep(6000);
      at = controls(d);
      for (const k of Object.keys(SAID)) if (!at[k]) throw new Error(`no "${k}" control on screen (${JSON.stringify(at)})`);
      // each visited once (the run-timing feed read, a first visit's coach marks closed as a reader would)
      for (const k of ['Salmon', 'Runs', 'Map', 'Trout']) {
        d.sh(`input tap ${at[k][0]} ${at[k][1]}`);
        await d.sleep(4000);
        for (let i = 0; i < 3; i++) {
          const w = d.screenText(), b = ['Skip', 'Got it'].find((s) => w.includes(s));
          if (!b) break;
          ctx.flow('tap-text', { TEXT: b }); await d.sleep(1500);
        }
      }
      return `controls ${JSON.stringify(at)}`;
    }, { stop: true });

    const out = { at: AT, controls: at, switches: [] };
    await ctx.step(`${ROUNDS} rounds of Trout → ${FORWARD.join(' → ')} and Trout → ${BACKWARD.join(' → ')} (${WAIT_MS} ms each), recorded`, async () => {
      d.logcatClear();
      const n = ROUNDS * (FORWARD.length + BACKWARD.length);
      const secs = Math.min(175, Math.ceil((n * WAIT_MS) / 1000) + 5);
      const rec = spawn(process.env.ADB ?? 'adb', ['-s', d.serial(), 'shell', 'screenrecord', '--bit-rate', '20000000', '--time-limit', String(secs), '/sdcard/t566.mp4'], { stdio: 'ignore' });
      await d.sleep(1500);
      const t0 = Date.now();
      let from = 'Trout';
      for (let r = 0; r < ROUNDS; r++) for (const k of [...FORWARD, ...BACKWARD]) {
        d.sh(`input tap ${at[k][0]} ${at[k][1]}`);
        out.switches.push({ from, to: k, ms: Date.now() - t0 });
        from = k;
        await d.sleep(WAIT_MS);
      }
      await new Promise((res) => { rec.on('exit', res); setTimeout(() => { try { d.sh('kill -2 $(pidof screenrecord)'); } catch { /* done */ } }, 2000); });
      await d.sleep(1500);
      const mp4 = path.join(ctx.dir, 'T566-switch.mp4');
      execFileSync(process.env.ADB ?? 'adb', ['-s', d.serial(), 'pull', '/sdcard/t566.mp4', mp4], { stdio: 'ignore' });
      fs.writeFileSync(path.join(ctx.dir, 'T566-switch.json'), `${JSON.stringify(out, null, 1)}\n`);
      const held = execFileSync(process.env.ADB ?? 'adb', ['-s', d.serial(), 'logcat', '-d', '-s', 'CifSwap:*']).toString();
      fs.writeFileSync(path.join(ctx.dir, 'T566-hold.log'), held);
      const ends = [...held.matchAll(/ end (\w+) (\d+)/g)].map((m) => ({ why: m[1], ms: Number(m[2]) }));
      const frames = execFileSync('python3', ['-I', path.join(path.dirname(new URL(import.meta.url).pathname), '..', 'regs-switch-frames.py'), ctx.dir]).toString().trim();
      const ms = ends.map((e) => e.ms).sort((a, b) => a - b);
      const q = (a, p) => (a.length ? a[Math.min(a.length - 1, Math.floor(p * a.length))] : null);
      const whys = Object.fromEntries([...new Set(ends.map((e) => e.why))].map((w) => [w, ends.filter((e) => e.why === w).length]));
      return `${out.switches.length} switches; held swaps ${ends.length} ${JSON.stringify(whys)}, hold p50 ${q(ms, 0.5)} / max ${ms.at(-1) ?? 0} ms; per direction:\n${frames}`;
    });
  },
};
