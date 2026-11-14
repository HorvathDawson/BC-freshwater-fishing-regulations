// (T498, T499) THE WATER PANE, FRAME BY FRAME. Owner 2026-11-08 on 0.6.9:
//   T498  a stream tapped on the map opens its pane "janky and flickery": the pane shows, then its content pops in.
//   T499  the pane's Map tab left open on the lowest Fraser stretch flickered now and then with no input.
//
// `pane-open`: each way in (a map tap; search → Open) recorded from the tap to a settled pane (`screenrecord` emits a
// frame only when the screen changes), the frames cut into the distinct screens shown (`pane-frames.py open`), each
// one saved; the open probe's `[open]` line (each stage's arrival) beside it.
// `map-idle`: Fraser River → Map → the parts list's lowest part, then hands off for IDLE_MIN minutes (default 5),
// recorded; every frame drawn while idle is a change nobody asked for (`pane-frames.py idle`), and the pan probe's
// `[pan]` lines (frames drawn, renders, feature-state sends, GeoJSON writes per second), `[swap]` (style loads), `[cam]`
// / `[move]` (camera) say what the map did then.
//
// (T508) `pane-open` also opens each case by search → Open (the Fraser lands on its Map view: a live map built under the
// hold), then repeats that Map-view open HOLD_RUNS times (default 20) and reads the whole log for a native crash
// (`Fatal signal` / SIGSEGV: one was seen once holding a Map view before T498's collapsable fix).
//
//   node app/tools/testbed/run.mjs pane-open map-idle      (OPEN_CASES=small,fraser; IDLE_MIN=n; HOLD_RUNS=n)
import { execFileSync, spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { closeWaters, coldStart, toMap } from './common.mjs';
import { settledTarget } from './regs-open.mjs';
import { cameraTo } from './perf.mjs';

const ADB = process.env.ADB ?? 'adb';
const FRAMES = path.join(path.dirname(new URL(import.meta.url).pathname), '..', 'pane-frames.py');
const CASES = [
  { id: 'small', q: 'Norrish Creek', row: 'Stream · Region 2 · MU 2-8 · near Dewdney.*', title: /Norrish Creek/ },
  // (T508) the Fraser framed whole is at a zoom where the Conditions map draws its watershed field, not streams: that
  // map is first sent to the Fraser at Hope at z10 (the camera link: the two tabs share one map), and tapped on the
  // river's line above Hope (a point read off that camera's screenshot, emulator 1080×2400; no water is lit there for
  // the probe's [target])
  { id: 'fraser', q: 'Fraser River', row: 'Stream · .*', title: /Fraser River/, condAt: { lon: -121.4419, lat: 49.3858, zoom: 10 },
    condTap: { x: 635, y: 720 } },
].filter((c) => !process.env.OPEN_CASES || process.env.OPEN_CASES.split(',').includes(c.id));
const IDLE_MIN = Number(process.env.IDLE_MIN ?? 5);
const HOLD_RUNS = Number(process.env.HOLD_RUNS ?? 20);

/** (T508) a native crash in the whole log since the last clear: the first lines of each (`Fatal signal`, a tombstone) */
function nativeCrash(d) {
  const log = execFileSync(ADB, ['-s', d.serial(), 'logcat', '-d', '-b', 'main,system,crash']).toString();
  const hit = log.split('\n').filter((l) => /Fatal signal|SIGSEGV|SIGABRT|backtrace:|FATAL EXCEPTION/.test(l));
  return hit.length ? hit.slice(0, 6).join(' / ').slice(0, 600) : null;
}

/** (T508) a recording of one way in, from `go()` (the tap) until settled, cut into the screens shown */
export async function recordOpen(ctx, name, go, settleMs = 7000) {
  const d = ctx.device;
  d.logcatClear();
  const r = record(d, path.join(ctx.dir, `${name}.mp4`), 14);
  await d.sleep(1200);
  const tapAt = Date.now() - r.t0;
  await go();
  await d.sleep(settleMs);
  await r.stop();
  fs.writeFileSync(path.join(ctx.dir, `${name}.log`), `${jsLines(d).join('\n')}\n`);
  return execFileSync('python3', ['-I', FRAMES, 'open', path.join(ctx.dir, `${name}.mp4`), String(tapAt / 1000)]).toString().trim();
}
const TAGS = /\[(pan|swap|cam|move|open|drawn|whole|hold)\] /;

/** A recording on the phone (≤ 180 s, the recorder's cap), pulled to `file` when `stop` is awaited. */
function record(d, file, secs) {
  const remote = `/sdcard/${path.basename(file)}`;
  const rec = spawn(ADB, ['-s', d.serial(), 'shell', 'screenrecord', '--bit-rate', '16000000', '--time-limit', String(secs), remote], { stdio: 'ignore' });
  const t0 = Date.now();
  // the recorder may have stopped by itself (its time limit) before `stop` is called: its exit is caught from the start
  const exited = new Promise((res) => rec.on('exit', res));
  let done = false;
  void exited.then(() => { done = true; });
  return {
    t0,
    async stop() {
      if (!done) setTimeout(() => { try { d.sh('pidof screenrecord >/dev/null && kill -2 $(pidof screenrecord)'); } catch { /* done */ } }, 500);
      await exited;
      await d.sleep(1500);
      execFileSync(ADB, ['-s', d.serial(), 'pull', remote, file], { stdio: 'ignore' });
      return file;
    },
  };
}

/** (T498) the app died under a step (a native crash the runner's crash-buffer check misses: its line names the
 *  process by a cut name): its pid moved or is gone */
function samePid(d, pid) {
  const now = d.pidOf();
  if (now !== pid) throw new Error(`the app died (pid ${pid} → ${now ?? 'none'}): see the step's Maestro device log for "Fatal signal"`);
}

const jsLines = (d) => d.logcatJs().split('\n').filter((l) => TAGS.test(l)).map((l) => l.replace(/^.*?(\[\w+\] )/, '$1'));

export const paneOpen = {
  id: 'pane-open',
  title: `A water's pane opened, frame by frame (${CASES.map((c) => c.id).join(', ')}; map tap and search → Open) (T498)`,
  async run(ctx) {
    const d = ctx.device;
    await ctx.step('the map up (cold start, Wi-Fi)', async () => {
      await d.net('wifi'); d.link('full', 'none');
      await coldStart(ctx, { ms: 90_000 });
      await d.sleep(4000);
    }, { stop: true });
    for (const c of CASES) {
      let at = null;
      await ctx.step(`${c.id}: search "${c.q}" → Open → Back (the map framed on it)`, async () => {
        await closeWaters(ctx);
        await toMap(ctx);
        d.logcatClear();
        const pid = d.pidOf();
        ctx.flow('search', { Q: c.q });
        ctx.flow('open-result', { LINE: c.row });
        await d.sleep(3500);
        samePid(d, pid);
        d.sh('input keyevent 4'); await d.sleep(3000);
        samePid(d, pid);
        at = await settledTarget(ctx);
        if (!at) throw new Error('the app named no tap target for the water (no [target] line)');
        return `target at ${at.x},${at.y} px`;
      });
      if (!at) continue;
      // twice: the first open of the water (cold) and the second (warm: its files read already)
      for (const pass of ['cold', 'warm']) {
        await ctx.step(`${c.id} (${pass}): the map tap → the pane, recorded`, async () => {
          d.logcatClear();
          const pid = d.pidOf();
          const name = `pane-open-${c.id}-${pass}`;
          const r = record(d, path.join(ctx.dir, `${name}.mp4`), 12);
          await d.sleep(1200);
          const tapAt = Date.now() - r.t0;
          d.sh(`input tap ${at.x} ${at.y}`);
          await d.sleep(7000);
          await r.stop();
          fs.writeFileSync(path.join(ctx.dir, `${name}.log`), `${jsLines(d).join('\n')}\n`);
          const out = execFileSync('python3', ['-I', FRAMES, 'open', path.join(ctx.dir, `${name}.mp4`), String(tapAt / 1000)]).toString().trim();
          d.sh('input keyevent 4'); await d.sleep(2500);
          samePid(d, pid);
          return out;
        });
      }
      // the same point on the Conditions map (the one browse map, its Conditions colouring): the Conditions view in front
      await ctx.step(`${c.id} (conditions): the Conditions-map tap → the pane, recorded`, async () => {
        // (T508) the camera moved on the Map tab (the two tabs share one map)
        let tapAt = at;
        if (c.condAt) {
          cameraTo(ctx, c.condAt);
          await d.sleep(4000);
          tapAt = c.condTap;                              // the river's line there, read off the Map tab's screenshot
        }
        ctx.flow('tab', { TAB: 'Conditions' });
        await d.sleep(5000);
        for (let k = 0; k < 3; k++) {                      // a first visit's coach marks, closed as a reader would
          const w = d.screenText(), btn = ['Skip', 'Got it'].find((x) => w.includes(x));
          if (!btn) break;
          ctx.flow('tap-text', { TEXT: btn }); await d.sleep(1500);
        }
        const pid = d.pidOf();
        const name = `pane-open-${c.id}-conditions`;
        d.screenshot(path.join(ctx.dir, `${name}-before-tap.png`));
        const out = await recordOpen(ctx, name, async () => { d.sh(`input tap ${tapAt.x} ${tapAt.y}`); });
        // (T508) the tap must have opened the water (a Conditions tap that lands on nothing measures nothing)
        const w = d.screenText();
        const opened = w.some((x) => c.title.test(x));
        d.sh('input keyevent 4'); await d.sleep(2500);
        if (!opened) throw new Error(`the Conditions-map tap opened no ${c.q} pane (${out})`);
        samePid(d, pid);
        ctx.flow('tab', { TAB: 'Map' });
        await d.sleep(2000);
        return out;
      });
    }
    // the other way in: search → Open (the map is replaced by the pane, as with a tap): for every case (T508: the Fraser
    // lands on its Map view, a live map built under the hold)
    for (const c of CASES) {
      await ctx.step(`${c.id}: search → Open → the pane, recorded`, async () => {
        await closeWaters(ctx);
        await toMap(ctx);
        ctx.flow('search', { Q: c.q });
        ctx.flow('tap-text', { TEXT: c.row });                // the preview (the map frames it), then Open, recorded
        await d.sleep(2500);
        // Maestro's tap lands a little after `tapAt`: the cut starts at the first change after it
        const out = await recordOpen(ctx, `pane-open-${c.id}-search`, async () => { ctx.flow('tap-text', { TEXT: 'Open .*' }); }, 6000);
        const w = d.screenText();
        return `${out}\nview: ${w.includes('Show regulations for this part') ? 'Map' : 'Regulations/Conditions'}`;
      });
    }
    // (T508) THE MAP-VIEW HOLD, AGAIN AND AGAIN: search → Open on the case that lands on its Map view, closed, HOLD_RUNS
    // times; the app's pid and the whole log (a native crash: `Fatal signal`) read after each
    const m = CASES.find((x) => x.id === 'fraser') ?? CASES[0];
    if (m && HOLD_RUNS > 0) await ctx.step(`${m.id}: search → Open (Map view held) × ${HOLD_RUNS}, no native crash`, async () => {
      await closeWaters(ctx);
      await toMap(ctx);
      execFileSync(ADB, ['-s', d.serial(), 'logcat', '-b', 'all', '-c']);
      const pid = d.pidOf();
      let mapViews = 0;
      for (let k = 0; k < HOLD_RUNS; k++) {
        ctx.flow('search', { Q: m.q });
        ctx.flow('open-result', { LINE: m.row });
        await d.sleep(3000);
        samePid(d, pid);
        const crash = nativeCrash(d);
        if (crash) throw new Error(`open ${k + 1}: ${crash}`);
        if (d.screenText().includes('Show regulations for this part')) mapViews++;
        await closeWaters(ctx);
        await d.sleep(1200);
        samePid(d, pid);
      }
      const crash = nativeCrash(d);
      if (crash) throw new Error(`after ${HOLD_RUNS} opens: ${crash}`);
      return `${HOLD_RUNS} opens (${mapViews} on the Map view), pid ${pid} throughout, no Fatal signal / SIGSEGV in logcat`;
    });
  },
};

export const mapIdle = {
  id: 'map-idle',
  title: `Fraser River → Map → the lowest part, left idle ${IDLE_MIN} min, recorded (T499)`,
  async run(ctx) {
    const d = ctx.device;
    await ctx.step('the map up (cold start, Wi-Fi)', async () => {
      await d.net('wifi'); d.link('full', 'none');
      await coldStart(ctx, { ms: 90_000 });
      await d.sleep(4000);
    }, { stop: true });
    await ctx.step('Fraser River: search → Open → Map → the lowest part', async () => {
      await closeWaters(ctx);
      await toMap(ctx);
      ctx.flow('search', { Q: 'Fraser River' });
      ctx.flow('open-result', { LINE: 'Stream · .*' });
      await ctx.waitText(/Fraser River/, 30_000);
      await d.sleep(2500);
      ctx.flow('tap-id', { ID: 'water-view-map' });
      await ctx.waitText(/Show all \d+ parts|Upstream|Downstream/, 30_000);
      await d.sleep(3000);
      // ▼ until the button says it is the lowest part (the mouth)
      const seen = [];
      for (let i = 0; i < 40; i++) {
        const w = d.screenText();
        seen.push(w.find((x) => /part \d+ of \d+/i.test(x)) ?? '?');
        if (w.some((x) => /Lowest part/i.test(x))) break;
        ctx.flow('tap-id', { ID: 'water-map-step-down' });
        await d.sleep(2500);
      }
      await d.sleep(8000);                                  // the fit, the copy's pass, the last render
      d.screenshot(path.join(ctx.dir, 'map-idle-start.png'));
      const w = d.screenText();
      if (!w.some((x) => /Lowest part/i.test(x))) throw new Error(`never reached the lowest part (${seen.join(' → ')})`);
      return seen.join(' → ');
    }, { stop: true });
    await ctx.step(`hands off for ${IDLE_MIN} min, recorded; frames drawn and the map's writes`, async () => {
      d.logcatClear();
      // streamed to a file: over minutes the device's log ring turns over and a read at the end finds almost nothing
      const jsFile = path.join(ctx.dir, 'map-idle-js.txt');
      const out = fs.openSync(jsFile, 'w');
      const cat = spawn(ADB, ['-s', d.serial(), 'logcat', '-v', 'epoch', '-s', 'ReactNativeJS:*'], { stdio: ['ignore', out, 'ignore'] });
      const parts = [];
      const t0 = Date.now();
      let left = IDLE_MIN * 60;
      for (let i = 0; left > 0; i++) {
        const secs = Math.min(175, left);
        const file = path.join(ctx.dir, `map-idle-${i}.mp4`);
        const r = record(d, file, secs);
        await d.sleep(secs * 1000 + 500);
        await r.stop();
        parts.push({ file, at: (r.t0 - t0) / 1000 });
        left -= secs;
      }
      cat.kill('SIGTERM'); fs.closeSync(out);
      const log = fs.readFileSync(jsFile, 'utf8').split('\n').filter((l) => TAGS.test(l)).map((l) => {
        const at = /^\s*(\d+\.\d+)/.exec(l)?.[1];
        return `${at ? `${Math.round((Number(at) * 1000 - t0) / 100) / 10}s ` : ''}${l.replace(/^.*?(\[\w+\] )/, '$1')}`;
      });
      fs.writeFileSync(path.join(ctx.dir, 'map-idle.log'), `${log.join('\n')}\n`);
      d.screenshot(path.join(ctx.dir, 'map-idle-end.png'));
      const cuts = parts.map((p) => execFileSync('python3', ['-I', FRAMES, 'idle', p.file, String(p.at)]).toString().trim());
      const pans = log.filter((l) => l.includes('[pan] ')).map((l) => JSON.parse(l.slice(l.indexOf('[pan] ') + 6)));
      const sum = (k) => pans.reduce((a, p) => a + (p[k] ?? 0), 0);
      const src = {};
      for (const p of pans) for (const [k, v] of Object.entries(p.src ?? {})) src[k] = (src[k] ?? 0) + v;
      return `${cuts.join('\n')}\nprobe: ${pans.length} busy seconds; frames ${sum('frames')} (full ${sum('full')}), renders ${sum('renders')}, ` +
        `sends ${sum('sends')}, tiles ${sum('tiles')}, writes ${JSON.stringify(src)}; swaps ${log.filter((l) => l.includes('[swap]')).length}, ` +
        `camera ${log.filter((l) => /\[(cam|move)\]/.test(l)).length}; busy seconds: ${log.filter((l) => l.includes('[pan] ')).map((l) => l.split(' [')[0]).join(', ')}`;
    });
  },
};
