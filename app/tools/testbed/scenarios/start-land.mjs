// (T1049) A START LINK LANDS ON ITS TAB FROM A COLD START: force-stop, (root) drop the page cache, open
// `https://<host>/?regs=<water>&tab=<tab>` as a VIEW intent, and read where the page came to rest: the start probe's
// `[start]` lines (RegsScreen, test bed only: each jump asked, each scroll event, each change of the section tops and the
// content height, when the follow ended and why) and the screen (the tab's section top against the scroll view's top).
// Landed = the page at rest with the tab's section at the top (scroll = its top + 2, or the page's end for a section
// that cannot reach the top). Then a run with a reader's drag during the follow (it must stop following) and a warm
// re-apply (the same link to the page already open).
//
//   node app/tools/testbed/run.mjs --no-r2 start-land
//   START_LAND_RUNS=6  START_LAND_TABS=gear,licence  START_LAND_WATER=gnis:3062  START_LAND_HOST=staging.canifishthis.ca
//   START_LAND_SETTLE=30 (s after the page mounts before it is read)  START_LAND_DRAG=1  START_LAND_WARM=1
//   START_LAND_PROXY=Infinity:8000 (each connection held 8 s: the feeds land after the page)
//   START_LAND_HOG=6 (busy loops on the phone during each cold start)
//   START_LAND_LINK=gprs:gprs (the emulator's line for the runs)
//   START_LAND_VARIANTS=a,b (each run's link carries `#<variant>`, alternated: a fragment the link reader ignores)
import fs from 'node:fs';
import path from 'node:path';
import { startThrottleProxy } from '../lib/throttle-proxy.mjs';

const RUNS = Number(process.env.START_LAND_RUNS ?? 6);
const TABS = (process.env.START_LAND_TABS ?? 'gear,licence').split(',');
const WATER = process.env.START_LAND_WATER ?? 'gnis:3062';
const HOST = process.env.START_LAND_HOST ?? 'staging.canifishthis.ca';
const SETTLE = Number(process.env.START_LAND_SETTLE ?? 30) * 1000;
const VARIANTS = (process.env.START_LAND_VARIANTS ?? '').split(',').filter(Boolean);
const APP = 'ca.canifishthis.app';
/** the emulator's link for every run, `<speed>:<delay>` (`adb emu network`; e.g. `gprs:gprs`): a slow phone's line */
const LINK = process.env.START_LAND_LINK ?? null;
/** busy loops run on the phone through each cold start (a CPU-starved phone: the slow cold start), killed after it */
const HOG = Number(process.env.START_LAND_HOG ?? 0);
/** `<bytes/s>:<ms>` the phone's line through the bench's proxy (`lib/throttle-proxy.mjs`): a rate (`Infinity`: none) and a
 *  delay before each connection opens. A slow line makes the live feeds (DFO salmon, in-season) land after the page */
const PROXY = process.env.START_LAND_PROXY ?? null;

function hogStart(ctx) {
  const pids = [];
  for (let i = 0; i < HOG; i++) {
    const out = ctx.device.sh(`nohup sh -c 'while :; do :; done' >/dev/null 2>&1 & echo $!`).trim();
    if (/^\d+$/.test(out)) pids.push(out);
  }
  if (pids.length !== HOG) throw new Error(`START_LAND_HOG=${HOG}: started ${pids.length} busy loops`);
  return pids;
}
const hogStop = (ctx, pids) => { if (pids.length) ctx.device.sh(`kill ${pids.join(' ')} 2>/dev/null; true`); };

const urlOf = (tab, variant) => `https://${HOST}/?regs=${WATER}&tab=${tab}${variant ? `#${variant}` : ''}`;

/** The `[start]` lines of the log, parsed. */
export function startLines(log) {
  const out = [];
  for (const l of log.split('\n')) {
    const i = l.indexOf('[start] ');
    if (i < 0) continue;
    try { out.push(JSON.parse(l.slice(i + 8))); } catch { /* a cut line */ }
  }
  return out;
}

/** Where the page came to rest, from the probe's lines: the last `tops` line and the start's end. */
export function verdict(lines, tab, t0) {
  const last = (ev) => [...lines].reverse().find((x) => x.ev === ev) ?? null;
  const tops = last('tops'), mount = lines.find((x) => x.ev === 'mount') ?? null, done = lines.find((x) => x.ev === 'done') ?? null;
  const jumps = lines.filter((x) => x.ev === 'jump');
  const sizes = lines.filter((x) => x.ev === 'size');
  if (!tops) return { landed: false, why: 'no [start] tops line (page not shown?)', jumps: jumps.length };
  const top = tops.tops?.[tab];
  const maxY = tops.ch - tops.vh;
  const want = top == null ? null : Math.min(top + 2, maxY);
  const landed = want != null && Math.abs(tops.sy - want) <= 2;
  const rel = (x) => (x == null ? null : Math.round((x.t - t0) / 100) / 10);
  return {
    landed, sy: Math.round(tops.sy), want: want == null ? null : Math.round(want), top: top == null ? null : Math.round(top),
    ch: Math.round(tops.ch), mountS: rel(mount), doneS: rel(done), doneWhy: done?.why ?? null, jumps: jumps.length,
    lastJumpS: rel(jumps.at(-1)), lastSizeS: rel(sizes.at(-1)), lastTopsS: rel(tops),
    takenBy: lines.filter((x) => x.ev === 'scroll' && x.own === false).map((x) => ({ s: rel(x), y: Math.round(x.y), ch: Math.round(x.ch ?? 0) })).slice(0, 3),
    rule: lines.find((x) => x.ev === 'rule')?.rule ?? null,
  };
}

/** The tab's section and the scroll view on screen (device px), from one uiautomator dump. */
function onScreen(ctx, tab) {
  try {
    const r = ctx.device.sh('uiautomator dump /sdcard/tb-start.xml 2>&1');
    if (!/dumped to/.test(r)) return null;
    const x = ctx.device.sh('cat /sdcard/tb-start.xml');
    const box = (id) => {
      const m = new RegExp(`resource-id="${id}"[^>]*bounds="\\[(\\d+),(\\d+)\\]\\[(\\d+),(\\d+)\\]"`).exec(x);
      return m ? { y0: Number(m[2]), y1: Number(m[4]) } : null;
    };
    return { section: box(`regs-${tab}`), scroll: box('regs-scroll') };
  } catch { return null; }
}

function dropCaches(ctx) {
  try { ctx.device.sh(`su 0 sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'`); return true; } catch { return false; }
}

const deviceMs = (ctx) => Number(ctx.device.sh('date +%s%3N').trim());

async function waitDone(ctx, ms, onJump) {
  const end = Date.now() + ms;
  let lines = [];
  while (Date.now() < end) {
    lines = startLines(ctx.device.logcatJs());
    if (onJump && lines.some((x) => x.ev === 'jump')) { await onJump(); onJump = null; }
    if (lines.some((x) => x.ev === 'done')) return lines;
    await ctx.device.sleep(300);
  }
  return lines;
}

async function settleAfter(ctx, lines) {
  const mount = lines.find((x) => x.ev === 'mount');
  const now = deviceMs(ctx);
  const wait = mount ? mount.t + SETTLE - now : 0;
  if (wait > 0) await ctx.device.sleep(wait);
}

export const startLand = {
  id: 'start-land',
  title: `A start link lands on its tab from a cold start (${TABS.join(', ')} × ${RUNS}${VARIANTS.length ? `, variants ${VARIANTS.join('/')}` : ''})`,
  async run(ctx) {
    // the phone online on Wi-Fi first: a run left offline (a line the system judged dead) reads no feed, and its salmon
    // card is the capture's, in at the first layout: not the slow start this scenario is for
    if (ctx.device.netState() === 'none') await ctx.device.net('wifi');
    ctx.finding(`the phone's network at the start: ${ctx.device.netState()}`);
    if (!PROXY) return startLand.steps(ctx);
    const [rate, delay] = PROXY.split(':').map(Number);
    const proxy = await startThrottleProxy({ rate, connectDelayMs: delay || 0 });
    const unset = () => { try { ctx.device.sh('settings put global http_proxy :0'); } catch { /* the device is gone */ } };
    try {
      ctx.device.sh(`settings put global http_proxy 10.0.2.2:${proxy.port}`);
      await startLand.steps(ctx);
      ctx.finding(`the phone's line: the bench's proxy (${PROXY}), ${proxy.bytes} bytes through it`);
    } finally { unset(); await proxy.close(); }
  },
  async steps(ctx) {
    const rows = [];
    if (LINK) { const [sp, dl] = LINK.split(':'); ctx.device.link(sp, dl ?? 'none'); }
    const save = () => fs.writeFileSync(path.join(ctx.dir, 'T1049-start-land.json'), JSON.stringify(rows, null, 1));
    for (const tab of TABS) {
      for (let i = 0; i < RUNS * Math.max(1, VARIANTS.length); i++) {
        const variant = VARIANTS.length ? VARIANTS[i % VARIANTS.length] : null;
        await ctx.step(`cold ${tab} ${i + 1}${variant ? ` (${variant})` : ''}`, async () => {
          ctx.device.stop();
          await ctx.device.sleep(800);
          const dropped = dropCaches(ctx);
          ctx.device.logcatClear();
          const hog = hogStart(ctx);
          const t0 = deviceMs(ctx);
          let lines;
          try {
            ctx.device.sh(`am start -a android.intent.action.VIEW -d '${urlOf(tab, variant)}' ${APP}`);
            lines = await waitDone(ctx, 120_000);
            await settleAfter(ctx, lines);
          } finally { hogStop(ctx, hog); }
          const all = startLines(ctx.device.logcatJs());
          const v = verdict(all, tab, t0);
          const scr = onScreen(ctx, tab);
          fs.writeFileSync(path.join(ctx.dir, `T1049-${tab}-${i + 1}${variant ? `-${variant}` : ''}.log`), all.map((x) => JSON.stringify(x)).join('\n'));
          rows.push({ kind: 'cold', tab, run: i + 1, variant, dropped, ...v, screen: scr }); save();
          const s = `${v.landed ? 'LANDED' : 'SHORT'} sy=${v.sy} want=${v.want} ch=${v.ch} page@${v.mountS}s done@${v.doneS}s (${v.doneWhy}) jumps=${v.jumps} lastSize@${v.lastSizeS}s` +
            (scr?.section && scr?.scroll ? ` screen: section ${scr.section.y0} / scroll ${scr.scroll.y0}` : '');
          if (!v.landed) ctx.finding(`cold ${tab} ${i + 1}${variant ? ` (${variant})` : ''}: ${s}`);
          return s;
        });
      }
    }
    if (process.env.START_LAND_DRAG !== '0') {
      await ctx.step('cold licence, a reader drags during the follow: it stops following', async () => {
        ctx.device.stop();
        await ctx.device.sleep(800);
        dropCaches(ctx);
        ctx.device.logcatClear();
        const hog = hogStart(ctx);
        const t0 = deviceMs(ctx);
        let dragAt = null;
        try {
          ctx.device.sh(`am start -a android.intent.action.VIEW -d '${urlOf('licence', null)}' ${APP}`);
          const lines = await waitDone(ctx, 120_000, async () => { dragAt = deviceMs(ctx); ctx.device.sh('input swipe 30 1700 30 1300 250'); });
          await settleAfter(ctx, lines);
        } finally { hogStop(ctx, hog); }
        const all = startLines(ctx.device.logcatJs());
        fs.writeFileSync(path.join(ctx.dir, 'T1049-drag.log'), all.map((x) => JSON.stringify(x)).join('\n'));
        const drag = all.find((x) => x.ev === 'drag'), done = all.find((x) => x.ev === 'done');
        const after = drag ? all.filter((x) => x.ev === 'jump' && x.t > drag.t).length : null;
        rows.push({ kind: 'drag', dragAt: dragAt && dragAt - t0, drag: drag && drag.t - t0, done, jumpsAfterDrag: after }); save();
        if (!drag) throw new Error('no drag event seen');
        if (done?.why !== 'taken' || after) throw new Error(`the follow went on after the drag (done ${done?.why}, ${after} jumps after)`);
        return `drag at +${drag.t - t0} ms, done: ${done.why} at +${done.t - t0} ms, no jump after it`;
      });
    }
    if (process.env.START_LAND_WARM !== '0') {
      await ctx.step('warm re-apply: tab=gear to the page already open', async () => {
        ctx.device.logcatClear();
        const t0 = deviceMs(ctx);
        ctx.device.sh(`am start -a android.intent.action.VIEW -d '${urlOf('gear', null)}' ${APP}`);
        await waitDone(ctx, 60_000);
        await ctx.device.sleep(5000);
        const all = startLines(ctx.device.logcatJs());
        fs.writeFileSync(path.join(ctx.dir, 'T1049-warm.log'), all.map((x) => JSON.stringify(x)).join('\n'));
        // the page did not remount: its tops lines are the mount's watcher's (60 s), or none; the end is read off the start's done
        const done = all.find((x) => x.ev === 'done');
        if (!done) throw new Error('the start never ended');
        const top = done.tops?.gear, want = top == null ? null : Math.min(top + 2, done.ch - (all.find((x) => x.ev === 'jump')?.vh ?? 0));
        const scr = onScreen(ctx, 'gear');
        const landed = want != null && Math.abs(done.sy - want) <= 2;
        rows.push({ kind: 'warm', done, landed, screen: scr, t: done.t - t0 }); save();
        const s = `${landed ? 'LANDED' : 'SHORT'} sy=${Math.round(done.sy)} want=${want} (${done.why})` + (scr?.section ? ` screen: section ${scr.section.y0} / scroll ${scr.scroll?.y0}` : '');
        if (!landed) throw new Error(s);
        return s;
      });
    }
  },
};
