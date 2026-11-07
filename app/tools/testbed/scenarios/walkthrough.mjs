// (T293, T456) THE GUIDED WALKTHROUGH, END TO END ON A PHONE: replayed from More → Help, then every step until the card is
// gone. Each step's count and title are recorded with a screenshot (out/t456/step-NN.png), and the card's box is checked
// against the ring's (uiautomator bounds of `guide-step-*` and `guide-ring`): they must never meet (T456 item 1).
//
// INTERACTIVE (T456, owner 2026-11-06): on "Back to the map" the reader's own tap on the page's "‹ Map" (`water-back`) is
// what moves the walk on — the walk must land on the NEXT step (Layers), not step back. Every other step: its primary
// button (Show me on a DO step, Next on a SHOW step, Start exploring at the end).
//
// GUARDRAIL (T456 item 4): on a Regulations step, Android back leaves the water page; the walk must pause behind
// "Resume walkthrough", and Resume must put the page back with the same step up.
//
// The walk is screen-recorded (adb screenrecord, chunks of at most 3 minutes) into out/t456/walk-N.mp4.
//
//   node app/tools/testbed/run.mjs walkthrough
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { coldStart, toMap } from './common.mjs';

/** the card's count line: "Find a water · 3 of 25" (upper-cased on screen) */
const COUNT = /(\d+) of (\d+)$/i;
const HERE = path.dirname(new URL(import.meta.url).pathname);
const OUT = path.join(HERE, '..', 'out', 't456');
const CHUNK_S = 170;

/** uiautomator's bounds for each resource-id we ask for: { id: {x, y, w, h} } (first match) */
function boundsOf(d, ids) {
  const r = d.sh('uiautomator dump /sdcard/tb-ui.xml 2>&1');
  if (!/dumped to/.test(r)) return null;
  const x = d.sh('cat /sdcard/tb-ui.xml');
  const out = {};
  for (const m of x.matchAll(/resource-id="([^"]+)"[^>]*?bounds="\[(\d+),(\d+)\]\[(\d+),(\d+)\]"/g)) {
    const [, id, x0, y0, x1, y1] = m;
    if (out[id] || !ids.some((want) => (want instanceof RegExp ? want.test(id) : want === id))) continue;
    out[id] = { x: +x0, y: +y0, w: +x1 - +x0, h: +y1 - +y0 };
  }
  return out;
}
const meets = (a, b) => a.x < b.x + b.w && b.x < a.x + a.w && a.y < b.y + b.h && b.y < a.y + a.h;

export const walkthrough = {
  id: 'walkthrough',
  title: 'The guided walkthrough (T293, T456) replayed from Help: every step, the card never over its ring, ‹ Map moves it on, back pauses it',
  async run(ctx) {
    const d = ctx.device;
    fs.mkdirSync(OUT, { recursive: true });
    const adbBin = path.join(process.env.ANDROID_HOME ?? `${process.env.HOME}/Library/Android/sdk`, 'platform-tools/adb');
    const chunks = [];
    let rec = null, recAt = 0;
    const record = () => {
      const n = chunks.length + 1, remote = `/sdcard/t456-walk-${n}.mp4`;
      chunks.push(remote);
      rec = spawn(adbBin, ['-s', d.serial(), 'shell', 'screenrecord', '--time-limit', String(CHUNK_S + 5), remote], { stdio: 'ignore' });
      recAt = Date.now();
    };
    const stopRecording = async () => {
      // our own screenrecord only (device-side pid, by its file): it finalises the mp4
      for (const p of d.sh('ps -A -o PID,ARGS').split('\n').filter((l) => /screenrecord .*t456-walk/.test(l))) {
        const pid = p.trim().split(/\s+/)[0];
        if (pid) d.sh(`kill -2 ${pid}`);
      }
      await d.sleep(2500);
      try { rec?.kill(); } catch { /* ended */ }
      rec = null;
    };
    const rotate = async () => { if (rec && Date.now() - recAt > CHUNK_S * 1000) { await stopRecording(); record(); } };
    const at = () => { const c = d.screenText().find((x) => COUNT.test(x)); return c ? Number(COUNT.exec(c)[1]) : null; };

    await ctx.step('More → Help → Replay the walkthrough', async () => {
      await d.net('wifi'); d.link('full', 'none');
      await coldStart(ctx, { ms: 90_000 });
      await toMap(ctx);
      ctx.flow('tab', { TAB: 'More' });
      await d.sleep(1500);
      ctx.flow('scroll-to', { TEXT: 'Help' });                                 // below the Data list on the More tab
      ctx.flow('tap-text', { TEXT: 'Help' });
      await d.sleep(1500);
      ctx.flow('scroll-to', { TEXT: 'Replay the walkthrough' });
      record();
      ctx.flow('tap-text', { TEXT: 'Replay the walkthrough' });
      await d.sleep(3000);
      const w = d.screenText();
      const c = w.find((x) => COUNT.test(x));
      if (!c) throw new Error(`no walkthrough card (saw: ${w.slice(0, 10).join(' | ').slice(0, 300)})`);
      return c;
    });

    const checks = [];
    let pausedChecked = false, mapChecked = false;
    await ctx.step('every step: the reader\'s tap (‹ Map on its step), never a card over its ring, until the card closes', async () => {
      const seen = [];
      let last = null, same = 0, total = 0, reached = 0, closed = false;
      for (let i = 0; i < 90; i++) {
        await rotate();
        const w = d.screenText();
        const c = w.find((x) => COUNT.test(x));
        if (!c) {
          if (w.includes('Resume walkthrough')) throw new Error(`paused unexpectedly after ${last}`);
          closed = true; break;
        }
        const [, n0, n] = COUNT.exec(c).map(Number);
        total = n; reached = Math.max(reached, n0);
        const title = w.slice(w.indexOf(c) + 1).find((x) => !/^Skip/i.test(x)) ?? '';
        if (c !== last) {
          await d.sleep(900);                                                  // the ring is measured every 400 ms
          seen.push(`${c} ${title}`);
          d.screenshot(path.join(OUT, `step-${String(n0).padStart(2, '0')}.png`));
          const b = boundsOf(d, [/^guide-step-/, 'guide-ring']);
          const card = b && Object.entries(b).find(([k]) => k.startsWith('guide-step-'));
          const ring = b?.['guide-ring'];
          const line = `${n0}/${n} ${title}: card ${card ? JSON.stringify(card[1]) : '-'} ring ${ring ? JSON.stringify(ring) : 'none'}`;
          checks.push(line + (card && ring && meets(card[1], ring) ? '  << COVERS' : ''));
          last = c; same = 0;
        } else if (++same > 4) throw new Error(`stuck at ${c} "${title}" (${seen.length} steps seen: ${seen.slice(-4).join(' / ')})`);

        // (T456 item 4) once, on a Regulations reading step: Android back leaves the page → paused; Resume → same step
        if (!pausedChecked && /Open or closed/i.test(title)) {
          pausedChecked = true;
          d.sh('input keyevent 4'); await d.sleep(3500);
          const p = d.screenText();
          d.screenshot(path.join(OUT, `step-${String(n0).padStart(2, '0')}-paused.png`));
          if (!p.includes('Resume walkthrough')) throw new Error(`back on "${title}" did not pause the walk (saw ${p.slice(0, 8).join(' | ')})`);
          ctx.flow('tap-id', { ID: 'guide-resume' }); await d.sleep(4000);
          if (at() !== n0) throw new Error(`Resume did not bring back step ${n0} (at ${at()})`);
          checks.push(`${n0}/${n} back → paused → Resume → step ${at()}`);
          continue;
        }
        // (T456 item 3 report) "Back to the map": the reader's own ‹ Map — the page closes and the walk goes ON
        if (/Back to the map/i.test(title)) {
          mapChecked = true;
          ctx.flow('tap-id', { ID: 'water-back' }); await d.sleep(3500);
          const now = at();
          if (now !== n0 + 1) throw new Error(`‹ Map on step ${n0} left the walk at ${now}, not ${n0 + 1}`);
          checks.push(`${n0}/${n} ‹ Map (the page's own) → step ${now}`);
          continue;
        }
        // the primary button: Show me / Next / Done / Start exploring (its own view: guide-primary)
        ctx.flow('tap-id', { ID: 'guide-primary' });
        await d.sleep(n0 === 1 ? 2500 : 3500);                                 // a Show me moves the route and the camera
      }
      fs.writeFileSync(path.join(OUT, 'checks.txt'), `${checks.join('\n')}\n`);
      if (!seen.length) throw new Error('no walkthrough card was up');
      if (!closed) throw new Error(`the card never closed (${seen.length} steps: ${seen.slice(-3).join(' / ')})`);
      if (reached < total) throw new Error(`closed at ${reached} of ${total}: ${seen.join(' / ')}`);
      if (!pausedChecked || !mapChecked) throw new Error(`a guardrail step was not reached (paused ${pausedChecked}, ‹ Map ${mapChecked})`);
      const covers = checks.filter((l) => l.includes('COVERS'));
      if (covers.length) throw new Error(`the card covers its ring on ${covers.length} step(s): ${covers.join(' / ').slice(0, 600)}`);
      return `${reached} of ${total} · ${seen.join(' / ').slice(0, 900)}`;
    });
    await ctx.step('back on the map; nothing left over (no card, no sheet); the recording pulled', async () => {
      await d.sleep(1500);
      d.screenshot(path.join(OUT, 'after.png'));
      await stopRecording();
      chunks.forEach((remote, i) => d.adb(['pull', remote, path.join(OUT, `walk-${i + 1}.mp4`)]));
      fs.writeFileSync(path.join(OUT, 'checks.txt'), `${checks.join('\n')}\n`);
      await toMap(ctx);
      const w = d.screenText();
      if (w.some((x) => COUNT.test(x))) throw new Error('a walkthrough card is still up');
      return `${chunks.length} recording chunk(s)`;
    });
  },
};
