// (T293, T456) THE GUIDED WALKTHROUGH, END TO END ON A PHONE: replayed from More → Help, then every step until the card is
// gone. Each step's count and title are recorded with a screenshot, and the card's box is checked against the ring's
// (uiautomator bounds of `guide-step-*` and `guide-ring`): they must never meet (T456 item 1).
//
// GUARDRAIL (T456 item 4): on a Regulations step, Android back leaves the water page; the walk must pause behind
// "Resume walkthrough", and Resume must put the page back with the same step up.
//
// (T842, owner on 0.7.12, two screenshots: the box trailed a scroll of the search list; the keyboard hid the Vedder River
// row) SIX STEPS (his pick): search for it, open its page, open or closed, another day, how the river is, you're all set.
//   - step 1 is done by the reader: the field tapped and "ved" typed, so step 2 comes up WITH THE KEYBOARD UP. Step 2 must
//     close it (`dumpsys input_method`), and its card must not meet the ring, with the ring wholly on the screen.
//   - step 2, A SCROLL DURING IT: a slow drag that starts inside the ring (the dim takes drags elsewhere) scrolls the list;
//     frames are grabbed while it moves (out/t842/pick-scroll-N.png: the ring on the row in every one is the check by eye)
//     and, once it settles, the ring must still hold the "Vedder River" row (the ring rides the scroll; it is not re-placed).
//   - then the keyboard up again (the reader taps the field): the card stays above it and off the ring; back closes it.
// Shots and checks: out/t842/ (step-NN.png, pick-*.png, checks.txt). The walk is screen-recorded into out/t842/walk-N.mp4.
//
//   node app/tools/testbed/run.mjs walkthrough
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { coldStart, toMap } from './common.mjs';
import { look } from '../lib/screen.mjs';

/** the card's count line: "Find a water · 2 of 6" (upper-cased on screen) */
const COUNT = /(\d+) of (\d+)$/i;
const HERE = path.dirname(new URL(import.meta.url).pathname);
const OUT = path.join(HERE, '..', 'out', 't842');
const CHUNK_S = 170;
const STEPS = 6;

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
const holds = (outer, inner, slack = 4) => inner.x >= outer.x - slack && inner.y >= outer.y - slack
  && inner.x + inner.w <= outer.x + outer.w + slack && inner.y + inner.h <= outer.y + outer.h + slack;
/** the keyboard is up (Android's input method window shown) */
const keyboardUp = (d) => /mInputShown=true/.test(d.sh('dumpsys input_method | grep -m1 mInputShown || true'));
/** the screen's height in pixels */
const screenH = (d) => Number(/(\d+)x(\d+)/.exec(d.sh('wm size'))[2]);

export const walkthrough = {
  id: 'walkthrough',
  title: 'The guided walkthrough (T293, T456, T842) replayed from Help: its six steps, the keyboard closed and the ring riding a scroll on "Open its page", the card never over its highlight, back pauses it',
  async run(ctx) {
    const d = ctx.device;
    fs.mkdirSync(OUT, { recursive: true });
    const adbBin = path.join(process.env.ANDROID_HOME ?? `${process.env.HOME}/Library/Android/sdk`, 'platform-tools/adb');
    const chunks = [];
    let rec = null, recAt = 0;
    const record = () => {
      const n = chunks.length + 1, remote = `/sdcard/t842-walk-${n}.mp4`;
      chunks.push(remote);
      rec = spawn(adbBin, ['-s', d.serial(), 'shell', 'screenrecord', '--time-limit', String(CHUNK_S + 5), remote], { stdio: 'ignore' });
      recAt = Date.now();
    };
    const stopRecording = async () => {
      // our own screenrecord only (device-side pid, by its file): it finalises the mp4
      for (const p of d.sh('ps -A -o PID,ARGS').split('\n').filter((l) => /screenrecord .*t842-walk/.test(l))) {
        const pid = p.trim().split(/\s+/)[0];
        if (pid) d.sh(`kill -2 ${pid}`);
      }
      await d.sleep(2500);
      try { rec?.kill(); } catch { /* ended */ }
      rec = null;
    };
    const rotate = async () => { if (rec && Date.now() - recAt > CHUNK_S * 1000) { await stopRecording(); record(); } };
    const at = () => { const c = d.screenText().find((x) => COUNT.test(x)); return c ? Number(COUNT.exec(c)[1]) : null; };
    const shot = (name) => d.screenshot(path.join(OUT, `${name}.png`));

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
    /** the card and the ring now: a line for checks.txt, and whether the card meets the ring */
    const cardAndRing = (label) => {
      const b = boundsOf(d, [/^guide-step-/, 'guide-ring']);
      const card = b && Object.entries(b).find(([k]) => k.startsWith('guide-step-'));
      const ring = b?.['guide-ring'];
      const covers = !!(card && ring && meets(card[1], ring));
      checks.push(`${label}: card ${card ? JSON.stringify(card[1]) : '-'} ring ${ring ? JSON.stringify(ring) : 'none'}${covers ? '  << COVERS' : ''}`);
      return { card: card?.[1] ?? null, ring: ring ?? null, covers };
    };

    await ctx.step('(T842) step 1 by the reader: the field tapped, "ved" typed; step 2 comes up and closes the keyboard', async () => {
      if (at() !== 1) throw new Error(`the walk did not start at step 1 (at ${at()})`);
      shot('step-01');
      cardAndRing('1/6 Search for it');
      ctx.flow('tap-id', { ID: 'search-field' });
      await d.sleep(1200);
      d.sh('input text ved');                                                  // typed with the keyboard up
      const typedUp = keyboardUp(d);
      await d.sleep(2500);                                                     // done at two letters, on after a moment
      if (at() !== 2) throw new Error(`typing "ved" did not move the walk to step 2 (at ${at()})`);
      await d.sleep(1200);                                                     // the row scrolled into view, the ring placed
      shot('step-02');
      const up = keyboardUp(d);
      checks.push(`2/6 keyboard while typing ${typedUp ? 'up' : 'DOWN?'}; on "Open its page" ${up ? 'STILL UP' : 'closed'}`);
      if (up) throw new Error('"Open its page" came up with the keyboard still up (T842: it hid the Vedder River row)');
      const { ring, covers } = cardAndRing('2/6 Open its page');
      if (!ring) throw new Error('no ring round the Vedder River row on "Open its page"');
      if (covers) throw new Error('the card covers the Vedder River row');
      if (ring.y + ring.h > screenH(d)) throw new Error(`the ring runs off the screen (${JSON.stringify(ring)})`);
      return `keyboard closed; ring ${JSON.stringify(ring)}`;
    });

    await ctx.step('(T842) a scroll during "Open its page": frames while it moves; settled, the ring still holds the row', async () => {
      const before = boundsOf(d, ['guide-ring'])?.['guide-ring'];
      if (!before) throw new Error('no ring to scroll with');
      // a slow drag that starts inside the ring (the dim takes drags anywhere else), 240 px up over 1.8 s
      const x = Math.round(before.x + before.w * 0.3), y0 = Math.round(before.y + before.h / 2);
      const drag = spawn(adbBin, ['-s', d.serial(), 'shell', 'input', 'swipe', String(x), String(y0), String(x), String(y0 - 240), '1800'],
                         { stdio: 'ignore' });
      for (let i = 1; i <= 4; i++) { await d.sleep(300); shot(`pick-scroll-${i}`); }   // mid-scroll: the ring on the row in each
      await new Promise((r) => drag.on('exit', r));
      await d.sleep(1200);
      shot('pick-scrolled');
      const nodes = await look(d);
      const row = nodes.find((n) => n.text === 'Vedder River');
      const ring = boundsOf(d, ['guide-ring'])?.['guide-ring'];
      const rowBox = row && { x: row.box.x0, y: row.box.y0, w: row.box.x1 - row.box.x0, h: row.box.y1 - row.box.y0 };
      checks.push(`2/6 after a scroll: ring ${JSON.stringify(ring ?? null)} row ${JSON.stringify(rowBox ?? null)} (before ${JSON.stringify(before)})`);
      if (!ring || !rowBox) throw new Error(`after the scroll: ring ${JSON.stringify(ring)} row ${JSON.stringify(rowBox)}`);
      if (!holds(ring, rowBox)) throw new Error(`the ring left the row it rings after a scroll: ring ${JSON.stringify(ring)} row ${JSON.stringify(rowBox)}`);
      if (cardAndRing('2/6 after the scroll').covers) throw new Error('after the scroll the card covers the row');
      return `ring ${JSON.stringify(ring)} on the row ${JSON.stringify(rowBox)}`;
    });

    await ctx.step('(T842) the keyboard up again on "Open its page": the card above it and off the ring; back closes it', async () => {
      ctx.flow('tap-id', { ID: 'search-field' });
      await d.sleep(2500);
      shot('pick-keyboard');
      const up = keyboardUp(d);
      const { covers } = cardAndRing(`2/6 keyboard ${up ? 'up' : 'not up'}`);
      if (covers) throw new Error('with the keyboard up the card covers the row');
      d.sh('input keyevent 4');                                                // back: the keyboard goes
      await d.sleep(1500);
      if (at() !== 2) throw new Error(`back with the keyboard up left the step (at ${at()})`);
      return up ? 'keyboard up: card off the ring' : 'the keyboard did not come up (nothing to check)';
    });

    let pausedChecked = false;
    await ctx.step('steps 2–6: the primary button (T874: Next / Start exploring), never a card over its ring, until the card closes', async () => {
      const seen = [];
      let last = null, same = 0, total = 0, reached = 0, closed = false;
      for (let i = 0; i < 40; i++) {
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
          await d.sleep(800);                                                  // the card fades in once its ring is placed
          seen.push(`${c} ${title}`);
          if (n0 > 2) shot(`step-${String(n0).padStart(2, '0')}`);
          cardAndRing(`${n0}/${n} ${title}`);
          last = c; same = 0;
        } else if (++same > 4) throw new Error(`stuck at ${c} "${title}" (${seen.length} steps seen: ${seen.slice(-4).join(' / ')})`);

        // (T456 item 4) once, on a Regulations reading step: Android back leaves the page → paused; Resume → same step
        if (!pausedChecked && /Open or closed/i.test(title)) {
          pausedChecked = true;
          d.sh('input keyevent 4'); await d.sleep(3500);
          const p = d.screenText();
          shot(`step-${String(n0).padStart(2, '0')}-paused`);
          if (!p.includes('Resume walkthrough')) throw new Error(`back on "${title}" did not pause the walk (saw ${p.slice(0, 8).join(' | ')})`);
          ctx.flow('tap-id', { ID: 'guide-resume' }); await d.sleep(4000);
          if (at() !== n0) throw new Error(`Resume did not bring back step ${n0} (at ${at()})`);
          checks.push(`${n0}/${n} back → paused → Resume → step ${at()}`);
          continue;
        }
        // the primary button: Next (T874: a DO step's move, then on) / Start exploring (its own view: guide-primary)
        ctx.flow('tap-id', { ID: 'guide-primary' });
        await d.sleep(3500);                                                   // a Next moves the route and the camera
      }
      fs.writeFileSync(path.join(OUT, 'checks.txt'), `${checks.join('\n')}\n`);
      if (!seen.length) throw new Error('no walkthrough card was up');
      if (!closed) throw new Error(`the card never closed (${seen.length} steps: ${seen.slice(-3).join(' / ')})`);
      if (reached < total) throw new Error(`closed at ${reached} of ${total}: ${seen.join(' / ')}`);
      if (!pausedChecked) throw new Error('the back-pauses guardrail step was not reached');
      const covers = checks.filter((l) => l.includes('COVERS'));
      if (covers.length) throw new Error(`the card covers its ring on ${covers.length} step(s): ${covers.join(' / ').slice(0, 600)}`);
      if (total !== STEPS) throw new Error(`the walk has ${total} steps, not the six of T842`);
      return `${reached} of ${total} · ${seen.join(' / ').slice(0, 1400)}`;
    });
    await ctx.step('back on the map; nothing left over (no card, no dim); the recording pulled', async () => {
      await d.sleep(1500);
      shot('after');
      await stopRecording();
      chunks.forEach((remote, i) => d.adb(['pull', remote, path.join(OUT, `walk-${i + 1}.mp4`)]));
      fs.writeFileSync(path.join(OUT, 'checks.txt'), `${checks.join('\n')}\n`);
      await toMap(ctx);
      const w = d.screenText();
      if (w.some((x) => COUNT.test(x))) throw new Error('a walkthrough card is still up');
      if (ctx.hasId('guide-dim')) throw new Error('the walkthrough\'s dim is still up');
      return `${chunks.length} recording chunk(s)`;
    });
  },
};
