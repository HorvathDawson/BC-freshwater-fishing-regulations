// (T984, owner 2026-11-21: "the tutorial should be only a walk through not modify the state") THE GUIDED WALKTHROUGH ON A
// PHONE: replayed from More → Help, it is FIVE look-only cards on the Map tab (search, the map, the day, Trout / Salmon,
// all set). Nothing it shows is done for the reader and nothing the reader does reaches the app:
//   - each step: its count "n of 5", and the card's box must never meet the ring's (uiautomator bounds of `guide-step-*`
//     and `guide-ring`); no water page, search or keyboard may come up as it goes (the app stays on the Map tab);
//   - the dim takes every touch: on step 1 a tap on the search field (inside the ring) must not raise the keyboard, and on
//     step 2 a swipe across the map must change nothing;
//   - Next walks it to the end ("Start exploring"); then the app is exactly as it was before the tour: the screen's words
//     equal those read on the Map tab before replaying (no search open, no day picked, no mode switched, no tab changed);
//   - a second replay is skipped half way: the same equality.
// Shots and checks: out/t984/ (step-NN.png, checks.txt).
//
//   node app/tools/testbed/run.mjs walkthrough
import fs from 'node:fs';
import path from 'node:path';
import { coldStart, toMap } from './common.mjs';

/** the card's count line: "Find a water · 2 of 5" (upper-cased on screen) */
const COUNT = /(\d+) of (\d+)$/i;
const HERE = path.dirname(new URL(import.meta.url).pathname);
const OUT = path.join(HERE, '..', 'out', 't984');
const STEPS = 5;

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
/** the keyboard is up (Android's input method window shown) */
const keyboardUp = (d) => /mInputShown=true/.test(d.sh('dumpsys input_method | grep -m1 mInputShown || true'));
export const walkthrough = {
  id: 'walkthrough',
  title: 'The guided walkthrough (T984) replayed from Help: five look-only cards, the dim takes every touch, the app exactly as it was after Next and after Skip',
  async run(ctx) {
    const d = ctx.device;
    fs.mkdirSync(OUT, { recursive: true });
    const shot = (name) => d.screenshot(path.join(OUT, `${name}.png`));
    const at = () => { const c = d.screenText().find((x) => COUNT.test(x)); return c ? Number(COUNT.exec(c)[1]) : null; };
    const checks = [];
    let baseline = '';

    /** More → Help → Replay: the tour starts on the Map tab at step 1 */
    const replay = async () => {
      ctx.flow('tab', { TAB: 'More' });
      await d.sleep(1500);
      ctx.flow('scroll-to', { TEXT: 'Help' });                                 // below the Data list on the More tab
      ctx.flow('tap-text', { TEXT: 'Help' });
      await d.sleep(1500);
      ctx.flow('scroll-to', { TEXT: 'Replay the walkthrough' });
      ctx.flow('tap-text', { TEXT: 'Replay the walkthrough' });
      await d.sleep(3000);
      if (at() !== 1) throw new Error(`the replay did not start at step 1 (at ${at()}; saw ${d.screenText().slice(0, 8).join(' | ').slice(0, 300)})`);
    };
    /** the app now, as the reader would find it with no tour: the Map tab, its words the baseline's */
    const asBefore = async (label) => {
      await d.sleep(1500);
      if (d.screenText().some((x) => COUNT.test(x))) throw new Error(`${label}: a walkthrough card is still up`);
      if (ctx.hasId('guide-dim')) throw new Error(`${label}: the walkthrough's dim is still up`);
      const now = d.screenText().slice().sort().join('\n');
      checks.push(`${label}: ${now === baseline ? 'same words as before the tour' : 'DIFFERENT'}`);
      if (now !== baseline) throw new Error(`${label}: the app is not as it was before the tour.\nbefore: ${baseline.replace(/\n/g, ' | ').slice(0, 500)}\nafter:  ${now.replace(/\n/g, ' | ').slice(0, 500)}`);
    };

    await ctx.step('the Map tab with no tour: its words are the baseline', async () => {
      await d.net('wifi'); d.link('full', 'none');
      await coldStart(ctx, { ms: 90_000 });                                    // closes the notice and skips the tour
      await toMap(ctx);
      await d.sleep(2000);
      baseline = d.screenText().slice().sort().join('\n');
      shot('before');
      return `${baseline.split('\n').length} words`;
    });

    await ctx.step('replay from Help: five steps, Next only; touches under the dim reach nothing; the card never over its ring', async () => {
      await replay();
      const seen = [];
      for (let n = 1; n <= STEPS; n++) {
        const c = d.screenText().find((x) => COUNT.test(x));
        if (!c) throw new Error(`the card closed before step ${n} (${seen.join(' / ')})`);
        const [, at0, total] = COUNT.exec(c).map(Number);
        if (at0 !== n || total !== STEPS) throw new Error(`expected ${n} of ${STEPS}, saw ${c}`);
        await d.sleep(800);                                                    // the card shows once its ring is placed
        shot(`step-${String(n).padStart(2, '0')}`);
        const b = boundsOf(d, [/^guide-step-/, 'guide-ring']);
        const card = b && Object.entries(b).find(([k]) => k.startsWith('guide-step-'));
        const ring = b?.['guide-ring'];
        const covers = !!(card && ring && meets(card[1], ring));
        checks.push(`${n}/${STEPS}: card ${card ? JSON.stringify(card[1]) : '-'} ring ${ring ? JSON.stringify(ring) : 'none'}${covers ? '  << COVERS' : ''}`);
        if (covers) throw new Error(`step ${n}: the card covers its ring`);
        seen.push(c);
        // the app stays on the Map tab: no water page, no search sheet
        if (ctx.hasId('water-back')) throw new Error(`step ${n}: a water's page is open (the tour must not open one)`);
        if (n === 1) {
          // the reader taps the search field inside the ring: the dim takes it, the keyboard does not come up
          ctx.flow('tap-id', { ID: 'search-field' });
          await d.sleep(1500);
          if (keyboardUp(d)) throw new Error('a tap on the highlighted search field raised the keyboard (the dim must take it)');
          if (at() !== 1) throw new Error(`a tap under the dim moved the tour (at ${at()})`);
        }
        if (n === 2) {
          // a pan across the map: nothing underneath moves (the words, the step) and the tour is still here
          d.sh('input swipe 200 900 600 500 400');
          await d.sleep(1500);
          if (at() !== 2) throw new Error(`a swipe under the dim moved or broke the tour (at ${at()})`);
        }
        ctx.flow('tap-id', { ID: 'guide-primary' });                           // Next, or Start exploring on the last card
        await d.sleep(1200);
      }
      if (d.screenText().some((x) => COUNT.test(x))) throw new Error('the card did not close on the last card');
      fs.writeFileSync(path.join(OUT, 'checks.txt'), `${checks.join('\n')}\n`);
      return `${seen.length} steps: ${seen.join(' / ')}`;
    });

    await ctx.step('after the tour finished: the app exactly as before it', async () => {
      shot('after-finish');
      await asBefore('after Next ×5');
      return 'same as the baseline';
    });

    await ctx.step('replay again, skip at step 3: the app exactly as before it', async () => {
      await replay();
      ctx.flow('tap-id', { ID: 'guide-primary' }); await d.sleep(1200);
      ctx.flow('tap-id', { ID: 'guide-primary' }); await d.sleep(1200);
      if (at() !== 3) throw new Error(`expected step 3, at ${at()}`);
      ctx.flow('tap-id', { ID: 'guide-skip' });
      shot('after-skip');
      await asBefore('after Skip at 3');
      fs.writeFileSync(path.join(OUT, 'checks.txt'), `${checks.join('\n')}\n`);
      return 'same as the baseline';
    });
  },
};
