// (T293) THE GUIDED WALKTHROUGH, END TO END ON A PHONE: replayed from More → Help, then every step's primary button
// (Show me on a DO step, Next on a SHOW step, Start exploring at the end) until the card is gone. Each step's count and
// title are recorded with a screenshot; the walk passes when it reached "n of n" and closed. The Lower Mainland map the
// walk frames on the Vedder is also the T299 shot: stream names drawn at about z11.
//
//   node app/tools/testbed/run.mjs walkthrough
import path from 'node:path';
import { coldStart, toMap } from './common.mjs';

/** the card's count line: "Find a water · 3 of 25" (upper-cased on screen) */
const COUNT = /(\d+) of (\d+)$/i;

export const walkthrough = {
  id: 'walkthrough',
  title: 'The guided walkthrough (T293) replayed from Help and walked to its end with Show me / Next',
  async run(ctx) {
    await ctx.step('More → Help → Replay the walkthrough', async () => {
      await ctx.device.net('wifi'); ctx.device.link('full', 'none');
      await coldStart(ctx, { ms: 90_000 });
      await toMap(ctx);
      ctx.flow('tab', { TAB: 'More' });
      await ctx.device.sleep(1500);
      ctx.flow('tap-text', { TEXT: 'Help' });
      await ctx.device.sleep(1500);
      ctx.flow('scroll-to', { TEXT: 'Replay the walkthrough' });
      ctx.flow('tap-text', { TEXT: 'Replay the walkthrough' });
      await ctx.device.sleep(3000);
      const w = ctx.device.screenText();
      const c = w.find((x) => COUNT.test(x));
      if (!c) throw new Error(`no walkthrough card (saw: ${w.slice(0, 10).join(' | ').slice(0, 300)})`);
      return c;
    });
    await ctx.step('every step: its primary button, until the card closes', async () => {
      const seen = [];
      let last = null, same = 0, total = 0, reached = 0, closed = false;
      for (let i = 0; i < 70; i++) {
        const w = ctx.device.screenText();
        const c = w.find((x) => COUNT.test(x));
        if (!c) { closed = true; break; }
        const [, at, n] = COUNT.exec(c).map(Number);
        total = n; reached = Math.max(reached, at);
        const title = w.slice(w.indexOf(c) + 1).find((x) => !/^Skip/i.test(x)) ?? '';
        if (c !== last) {
          seen.push(`${c} ${title}`);
          ctx.device.screenshot(path.join(ctx.dir, `T293-step-${String(at).padStart(2, '0')}.png`));
          last = c; same = 0;
        } else if (++same > 4) throw new Error(`stuck at ${c} "${title}" (${seen.length} steps seen: ${seen.slice(-4).join(' / ')})`);
        // the primary button: Show me / Next / Done / Start exploring (its own view: guide-primary)
        ctx.flow('tap-id', { ID: 'guide-primary' });
        await ctx.device.sleep(at === 1 ? 2500 : 3500);                     // a Show me moves the route and the camera
      }
      if (!closed) throw new Error(`the card never closed (${seen.length} steps: ${seen.slice(-3).join(' / ')})`);
      if (reached < total) throw new Error(`closed at ${reached} of ${total}: ${seen.join(' / ')}`);
      return `${reached} of ${total} · ${seen.join(' / ').slice(0, 900)}`;
    });
    await ctx.step('back on the map; nothing left over (no card, no sheet)', async () => {
      await ctx.device.sleep(1500);
      ctx.device.screenshot(path.join(ctx.dir, 'T293-after.png'));
      await toMap(ctx);
      const w = ctx.device.screenText();
      if (w.some((x) => COUNT.test(x))) throw new Error('a walkthrough card is still up');
    });
  },
};
