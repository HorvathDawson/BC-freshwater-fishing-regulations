// (T884, owner 2026-11-18) A FISH SHEET'S BIG NUMBER ON iOS: Cultus Lake → Black crappie's "20 a day" was cut off at the top
// by its line's height. Looked at in dark, at the default text size and at the accessibility sizes past 200 % (iOS
// "accessibility-large" is RN's 2.14×), with the Keep tab's own big numbers (the tier lines) beside it. Shots and the
// screen's words for each look (`T884-<size>-<what>.png` / `.txt`) to `--out`; the verdict is the pictures.
//
//   node app/tools/testbed/run-ios.mjs t884 --out <dir>
import fs from 'node:fs';
import path from 'node:path';

// the search row by its accessibility text (a data name: kept out of the static check's literal scan, as regs-open's)
const CULTUS = 'Show Cultus Lake,.*Lindell Beach.*';
const FISH = 'Black crappie';
/** iOS text sizes: the default, and two past it (RN's font scale 1.79× and 2.14×) */
const SIZES = ['large', 'accessibility-medium', 'accessibility-large'];

export const t884Ios = {
  id: 't884',
  inAll: false,
  title: 'T884: the fish sheet\'s big number (Cultus Lake, Black crappie) at three text sizes, dark',
  async run(ctx) {
    const { coldStart, toMap } = ctx.ios;
    const d = ctx.device, sleep = d.sleep;
    const contentSize = (s) => d.simctl(['ui', d.udid(), 'content_size', s]);
    const save = (name) => {
      d.screenshot(path.join(ctx.dir, `T884-${name}.png`));
      const w = d.screenText();
      fs.writeFileSync(path.join(ctx.dir, `T884-${name}.txt`), [...w, '--- ids', ...(w.ids ?? [])].join('\n'));
      return w;
    };
    try {
      await ctx.step('dark, default text size, cold start, Cultus Lake\'s page', async () => {
        d.appearance('dark');
        contentSize('large');
        await coldStart(ctx, { ms: 600_000 });
        await sleep(2000);
        await toMap(ctx);
        ctx.flow('search', { Q: 'Cultus' });
        await sleep(2500);
        ctx.flow('open-result', { LINE: CULTUS });
        await ctx.until(() => d.hasId('water-back'), { ms: 30_000, what: 'Cultus Lake\'s page (water-back)' });
        await sleep(3000);
        const w = save('large-1-keep');
        return `words: ${w.slice(0, 16).join(' | ').slice(0, 300)}`;
      });
      await ctx.step(`the ${FISH} sheet`, async () => {
        ctx.flow('scroll-to', { TEXT: `${FISH}.*` });
        await sleep(1000);
        save('large-2-row');
        ctx.flow('tap-text', { TEXT: `${FISH}.*` });
        await sleep(3000);
        const w = save('large-3-sheet');
        return `sheet words: ${w.slice(0, 12).join(' | ').slice(0, 300)}`;
      });
      // the bigger sizes on the open sheet: the search's rows at 2.14x are taller than the screen, so the page is reached at
      // the default size and the text size changed under it (the app follows it live)
      for (const size of SIZES.slice(1)) {
        await ctx.step(`${size}: the same sheet`, async () => {
          contentSize(size);
          await sleep(3500);
          const w = save(`${size}-3-sheet`);
          return `sheet words: ${w.slice(0, 12).join(' | ').slice(0, 300)}`;
        });
      }
    } finally {
      contentSize('large');
    }
  },
};
