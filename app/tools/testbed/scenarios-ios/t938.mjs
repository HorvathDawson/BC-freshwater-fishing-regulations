// (T938) iPHONE: AFTER SEARCH → WATER → BACK, THE BROWSE MAP DRAWS ONLY ITS TOP PART. Found by the T928/T929 lane on the
// iPhone 17e simulator: below ~60 % of the map flat grey, through pans and a tab change. Here, light and dark: the map
// before (its lower band's commonest-colour share, `lib/screen.mjs blankShare`), then Cultus Lake from search, its page,
// Back, and the same band again, at once and after a pan (`T938-ios-<look>-<via>-<step>.png`). Back two ways: the page's ‹,
// and the left-edge swipe (EdgeBack: T928's suspect, the lane that found T938 may have gone back so; not reproduced by
// the ‹ on 418f6f3a1). FAILS when the lower band is one flat colour after Back while it was not before, or when the
// left strip is (the layer left standing off to the right: 6fdb3d8db, light and dark, after the edge swipe: the map
// and the day bar ~75 % to the right, white / black on the left, through a pan: the tester's T928 picture).
// T938_LOOKS=light,dark and T938_VIAS=chevron,edge,edge-low narrow a run.
//   node app/tools/testbed/run-ios.mjs t938-ios
import path from 'node:path';
import { BLANK_AT, blankShareOf } from '../lib/screen.mjs';
import { readPng } from '../lib/png.mjs';

const LOOKS = (process.env.T938_LOOKS ?? 'light,dark').split(',');
const VIAS = (process.env.T938_VIAS ?? 'chevron,edge,edge-low').split(',');

export const t938Ios = {
  id: 't938-ios',
  inAll: false,
  title: 'T938: the browse map is drawn whole after search → water → Back (light, dark)',
  async run(ctx) {
    const d = ctx.device;
    /** the map's lower band (60-82 % down: above the legend and the tab bar, below the half that stayed drawn) */
    const band = (file) => {
      const img = readPng(file);
      const b = { x0: 0, x1: img.w, y0: Math.round(img.h * 0.6), y1: Math.round(img.h * 0.82) };
      return blankShareOf(file, b);
    };
    const snap = (name) => { const f = path.join(ctx.dir, `T938-ios-${name}.png`); d.screenshot(f); return f; };
    /** the map's left strip (0-25 % across, 30-80 % down): one flat colour there is the layer standing off to the right */
    const left = (file) => {
      const img = readPng(file);
      return blankShareOf(file, { x0: 0, x1: Math.round(img.w * 0.25), y0: Math.round(img.h * 0.3), y1: Math.round(img.h * 0.8) });
    };
    /** the left-edge swipe back (as run-ios.mjs's EdgeBack step: a few heights and speeds until the page is gone); a shot
     *  after each drag, and the page read from the screen's words (`onPage`), not an id a kept layer may still carry */
    const edgeBack = async (look, flows) => {
      const tried = [];
      for (const f of flows) {
        ctx.flow(f);
        await d.sleep(1500);
        const w = d.screenText();
        const page = w.length === 0 || ctx.ios.onPage(w);
        const m = left(snap(`${look}-edge-drag-${f}`));
        tried.push(`${f}: ${page ? 'still on the page' : 'back'}, left strip ${(m.share * 100).toFixed(0)} % ${m.colour}`);
        if (!page) return tried.join(' · ');
      }
      throw new Error(`the edge swipe did not leave the water page (${tried.join(' · ')})`);
    };
    for (const look of LOOKS) for (const via of VIAS) {
      await ctx.step(`${look}: Cultus Lake from search, its page, Back (${via === 'chevron' ? 'the page\'s ‹' : via === 'edge' ? 'the left-edge swipe, after two that pan the page map' : 'one low left-edge swipe'}): the whole map drawn`, async () => {
        d.appearance(look);
        await ctx.ios.coldStart(ctx, { ms: 600_000 });
        await d.sleep(4000);
        const before = band(snap(`${look}-${via}-1-before`));
        ctx.flow('search', { Q: 'Cultus' });
        await d.sleep(2000);
        ctx.flow('open-result', { LINE: '.*Lindell Beach.*' });
        await ctx.until(() => d.hasId('water-back'), { ms: 30_000, what: 'the water page (water-back)' });
        await d.sleep(3000);
        snap(`${look}-${via}-2-page`);
        // edge: mid, high, then low (on the page's Map view the first two land on its map and are cancelled: EdgeBack's
        // spring home runs first, so its offset is native-driven when the low one goes back); edge-low: the low drag alone
        const how = via === 'edge' ? await edgeBack(look, ['edge-back', 'edge-back-high', 'edge-back-low'])
          : via === 'edge-low' ? await edgeBack(look, ['edge-back-low'])
          : (await ctx.ios.pageBack(ctx), 'the ‹');
        await d.sleep(3000);
        const afterFile = snap(`${look}-${via}-3-after-back`);
        const after = band(afterFile), afterLeft = left(afterFile);
        ctx.flow('pan-up');
        await d.sleep(2500);
        const pannedFile = snap(`${look}-${via}-4-after-pan`);
        const panned = band(pannedFile), pannedLeft = left(pannedFile);
        const pct = (m) => `${(m.share * 100).toFixed(1)} % ${m.colour}`;
        const said = `back by ${how}; lower band one colour: before ${pct(before)}, after Back ${pct(after)}, after a pan ${pct(panned)}`;
        if (before.share < BLANK_AT && (after.share >= BLANK_AT || panned.share >= BLANK_AT)) throw new Error(`the map's lower part is flat after Back: ${said}`);
        const sideways = `left strip one colour after Back ${pct(afterLeft)}, after a pan ${pct(pannedLeft)}`;
        if (afterLeft.share >= BLANK_AT || pannedLeft.share >= BLANK_AT) throw new Error(`the map stands off to the right after Back (${sideways}); ${said}`);
        return `${said}; ${sideways}`;
      });
    }
    d.appearance('light');
  },
};
