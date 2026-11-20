// (T938) iPHONE: AFTER SEARCH → WATER → BACK, THE BROWSE MAP DRAWS ONLY ITS TOP PART. Found by the T928/T929 lane on the
// iPhone 17e simulator: below ~60 % of the map flat grey, through pans and a tab change. Here, light and dark: the map
// before (its lower band's commonest-colour share, `lib/screen.mjs blankShare`), then Cultus Lake from search, its page,
// Back, and the same band again, at once and after a pan (`T938-ios-<look>-<via>-<step>.png`). Back two ways: the page's ‹,
// and the left-edge swipe (EdgeBack: T928's suspect, the lane that found T938 may have gone back so; not reproduced by
// the ‹ on 418f6f3a1). FAILS when the lower band is one flat colour after Back while it was not before.
//   node app/tools/testbed/run-ios.mjs t938-ios
import path from 'node:path';
import { BLANK_AT, blankShareOf } from '../lib/screen.mjs';
import { readPng } from '../lib/png.mjs';

const LOOKS = ['light', 'dark'];
const VIAS = ['chevron', 'edge'];

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
    /** the left-edge swipe back (as run-ios.mjs's EdgeBack step: a few heights and speeds until the page is gone) */
    const edgeBack = async () => {
      for (const f of ['edge-back', 'edge-back-high', 'edge-back-low']) {
        ctx.flow(f);
        await d.sleep(1500);
        if (!d.hasId('water-back')) return f;
      }
      throw new Error('the edge swipe did not leave the water page');
    };
    for (const look of LOOKS) for (const via of VIAS) {
      await ctx.step(`${look}: Cultus Lake from search, its page, Back (${via === 'edge' ? 'the left-edge swipe' : 'the page\'s ‹'}): the whole map drawn`, async () => {
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
        const how = via === 'edge' ? await edgeBack() : (await ctx.ios.pageBack(ctx), 'the ‹');
        await d.sleep(3000);
        const after = band(snap(`${look}-${via}-3-after-back`));
        ctx.flow('pan-up');
        await d.sleep(2500);
        const panned = band(snap(`${look}-${via}-4-after-pan`));
        const pct = (m) => `${(m.share * 100).toFixed(1)} % ${m.colour}`;
        const said = `back by ${how}; lower band one colour: before ${pct(before)}, after Back ${pct(after)}, after a pan ${pct(panned)}`;
        if (before.share < BLANK_AT && (after.share >= BLANK_AT || panned.share >= BLANK_AT)) throw new Error(`the map's lower part is flat after Back: ${said}`);
        return said;
      });
    }
    d.appearance('light');
  },
};
