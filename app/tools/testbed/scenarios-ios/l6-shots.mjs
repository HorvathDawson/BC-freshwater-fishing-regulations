// (T831, T836) THE iPHONE SHOTS OWED BY TWO MERGED FIXES, light and dark, by name only (`inAll: false`):
//   t831-ios  a gauged water's Conditions (Coquitlam River, Region 2): the route map draws its lines, the gauge dots and
//             the accent-violet "you are here" pin (`T831-ios-route-<look>.png`; accent pixels counted on the screen
//             below the page's head, said, the shot is the proof to set beside the web's);
//   t836-ios  the spot photo viewer: a seeded spot with three photos, opened from its thumbnail, the counter, a tap that
//             hides the buttons and one that brings them back, a swipe to the next photo, a drag down that closes it
//             (`T836-ios-<step>-<look>.png`).
//   node app/tools/testbed/run-ios.mjs t831-ios t836-ios
import fs from 'node:fs';
import path from 'node:path';
import { count, readPng } from '../lib/png.mjs';
import { runYaml } from '../lib/ios-flow.mjs';

const HERE = path.dirname(new URL(import.meta.url).pathname);
const COQUITLAM = '.* · Region 2 · MU 2-8.*';
const LOOKS = ['light', 'dark'];
/** a swipe by screen percentages (Maestro's own), throwing with Maestro's words */
function swipe(ctx, name, from, to, ms) {
  const r = runYaml(ctx.dir, name, `appId: ca.canifishthis.app\n---\n- swipe: { start: "${from}", end: "${to}", duration: ${ms} }\n`);
  if (!r.ok) throw new Error(`${name}: ${r.said || 'see flows-ios/'}`);
}

/** the accent violet (color.ui.accent: light #5F26E0, dark #A97CFF), within 60 summed */
const near = (r, g, b, [R, G, B]) => Math.abs(r - R) + Math.abs(g - G) + Math.abs(b - B) < 60;
const accent = (r, g, b) => near(r, g, b, [0x5f, 0x26, 0xe0]) || near(r, g, b, [0xa9, 0x7c, 0xff]);

export const t831Ios = {
  id: 't831-ios',
  inAll: false,
  title: 'T831: a gauged water\'s route map on iOS draws its lines, gauge dots and the "you are here" pin (light, dark)',
  async run(ctx) {
    const d = ctx.device;
    for (const look of LOOKS) {
      await ctx.step(`${look}: Coquitlam River → Conditions, the route map`, async () => {
        d.appearance(look);
        await ctx.ios.coldStart(ctx, { ms: 600_000 });
        ctx.flow('search', { Q: 'Coquitlam River' });
        await d.sleep(2500);
        ctx.flow('open-result', { LINE: COQUITLAM });
        await ctx.until(() => d.hasId('water-back'), { ms: 30_000, what: 'the water page (water-back)' });
        await d.sleep(2500);
        ctx.flow('tap-id', { ID: 'water-view-conditions' });
        await ctx.until(() => d.hasId('gauge-card'), { ms: 30_000, what: 'the Conditions view (gauge-card)' });
        await d.sleep(9000);                              // the route map's lines are read out of the atlas, then drawn
        const file = path.join(ctx.dir, `T831-ios-route-${look}.png`);
        d.screenshot(file);
        const img = readPng(file);
        // below the page's head and above the tab bar: the route map is the first map on the Conditions view
        const n = count(img, { x: 0, y: Math.round(img.h * 0.18), w: img.w, h: Math.round(img.h * 0.62) }, accent);
        const w = d.screenText();
        await ctx.ios.pageBack(ctx);
        if (n < 40) throw new Error(`no accent pixels under the page head (${n}): no "you are here" pin (${path.basename(file)})`);
        return `${n} accent pixels (the pin); gauge named: ${w.find((x) => /^Coquitlam River (above|at|near) /i.test(x)) ?? 'see the shot'} · ${path.basename(file)}`;
      });
    }
    d.appearance('light');
  },
};

export const t836Ios = {
  id: 't836-ios',
  inAll: false,
  title: 'T836: the spot photo viewer on iOS (from its thumbnail, counter, tap to hide, strip, drag down) (light, dark)',
  async run(ctx) {
    const d = ctx.device;
    const shot = (name) => d.screenshot(path.join(ctx.dir, `T836-ios-${name}.png`));
    const words = () => d.screenText();
    const counter = () => words().find((x) => /^\d+ of \d+$/.test(x)) ?? null;
    await ctx.step('a spot with three photos seeded into the app\'s files', async () => {
      d.stop(); await d.sleep(1000);
      const root = path.join(d.dataDir(), 'Documents', 'spots');
      fs.mkdirSync(path.join(root, 'photos'), { recursive: true });
      const refs = ['a', 'b', 'c'].map((x) => `photos/l6-${x}.jpg`);
      for (const r of refs) fs.copyFileSync(path.join(HERE, '..', 'media', 'testbed-photo.jpg'), path.join(root, r));
      const now = Date.now();
      const spot = { id: 'l6-photos', createdAt: now, updatedAt: now, visitedAt: now, lat: 49.1, lon: -121.95, item: null,
        waterName: null, title: 'Photo spot', notes: '', photos: refs, reading: null, weather: null, trace: null, panel: null };
      fs.writeFileSync(path.join(root, 'spots.spots'), JSON.stringify([spot]));
      return `${refs.length} photos`;
    });
    for (const look of LOOKS) {
      await ctx.step(`${look}: opened from its thumbnail; the counter`, async () => {
        d.appearance(look);
        await ctx.ios.coldStart(ctx);
        ctx.flow('tab', { TAB: 'Spots' }); await d.sleep(2500);
        ctx.flow('tap-text', { TEXT: 'Photo spot.*' }); await d.sleep(2500);
        ctx.flow('scroll-to', { TEXT: 'Photo 1 of .*' });
        shot(`1-thumbs-${look}`);
        ctx.flow('tap-text', { TEXT: 'Photo 1 of .*' });
        await d.sleep(250);
        shot(`2-opening-${look}`);                         // mid-way: growing out of the thumbnail
        await d.sleep(1500);
        shot(`3-open-${look}`);
        if (!d.hasId('photo-viewer') && !d.hasId('photo-image')) throw new Error('the viewer did not open');
        return `counter: ${counter() ?? 'none in the tree'}`;
      });
      await ctx.step(`${look}: a tap hides the buttons, a second brings them back`, async () => {
        ctx.flow('tap-point', { POINT: '50%,50%' });
        await d.sleep(1200);
        const hid = counter();
        shot(`4-hidden-${look}`);
        ctx.flow('tap-point', { POINT: '50%,50%' });
        await d.sleep(1200);
        const back = counter();
        shot(`5-shown-${look}`);
        if (hid || !back) throw new Error(`after one tap the counter was ${hid ?? 'gone'}; after two ${back ?? 'gone'}`);
        return `hidden, then back ("${back}")`;
      });
      await ctx.step(`${look}: a swipe to the next photo`, async () => {
        swipe(ctx, `t836-next-${look}`, '85%, 50%', '15%, 50%', 300);
        await d.sleep(1500);
        const c = counter();
        shot(`6-next-${look}`);
        if (c !== '2 of 3') throw new Error(`the counter says ${c ?? 'nothing'} after a swipe left (wanted 2 of 3)`);
        return c;
      });
      await ctx.step(`${look}: a drag down closes it, back on the spot`, async () => {
        swipe(ctx, `t836-down-${look}`, '50%, 45%', '50%, 85%', 400);
        await d.sleep(300);
        shot(`7-closing-${look}`);
        await d.sleep(1500);
        shot(`8-closed-${look}`);
        if (d.hasId('photo-viewer') || d.hasId('photo-image')) throw new Error('the viewer is still open after a drag down');
        return 'closed';
      });
    }
    d.appearance('light');
  },
};
