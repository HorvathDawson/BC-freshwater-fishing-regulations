// (T723 F7) DOES A LEFT-EDGE SWIPE GO BACK OVER EACH iOS MODAL? Menu pages, the PDF viewer, a sheet and the photo
// viewer are React Native Modals (their own windows). Each step opens one and swipes in from the left edge: first a
// quick drag (start under 3% of the width, mid height, 300 ms), then, when the modal is still up, a slow one (1200 ms).
// The verdict is the modal's own marker (its testID or words) gone. One modal per step: a swipe must close only it.
//
//   node app/tools/testbed/run-ios.mjs modal-back
import path from 'node:path';
import { TABS, home, menuPage } from './keyboard.mjs';

export const modalBackIos = {
  id: 'modal-back',
  inAll: false,
  title: 'T723 F7: left-edge swipe back over each Modal (iOS)',
  async run(ctx) {
    const d = ctx.device;
    /** the screen in iOS points (the tree's widest and tallest node) */
    // (a read of the tree can come back empty under load: looked at again, and the iPhone 17e's 390 × 844 when it stays empty)
    // the app's own window (the first node at 0,0 with a size), not the widest node: a page's content can run past the
    // right edge (Help: its swatch rows), and 2% of that width started the drag outside the edge strip
    const screen = () => {
      const win = d.nodes().find((n) => n.box.x0 === 0 && n.box.y0 === 0 && n.box.x1 > 0 && n.box.y1 > 0);
      return win ? { W: win.box.x1, H: win.box.y1 } : { W: 390, H: 844 };
    };
    // the modal's marker read off a tree that is not empty (an empty read is neither "up" nor "gone": read again)
    const look = async (up0) => {
      for (let i = 0; i < 3; i++) { if (d.screenText().length) return up0(); await d.sleep(1500); }
      return null;
    };
    const swipe = async (name, up0) => {
      if ((await look(up0)) !== true) throw new Error(`${name}: the modal is not up before the swipe`);
      const { W, H } = screen();
      d.screenshot(path.join(ctx.dir, `T723-mb-${name}-before.png`));
      const tried = [];
      for (const [label, ms] of [['quick 300 ms', 300], ['slow 1200 ms', 1200]]) {
        ctx.swipe([Math.round(W * 0.02), Math.round(H * 0.5)], [Math.round(W * 0.75), Math.round(H * 0.5)], ms);
        await d.sleep(1500);
        const seen = await look(up0);
        const gone = seen === false;
        tried.push(`${label}: ${gone ? 'CLOSED' : seen === null ? 'unread (empty tree)' : 'stayed'}`);
        d.screenshot(path.join(ctx.dir, `T723-mb-${name}-after-${ms}.png`));
        if (gone) break;
      }
      const note = `${name}: ${tried.join(' · ')}`;
      if (!tried.some((t) => t.includes('CLOSED'))) ctx.finding(`F7 ${note}`);
      return note;
    };
    const words = () => d.screenText();
    // a staging release published meanwhile is downloaded whole at the next start (~7 min): waited out, never restarted
    await ctx.step('the app up (a release download waited out)', async () => {
      await ctx.ios.appUp(ctx, { launch: !d.pidOf(), ms: 1_200_000 });
    });
    const menu = (key, title) => ctx.step(`menu page: ${title}`, async () => {
      await home(ctx);
      await menuPage(ctx, key);
      return swipe(key, () => { const w = words(); return w.includes(title) && w.includes('Back'); });
    });
    await menu('about', 'About');
    await menu('help', 'Help');
    await ctx.step('sheet: Layers', async () => {
      await home(ctx);
      ctx.flow('tap-text', { TEXT: 'Choose layers.*' });
      await d.sleep(1200);
      return swipe('layers-sheet', () => { const w = words(); return w.includes('Done') && w.includes('Layers'); });
    });
    await ctx.step('PDF viewer: a Rule book link on Cultus Lake', async () => {
      await home(ctx);
      ctx.flow('search', { Q: 'Cultus' });
      ctx.flow('open-result', { LINE: '.*Lindell Beach.*' });
      await ctx.waitText(/Regulations/, 30_000);
      ctx.flow('scroll-to', { TEXT: 'Rule book p\\.[0-9]+ ↗.*' });
      ctx.flow('tap-text', { TEXT: 'Rule book p\\.[0-9]+ ↗.*' });
      for (let i = 0; i < 20 && !words().some((x) => /^Page \d+ of \d+$/.test(x)); i++) await d.sleep(3000);
      return swipe('pdf', () => d.hasId('pdf-viewer') || words().some((x) => /^Page \d+ of \d+$/.test(x)));
    });
    await ctx.step('photo viewer: a photo added to the saved probe spot, enlarged', async () => {
      await home(ctx);
      d.addMedia(path.join(path.dirname(new URL(import.meta.url).pathname), '..', 'media', 'testbed-photo.jpg'));
      ctx.flow('tap-point', { POINT: TABS.spots });
      await d.sleep(1500);
      try { ctx.flow('tap-text', { TEXT: 'kb probe.*' }); } catch { ctx.skip('no saved "kb probe" spot (the keyboard scenario saves one)'); }
      await d.sleep(2000);
      if (!d.screenText().some((x) => /^Photo \d+ of \d+/.test(x))) {
        // the iOS photo picker: its tiles are "Photo, <date>" in the tree; one pick returns at once
        ctx.flow('tap-text', { TEXT: 'Edit( .*)?' });   // its label is "Edit spot" or the like, its words "Edit"
        await d.sleep(1500);
        ctx.flow('scroll-to', { TEXT: 'Add from photos' });
        ctx.flow('tap-text', { TEXT: 'Add from photos' });
        await d.sleep(4000);
        d.screenshot(path.join(ctx.dir, 'T723-mb-picker.png'));
        // the newest photo is the first tile (its words are not in the tree, and "Photos" is the picker's own tab): by place,
        // under the picker's "Private Access" card (iPhone 17e, T723-mb-picker.png)
        try { ctx.flow('tap-point', { POINT: '16%,43%' }); } catch (e) { ctx.skip(`the iOS photo picker could not be driven (${e.message.slice(0, 120)}): photo viewer by reading only`); }
        await d.sleep(2000);
        try { ctx.flow('tap-text', { TEXT: '(Add|Done)' }); } catch { /* a single pick returns by itself */ }
        await d.sleep(3000);
        ctx.flow('scroll-to', { TEXT: 'Save changes' });
        ctx.flow('tap-text', { TEXT: 'Save changes' });
        await d.sleep(3000);
      }
      ctx.flow('scroll-to', { TEXT: 'Photo 1 of .*' });
      ctx.flow('tap-text', { TEXT: 'Photo 1 of .*' });
      await d.sleep(1500);
      return swipe('photo-viewer', () => d.hasId('photo-viewer') || d.hasId('photo-image'));
    });
  },
};
