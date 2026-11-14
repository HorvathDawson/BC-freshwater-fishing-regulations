// (T723 F7) DOES A LEFT-EDGE SWIPE GO BACK OVER EACH iOS MODAL? Menu pages, the PDF viewer, a sheet and the photo
// viewer are React Native Modals (their own windows). Each step opens one, swipes in from the left edge (the same
// swipe `flows/edge-back.yaml` does on a page), and says whether the modal is still up. One step per swipe: the swipe
// must close only the modal, never a page under it.
//
//   node app/tools/testbed/run-ios.mjs modal-back
import path from 'node:path';

export const modalBackIos = {
  id: 'modal-back',
  inAll: false,
  title: 'T723 F7: left-edge swipe back over each Modal (iOS)',
  async run(ctx) {
    const { toMap } = ctx.ios;
    const swipe = async (name, still) => {
      ctx.device.screenshot(path.join(ctx.dir, `T723-mb-${name}-before.png`));
      ctx.flow('edge-back');
      await ctx.device.sleep(1200);
      ctx.device.screenshot(path.join(ctx.dir, `T723-mb-${name}-after.png`));
      const w = ctx.device.screenText();
      if (still(w)) throw new Error(`${name}: the edge swipe left the modal up (saw: ${w.slice(0, 8).join(' | ').slice(0, 200)})`);
      return `${name}: the edge swipe closed it`;
    };
    const menu = (label, title) => ctx.step(`menu page: ${label}`, async () => {
      await toMap(ctx);
      ctx.flow('tab', { TAB: 'More' });
      ctx.flow('scroll-to', { TEXT: label });
      ctx.flow('tap-text', { TEXT: label });
      await ctx.device.sleep(1500);
      return swipe(label.toLowerCase().replace(/\W+/g, '-'), (w) => w.some((x) => title.test(x)) && w.some((x) => /^(Back|Glossary)$/.test(x)));
    });
    await menu('About', /^About$/);
    await menu('Help', /^Help$/);
    await ctx.step('sheet: Layers', async () => {
      await toMap(ctx);
      ctx.flow('tap-text', { TEXT: 'Choose layers.*' });
      await ctx.device.sleep(1200);
      return swipe('layers-sheet', (w) => w.includes('Done'));
    });
    await ctx.step('PDF viewer: More → the synopsis', async () => {
      await toMap(ctx);
      ctx.flow('tab', { TAB: 'More' });
      ctx.flow('scroll-to', { TEXT: 'Fishing regulations.*' });
      ctx.flow('tap-text', { TEXT: 'Fishing regulations.*' });
      await ctx.device.sleep(6000);
      return swipe('pdf', (w) => w.some((x) => /^(Close|Done)$/.test(x)));
    });
  },
};
