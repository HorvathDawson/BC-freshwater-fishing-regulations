// (T277) SATELLITE ON A PHONE: on Wi-Fi, Satellite chosen in Layers → the imagery is drawn (screenshot, the Layers
// sheet's words, logcat's imagery lines); in airplane mode the sheet says it needs a connection. Satellite is left off.
//
//   node app/tools/testbed/run.mjs satellite
import fs from 'node:fs';
import path from 'node:path';
import { coldStart, toMap } from './common.mjs';

const NEEDS = /NEEDS A CONNECTION|Satellite needs a connection/;
const IMAGERY_LOG = /imagery|arcgis|World_Imagery|satellite|Mbgl|MapLibre|HTTP/i;

/** Satellite on (the row is a toggle: a run that left it on is turned off and on again). */
function satelliteOn(ctx) {
  ctx.flow('layers-satellite');
  let w = ctx.device.screenText();
  const on = w.some((x) => /^Satellite/.test(x) && /on|selected|checked/i.test(x));
  return { w, on };
}

export const satellite = {
  id: 'satellite',
  title: 'Satellite imagery drawn whenever online; offline, the Layers sheet says it needs a connection',
  async run(ctx) {
    await ctx.step('Wi-Fi: Satellite on → imagery drawn', async () => {
      await ctx.device.net('wifi'); ctx.device.link('full', 'none');
      await coldStart(ctx, { ms: 90_000 });
      await toMap(ctx);
      ctx.device.logcatClear();
      const { w } = satelliteOn(ctx);
      ctx.device.screenshot(path.join(ctx.dir, 'T277-layers-online.png'));
      ctx.flow('tap-text', { TEXT: 'Done' });
      await ctx.device.sleep(8000);                                    // the imagery tiles
      ctx.device.screenshot(path.join(ctx.dir, 'T277-satellite-online.png'));
      const log = ctx.device.adb(['logcat', '-d']).toString().split('\n').filter((l) => IMAGERY_LOG.test(l)).slice(-60);
      fs.writeFileSync(path.join(ctx.dir, 'T277-online-logcat.txt'), log.join('\n') + '\n');
      const said = w.find((x) => NEEDS.test(x));
      if (said) throw new Error(`online, the sheet says: ${said}`);
      return `sheet: ${w.filter((x) => /satellite|imagery|esri/i.test(x)).join(' | ').slice(0, 200)} · log lines ${log.length}`;
    });
    await ctx.step('airplane mode: the Layers sheet says satellite needs a connection', async () => {
      await ctx.device.net('offline');
      await ctx.device.sleep(3000);
      ctx.flow('tap-text', { TEXT: 'Choose layers.*' });
      await ctx.device.sleep(1500);
      const w = ctx.device.screenText();
      ctx.device.screenshot(path.join(ctx.dir, 'T277-layers-offline.png'));
      const said = w.find((x) => NEEDS.test(x));
      ctx.flow('tap-text', { TEXT: 'Satellite' });                    // left off for the next run
      ctx.flow('tap-text', { TEXT: 'Done' });
      await ctx.device.net('wifi');
      if (!said) throw new Error('offline with Satellite on: nothing says it needs a connection');
      return said;
    });
  },
};
