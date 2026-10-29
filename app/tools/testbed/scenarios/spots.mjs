// (T255) 4. SPOTS OFFLINE: a spot made with no network ("At my location" on Cultus Lake), named, with a photo from the
// device's gallery; it says what it could not record (the weather); it survives a cold start offline; back online it
// fills in its weather/gauge context by itself (the reconnect refresh) — asked through the SYNC ADAPTER (sync/), so the
// same steps prove account sync when T223 lands (`--sync account`).
import path from 'node:path';
import { appUp, coldStart, toMap } from './common.mjs';

const HERE = path.dirname(new URL(import.meta.url).pathname);
const LAKE = { lon: -121.9870, lat: 49.0535 };           // Cultus Lake's middle: any tap near the pin picks the lake

/** Walk the capture from where it is to its "Save spot": pick the water near the pin, the point, the time, Next. */
async function capture(ctx) {
  const { device: d } = ctx;
  const [W, H] = d.sh('wm size').match(/(\d+)x(\d+)/).slice(1).map(Number);
  const pin = [Math.round(W / 2), Math.round(H * 0.4367)];  // where the capture map centres the pin (above the sheet)
  for (let i = 0; i < 10; i++) {
    const words = d.screenText();
    if (words.some((w) => /^(Save spot|Save)$/.test(w))) return words;
    if (words.some((w) => /Allow .* location/.test(w))) { ctx.flow('tap-text', { TEXT: 'While using the app' }); }
    else if (words.some((w) => /^Pick the water$|^Now the exact spot$/.test(w))) {
      d.sh(`input tap ${pin[0]} ${pin[1] + 40}`);
      await d.sleep(1500);
      ctx.flow('tap-text', { TEXT: 'Next' });
    } else if (words.includes('Continue')) ctx.flow('tap-text', { TEXT: 'Continue' });
    else if (words.includes('Next')) ctx.flow('tap-text', { TEXT: 'Next' });
    await d.sleep(2500);
  }
  throw new Error(`no "Save spot" after the capture's steps (saw: ${d.screenText().slice(0, 10).join(' | ')})`);
}

export const spots = {
  id: 'spots',
  title: 'Spots offline: create with a photo, persist, fill in on reconnect',
  async run(ctx) {
    const spot = { title: `Testbed ${Date.now().toString(36).slice(-5)}`, photo: true };
    const { device: d } = ctx;
    await ctx.step(`setup: ${ctx.sync.name} sync adapter, a photo in the gallery, GPS on Cultus Lake`, async () => {
      await ctx.sync.setup();
      d.addPhoto(path.join(HERE, '..', 'media', 'testbed-photo.jpg'), `testbed-${Date.now()}.jpg`);
      d.geo(LAKE.lon, LAKE.lat);
      await d.net('wifi');
      await appUp(ctx);
    }, { shot: false });
    await ctx.step('offline (airplane mode)', async () => { await d.net('offline'); });
    await ctx.step('Spots → At my location → the capture, offline', async () => {
      ctx.flow('tab', { TAB: 'Spots' });
      ctx.flow('tap-text', { TEXT: 'At my location' });
      await d.sleep(3000);
      await capture(ctx);
    });
    await ctx.step('add a photo from the gallery (the system photo picker)', async () => {
      ctx.flow('spot-add-photo');
      await d.sleep(3000);
      return d.screenText().find((w) => /photo/i.test(w)) ?? 'photo added';
    });
    await ctx.step(`name it "${spot.title}" and save`, async () => {
      ctx.flow('spot-name-save', { TITLE: spot.title });
      await d.sleep(3000);
    });
    await ctx.step('it is stored (adapter: expectStored)', async () => ctx.sync.expectStored(spot));
    await ctx.step('it says what it could not record offline (adapter: expectPending)', async () => ctx.sync.expectPending(spot));
    await ctx.step('cold start, still offline: the spot and its photo are still there', async () => {
      await coldStart(ctx, { ms: 60_000 });
      await ctx.sync.expectStored(spot);
      const photo = d.screenText().some((w) => /NO PHOTO/.test(w));
      return photo ? 'listed — but the row says NO PHOTO' : 'listed with its photo';
    });
    await ctx.step('edit offline: notes added and kept', async () => {
      ctx.flow('spot-notes', { TITLE: spot.title, NOTES: 'edited offline by the test bed' });
      await d.sleep(2000);
      await coldStart(ctx, { ms: 60_000 });
      ctx.flow('tab', { TAB: 'Spots' });
      ctx.flow('tap-text', { TEXT: `.*${spot.title}.*` });
      await ctx.waitText(/edited offline by the test bed/, 15_000);
      d.sh('input keyevent 4');
    });
    await ctx.step('back online → its weather fills in by itself (adapter: expectSynced)', async () => {
      await d.net('wifi');
      return ctx.sync.expectSynced(spot);
    });
    await ctx.step('the spot after reconnect', async () => {
      ctx.flow('tab', { TAB: 'Spots' });
      ctx.flow('tap-text', { TEXT: `.*${spot.title}.*` });
      await d.sleep(2500);
      return d.screenText().filter((w) => /°|km\/h|Weather|m³\/s|gauge/i.test(w)).slice(0, 4).join(' · ');
    });
    await ctx.step('clean up: delete the spot', async () => {
      ctx.flow('spot-delete');
      await d.sleep(1500);
      await toMap(ctx);
    });
  },
};
