// (T255) 4. SPOTS OFFLINE: a spot made with no network ("At my location" on Cultus Lake), named, with a photo from the
// device's gallery; it says what it could not record (the weather); it survives a cold start offline; back online it
// fills in its weather/gauge context by itself (the reconnect refresh) — asked through the SYNC ADAPTER (sync/), so the
// same steps prove account sync when T223 lands (`--sync account`).
import path from 'node:path';
import { appUp, coldStart, toMap } from './common.mjs';

const HERE = path.dirname(new URL(import.meta.url).pathname);
export const LAKE = { lon: -121.9870, lat: 49.0535 };
/** SPOTS_AT="lon,lat" (T314): save the spot elsewhere — e.g. on a gauged river, so its gauges are pending offline too */
const AT = process.env.SPOTS_AT ? (([lon, lat]) => ({ lon, lat }))(process.env.SPOTS_AT.split(',').map(Number)) : LAKE;           // Cultus Lake's middle: any tap near the pin picks the lake

/** Walk the capture from where it is to its "Save spot": pick the water near the pin, the point, the time, Next. */
export async function capture(ctx) {
  const { device: d } = ctx;
  const [W, H] = d.sh('wm size').match(/(\d+)x(\d+)/).slice(1).map(Number);
  const pin = [Math.round(W / 2), Math.round(H * 0.4367)];  // where the capture map centres the pin (above the sheet)
  let picks = 0;
  for (let i = 0; i < 10; i++) {
    const words = d.screenText();
    if (words.some((w) => /^(Save spot|Save)$/.test(w))) return words;
    if (words.some((w) => /Allow .* location/.test(w))) { ctx.flow('tap-text', { TEXT: 'While using the app' }); }
    else if (words.some((w) => /^Pick the water$|^Now the exact spot$/.test(w))) {
      // the lake answers a tap just below the pin; a river is a line: try on the pin, then either side of it
      const off = [40, 0, 12, -12, 24, -24][picks++ % 6];
      d.sh(`input tap ${pin[0]} ${pin[1] + off}`);
      await d.sleep(1500);
      ctx.flow('tap-text', { TEXT: 'Next' });
    } else if (words.includes('When were you there?') && words.includes('Next')) {
      // (T307/T323) the ONE step that reads (cap.capture: gauge, weather, the day's flow) is the "When were you there?"
      // Next → the draft with "Save spot"; offline it must not wait on the network. (T323: this was timed from the
      // pick-the-water Next, which lands on THIS screen, not on Save spot — so it always ran to its 15 s cap.)
      // (T603) `ctx.captureDay`: the day chip to pick first ("Yesterday": a day whose record is whole); today otherwise
      if (ctx.captureDay) ctx.flow('tap-text', { TEXT: ctx.captureDay });
      ctx.flow('tap-text', { TEXT: 'Next' });
      const t0 = Date.now();
      while (Date.now() - t0 < 15_000 && !d.screenText().some((w) => /^(Save spot|Save)$/.test(w))) await d.sleep(200);
      ctx.captureMs = Date.now() - t0;
      continue;
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
      d.geo(AT.lon, AT.lat);
      await d.net('wifi');
      await coldStart(ctx);   // every scenario starts from a fresh launch (the data stays)
    }, { shot: false });
    await ctx.step('offline (airplane mode)', async () => { await d.net('offline'); });
    await ctx.step('Spots → At my location → the capture, offline', async () => {
      ctx.flow('tab', { TAB: 'Spots' });
      ctx.flow('tap-text', { TEXT: 'At my location' });
      await d.sleep(3000);
      await capture(ctx);
      // each screen read costs ~1 s itself, so < 3 s here means the capture did not wait on the network (was 10 s+)
      if (ctx.captureMs != null && ctx.captureMs > 3000) throw new Error(`the capture took ${ctx.captureMs} ms offline (T307: should be instant)`);
      return `Next → Save spot in ${ctx.captureMs ?? '?'} ms (T307)`;
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
      // the row's words: title, then its lines; a row without a photo says NO PHOTO before its title
      const w = d.screenText();
      const at = w.findIndex((x) => x === spot.title);
      if (at > 0 && /NO PHOTO/.test(w[at - 1] ?? '')) throw new Error('the row says NO PHOTO: the photo was not kept');
      return `listed with its photo · ${w[at + 1] ?? ''}`;
    });
    let editing = false;
    await ctx.step('edit offline: notes typed (keyboard up: is "Save changes" above it?)', async () => {
      ctx.flow('spot-notes', { TITLE: spot.title, NOTES: 'edited offline by the test bed', SHOT: path.join(ctx.dir, 'spots-notes') });
      await d.sleep(1500);
      const w = d.screenText();
      editing = w.some((x) => /edited offline by the test bed/.test(x));
      if (!editing) throw new Error(`the notes field is not on screen after typing (saw: ${w.slice(0, 8).join(' | ')})`);
      const visible = w.includes('Save changes');
      if (!visible) ctx.finding('spot edit (Android 15, edge-to-edge): with the keyboard up, "Save changes" is under the keyboard');
      return visible ? '"Save changes" visible above the keyboard' : '"Save changes" hidden by the keyboard';
    });
    await ctx.step('system back closes the keyboard only (the edit stays)', async () => {
      if (!editing) ctx.skip('the edit form was not reached');
      d.sh('input keyevent 4');
      await d.sleep(1500);
      const w = d.screenText();
      if (!w.some((x) => /edited offline by the test bed/.test(x))) {
        const asked = w.some((x) => /unsaved changes/i.test(x));
        ctx.finding(`spot edit: one system back with the keyboard up ${asked ? 'asked about unsaved changes' : 'left the edit and its unsaved notes without asking'}`);
        throw new Error(`back left the edit (saw: ${w.slice(0, 6).join(' | ')})`);
      }
    });
    await ctx.step('Save changes; cold start offline; the notes are kept', async () => {
      ctx.flow('tap-text', { TEXT: 'Save changes' });
      await d.sleep(2000);
      await coldStart(ctx, { ms: 60_000 });
      ctx.flow('tab', { TAB: 'Spots' });
      ctx.flow('tap-text', { TEXT: spot.title });
      ctx.flow('scroll-to', { TEXT: 'edited offline by the test bed' });     // the notes sit at the page's foot
      const said = await ctx.waitText(/edited offline by the test bed/, 5_000);
      d.sh('input keyevent 4');
      return `kept: "${said}"`;
    });
    let synced = false;
    await ctx.step('back online → its weather fills in by itself (adapter: expectSynced)', async () => {
      d.logcatClear();
      await d.net('wifi');
      const r = await ctx.sync.expectSynced(spot);
      synced = true;
      return r;
    });
    if (!synced) {
      await ctx.step("diagnosis: does the spot page's own Refresh fill it?", async () => {
        const js = d.logcatJs().split('\n').filter((l) => /ReactNativeJS/.test(l)).slice(-6).join(' / ');
        ctx.flow('tab', { TAB: 'Spots' });
        ctx.flow('tap-text', { TEXT: spot.title });
        // (T314) what the page says BEFORE the manual Refresh and after it: did the reconnect refresh leave anything the
        // button then fills, or only what no refresh can fill yet (the visit day's own days)?
        await d.sleep(2000);
        const said = () => d.screenText().filter((w) => /gauge|not available|not recorded|not been read|pending|°C|for the time of year|No weather/i.test(w)).slice(0, 8).join(' | ');
        ctx.finding(`before manual Refresh: ${said()}`);
        ctx.flow('tap-text', { TEXT: 'Refresh' });
        await d.sleep(8000);
        ctx.finding(`after manual Refresh: ${said()}`);
        const left = d.screenText().find((w) => /No weather was recorded/.test(w));
        ctx.finding(`spot made offline not filled after reconnect (2 min); manual Refresh ${left ? 'did NOT fill it either' : 'filled it'}${js ? ` — JS log: ${js.slice(0, 400)}` : ''}`);
        if (left) throw new Error('manual Refresh did not fill it either');
        return 'manual Refresh filled it: the reconnect trigger did not fire';
      });
    }
    await ctx.step('the spot after reconnect', async () => {
      ctx.flow('tab', { TAB: 'Spots' });
      ctx.flow('tap-text', { TEXT: spot.title });
      await d.sleep(2500);
      // (T314) its gauges filled too, not only its weather: no "not read yet" / "not recorded" line left on the page
      // (the visit day's own flow "can be filled in from the day's record" stays until the day is over: not a refresh's)
      const left = d.screenText().find((w) => /have not been read from their record yet|was not recorded — it can be filled in/.test(w)
                                              && !/from the day's record/.test(w));
      if (left) throw new Error(`the spot's gauges did not fill on reconnect: "${left}"`);
      return d.screenText().filter((w) => /°|km\/h|Weather|m³\/s|gauge/i.test(w)).slice(0, 4).join(' · ');
    });
    await ctx.step('clean up: delete the spot', async () => {
      ctx.flow('spot-delete');
      await d.sleep(1500);
      await toMap(ctx);
    });
  },
};
