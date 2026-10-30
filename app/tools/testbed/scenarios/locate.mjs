// (T303) MY LOCATION ON A PHONE, with the emulator's mock GPS (`adb emu geo fix`): refused → the line saying how to
// turn it on; allowed → the blue dot, centred; tap again → following (a moved fix moves the map); a pan stops it.
//
//   node app/tools/testbed/run.mjs locate          (LOCATE_AT="-121.95,49.1")
import path from 'node:path';
import { coldStart, toMap } from './common.mjs';
import { changed, count, readPng } from '../lib/png.mjs';

const [LON, LAT] = (process.env.LOCATE_AT ?? '-121.95,49.1').split(',').map(Number);
const PERMS = ['android.permission.ACCESS_FINE_LOCATION', 'android.permission.ACCESS_COARSE_LOCATION'];
const DENIED = /^Location is off for this app/;

/** The rail button's label now (My location | Follow my location | Stop following my location), or null. */
const railLabel = (w) => w.find((x) => /^(My location|Follow my location|Stop following my location)$/.test(x)) ?? null;

/** The screen's middle, where a centred camera puts the fix (the map is full-bleed behind the chrome). */
const middle = (img) => ({ x: Math.round(img.w / 2) - 60, y: Math.round(img.h / 2) - 60, w: 120, h: 120 });
/** The dot: a saturated blue beside white (MapLibre's puck: a blue disc in a white ring). */
const blue = (r, g, b) => b > 180 && r < 110 && b - r > 110 && b - g > 40;
const white = (r, g, b) => r > 235 && g > 235 && b > 235;

export const locate = {
  id: 'locate',
  title: 'My location (T303): denied line, blue dot centred, follow a moved fix, a pan stops following',
  async run(ctx) {
    const shot = (name) => ctx.device.screenshot(path.join(ctx.dir, `T303-${name}.png`));
    await ctx.step('location never asked: permissions revoked, mock fix set, the map up', async () => {
      for (const p of PERMS) { try { ctx.device.sh(`pm revoke ${ctx.device.APP_ID} ${p}`); } catch { /* not held */ } }
      try { ctx.device.sh('cmd location set-location-enabled true'); } catch { /* older Android */ }
      ctx.device.geo(LON, LAT);
      await ctx.device.net('wifi'); ctx.device.link('full', 'none');
      await coldStart(ctx, { ms: 90_000 });
      await toMap(ctx);
      await ctx.device.sleep(3000);
      const l = railLabel(ctx.device.screenText());
      if (l !== 'My location') throw new Error(`the rail button reads ${l ?? '(none)'}`);
      return `fix ${LON},${LAT}`;
    });
    await ctx.step('tap 1, refused at the prompt → the line says how to turn it on', async () => {
      ctx.flow('tap-text', { TEXT: 'My location' });
      await ctx.device.sleep(2500);
      let w = ctx.device.screenText();
      const prompt = w.find((x) => /access this device.s location|location/i.test(x) && /Allow/i.test(w.join(' ')));
      if (!w.some((x) => /^Don.t allow$/i.test(x))) throw new Error(`no permission prompt (saw: ${w.slice(0, 10).join(' | ').slice(0, 300)})`);
      ctx.flow('tap-text', { TEXT: "Don.t allow" });
      await ctx.device.sleep(2500);
      w = ctx.device.screenText();
      shot('denied');
      const line = w.find((x) => DENIED.test(x));
      if (!line) throw new Error(`refused: no line (saw: ${w.slice(0, 12).join(' | ').slice(0, 300)})`);
      ctx.flow('tap-text', { TEXT: 'Location is off for this app.*' });          // a tap on the line puts it away
      await ctx.device.sleep(1200);
      if (ctx.device.screenText().some((x) => DENIED.test(x))) throw new Error('the line stayed after a tap');
      return `prompt: ${prompt ?? 'shown'} · line: ${line}`;
    });
    let at = null;
    await ctx.step('allowed (granted, as "While using the app"): tap → the blue dot, centred; the button says Follow', async () => {
      for (const p of PERMS) ctx.device.sh(`pm grant ${ctx.device.APP_ID} ${p}`);
      ctx.device.geo(LON, LAT);
      await ctx.device.sleep(1500);
      const before = readPng(shot('before'));
      ctx.flow('tap-text', { TEXT: 'My location' });
      let l = null;
      for (let i = 0; i < 10 && l !== 'Follow my location'; i++) { await ctx.device.sleep(1500); l = railLabel(ctx.device.screenText()); }
      await ctx.device.sleep(2500);                                           // the camera's ease + the tiles
      const img = readPng(shot('dot'));
      at = img;
      const m = middle(img), b = count(img, m, blue), wh = count(img, m, white);
      const moved = changed(before, img, { x: 0, y: Math.round(img.h * 0.15), w: img.w, h: Math.round(img.h * 0.6) });
      const note = `button: ${l} · middle blue ${b} px, white ${wh} px · map changed ${(moved * 100).toFixed(0)} %`;
      if (l !== 'Follow my location') throw new Error(`the button reads ${l} — ${note}`);
      if (b < 150 || wh < 40) throw new Error(`no blue dot at the middle — ${note}`);
      return note;
    });
    await ctx.step('(T308) a series of mock fixes WITHOUT following: the dot moves, the camera stays', async () => {
      const base = readPng(shot('series-0'));
      const m = middle(base);
      if (count(base, m, blue) < 150) throw new Error('no dot at the middle to start from');
      const seen = [];
      for (let i = 1; i <= 3; i++) {
        ctx.device.geo(LON + 0.004 * i, LAT + 0.002 * i);                     // ~300 m further north-east each time
        await ctx.device.sleep(4000);
        const img = readPng(shot(`series-${i}`));
        seen.push(count(img, m, blue));
      }
      const last = readPng(path.join(ctx.dir, 'T303-series-3.png'));
      const mapMoved = changed(base, last, { x: 0, y: Math.round(last.h * 0.15), w: last.w, h: Math.round(last.h * 0.6) });
      const l = railLabel(ctx.device.screenText());
      const note = `middle blue after each fix: ${seen.join(', ')} · map changed ${(mapMoved * 100).toFixed(0)} % · button: ${l}`;
      if (seen[seen.length - 1] > 60) throw new Error(`the dot stayed at the middle (stuck at the first fix) — ${note}`);
      if (l !== 'Follow my location') throw new Error(`the button changed without a tap — ${note}`);
      // the dot is somewhere on the map now: blue + white ring anywhere in the map area
      const area = { x: 0, y: Math.round(last.h * 0.15), w: last.w, h: Math.round(last.h * 0.6) };
      if (count(last, area, blue) < 150) throw new Error(`no dot anywhere on the map after the fixes — ${note}`);
      return note;
    });
    await ctx.step('tap 2 → following: a moved fix moves the map; the button says Stop following', async () => {
      ctx.flow('tap-text', { TEXT: 'Follow my location' });
      await ctx.device.sleep(1500);
      const l = railLabel(ctx.device.screenText());
      if (l !== 'Stop following my location') throw new Error(`the button reads ${l}`);
      const a = at ?? readPng(shot('follow-a'));
      ctx.device.geo(LON + 0.004, LAT + 0.002);                              // ~350 m north-east
      await ctx.device.sleep(4000);
      const b = readPng(shot('follow-moved'));
      const moved = changed(a, b, { x: 0, y: Math.round(b.h * 0.15), w: b.w, h: Math.round(b.h * 0.6) });
      const dot = count(b, middle(b), blue);
      const note = `map changed ${(moved * 100).toFixed(0)} % · middle blue ${dot} px`;
      if (moved < 0.05) throw new Error(`the map did not follow the moved fix — ${note}`);
      if (dot < 150) throw new Error(`the dot is not at the middle after the move — ${note}`);
      return note;
    });
    await ctx.step('a one-finger pan stops following (the button says Follow again)', async () => {
      const W = Number(/(\d+)x(\d+)/.exec(ctx.device.sh('wm size'))?.[1] ?? 1080), H = Number(/(\d+)x(\d+)/.exec(ctx.device.sh('wm size'))?.[2] ?? 2400);
      ctx.device.sh(`input swipe ${Math.round(W * 0.3)} ${Math.round(H * 0.45)} ${Math.round(W * 0.7)} ${Math.round(H * 0.55)} 400`);
      await ctx.device.sleep(2000);
      const l = railLabel(ctx.device.screenText());
      shot('panned');
      ctx.device.geo(LON, LAT);                                               // a new fix must not pull the map back
      await ctx.device.sleep(3000);
      const l2 = railLabel(ctx.device.screenText());
      if (l !== 'Follow my location') throw new Error(`after the pan the button reads ${l}`);
      return `after pan: ${l}; after a new fix: ${l2}`;
    });
    await ctx.step('put back: dot off (the next scenarios start on a plain map)', async () => {
      ctx.device.stop();
      for (const p of PERMS) { try { ctx.device.sh(`pm revoke ${ctx.device.APP_ID} ${p}`); } catch { /* fine */ } }
    });
  },
};
