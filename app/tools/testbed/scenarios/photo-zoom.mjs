// (T454) PHOTO ZOOM ON A SCREEN: a spot with two photos, its photo viewer opened; double-tap zooms in, a drag pans, a
// sideways swipe while zoomed does NOT change the photo, a pinch zooms (two touch slots by `sendevent`, as `gestures`),
// double-tap again returns to the fit, a swipe at the fit changes the photo. Each step is judged on the pixels of the
// viewer's photo box (`photo-zoom-area`) against the one before; the viewer is screen-recorded (<= 30 s) to out/t454/.
//
//   node app/tools/testbed/run.mjs photo-zoom
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { coldStart, toMap } from './common.mjs';
import { capture, LAKE } from './spots.mjs';
import { touchscreen, pinch } from './gestures.mjs';
import { changed, readPng } from '../lib/png.mjs';

const HERE = path.dirname(new URL(import.meta.url).pathname);
const OUT = path.join(HERE, '..', 'out', 't454');
const LABEL = /^Photo \d of \d$/;

export const photoZoom = {
  id: 'photo-zoom',
  title: 'T454: spot photo viewer — double-tap zoom, pan, swipe while zoomed keeps the photo, pinch, back to fit, swipe changes photo',
  async run(ctx) {
    const { device: d } = ctx;
    fs.mkdirSync(OUT, { recursive: true });
    const title = `Zoom ${Date.now().toString(36).slice(-5)}`;
    let size, ts, box, cx, cy, rec = null, n = 0, prev = null;
    const checks = [];
    const png = (name) => { const f = path.join(OUT, `${String(++n).padStart(2, '0')}-${name}.png`); d.screenshot(f); return readPng(f); };
    const area = () => {
      d.sh('uiautomator dump /sdcard/tb-z.xml >/dev/null 2>&1');
      const x = d.sh('cat /sdcard/tb-z.xml');
      const m = /resource-id="photo-zoom-area"[^>]*bounds="\[(\d+),(\d+)\]\[(\d+),(\d+)\]"/.exec(x)
        ?? /bounds="\[(\d+),(\d+)\]\[(\d+),(\d+)\]"[^>]*resource-id="photo-zoom-area"/.exec(x);
      if (!m) throw new Error(`photo viewer not on screen (saw: ${d.screenText().slice(0, 8).join(' | ')})`);
      return { x: +m[1], y: +m[2], w: +m[3] - +m[1], h: +m[4] - +m[2] };
    };
    const which = () => d.screenText().find((w) => LABEL.test(w)) ?? '?';
    /** screenshot `name`, compared with the one before inside the photo box */
    const judge = (name, expectChange) => {
      const cur = png(name);
      const frac = prev ? changed(prev, cur, box) : 0;
      prev = cur;
      const msg = `${name}: ${(frac * 100).toFixed(0)} % of the photo box changed`;
      checks.push(msg);
      if (expectChange ? frac < 0.05 : frac > 0.02) throw new Error(`${msg} (expected ${expectChange ? 'a change' : 'none'})`);
      return msg;
    };
    const dtap = () => d.sh(`input tap ${cx} ${cy}; input tap ${cx} ${cy}`);

    await ctx.step('setup: two photos in the gallery, GPS on Cultus Lake, a spot made with both', async () => {
      await ctx.sync.setup();
      const m = path.join(HERE, '..', 'media', 'testbed-photo.jpg');
      d.addPhoto(m, `t454-a-${Date.now()}.jpg`);
      d.addPhoto(m, `t454-b-${Date.now()}.jpg`);
      d.geo(LAKE.lon, LAKE.lat);
      await d.net('wifi');
      await coldStart(ctx);
      const mm = /(\d+)x(\d+)/.exec(d.sh('wm size'));
      size = { w: +mm[1], h: +mm[2] };
      ts = touchscreen(ctx);
      ctx.flow('tab', { TAB: 'Spots' });
      ctx.flow('tap-text', { TEXT: 'At my location' });
      await d.sleep(3000);
      await capture(ctx);
      for (let i = 0; i < 2; i++) { ctx.flow('spot-add-photo'); await d.sleep(3000); }
      ctx.flow('spot-name-save', { TITLE: title });
      await d.sleep(3000);
      return `spot "${title}"; touchscreen ${ts ? ts.dev : 'none'}`;
    }, { shot: false, stop: true });

    await ctx.step('open the spot, then its first photo in the viewer (recording starts)', async () => {
      ctx.flow('tab', { TAB: 'Spots' });
      ctx.flow('tap-text', { TEXT: title });
      await d.sleep(2000);
      ctx.flow('scroll-to', { TEXT: 'Photo 1 of 2' });
      ctx.flow('tap-text', { TEXT: 'Photo 1 of 2' });
      await d.sleep(2000);
      box = area();
      cx = Math.round(box.x + box.w / 2); cy = Math.round(box.y + box.h / 2);
      prev = png('fit');
      rec = spawn(path.join(process.env.ANDROID_HOME ?? `${process.env.HOME}/Library/Android/sdk`, 'platform-tools/adb'),
        ['-s', d.serial(), 'shell', 'screenrecord', '--time-limit', '30', '/sdcard/t454.mp4'], { stdio: 'ignore' });
      await d.sleep(1500);
      return `viewer box ${box.w}x${box.h}@${box.x},${box.y}; ${which()}`;
    }, { stop: true });

    await ctx.step('double-tap zooms in', async () => { dtap(); await d.sleep(1200); return judge('double-tap-in', true); });
    await ctx.step('one-finger drag pans the zoomed photo', async () => {
      d.sh(`input swipe ${cx + 150} ${cy + 100} ${cx - 150} ${cy - 100} 500`);
      await d.sleep(1200);
      return judge('pan', true);
    });
    await ctx.step('sideways swipe while zoomed does NOT change the photo', async () => {
      const w0 = which();
      d.sh(`input swipe ${cx + 200} ${cy} ${cx - 200} ${cy} 200`);
      await d.sleep(1200);
      const w1 = which();
      const msg = judge('swipe-zoomed', true);       // the zoomed photo pans: pixels move, the photo stays
      if (w0 !== w1) throw new Error(`the photo changed while zoomed: ${w0} -> ${w1}`);
      return `${msg}; still ${w1}`;
    });
    await ctx.step('pinch (two-finger spread) zooms', async () => {
      if (!ts) { checks.push('pinch: no multi-touch screen found'); ctx.skip('no touchscreen'); }
      try { pinch(ctx, ts, size, cx, cy, 120, 600); } catch (e) {
        checks.push(`pinch: not driven (${String(e.message).split('\n').pop().slice(0, 80)})`);
        ctx.skip('pinch not driven');
      }
      await d.sleep(1200);
      return judge('pinch', true);
    });
    await ctx.step('double-tap returns to the fit', async () => {
      dtap(); await d.sleep(1500);
      const msg = judge('double-tap-out', true);
      return msg;
    });
    await ctx.step('swipe at the fit changes the photo', async () => {
      const t0 = which();
      d.sh(`input swipe ${cx + 250} ${cy} ${cx - 250} ${cy} 250`);
      await d.sleep(1500);
      const t1 = which();
      checks.push(`swipe at fit: ${t0} -> ${t1}`);
      png('swipe-fit');
      if (t0 === t1) throw new Error(`the photo did not change: ${t0}`);
      return `${t0} -> ${t1}`;
    });
    await ctx.step('finish the recording, pull it, close the viewer, delete the spot', async () => {
      // stop our own screenrecord (device-side pid), which finalises the mp4
      const pid = d.sh('pidof screenrecord').trim().split(/\s+/)[0];
      if (pid) d.sh(`kill -2 ${pid}`);
      await d.sleep(3000);
      try { rec?.kill(); } catch { /* ended */ }
      d.adb(['pull', '/sdcard/t454.mp4', path.join(OUT, 't454-photo-zoom.mp4')]);
      fs.writeFileSync(path.join(OUT, 'checks.txt'), `${checks.join('\n')}\n`);
      d.sh('input keyevent 4');
      await d.sleep(1000);
      ctx.flow('spot-delete');
      await d.sleep(1500);
      await toMap(ctx);
      return 'recording pulled';
    }, { shot: false });
  },
};
