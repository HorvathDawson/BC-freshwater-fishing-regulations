// (T454) PHOTO ZOOM ON A SCREEN: a spot with two photos, its photo viewer opened; double-tap zooms in, a drag pans, a
// sideways swipe while zoomed does NOT change the photo, a pinch zooms (two touch slots by `sendevent`, as `gestures`),
// double-tap again returns to the fit, a swipe at the fit changes the photo. Each step is judged on the pixels of the
// viewer's photo box (`photo-zoom-area`) against the one before; the viewer is screen-recorded (<= 30 s) to out/t454/.
//
//   node app/tools/testbed/run.mjs photo-zoom
import fs from 'node:fs';
import path from 'node:path';
import { spawn, execFileSync } from 'node:child_process';
import { coldStart, toMap } from './common.mjs';
import { capture, LAKE } from './spots.mjs';
import { touchscreen, pinch } from './gestures.mjs';
import { changed, readPng } from '../lib/png.mjs';
import { find, look, shown, tap } from '../lib/screen.mjs';

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
      // the touchscreen node is root's on the Play image (zoom-flick's way): adbd as root for the pinch, put back after
      try { d.adb(['root'], { timeout: 30_000 }); await d.sleep(3000); } catch { /* the pinch says so below */ }
      try { pinch(ctx, ts, size, cx, cy, 120, 600); } catch (e) {
        checks.push(`pinch: not driven (${String(e.message).split('\n').pop().slice(0, 80)})`);
        try { d.adb(['unroot'], { timeout: 30_000 }); } catch { /* was not root */ }
        ctx.skip('pinch not driven');
      }
      try { d.adb(['unroot'], { timeout: 30_000 }); await d.sleep(3000); } catch { /* was not root */ }
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
    // ---- (T836) the viewer's four behaviours, looked at on the screen. Shots and recorded sheets go to the run's folder as
    // `T836-*`. A sheet is a recording cut into 10 frames a second (ffmpeg), so a motion can be read frame by frame.
    const adbBin = path.join(process.env.ANDROID_HOME ?? `${process.env.HOME}/Library/Android/sdk`, 'platform-tools/adb');
    const shot836 = (name) => { const f = path.join(ctx.dir, `T836-${name}.png`); d.screenshot(f); return f; };
    const chromeShown = async () => {
      const ns = await look(d);
      const c = find(ns, 'resource-id="photo-counter"'), x = find(ns, 'resource-id="photo-chrome"');
      return { counter: c && shown(c) ? c.text : null, chrome: !!(x && shown(x)) };
    };
    /** a swipe that does not block: the screen can be photographed while the finger is still down */
    const slowSwipe = (x0, y0, x1, y1, ms) => spawn(adbBin, ['-s', d.serial(), 'shell', `input swipe ${x0} ${y0} ${x1} ${y1} ${ms}`], { stdio: 'ignore' });
    const sheet = async (name, act, secs = 6) => {
      const dev = `/sdcard/t836-${name}.mp4`, local = path.join(ctx.dir, `T836-${name}.mp4`);
      const r = spawn(adbBin, ['-s', d.serial(), 'shell', 'screenrecord', '--time-limit', String(secs), dev], { stdio: 'ignore' });
      await d.sleep(1500);
      await act();
      await d.sleep(Math.max(0, secs * 1000 - 1500 - 200) + 1800);
      try { r.kill(); } catch { /* ended */ }
      d.adb(['pull', dev, local]);
      execFileSync('ffmpeg', ['-v', 'error', '-y', '-i', local, '-vf', 'fps=10,scale=180:-1,tile=12x5', '-frames:v', '1', path.join(ctx.dir, `T836-${name}-sheet.png`)]);
      return local;
    };
    const thumb = async (n) => {
      const t = (await look(d)).find((x) => x.desc === `Photo ${n} of 2` && shown(x));
      if (!t) throw new Error(`no thumbnail "Photo ${n} of 2" on the spot page`);
      return t;
    };

    await ctx.step('T836: the counter, and one tap hides the buttons and the next brings them back', async () => {
      const a = await chromeShown();
      if (!a.counter || !/^\d of 2$/.test(a.counter)) throw new Error(`no "N of 2" counter at the top (${JSON.stringify(a)})`);
      shot836('1-chrome-shown');
      d.sh(`input tap ${cx} ${cy}`);
      await d.sleep(900);   // the single tap waits for a possible double tap
      const hidden = await chromeShown();
      shot836('2-chrome-hidden');
      d.sh(`input tap ${cx} ${cy}`);
      await d.sleep(900);
      const back = await chromeShown();
      shot836('3-chrome-back');
      if (hidden.counter) throw new Error(`a tap did not hide the counter (${JSON.stringify(hidden)})`);
      if (!back.counter) throw new Error('a second tap did not bring the counter back');
      return `counter "${a.counter}"; hidden by a tap; back by the next "${back.counter}"`;
    });

    await ctx.step('T836: the strip follows the finger (mid-drag shot), then settles on the other photo', async () => {
      const t0 = which();
      // toward the other photo: left from the first, right from the last (a drag past either end is the rubber band)
      const dx = /^Photo 1 of/.test(t0) ? 300 : -300;
      const sw = slowSwipe(cx + dx, cy, cx - dx, cy, 1800);
      await d.sleep(800);
      const mid = shot836('4-strip-mid-drag');
      await new Promise((r) => sw.on('close', r));
      await d.sleep(1200);
      shot836('5-strip-settled');
      const t1 = which();
      checks.push(`T836 strip: ${t0} -> ${t1}`);
      if (t0 === t1) throw new Error(`the photo did not change after a 600 px drag (${t0})`);
      return `${t0} -> ${t1}; mid-drag ${path.basename(mid)}`;
    });

    await ctx.step('T836: a short drag down springs back; a long one closes (mid-drag shots)', async () => {
      const before = path.join(ctx.dir, 'T836-6-fit-before-drag.png');
      d.screenshot(before);
      const pre = readPng(before);
      const short = slowSwipe(cx, cy, cx, cy + Math.round(size.h * 0.06), 1500);
      await d.sleep(750);
      shot836('7-drag-short-mid');
      await new Promise((r) => short.on('close', r));
      await d.sleep(1200);
      const afterShort = readPng(shot836('8-after-short-drag'));
      const frac = changed(pre, afterShort, box);
      const stillOpen = !!find(await look(d), 'resource-id="photo-zoom-area"');
      if (!stillOpen) throw new Error('a short drag down (6 % of the height) closed the viewer');
      if (frac > 0.03) throw new Error(`after a short drag the photo did not spring back (${(frac * 100).toFixed(0)} % of its box differs)`);
      const long = slowSwipe(cx, cy, cx, cy + Math.round(size.h * 0.4), 1800);
      await d.sleep(900);
      shot836('9-drag-long-mid');
      await new Promise((r) => long.on('close', r));
      await d.sleep(1500);
      const open = !!find(await look(d), 'resource-id="photo-zoom-area"');
      shot836('10-after-long-drag');
      if (open) throw new Error('a drag down of 40 % of the height did not close the viewer');
      return `short: sprang back (${(frac * 100).toFixed(1)} % differs); long: closed`;
    });

    for (const mode of ['light', 'dark']) {
      await ctx.step(`T836 ${mode}: opens from its thumbnail and closes into it (recorded, cut into 10 frames a second)`, async () => {
        d.sh(`cmd uimode night ${mode === 'dark' ? 'yes' : 'no'}`);
        await d.sleep(2500);
        if (!find(await look(d), 'resource-id="spot-photo-grid"')) {   // the viewer may still be up (an earlier step failed)
          d.sh('input keyevent 4'); await d.sleep(1200);
        }
        let t = await thumb(1);
        shot836(`11-${mode}-page-thumbnails`);
        await sheet(`${mode}-open`, () => d.sh(`input tap ${Math.round((t.box.x0 + t.box.x1) / 2)} ${Math.round((t.box.y0 + t.box.y1) / 2)}`));
        const up = !!find(await look(d), 'resource-id="photo-zoom-area"');
        shot836(`12-${mode}-viewer-open`);
        if (!up) throw new Error('a tap on the thumbnail did not open the viewer');
        await sheet(`${mode}-close`, () => d.sh('input keyevent 4'));
        const gone = !find(await look(d), 'resource-id="photo-zoom-area"');
        shot836(`13-${mode}-closed`);
        if (!gone) throw new Error('Back did not close the viewer');
        t = await thumb(1);
        return `opened and closed; frames in T836-${mode}-open-sheet.png / T836-${mode}-close-sheet.png`;
      });
    }
    await ctx.step('T836: the viewer reopened for the steps that follow', async () => {
      d.sh('cmd uimode night no');
      await d.sleep(2000);
      const t = await thumb(1);
      tap(d, t);
      await d.sleep(1800);
      box = area();
      return 'open';
    }, { shot: false });

    await ctx.step('finish the recording, pull it, close the viewer, delete the spot', async () => {
      // stop our own screenrecord (device-side pid), which finalises the mp4
      const pid = d.sh('pidof screenrecord || true').trim().split(/\s+/)[0];
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
