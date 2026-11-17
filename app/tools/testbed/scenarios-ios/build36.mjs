// (T814, T815, T816, T821) THE OWNER'S iPHONE BUILD 36 FINDINGS, on the simulator, one look each, before and after a fix.
// Run on a FRESH install (session-ios.sh --fresh): the first download is what T814 is about.
//   T814  the first download's fish: does it move (two shots 600 ms apart, the fish's box), then the app terminated
//         mid-download and launched again: does it still move
//   T816  "When were you there?" (the hour, 18:00-style) and the saved spot's WEATHER line (the big temperature)
//   T815  the spot's photo enlarged: the close ✕ against the status bar
//   T821  a left-edge back swipe over a menu page (About) and the Layers sheet, screen-recorded; frames after the
//         finger lets go are cut out (ffmpeg) so a flash of the page back at its place shows as a picture
// Pictures to `--out` (the run's folder): T81x-*.png. Verdicts are the shots; the steps say what they measured.
//
//   node app/tools/testbed/run-ios.mjs build36 --out <dir>
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { TABS, home, menuPage } from './keyboard.mjs';
import { changed, readPng } from '../lib/png.mjs';

const HERE = path.dirname(new URL(import.meta.url).pathname);
const installing = (w) => w.some((x) => / MB downloaded$/.test(x)) || (w.ids ?? []).includes('boot-close-ok');

export const build36Ios = {
  id: 'build36',
  inAll: false,
  title: 'T814 T815 T816 T821: the owner\'s iPhone build 36 findings, looked at on the simulator',
  async run(ctx) {
    const d = ctx.device;
    const shot = (name) => d.screenshot(path.join(ctx.dir, `${name}.png`));
    /** the fish moving? two shots 600 ms apart, the share of the fish's box (the window's centre, 140 pt) that changed */
    const fish = async (name) => {
      const a = readPng(shot(`T814-${name}-a`));
      await d.sleep(600);
      const b = readPng(shot(`T814-${name}-b`));
      const s = a.w / 402, side = Math.round(140 * s);
      const r = { x: Math.round(a.w / 2 - side / 2), y: Math.round(a.h / 2 - side / 2), w: side, h: side };
      const share = changed(a, b, r);
      return `${name}: ${(share * 100).toFixed(1)} % of the fish's box changed in 600 ms → ${share > 0.01 ? 'MOVING' : 'STILL'}`;
    };

    await ctx.step('T814: the first download, the fish', async () => {
      d.geo(-121.987, 49.0535);   // Cultus Lake (keyboard.mjs LAKE): the capture picks a water
      d.launch();
      for (let i = 0; i < 60 && !installing(d.screenText()); i++) await d.sleep(2000);
      if (!installing(d.screenText())) ctx.skip('no download screen (the release already on the device?)');
      await d.sleep(4000);
      return fish('first');
    });
    await ctx.step('T814: terminated mid-download, launched again: the fish', async () => {
      d.stop();
      await d.sleep(2000);
      d.launch();
      for (let i = 0; i < 30 && !installing(d.screenText()); i++) await d.sleep(1000);
      const notes = [await fish('reopened')];
      await d.sleep(5000);
      notes.push(await fish('reopened-5s'));
      return notes.join('; ');
    });
    await ctx.step('the app up (the download waited out)', async () => {
      await ctx.ios.appUp(ctx, { launch: !d.pidOf(), ms: 1_500_000 });
    });

    let saved = false;
    await ctx.step('T816: "When were you there?" (the hour)', async () => {
      d.geo(-121.987, 49.0535);
      await home(ctx);
      ctx.flow('tap-point', { POINT: TABS.spots });
      await d.sleep(1500);
      ctx.flow('tap-text', { TEXT: 'At my location' });
      await d.sleep(2500);
      let picks = 0;
      for (let i = 0; i < 12; i++) {
        const w = d.screenText();
        if (w.includes('When were you there?')) { shot('T816-when'); return 'shot T816-when.png'; }
        if (w.some((x) => /Allow .* to use your location/.test(x))) ctx.flow('ios-alert', { BUTTON: 'Allow While Using App' });
        else if (w.some((x) => /^Pick the water$|^Now the exact spot$/.test(x))) {
          const off = [4, 0, 1, -1, 2, -2][picks++ % 6];
          ctx.flow('tap-point', { POINT: `50%,${44 + off}%` });
          await d.sleep(1500);
          ctx.flow('tap-text', { TEXT: 'Next' });
        } else if (w.includes('Continue')) ctx.flow('tap-text', { TEXT: 'Continue' });
        else if (w.includes('Next')) ctx.flow('tap-text', { TEXT: 'Next' });
        await d.sleep(2500);
      }
      throw new Error(`no "When were you there?" (saw: ${d.screenText().slice(0, 10).join(' | ').slice(0, 300)})`);
    });
    await ctx.step('T816: the saved spot\'s WEATHER line', async () => {
      for (let i = 0; i < 8 && !d.screenText().some((x) => /^Save spot$/.test(x)); i++) {
        ctx.flow('tap-text', { TEXT: 'Next' });
        await d.sleep(3000);
      }
      ctx.flow('scroll-to', { TEXT: 'Save spot' });
      ctx.flow('tap-text', { TEXT: 'Save spot' });
      saved = true;
      await d.sleep(8000);                                   // the weather is read once saved
      ctx.flow('scroll-to', { TEXT: 'WEATHER' });
      await d.sleep(1500);
      shot('T816-weather');
      return 'shot T816-weather.png';
    });
    await ctx.step('T815: the spot\'s photo enlarged (the close ✕)', async () => {
      if (!saved) ctx.skip('no saved spot');
      d.addMedia(path.join(HERE, '..', 'media', 'testbed-photo.jpg'));
      ctx.flow('tap-text', { TEXT: 'Edit( .*)?' });
      await d.sleep(1500);
      ctx.flow('scroll-to', { TEXT: 'Add from photos' });
      ctx.flow('tap-text', { TEXT: 'Add from photos' });
      await d.sleep(4000);
      shot('T815-picker');
      ctx.flow('tap-point', { POINT: '16%,43%' });
      await d.sleep(2000);
      try { ctx.flow('tap-text', { TEXT: '(Add|Done)' }); } catch { /* a single pick returns by itself */ }
      await d.sleep(3000);
      ctx.flow('scroll-to', { TEXT: 'Save changes' });
      ctx.flow('tap-text', { TEXT: 'Save changes' });
      await d.sleep(3000);
      ctx.flow('scroll-to', { TEXT: 'Photo 1 of .*' });
      ctx.flow('tap-text', { TEXT: 'Photo 1 of .*' });
      await d.sleep(2000);
      shot('T815-viewer');
      const close = d.nodes().find((n) => n.desc === 'Close photo');
      ctx.flow('tap-text', { TEXT: 'Close photo' });
      await d.sleep(1000);
      return close ? `the ✕'s box: ${Math.round(close.box.y0)}-${Math.round(close.box.y1)} pt from the top` : 'the ✕ not in the tree';
    });

    /** a left-edge swipe recorded; frames from the finger's lift on (every 1/30 s for 0.6 s) to T821-<name>-NN.png */
    const swipeFilmed = async (name) => {
      const video = path.join(ctx.dir, `T821-${name}.mp4`);
      const rec = d.recordVideo(video);
      await d.sleep(1500);
      ctx.swipe([8, 437], [260, 437], 350);
      await d.sleep(2500);
      await rec.stop();
      const frames = path.join(ctx.dir, `T821-${name}-%02d.png`);
      try {
        execFileSync('ffmpeg', ['-loglevel', 'error', '-y', '-i', video, '-vf', 'fps=30,scale=402:-1', frames]);
      } catch (e) { return `${name}: ffmpeg could not cut frames (${e.message.split('\n')[0]})`; }
      return `${name}: ${fs.readdirSync(ctx.dir).filter((f) => f.startsWith(`T821-${name}-`) && f.endsWith('.png')).length} frames`;
    };
    await ctx.step('T821: back swipe over About (a menu page)', async () => {
      await home(ctx);
      await menuPage(ctx, 'about');
      return swipeFilmed('about');
    });
    await ctx.step('T821: back swipe over the Layers sheet', async () => {
      await home(ctx);
      ctx.flow('tap-text', { TEXT: 'Choose layers.*' });
      await d.sleep(1500);
      return swipeFilmed('layers');
    });
  },
};

/**
 * (T814) THE COLD START THAT HAPPENS IN THE BACKGROUND: the app terminated mid-download (as iOS does to a suspended app it
 * needs the memory of), the system downloader finishing without it, iOS launching the app in the background to hand it
 * the finished files, and the reader opening it afterwards. The fish is mounted and started while the app is in the
 * background. Shots and a share of the fish's box changed in 600 ms, as `build36`. First (T814's restart on "active"), the
 * first download sent to the home screen for 10 s and brought back: the fish must move after.
 */
export const t814BgIos = {
  id: 't814-bg',
  inAll: false,
  title: 'T814: the fish after a launch iOS made in the background (terminated mid-download, opened later)',
  async run(ctx) {
    const d = ctx.device;
    const shot = (name) => d.screenshot(path.join(ctx.dir, `${name}.png`));
    const fish = async (name) => {
      const a = readPng(shot(`T814-${name}-a`));
      await d.sleep(600);
      const b = readPng(shot(`T814-${name}-b`));
      const s = a.w / 402, side = Math.round(140 * s);
      const r = { x: Math.round(a.w / 2 - side / 2), y: Math.round(a.h / 2 - side / 2), w: side, h: side };
      const share = changed(a, b, r);
      return `${name}: ${(share * 100).toFixed(1)} % changed → ${share > 0.01 ? 'MOVING' : 'STILL'}`;
    };
    await ctx.step('first download: the fish, then sent home for 10 s and brought back: the fish (the T814 restart)', async () => {
      d.geo(-121.987, 49.0535);   // Cultus Lake (keyboard.mjs LAKE): the capture picks a water
      d.launch();
      for (let i = 0; i < 60 && !installing(d.screenText()); i++) await d.sleep(1000);
      if (!installing(d.screenText())) ctx.skip('no download screen');
      const notes = [await fish('bg-first')];
      d.background();
      await d.sleep(10_000);
      d.launch();
      await d.sleep(1500);
      notes.push(await fish('bg-home-10s'));
      return notes.join('; ');
    });
    await ctx.step('first download: terminated', async () => {
      if (!d.pidOf() || !installing(d.screenText())) ctx.skip('not on the download screen any more');
      d.stop();
      shot('T814-bg-terminated');
      return 'terminated on the download screen';
    });
    await ctx.step('not opened while the system downloader finishes (90 s): did iOS launch it in the background?', async () => {
      const seen = [];
      for (let i = 0; i < 18; i++) { await d.sleep(5000); seen.push(d.pidOf() ? 'up' : '-'); }
      return `the app's process every 5 s: ${seen.join(' ')}`;
    });
    await ctx.step('opened: the fish', async () => {
      d.launch();
      await d.sleep(1500);
      const notes = [await fish('bg-opened')];
      await d.sleep(4000);
      notes.push(await fish('bg-opened-5s'));
      return notes.join('; ');
    });
    await ctx.step('sent home mid-setup or later for 60 s, opened again: the fish', async () => {
      if (!installing(d.screenText()) && !d.screenText().some((x) => /Setting up/.test(x))) ctx.skip('the app is already up');
      d.background();
      await d.sleep(60_000);
      d.launch();
      await d.sleep(1500);
      return fish('bg-home-60s');
    });
  },
};

