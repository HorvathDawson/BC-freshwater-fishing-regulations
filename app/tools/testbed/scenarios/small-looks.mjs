// (T728, T730, T731) THE SMALL LOOKS ON THE EMULATOR, shots first. Each step keeps pictures as `T7xx-*.png` beside the
// report; the checks are the few that can be read from the screen's nodes. The pictures are the deliverable.
//
//   T728  a water's Conditions card opens on Flow; a pick of Level in the card does not move the map's own layer
//   T730  search: a swipe down from the grabber closes it; the recents are one compact block (light / dark); a water picked
//         then Back leaves the map where the pick put it and the next search starts empty
//   T731  a water's Map view: ▼ into another water and ▲ back keep the bar under the map at its height (the bar is
//         measured before and after; a recording is cut into 10 frames a second)
//
//   node app/tools/testbed/run.mjs small-looks      (SMALL_LOOKS=t728,t730,t731)
import path from 'node:path';
import { spawn, execFileSync } from 'node:child_process';
import { closeWaters, coldStart, openWater, toMap } from './common.mjs';
import { find, look, shown, tap } from '../lib/screen.mjs';

const ONLY = (process.env.SMALL_LOOKS ?? 't728,t730,t731').split(',');

export const smallLooks = {
  id: 'small-looks',
  title: 'T728 / T730 / T731 looks: the card pick, the search sheet, a step into another water',
  async run(ctx) {
    const d = ctx.device;
    const adbBin = path.join(process.env.ANDROID_HOME ?? `${process.env.HOME}/Library/Android/sdk`, 'platform-tools/adb');
    const shot = (name) => { const f = path.join(ctx.dir, `${name}.png`); d.screenshot(f); return f; };
    const night = (on) => d.sh(`cmd uimode night ${on ? 'yes' : 'no'}`);
    const sheet = async (name, act, secs = 8) => {
      const dev = `/sdcard/${name}.mp4`, local = path.join(ctx.dir, `${name}.mp4`);
      const r = spawn(adbBin, ['-s', d.serial(), 'shell', 'screenrecord', '--time-limit', String(secs), dev], { stdio: 'ignore' });
      await d.sleep(1500);
      await act();
      await d.sleep(Math.max(0, secs * 1000 - 1500 - 200) + 1800);
      try { r.kill(); } catch { /* ended */ }
      d.adb(['pull', dev, local]);
      execFileSync('ffmpeg', ['-v', 'error', '-y', '-i', local, '-vf', 'fps=10,scale=160:-1,tile=12x6', '-frames:v', '1', path.join(ctx.dir, `${name}-sheet.png`)]);
    };
    const words = () => d.screenText();

    await ctx.step('setup: Wi-Fi, light, a cold start', async () => {
      night(false); await d.net('wifi'); d.link('full', 'none');
      await coldStart(ctx);
    }, { shot: false, stop: true });

    if (ONLY.includes('t728')) {
      await ctx.step('T728: Coquitlam River Conditions opens on Flow; Level picked in the card; the map keeps its own layer', async () => {
        await openWater(ctx, { q: 'Coquitlam River', row: '.* · Region 2 · MU 2-8.*', view: 'conditions' });
        await ctx.until(() => ctx.hasId('gauge-card'), { ms: 30_000, what: 'the gauge card' });
        await d.sleep(4000);
        const strip = find(await look(d), 'resource-id="chart-quantity"');
        if (!strip) throw new Error('no Flow / Level control (chart-quantity) on the card');
        const before = shot('T728-1-card-opens');
        const opens = (await look(d)).filter((n) => shown(n) && n.box.y0 >= strip.box.y0 - 5 && n.box.y1 <= strip.box.y1 + 5 && n.box.x0 >= strip.box.x0 && n.box.x1 <= strip.box.x1 + 2);
        const said = opens.map((n) => `${n.text || n.desc}${n.desc && /selected|checked/i.test(n.desc) ? '*' : ''}`).filter(Boolean).join(' | ');
        const lvl = opens.find((n) => /^Level$/i.test(n.text) || /^Level/i.test(n.desc));
        if (!lvl) throw new Error(`no Level choice in the control (saw ${said})`);
        tap(d, lvl);
        await d.sleep(3500);
        const after = shot('T728-2-card-level');
        await closeWaters(ctx);
        await toMap(ctx);
        await d.sleep(2500);
        shot('T728-3-map-after');
        return `control: ${said}; shots ${path.basename(before)}, ${path.basename(after)}, T728-3-map-after.png`;
      });
    }

    if (ONLY.includes('t730')) {
      for (const mode of ['light', 'dark']) {
        await ctx.step(`T730 ${mode}: search opens; the recents block; a swipe down from the grabber closes it`, async () => {
          night(mode === 'dark'); await d.sleep(2500);
          await closeWaters(ctx); await toMap(ctx);
          // two searches so the recents hold something
          for (const q of ['Thompson', 'Coquitlam']) { ctx.flow('search', { Q: q }); await d.sleep(1500); await toMap(ctx); await d.sleep(800); }
          ctx.flow('tap-id', { ID: 'search-field' });
          await d.sleep(2000);
          shot(`T730-${mode}-1-search-open`);
          const ns = await look(d);
          const handle = find(ns, 'resource-id="search-sheet-handle"');
          const recents = find(ns, 'resource-id="search-recents"');
          shot(`T730-${mode}-2-keyboard-down`);
          if (!handle) throw new Error('no search sheet grabber (search-sheet-handle) on screen');
          const cx = Math.round((handle.box.x0 + handle.box.x1) / 2), y = Math.round((handle.box.y0 + handle.box.y1) / 2);
          const sw = spawn(adbBin, ['-s', d.serial(), 'shell', `input swipe ${cx} ${y} ${cx} ${y + 900} 1400`], { stdio: 'ignore' });
          await d.sleep(700);
          shot(`T730-${mode}-3-swipe-mid`);
          await new Promise((r) => sw.on('close', r));
          await d.sleep(1500);
          const gone = !find(await look(d), 'resource-id="search-list"');
          shot(`T730-${mode}-4-after-swipe`);
          if (!gone) throw new Error('a swipe down from the grabber did not close the search sheet');
          return `recents block ${recents && shown(recents) ? `${recents.box.y1 - recents.box.y0} px tall` : 'not shown'}; swipe closed the sheet`;
        });
      }
      night(false);
      await ctx.step('T730: Thompson River picked, then Back: the map stays there; search reopens empty', async () => {
        await d.sleep(2000);
        await closeWaters(ctx); await toMap(ctx);
        ctx.flow('search', { Q: 'Thompson River' });
        await d.sleep(2000);
        shot('T730-5-results');
        // the result row, not the field (which holds the same words): the first "Thompson River" below the sheet's header
        const row = (await look(d)).find((n) => shown(n) && n.box.y0 > 420 && (n.text === 'Thompson River' || n.desc === 'Thompson River' || /^Thompson River\b/.test(n.desc)));
        if (!row) throw new Error('no "Thompson River" result row to tap');
        tap(d, row);
        await d.sleep(3500);
        shot('T730-6-picked-framed');
        d.sh('input keyevent 4');
        await d.sleep(2500);
        shot('T730-7-after-back');
        ctx.flow('tap-id', { ID: 'search-field' });
        await d.sleep(2000);
        // the FIELD's own words (the recents block lists "Thompson River" as a tag: it is not the old query)
        const fieldNode = find(await look(d), 'resource-id="search-field"');
        const field = fieldNode && /Thompson/i.test(fieldNode.text) ? [fieldNode.text] : [];
        shot('T730-8-reopened');
        d.sh('input keyevent 4'); await d.sleep(500);
        if (field.length) throw new Error(`the next search still carries the old query: ${field.join(' | ')}`);
        return 'back left the map at the pick (see T730-6 / T730-7); the reopened search shows no old query';
      });
    }

    if (ONLY.includes('t731')) {
      await ctx.step('T731: Chilliwack River Map view: ▼ into the Vedder and ▲ back; the bar keeps its height', async () => {
        await closeWaters(ctx); await toMap(ctx);
        ctx.flow('search', { Q: 'Chilliwack River' });
        ctx.flow('open-result', { LINE: 'Stream · .*' });
        const end = Date.now() + 30_000;
        while (Date.now() < end && !find(await look(d), 'resource-id="water-view-map"')) await d.sleep(1500);
        ctx.flow('tap-id', { ID: 'water-view-map' });
        await d.sleep(3000);
        ctx.flow('tap-id', { ID: 'water-view-regs' });
        await d.sleep(2000);
        ctx.flow('tap-id', { ID: 'water-view-map' });
        await ctx.waitText(/Upstream|Downstream|Lowest part|Highest part/, 30_000);
        await d.sleep(4000);
        const bar = async () => { const b = find(await look(d), 'resource-id="water-map-bar"'); return b && shown(b) ? b.box.y1 - b.box.y0 : null; };
        const h0 = await bar();
        shot('T731-1-start');
        const dirs = find(await look(d), 'resource-id="water-map-step-down"') ? ['water-map-step-down', 'water-map-step-up'] : ['water-map-step-up', 'water-map-step-down'];
        const heights = [h0];
        for (const id of dirs) {
          await sheet(`T731-${id.endsWith('down') ? 'down' : 'up'}`, async () => { ctx.flow('tap-id', { ID: id }); await d.sleep(2500); });
          await d.sleep(2500);
          heights.push(await bar());
          shot(`T731-${id.endsWith('down') ? '2-after-down' : '3-after-up'}`);
        }
        const said = `bar heights (px): ${heights.join(' -> ')}`;
        if (heights.some((h) => h == null)) throw new Error(`the bar was not found at every moment: ${said}`);
        return said;
      });
    }
    night(false);
  },
};
