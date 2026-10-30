// (T300 item 5) THE SMALL MAPS TAKE A PAN AND A PINCH: on a water's Map view and on its Conditions route map, a
// one-finger drag moves the map (the page around it does not scroll instead) and a two-finger pinch zooms it. The
// drag is `input swipe`; the pinch is two touch slots written to the emulator's touchscreen (`sendevent`, multi-touch
// protocol B) in one shell call. Judged on the pixels inside the map's own box, before and after.
//
//   node app/tools/testbed/run.mjs gestures       (GESTURE_WATER="Vedder River")
import path from 'node:path';
import { coldStart, toMap } from './common.mjs';
import { changed, readPng } from '../lib/png.mjs';

const WATER = process.env.GESTURE_WATER ?? 'Vedder River';

/** The touchscreen's event node and its axis ranges (`getevent -pl`). */
function touchscreen(ctx) {
  const out = ctx.device.sh('getevent -pl');
  for (const blk of out.split(/\nadd device \d+: /)) {
    const dev = /^(?:add device \d+: )?(\/dev\/input\/event\d+)/.exec(blk)?.[1];
    const mx = /ABS_MT_POSITION_X\s*:.*?max (\d+)/.exec(blk), my = /ABS_MT_POSITION_Y\s*:.*?max (\d+)/.exec(blk);
    if (dev && mx && my) return { dev, maxX: Number(mx[1]), maxY: Number(my[1]), pressure: /ABS_MT_PRESSURE/.test(blk),
                                  slot: /ABS_MT_SLOT/.test(blk) };
  }
  return null;
}

/** Two fingers from `d0` apart to `d1` apart (px, along x) about (cx, cy), in `steps` frames. */
function pinch(ctx, ts, size, cx, cy, d0, d1, steps = 12) {
  const sx = ts.maxX / size.w, sy = ts.maxY / size.h, D = ts.dev;
  const ev = (t, c, v) => `sendevent ${D} ${t} ${c} ${v}`;
  const lines = [];
  for (let i = 0; i <= steps; i++) {
    const d = d0 + ((d1 - d0) * i) / steps;
    const pts = [[cx - d / 2, cy], [cx + d / 2, cy]];
    pts.forEach(([x, y], k) => {
      lines.push(ev(3, 47, k));                                              // ABS_MT_SLOT
      if (i === 0) lines.push(ev(3, 57, 100 + k));                           // ABS_MT_TRACKING_ID
      lines.push(ev(3, 53, Math.round(x * sx)), ev(3, 54, Math.round(y * sy)));
      if (ts.pressure) lines.push(ev(3, 58, 50));
    });
    if (i === 0) lines.push(ev(1, 330, 1));                                  // BTN_TOUCH down
    lines.push(ev(0, 0, 0));                                                 // SYN_REPORT
  }
  lines.push(ev(3, 47, 0), ev(3, 57, -1), ev(3, 47, 1), ev(3, 57, -1), ev(1, 330, 0), ev(0, 0, 0));
  ctx.device.sh(lines.join('; '));
}

/** Every MapLibre view on screen (`Showing a Map…`), its box in px. */
function mapBoxes(ctx) {
  ctx.device.sh('uiautomator dump /sdcard/tb-g.xml');
  const x = ctx.device.sh('cat /sdcard/tb-g.xml');
  return [...x.matchAll(/content-desc="Showing a Map[^"]*"[^>]*bounds="\[(\d+),(\d+)\]\[(\d+),(\d+)\]"/g)]
    .map((m) => ({ x: +m[1], y: +m[2], w: +m[3] - +m[1], h: +m[4] - +m[2] }));
}

/** The small map: the map box that is not the full-screen one (smallest area on screen). */
function smallMap(ctx, size) {
  const boxes = mapBoxes(ctx).filter((b) => b.w > 100 && b.h > 100 && b.y < size.h);
  boxes.sort((a, b) => a.w * a.h - b.w * b.h);
  return boxes.find((b) => b.w * b.h < size.w * size.h * 0.8) ?? boxes[0] ?? null;
}

async function drive(ctx, label, size, ts) {
  const shot = (n) => readPng(ctx.device.screenshot(path.join(ctx.dir, `T300-${label}-${n}.png`)));
  const box = smallMap(ctx, size);
  if (!box) throw new Error('no map on screen');
  const inner = { x: box.x + 20, y: box.y + 20, w: box.w - 40, h: box.h - 40 };
  const cx = Math.round(box.x + box.w / 2), cy = Math.round(box.y + box.h / 2);
  const a = shot('a');
  // one finger, mostly sideways with some vertical travel (a vertical drag is what a page scroll would take)
  ctx.device.sh(`input swipe ${cx - Math.round(box.w * 0.25)} ${cy + Math.round(box.h * 0.15)} ${cx + Math.round(box.w * 0.25)} ${cy - Math.round(box.h * 0.15)} 500`);
  await ctx.device.sleep(1800);
  const b = shot('pan');
  const after = smallMap(ctx, size);
  const panMoved = changed(a, b, inner), boxMoved = after ? Math.abs(after.y - box.y) : -1;
  const out = [`box ${box.w}x${box.h}@${box.x},${box.y}`, `pan: map ${(panMoved * 100).toFixed(0)} % changed, box moved ${boxMoved} px`];
  let fail = null;
  if (panMoved < 0.05) fail = 'a one-finger drag did not move the map';
  else if (boxMoved !== 0) fail = `the page moved under the drag (${boxMoved} px)`;
  if (ts) {
    const d0 = Math.round(Math.min(box.w, box.h) * 0.2), d1 = Math.round(Math.min(box.w, box.h) * 0.7);
    pinch(ctx, ts, size, cx, cy, d0, d1);
    await ctx.device.sleep(2000);
    const c = shot('pinch');
    const pinchMoved = changed(b, c, inner), box2 = smallMap(ctx, size);
    out.push(`pinch: map ${(pinchMoved * 100).toFixed(0)} % changed, box moved ${box2 ? Math.abs(box2.y - box.y) : -1} px`);
    if (!fail && pinchMoved < 0.05) fail = 'a two-finger pinch did not zoom the map';
  } else out.push('pinch: no multi-touch screen found (getevent), not driven');
  if (fail) throw new Error(`${fail} — ${out.join(' · ')}`);
  return out.join(' · ');
}

export const gestures = {
  id: 'gestures',
  title: `T300 item 5: pan + pinch on the water Map view and the Conditions route map (${WATER})`,
  async run(ctx) {
    let size = null, ts = null;
    await ctx.step(`${WATER} opened from search`, async () => {
      await ctx.device.net('wifi'); ctx.device.link('full', 'none');
      await coldStart(ctx, { ms: 90_000 });
      await toMap(ctx);
      const m = /(\d+)x(\d+)/.exec(ctx.device.sh('wm size'));
      size = { w: Number(m?.[1] ?? 1080), h: Number(m?.[2] ?? 2400) };
      ts = touchscreen(ctx);
      ctx.flow('search', { Q: WATER });
      ctx.flow('open-result', { LINE: WATER });
      await ctx.device.sleep(4000);
      const w = ctx.device.screenText();
      if (!w.some((x) => x === 'Map') || !w.some((x) => /^Regulations$/.test(x))) throw new Error(`no water views (saw: ${w.slice(0, 10).join(' | ').slice(0, 300)})`);
      return ts ? `touchscreen ${ts.dev} (${ts.maxX}x${ts.maxY}${ts.pressure ? ', pressure' : ''})` : 'no touchscreen found';
    });
    await ctx.step('the water Map view: pan + pinch', async () => {
      ctx.flow('tap-text', { TEXT: 'Map' });
      await ctx.device.sleep(5000);
      // the water's own map is the big one here; judge the topmost map box below the header
      return drive(ctx, 'mapview', size, ts);
    });
    await ctx.step('the Conditions route map: pan + pinch', async () => {
      ctx.flow('tap-text', { TEXT: 'Conditions' });
      await ctx.device.sleep(8000);                                           // the routes in before the map mounts
      return drive(ctx, 'route', size, ts);
    });
    await ctx.step('back to the map', async () => { await toMap(ctx); });
  },
};
