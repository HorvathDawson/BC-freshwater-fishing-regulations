// (T443, T459) A WATER'S MAP VIEW, STEPPED — measured and pictured for the owner's phone check:
//   T443  the first open's camera (A) against the same part reached by ▲ then ▼ (B): centre within 1 dp, zoom within
//         0.01 (`[cam]`, the camera probe); the Fraser's Landstrom Bar sign zone (one label for its two marks); the step
//         bar at 360 dp (`wm density 480`) on the part with the release's longest pair of names (Puntledge River).
//   T459  every other water grey, the receiving water kept or grey (the test-bed link `testbed/flag?name=receiving`),
//         light and dark (`cmd uimode night`): Fraser parts, Chilliwack → Vedder, a small creek into the Fraser.
// Shots in out/t443/ and out/t459/ (beside the run's own report).
//
//   node app/tools/testbed/run.mjs step-shots
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { coldStart, closeWaters, toMap } from './common.mjs';

const OUT = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'out');
const TAGS = /\[(cam|move|flag)\] /;

export const stepShots = {
  id: 'step-shots',
  title: 'A water\'s map stepped: first open == ▲ then ▼ (T443), Landstrom Bar, the step bar at 360 dp; other waters grey (T459)',
  async run(ctx) {
    const d = ctx.device;
    const log = [];
    const gather = () => {
      const seen = new Set(log);
      for (const l of d.logcatJs().split('\n')) if (TAGS.test(l) && !seen.has(l)) log.push(l);
    };
    const flow = (name, env) => { gather(); ctx.flow(name, env); gather(); };
    const cams = () => { gather(); return log.flatMap((l) => { const m = /\[cam\] (\{.*\})/.exec(l); try { return m ? [JSON.parse(m[1])] : []; } catch { return []; } }); };
    const part = () => d.screenText().find((w) => /part \d+ of \d+/i.test(w)) ?? '?';
    const dump = () => d.sh('uiautomator dump /sdcard/tb-ss.xml >/dev/null 2>&1; cat /sdcard/tb-ss.xml');
    const shot = (dir, name) => { fs.mkdirSync(path.join(OUT, dir), { recursive: true }); d.screenshot(path.join(OUT, dir, name)); };
    const link = (u) => d.sh(`am start -a android.intent.action.VIEW -d '${u}' ${d.APP_ID} >/dev/null`);
    const night = (on) => d.sh(`cmd uimode night ${on ? 'yes' : 'no'}`);

    /** search → Open → the water's Map tab, settled */
    const openMap = async (q, row) => {
      await closeWaters(ctx);
      await toMap(ctx);
      flow('search', { Q: q });
      flow('open-result', { LINE: row });
      const end = Date.now() + 30_000;
      while (Date.now() < end && !dump().includes('water-view-map')) await d.sleep(1500);
      flow('tap-id', { ID: 'water-view-map' });
      await ctx.waitText(/Upstream|Downstream|Lowest part|Highest part/, 30_000);
      await d.sleep(3000);
      // a search result opens the Map on the WHOLE water (T203); the part's own first open is the Map tab entered from
      // another view (the view switch drops ⤢, and the map view mounts anew)
      flow('tap-id', { ID: 'water-view-regs' });
      await d.sleep(2500);
      flow('tap-id', { ID: 'water-view-map' });
      await ctx.waitText(/Upstream|Downstream|Lowest part|Highest part/, 30_000);
      await d.sleep(5000);
      gather();
    };
    const lastCam = () => cams().at(-1) ?? null;
    const ONLY = (process.env.STEP_SHOTS ?? 'framing,landstrom,bar,grey').split(',');
    /** two cameras apart, in dp at the first's zoom, and in zoom */
    const apart = (a, b) => {
      const k = (512 * 2 ** a.zoom) / 360;
      const my = (lat) => (180 / Math.PI) * Math.log(Math.tan(Math.PI / 4 + (lat * Math.PI) / 360));
      return { dp: Math.hypot((b.lon - a.lon) * k, (my(b.lat) - my(a.lat)) * k), dz: Math.abs(b.zoom - a.zoom) };
    };

    await ctx.step('a cold start, light', async () => { night(false); await coldStart(ctx); }, { stop: true });

    const framing = {};
    if (ONLY.includes('framing')) for (const [name, q, row] of [['fraser', 'Fraser River', 'Stream · .*'], ['coquitlam', 'Coquitlam River', '.* · Region 2 · MU 2-8.*']]) {
      await ctx.step(`T443 ${q}: first open (A) against ▲ then ▼ (B)`, async () => {
        await openMap(q, row);
        const A = lastCam(), where = part();
        shot('t443', `${name}-A-first-open.png`);
        const up = /Highest part/.test(d.screenText().join(' ')) ? ['water-map-step-down', 'water-map-step-up'] : ['water-map-step-up', 'water-map-step-down'];
        for (const id of up) { flow('tap-id', { ID: id }); await d.sleep(5000); }
        const B = lastCam();
        shot('t443', `${name}-B-step-there-and-back.png`);
        if (!A || !B) throw new Error('no [cam] line (camera probe off?)');
        const x = apart(A, B);
        framing[name] = { part: where, A, B, ...x };
        const said = `${where}: A z${A.zoom.toFixed(3)}, B z${B.zoom.toFixed(3)}; centre ${x.dp.toFixed(2)} dp apart, zoom ${x.dz.toFixed(3)}`;
        if (x.dp > 1 || x.dz > 0.01) throw new Error(`${said} — A != B`);
        return said;
      });
    }
    fs.mkdirSync(path.join(OUT, 't443'), { recursive: true });
    if (ONLY.includes('framing')) fs.writeFileSync(path.join(OUT, 't443', 'framing.json'), `${JSON.stringify(framing, null, 1)}\n`);

    if (ONLY.includes('landstrom')) await ctx.step('T443 the Fraser at Landstrom Bar: the sign zone\'s two marks under ONE label', async () => {
      await openMap('Fraser River', 'Stream · .*');
      flow('tap-id', { ID: 'water-map-parts-toggle' });
      await d.sleep(1500);
      flow('tap-text', { TEXT: '.*Landstrom Bar.*' });
      await d.sleep(6000);
      shot('t443', 'fraser-landstrom-bar.png');
      return part();
    });

    if (ONLY.includes('bar')) await ctx.step('T443 the step bar at 360 dp: Puntledge River, the release\'s longest pair of boundary names', async () => {
      d.sh('wm density 480');
      try {
        await d.sleep(3000);
        await openMap('Puntledge River', 'Stream · .*');
        flow('tap-id', { ID: 'water-map-parts-toggle' });
        await d.sleep(1500);
        flow('tap-text', { TEXT: '.*hatchery fence.*' });
        await d.sleep(5000);
        shot('t443', 'step-bar-360dp-puntledge.png');
        return part();
      } finally { d.sh('wm density reset'); await d.sleep(3000); }
    });

    // each water at the part that shows the most: the mouth part (its receiving water in view), or a named part
    const waters = [
      ['fraser-hope', 'Fraser River', 'Stream · .*', { row: '.*Landstrom Bar.*', tidal: true }],
      ['chilliwack-mouth', 'Chilliwack River', 'Stream · .*', { lowest: true }],
      ['silverhope-creek-mouth', 'Silverhope Creek', 'Stream · .*', { lowest: true }],
    ];
    const flagLines = () => d.sh("logcat -d -s ReactNativeJS:* | grep '\\[flag\\]' || true").trim().split('\n').filter(Boolean);
    for (const [name, q, row, at] of ONLY.includes('grey') ? waters : []) {
      await ctx.step(`T459 ${q}: other waters grey — receiving kept / grey, light / dark`, async () => {
        night(false);
        await d.sleep(2000);
        await openMap(q, row);
        if (at.row) {
          flow('tap-id', { ID: 'water-map-parts-toggle' }); await d.sleep(1500);
          flow('tap-text', { TEXT: at.row }); await d.sleep(5000);
        }
        if (at.lowest) for (let i = 0; i < 6 && !/Lowest part/.test(d.screenText().join(' ')); i++) {
          flow('tap-id', { ID: 'water-map-step-down' }); await d.sleep(4500);
        }
        const where = part();
        const seen = [];
        for (const dark of [false, true]) {
          night(dark);
          await d.sleep(4000);
          for (const kept of [true, false]) {
            link(`canifishthis://testbed/flag?name=receiving&on=${kept ? 1 : 0}`);
            await d.sleep(3500);
            seen.push(...flagLines());
            shot('t459', `${name}-${dark ? 'dark' : 'light'}-${kept ? 'receiving-kept' : 'all-grey'}.png`);
          }
        }
        night(false);
        link('canifishthis://testbed/flag?name=receiving&on=1');
        const same = fs.readFileSync(path.join(OUT, 't459', `${name}-light-receiving-kept.png`))
          .equals(fs.readFileSync(path.join(OUT, 't459', `${name}-light-all-grey.png`)));
        if (same && !at.tidal) throw new Error(`${where}: the two options drew the same picture (flag lines: ${[...new Set(seen)].slice(-4).join(' | ') || 'none'})`);
        return `${where}; flag lines ${new Set(seen).size}`;
      });
    }
    gather();
    fs.mkdirSync(path.join(OUT, 't459'), { recursive: true });
    fs.writeFileSync(path.join(OUT, 't443', 'step-shots-log.txt'), log.join('\n') + '\n');
  },
};
