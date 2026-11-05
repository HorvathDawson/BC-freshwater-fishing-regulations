// (T411) A PART REACHED BY STEPS DRAWS ON ONE LINE. Owner 2026-11-05 on 0.6.0: Coquitlam River → Map → Downstream to
// the lowest part (Part 1 of 3) → Upstream once (Part 2 of 3): the pink casing and the yellow line ran on two paths
// along the whole part. T399 had checked Part 1 opened directly, never a part reached by steps.
//
// The measurement is the map's own (test-bed build, `camProbe.copy`): after each full render, `[copydraw]` lists the
// water's sections the water copy AND the atlas both draw. Two lines for one section is the zigzag, so the last step
// FAILS when Part 2's settled screen has any (or when no render was reported at all). `[copy]` (each kept section's
// `copy_until` and the tile zooms its pieces were cut at), `[move]` and `[cam]` (the camera) go beside the report in
// T411-copy-log.txt, the whole way in.
//
//   node app/tools/testbed/run.mjs part-steps        (STEPS_Q / STEPS_ROW / STEPS_TITLE pick another water)
import fs from 'node:fs';
import path from 'node:path';
import { coldStart, closeWaters, toMap } from './common.mjs';
import { settledTarget } from './regs-open.mjs';

const Q = process.env.STEPS_Q ?? 'Coquitlam River';
const ROW = process.env.STEPS_ROW ?? '.* · Region 2 · MU 2-8.*';
const TITLE = new RegExp(process.env.STEPS_TITLE ?? '^Coquitlam River');
/** `open` (default): the search result's Open; `tap`: Back to the map framed on it, the river tapped (water-nav's way) */
const VIA = process.env.STEPS_VIA ?? 'open';
const TAGS = /\[(cam|move|copy|copydraw)\] /;

export const partSteps = {
  id: 'part-steps',
  title: `${Q} (${VIA}): a part reached by ▼ to the lowest then ▲ once draws its casing and line on one path (T411)`,
  async run(ctx) {
    // Maestro clears logcat when a flow starts: the JS log is gathered before and after every flow
    const log = [];
    const gather = () => {
      const seen = new Set(log);
      for (const l of ctx.device.logcatJs().split('\n')) if (TAGS.test(l) && !seen.has(l)) log.push(l);
    };
    const flow = (name, env) => { gather(); ctx.flow(name, env); gather(); };
    const parsed = (kind) => { gather(); return log.flatMap((l) => {
      const m = new RegExp(`\\[${kind}\\] (\\{.*\\})`).exec(l);
      try { return m ? [JSON.parse(m[1])] : []; } catch { return []; }
    }); };
    const part = () => ctx.device.screenText().find((w) => /part \d+ of \d+/i.test(w)) ?? '?';
    const shot = (name) => ctx.device.screenshot(path.join(ctx.dir, name));

    await ctx.step('a cold start', () => coldStart(ctx), { stop: true });
    await ctx.step(`${Q}: search → Open → Map`, async () => {
      await closeWaters(ctx);
      await toMap(ctx);
      flow('search', { Q });
      flow('open-result', { LINE: ROW });
      if (VIA === 'tap') {
        await ctx.device.sleep(3500);
        ctx.device.sh('input keyevent 4');
        const at = await settledTarget(ctx);
        if (!at) throw new Error('the app named no tap target for the water (no [target] line)');
        gather();
        ctx.device.sh(`input tap ${at.x} ${at.y}`);
      }
      const end = Date.now() + 30_000;
      while (Date.now() < end) {
        const x = ctx.device.sh('uiautomator dump /sdcard/tb-ps.xml >/dev/null 2>&1; cat /sdcard/tb-ps.xml');
        if (x.includes('water-view-map') && ctx.device.screenText().some((w) => TITLE.test(w))) break;
        await ctx.device.sleep(1500);
      }
      flow('tap-id', { ID: 'water-view-map' });
      await ctx.waitText(/Show all \d+ parts|Upstream|Downstream/, 30_000);
      await ctx.device.sleep(4000);
      gather();
      shot('T411-open.png');
      return part();
    }, { stop: true });
    await ctx.step('▼ until the lowest part', async () => {
      const seen = [];
      for (let i = 0; i < 6 && !/part 1 of/i.test(seen.at(-1) ?? ''); i++) {
        flow('tap-id', { ID: 'water-map-step-down' });
        await ctx.device.sleep(4000);
        gather();
        seen.push(part());
      }
      if (!/part 1 of/i.test(seen.at(-1) ?? '')) throw new Error(`never reached Part 1 (${seen.join(' → ')})`);
      shot('T411-part1.png');
      return seen.join(' → ');
    }, { stop: true });
    await ctx.step('▲ once: Part 2 draws on ONE line (no section drawn by the copy and the atlas at once)', async () => {
      const before = parsed('copydraw').length;
      flow('tap-id', { ID: 'water-map-step-up' });
      await ctx.device.sleep(7000);                       // the fit, the copy's pass, the last render
      const now = part();
      shot('T411-part2.png');
      gather();
      fs.writeFileSync(path.join(ctx.dir, 'T411-copy-log.txt'), log.map((l) => l.replace(/^.*?(\[\w+\] )/, '$1')).join('\n') + '\n');
      if (!/part 2 of/i.test(now)) throw new Error(`▲ showed "${now}", not Part 2`);
      const draws = parsed('copydraw').slice(before), cams = parsed('cam'), kept = parsed('copy').at(-1)?.kept ?? [];
      const last = draws.at(-1);
      if (!last) throw new Error(`${now}: no render reported after the step (no [copydraw] line)`);
      const until = (id) => kept.find((k) => k.id === id)?.until ?? '?';
      const said = `${now}; camera z${Number(cams.at(-1)?.zoom ?? last.zoom).toFixed(2)}; the copy alone draws ${last.copyOnly.length}`;
      if (last.both.length)
        throw new Error(`${said}; copy AND atlas draw ${last.both.map((id) => `${id} (copy_until ${until(id)})`).join(', ')}: the zigzag`);
      return `${said}; none drawn twice`;
    });
  },
};
