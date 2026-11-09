// (T360) AN APPLIED UPDATE WITH A WATER PAGE OPEN: the replaced set is retired (its SQLite connections closed, a late read
// still answered), and the reader sees no error card and the app reports no failure. A new release on the test channel
// (same files, a new id: `test-updates.mjs release`) is checked for and staged, Cultus Lake's page is opened, the app goes
// to the background and back (the apply at the next pause), then the page, its Map view and the Map tab are read for any
// failure words, and the crash stub (the worker's own handler, local: `session.sh --crash-stub`) for any report.
//
//   node app/tools/testbed/run.mjs retire-swap        (stub APK; writes the STAGING test/ channel, reset at the end)
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { coldStart, openLog, toMap } from './common.mjs';

const HERE = path.dirname(new URL(import.meta.url).pathname);
const PORT = Number(process.env.TESTBED_STUB_PORT ?? 39256);
const BAD = /could ?n.t|could not|went wrong|failed|error|refused|was replaced/i;
const posts = (log) => { try { return fs.readFileSync(log, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l)); } catch { return []; } };

/** The screen's words, none of them a failure. */
function clean(ctx, what) {
  const w = ctx.device.screenText();
  const bad = w.filter((x) => BAD.test(x));
  if (bad.length) throw new Error(`${what}: ${bad.join(' | ')}`);
  return w;
}

export const retireSwap = {
  id: 'retire-swap',
  title: 'An update applied with a water page open: no error card, no failure report (T360 retire)',
  async run(ctx) {
    if (!process.env.TESTBED_STUB) ctx.skip('needs the stub APK: session.sh --crash-stub');
    const log = path.join(ctx.dir, 'T360-stub.jsonl');
    fs.rmSync(log, { force: true });
    const stub = spawn('node', [path.join(HERE, '../crash-stub/server.mjs'), '--port', String(PORT), '--log', log], { stdio: 'ignore' });
    let id = null;
    try {
      await ctx.step('the map up (Wi-Fi), the test channel = live staging', async () => {
        ctx.updates('reset');
        await ctx.device.net('wifi'); ctx.device.link('full', 'none');
        await coldStart(ctx, { ms: 600_000 });
      }, { stop: true });
      await ctx.step('a new release (same files, new id) published; Check now; staged', async () => {
        id = ctx.updates('release', `r${Date.now().toString(36).slice(-5)}`).release?.data_release ?? null;
        openLog(ctx);
        ctx.flow('updates-log-action', { ACTION: 'check' });
        await ctx.waitText(/Release, (staged|applied)/, 240_000);
        return `new release ${id}`;
      }, { stop: true });
      await ctx.step("Cultus Lake's page open (Regulations)", async () => {
        await toMap(ctx);
        ctx.flow('search', { Q: 'Cultus' });
        ctx.flow('open-result', { LINE: 'Lake · .*Lindell Beach.*' });
        await ctx.waitText(/Regulations/, 30_000);
        ctx.flow('tap-text', { TEXT: 'Regulations' });
        await ctx.device.sleep(3000);
        clean(ctx, 'before the apply');
        ctx.device.logcatClear();
      }, { stop: true });
      await ctx.step('applied at the next pause (background, back): the page as it was, no error', async () => {
        ctx.device.background(); await ctx.device.sleep(3000); ctx.device.launch();
        await ctx.device.sleep(12_000);
        const w = clean(ctx, 'after the apply');
        if (!w.some((x) => /Cultus Lake/.test(x))) throw new Error(`not on Cultus Lake's page: ${w.slice(0, 10).join(' | ')}`);
        ctx.device.screenshot(path.join(ctx.dir, 'T360-after-apply.png'));
        return w.slice(0, 6).join(' | ');
      });
      await ctx.step("the water's Map view, then Regulations again, then the Map tab: no error", async () => {
        ctx.flow('tap-text', { TEXT: 'Map' }); await ctx.device.sleep(4000); clean(ctx, 'water Map view');
        ctx.flow('tap-text', { TEXT: 'Regulations' }); await ctx.device.sleep(3000); clean(ctx, 'Regulations again');
        await toMap(ctx); await ctx.device.sleep(4000); clean(ctx, 'the Map tab');
      });
      await ctx.step('the new release is the one in use; nothing reported; no retire line in the log', async () => {
        const js = ctx.device.logcatJs();
        fs.writeFileSync(path.join(ctx.dir, 'T360-js.txt'), js);
        openLog(ctx);
        const head = ctx.device.screenText().find((x) => /release r-/.test(x)) ?? '';
        if (id && !head.includes(id)) throw new Error(`not applied (${head})`);
        await ctx.device.sleep(5000);                               // a report in flight lands
        const sent = posts(log);
        const lines = js.split('\n').filter((l) => /ReleaseError|was replaced|close failed|Unhandled promise/.test(l));
        if (sent.length || lines.length)
          throw new Error(`reported ${sent.length}: ${sent.map((p) => `${p.body?.name}: ${p.body?.message}`).join(' | ')} · log: ${lines.slice(0, 3).join(' | ')}`);
        return `${head} · 0 reports · 0 failure lines`;
      });
    } finally {
      try { ctx.updates('reset'); } catch (e) { console.log(`[retire-swap] reset failed: ${e.message}`); }
      stub.kill();
    }
  },
};
