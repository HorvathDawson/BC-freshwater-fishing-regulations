// (T242) THE CRASH TEST BED: each kind of app error fired on the phone, and each proved to reach the report path — the
// feeds worker's own `POST /crash` handler, run on THIS machine by `crash-stub/server.mjs` over an in-memory bucket (no
// write to staging or production). Needs the stub APK: `session.sh --crash-stub` (its crash address is the phone's
// localhost:39256, `adb reverse`d here).
//
//   1. an unhandled promise rejection (Updates log → Crash test → Reject a promise): sent at once, non-fatal
//   2. an uncaught JS error (→ Throw a JS error): fatal, the app ends; kept on the device and sent at the next start
//   3. a native crash: SIGSEGV to the app's process (adb root, `kill -11`), the shape of T351's reports; Android's exit
//      record (CRASH_NATIVE, status 11) is read and sent at the next start
// Each passes when the worker handler answered 201 AND the in-memory bucket holds that kind under crashes-test/.
//
//   node app/tools/testbed/run.mjs --no-r2 crash-bed
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { appUp, coldStart, openLog } from './common.mjs';

const HERE = path.dirname(new URL(import.meta.url).pathname);
const PORT = Number(process.env.TESTBED_STUB_PORT ?? 39256);

const posts = (log) => { try { return fs.readFileSync(log, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l)); } catch { return []; } };
async function kinds() { return (await fetch(`http://localhost:${PORT}/__stub/kinds`)).json(); }
async function waitPost(ctx, log, kind, from, ms) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    const hit = posts(log).slice(from).find((p) => p.body?.kind === kind);
    if (hit) return hit;
    await ctx.device.sleep(1000);
  }
  throw new Error(`no ${kind} report reached the stub in ${ms / 1000} s; app said: ${[...ctx.device.logcatJs().matchAll(/\[crash-reports\][^\n]*/g)].map((m) => m[0]).join(' | ') || 'nothing'}`);
}
async function stored(kind, name) {
  const k = (await kinds()).find((x) => x.sample?.kind === kind && (!name || x.sample?.name === name));
  if (!k) throw new Error(`the bucket holds no ${kind} kind`);
  if (!k.key.startsWith('private/crashes-test/')) throw new Error(`${kind} stored at ${k.key}, not under crashes-test/ (the test tag)`);
  return k;
}
const pid = (ctx) => { try { return ctx.device.pidOf(); } catch { return null; } };
async function died(ctx, ms = 20_000) {
  const end = Date.now() + ms;
  while (Date.now() < end && pid(ctx)) await ctx.device.sleep(500);
  if (pid(ctx)) throw new Error('the app did not die');
}
const line = (p) => `${p.status} ${p.body.kind} fatal=${p.body.fatal} ${p.body.name}: ${p.body.message}${p.body.stack ? ` · stack "${String(p.body.stack).split('\n')[0].slice(0, 60)}…"` : ''} · route ${p.body.route}`;

export const crashBed = {
  id: 'crash-bed',
  title: 'Crash test bed (T242): a JS error, an unhandled promise and a native crash each reach the worker handler (local stub)',
  async run(ctx) {
    if (!process.env.TESTBED_STUB) ctx.skip('needs the stub APK: session.sh --crash-stub');
    const log = path.join(ctx.dir, 'T242-stub.jsonl');
    fs.rmSync(log, { force: true });
    const stub = spawn('node', [path.join(HERE, '../crash-stub/server.mjs'), '--port', String(PORT), '--log', log], { stdio: ['ignore', 'pipe', 'pipe'] });
    stub.stdout.on('data', (b) => fs.appendFileSync(path.join(ctx.dir, 'T242-stub.out'), b));
    stub.stderr.on('data', (b) => fs.appendFileSync(path.join(ctx.dir, 'T242-stub.out'), b));
    try {
      await ctx.step('stub up (the worker\'s handler, in memory), app up online', async () => {
        await ctx.device.sleep(1500);
        await kinds();
        await ctx.device.net('wifi'); ctx.device.link('full', 'none');
        await coldStart(ctx, { ms: 90_000 });
        await ctx.device.sleep(4000);            // the start's own flush / exit records (earlier runs) go first
        return `${posts(log).length} report(s) from earlier runs at start`;
      }, { stop: true });

      await ctx.step('unhandled promise rejection → sent at once (non-fatal)', async () => {
        openLog(ctx);
        const from = posts(log).length;
        ctx.device.logcatClear();
        ctx.flow('updates-log-crash', { KIND: 'promise' });
        const p = await waitPost(ctx, log, 'promise', from, 30_000);
        if (p.status !== 201) throw new Error(`worker answered ${line(p)} ${p.answer}`);
        if (!pid(ctx)) throw new Error('the app ended on a rejection');
        await stored('promise');
        return line(p);
      });

      await ctx.step('uncaught JS error → the app ends; kept, sent at the next start (fatal)', async () => {
        openLog(ctx);
        const from = posts(log).length;
        ctx.flow('updates-log-crash', { KIND: 'js' });
        await died(ctx);
        ctx.device.adb(['logcat', '-b', 'crash', '-c']);           // this crash is the test, not a finding
        const before = posts(log).slice(from).filter((p) => p.body?.kind === 'js').length;
        ctx.device.logcatClear();
        await appUp(ctx);
        const p = await waitPost(ctx, log, 'js', from, 60_000);
        if (p.status !== 201 || p.body.fatal !== true) throw new Error(`worker answered ${line(p)} ${p.answer}`);
        await stored('js', 'Error');
        // the system's exit record of the same death (a Java CRASH) is skipped: the JS fatal already told it
        await ctx.device.sleep(5000);
        const dup = posts(log).slice(from).filter((p2) => p2.body?.kind === 'native');
        if (dup.length) throw new Error(`the same death was told twice: ${line(p)}, then the exit record ${dup.map(line).join('; ')}`);
        return `${line(p)}${before ? ` (${before} sent before the app died)` : ' (sent at the next start)'}; exit records sent with it: ${dup.length}`;
      });

      await ctx.step('native crash (SIGSEGV) → Android\'s exit record sent at the next start', async () => {
        const from = posts(log).length;
        ctx.device.adb(['root'], { timeout: 30_000 });
        await ctx.device.sleep(3000);
        ctx.device.adb(['wait-for-device'], { timeout: 60_000 });
        ctx.device.adb(['reverse', `tcp:${PORT}`, `tcp:${PORT}`]);   // adbd restarted as root: its port forwards went with it
        const p0 = pid(ctx);
        if (!p0) throw new Error('the app is not running');
        ctx.device.sh(`kill -11 ${p0}`);
        await died(ctx);
        ctx.device.adb(['logcat', '-b', 'crash', '-c']);
        const exit = ctx.device.sh(`dumpsys activity exit-info ${ctx.device.APP_ID}`).match(/reason=\d+ \((\w+)\)[^\n]*status=(\d+)/);
        ctx.device.logcatClear();
        await appUp(ctx);
        const p = await waitPost(ctx, log, 'native', from, 90_000);
        if (p.status !== 201 || !/CRASH_NATIVE/.test(p.body.message)) throw new Error(`worker answered ${line(p)} ${p.answer}`);
        await stored('native', 'NativeExit');
        return `${line(p)} · system record ${exit ? `${exit[1]} status ${exit[2]}` : '?'}`;
      });

      await ctx.step('the bucket: one kind per error, all under crashes-test/', async () => {
        const ks = await kinds();
        fs.writeFileSync(path.join(ctx.dir, 'T242-kinds.json'), JSON.stringify(ks, null, 1));
        return ks.map((k) => `${k.sample.kind}:${k.sample.name} ×${k.count}`).join(', ');
      }, { shot: false });
    } finally {
      try { ctx.device.adb(['unroot'], { timeout: 30_000 }); await ctx.device.sleep(3000); ctx.device.adb(['wait-for-device'], { timeout: 60_000 });
            ctx.device.adb(['reverse', `tcp:${PORT}`, `tcp:${PORT}`]); } catch { /* was not root */ }
      stub.kill();
    }
  },
};
