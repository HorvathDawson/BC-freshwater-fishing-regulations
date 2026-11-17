// (T242) 6. WHY THE LAST RUN ENDED: the app is crashed the way the system does it (`am crash`), then started again; it reads
// Android's ApplicationExitInfo, scrubs it and sends it to the feeds worker (POST /crash) under the Data switch: STAGING on
// a plain APK; on a stub APK (`session.sh --crash-stub`) the worker's own handler on this machine, which this scenario
// starts (`lib/crash-stub.mjs`: session.sh only carries the port, and with nothing listening the app saw "unexpected end of
// stream", 2026-11-16). The app logs "[crash-reports] sent N exit record(s)" (ReactNativeJS); a third start sends none
// (each exit is looked at once). The run token is not used: the worker's answer (201) is what the app's count rests on.
import { startCrashStub } from '../lib/crash-stub.mjs';
import { appUp, coldStart } from './common.mjs';

const SENT = /\[crash-reports\] sent (\d+) exit record[^\n]*/;
const said = (ctx) => [...ctx.device.logcatJs().matchAll(/\[crash-reports\][^\n]*/g)].map((m) => m[0]).join(' | ');
const sentCount = (ctx) => { const m = [...ctx.device.logcatJs().matchAll(new RegExp(SENT, 'g'))].at(-1); return m ? Number(m[1]) : null; };

export const crash = {
  id: 'crash',
  title: 'A crashed run is reported on the next start (Android exit record), once',
  async run(ctx) {
    const stub = process.env.TESTBED_STUB
      ? await startCrashStub({ dir: ctx.dir, port: Number(process.env.TESTBED_STUB_PORT ?? 39256), name: 'crash-stub' })
      : null;
    try {
      await ctx.step('online, app up', async () => { await ctx.device.net('wifi'); await coldStart(ctx); return stub ? `stub on :${process.env.TESTBED_STUB_PORT ?? 39256}` : 'staging worker'; });
      await ctx.step('the app is crashed (am crash)', async () => {
        ctx.device.sh(`am crash ${ctx.device.APP_ID}`);
        const end = Date.now() + 15_000;
        const alive = () => { try { return ctx.device.pidOf(); } catch { return null; } };   // pidof exits 1 when there is none
        while (Date.now() < end && alive()) await ctx.device.sleep(500);
        if (alive()) throw new Error('the app did not die');
        ctx.device.adb(['logcat', '-b', 'crash', '-c']);          // this crash is the test, not a finding
        return 'process gone';
      });
      await ctx.step('next start sends 1 exit record', async () => {
        ctx.device.logcatClear();
        const from = stub ? stub.posts().length : 0;
        await appUp(ctx);
        const end = Date.now() + 90_000;
        let n = null;
        while (Date.now() < end && (n = sentCount(ctx)) === null) await ctx.device.sleep(3000);
        if (n === null) throw new Error('no "[crash-reports] sent" line in 90 s');
        if (n < 1) throw new Error(`the exit record was not accepted by the worker (${said(ctx)}); system record: ${ctx.device.sh('dumpsys activity exit-info ' + ctx.device.APP_ID).replace(/\s+/g, ' ').slice(0, 500)}`);
        if (!stub) return `sent ${n}`;
        // the stub's side: the native exit answered 201 by the worker's own handler
        const hit = stub.posts().slice(from).find((p) => p.body?.kind === 'native' && p.status === 201);
        if (!hit) throw new Error(`the app said sent ${n}, but the stub logged no 201 native report: ${JSON.stringify(stub.posts().slice(from)).slice(0, 400)}`);
        return `sent ${n} · stub 201 ${hit.body.name}: ${hit.body.message}`;
      });
      await ctx.step('a start after a normal stop sends none', async () => {
        ctx.device.logcatClear();
        await coldStart(ctx);
        const end = Date.now() + 60_000;
        let n = null;
        while (Date.now() < end && (n = sentCount(ctx)) === null) await ctx.device.sleep(3000);
        if (n === null) throw new Error('no "[crash-reports] sent" line in 60 s');
        if (n !== 0) throw new Error(`sent ${n} again`);
        return 'sent 0';
      });
    } finally {
      stub?.stop();
    }
  },
};
