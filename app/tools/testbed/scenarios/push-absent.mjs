// (T739, owner 2026-11-16 "we dont want this right now") ANDROID HAS NO PUSH: with notifications on, the phone sends our
// worker no push token and More → Notifications says the periodic check's words, with no "Server wake-ups" line. Run on a
// stub APK (`session.sh --crash-stub`): the app's report address is the local stub (the worker's own /push handler), so a
// register would land there, and nothing reaches staging.
//
//   scripts/heavy.sh testbed 45 -- bash app/tools/testbed/session.sh --build --crash-stub -- node app/tools/testbed/run.mjs push-absent
import { startCrashStub } from '../lib/crash-stub.mjs';
import { coldStart, toMap } from './common.mjs';

const PORT = Number(process.env.TESTBED_STUB_PORT ?? 39256);
/** the Android intro (core NOTIFY_WORDS.intro), the start of it */
const INTRO = /^Notifications are made on this phone\. Nothing is sent to a server: .*about every 15 minutes at best\./;

export const pushAbsent = {
  id: 'push-absent',
  inAll: false,
  title: 'T739: Android registers no push token (notifications on, the stub holds none) and shows no wake-ups line',
  async run(ctx) {
    if (!process.env.TESTBED_STUB) {
      await ctx.step('a stub APK only', async () => ctx.skip('session.sh --crash-stub: a register must land on this machine, never staging'));
      return;
    }
    const stub = await startCrashStub({ dir: ctx.dir, port: PORT, name: 'push-stub' });
    const tokens = async () => (await fetch(`http://localhost:${PORT}/__stub/push`)).json();
    try {
      await ctx.step('app up, More → Notifications', async () => {
        await ctx.device.net('wifi');
        await coldStart(ctx);
        await toMap(ctx);
        ctx.flow('tab', { TAB: 'More' });
        ctx.flow('scroll-to', { TEXT: 'Notifications' });
        ctx.flow('tap-text', { TEXT: 'Notifications' });
        await ctx.until(() => ctx.hasId('notifications-page'), { ms: 10_000, what: 'the Notifications page' });
        return `stub on :${PORT}`;
      });
      await ctx.step('"New regulations data" on: no token at the stub, the periodic intro, no wake-ups line', async () => {
        ctx.flow('scroll-to-id', { ID: 'notify-update-ready' });
        ctx.flow('tap-id', { ID: 'notify-update-ready' });
        for (let i = 0; i < 5; i++) {
          if (ctx.device.screenText().some((x) => /send you notifications/i.test(x))) { ctx.flow('tap-text', { TEXT: 'Allow' }); break; }
          await ctx.device.sleep(1000);
        }
        await ctx.timer(15_000, 'a register is sent at once when there is one (the iPhone run sees it in seconds): 15 s with none is the proof');
        const held = await tokens();
        if (held.length) throw new Error(`the stub holds ${held.length} push token(s): ${JSON.stringify(held).slice(0, 300)}`);
        const pushPosts = stub.posts().filter((p) => /"platform"\s*:/.test(String(p.body ?? '')));
        if (pushPosts.length) throw new Error(`${pushPosts.length} push call(s) reached the stub`);
        const w = ctx.device.screenText();
        if (ctx.hasId('notify-push-status') || w.some((x) => /^Server wake-ups/.test(x))) throw new Error('a "Server wake-ups" line is shown on Android');
        const intro = w.find((x) => INTRO.test(x));
        if (!intro) throw new Error(`the periodic intro is not shown (saw: ${w.slice(0, 12).join(' | ').slice(0, 300)})`);
        return `stub tokens 0, no push call, no wake-ups line; intro: "${intro.slice(0, 120)}…"`;
      });
      await ctx.step('Notifications off again (the switch left as found)', async () => {
        ctx.flow('scroll-to-id', { ID: 'notify-master' });
        ctx.flow('tap-id', { ID: 'notify-master' });
        await ctx.settle({ ms: 2500 });
        return `stub tokens ${(await tokens()).length}`;
      });
    } finally {
      stub.stop();
    }
  },
};
