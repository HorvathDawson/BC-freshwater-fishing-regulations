// (T739, T822) iPHONE PUSH ON THE SIMULATOR: register and unregister against the local crash stub (the feeds worker's own
// push handler over a memory bucket, crash-stub/server.mjs), and the More → Notifications intro + "Server wake-ups" line
// in light and dark. The build must point its report address at the stub (the app derives /push from it):
//
//   EXPO_PUBLIC_REPORT_URL=http://127.0.0.1:<port>/report deploy/staging/build-ios-sim.sh --testbed
//   node app/tools/testbed/crash-stub/server.mjs --port <port> &
//   PUSH_STUB=http://127.0.0.1:<port> node app/tools/testbed/run-ios.mjs push --out <dir>
//
// Pass: turning "New regulations data" on stores exactly one {platform: "ios", env: "sandbox", token: hex} at the stub and
// the line says "registered"; turning Notifications off empties the stub and the line says so. Shots `T739-*.png` and the
// screen's words beside each (`.txt`) to `--out`.
import fs from 'node:fs';
import path from 'node:path';
import { menuPage } from './keyboard.mjs';

const STUB = process.env.PUSH_STUB ?? '';
async function stubTokens() {
  const r = await fetch(`${STUB}/__stub/push`);
  if (!r.ok) throw new Error(`stub ${STUB}/__stub/push: HTTP ${r.status}`);
  return r.json();
}

export const pushIos = {
  id: 'push',
  inAll: false,
  title: 'T739 T822: the push token registers and unregisters (local stub); the Notifications intro and wake-ups line, light and dark',
  async run(ctx) {
    if (!STUB) throw new Error('PUSH_STUB is not set (the crash stub\'s address, e.g. http://127.0.0.1:39261)');
    const { coldStart } = ctx.ios;
    const d = ctx.device, sleep = d.sleep;
    const save = (name) => {
      d.screenshot(path.join(ctx.dir, `T739-${name}.png`));
      const w = d.screenText();
      fs.writeFileSync(path.join(ctx.dir, `T739-${name}.txt`), [...w, '--- ids', ...(w.ids ?? [])].join('\n'));
      return w;
    };
    /** the system's notification prompt, answered as a reader would (it is SpringBoard's, not the app's) */
    const allow = async () => {
      for (let i = 0; i < 6; i++) {
        if (d.screenText().some((x) => /Would Like to Send You Notifications/i.test(x))) { ctx.flow('ios-alert', { BUTTON: 'Allow' }); return 'allowed'; }
        await sleep(1000);
      }
      return 'no prompt (already answered)';
    };
    const line = (w) => w.find((x) => /^Server wake-ups/.test(x)) ?? '(no line)';
    try {
      await ctx.step('light: cold start, More → Notifications, before', async () => {
        d.appearance('light');
        await coldStart(ctx, { ms: 600_000 });
        await sleep(2000);
        await menuPage(ctx, 'notifications');
        const before = await stubTokens();
        if (before.length) throw new Error(`the stub already holds ${before.length} token(s)`);
        const w = save('0-off-light');
        return `stub tokens 0; words: ${w.slice(0, 10).join(' | ').slice(0, 300)}`;
      });
      await ctx.step('register: "New regulations data" on → one iOS token at the stub, the line says registered', async () => {
        ctx.flow('scroll-to-id', { ID: 'notify-update-ready' });
        ctx.flow('tap-id', { ID: 'notify-update-ready' });
        const said = await allow();
        let toks = [];
        await ctx.until(async () => { toks = await stubTokens(); return toks.length > 0; }, { ms: 60_000, what: 'a token at the stub' });
        if (toks.length !== 1) throw new Error(`${toks.length} tokens at the stub, want 1`);
        const t = toks[0];
        if (t.platform !== 'ios' || t.env !== 'sandbox' || !/^[0-9a-f]{64,200}$/.test(t.token)) throw new Error(`stored ${JSON.stringify(t)}`);
        if (Object.keys(t).sort().join(',') !== 'at,env,format,key,platform,token') throw new Error(`stored fields ${Object.keys(t)}`);
        ctx.flow('scroll-to-id', { ID: 'notify-push-status' });
        await ctx.until(() => /registered/.test(line(d.screenText())), { ms: 30_000, what: 'the line says registered' });
        const w = save('1-on-light');
        if (!w.some((x) => /our server sends this phone a silent signal to check now/.test(x))) throw new Error('the push intro is not shown');
        return `${said}; stub: {platform: ios, env: sandbox, token: ${t.token.length} hex}; line: "${line(w)}"`;
      });
      await ctx.step('dark: the same page', async () => {
        d.appearance('dark');
        await sleep(2500);
        const w = save('2-on-dark');
        return `line: "${line(w)}"`;
      });
      await ctx.step('unregister: Notifications off → the stub holds no token, the line says so', async () => {
        ctx.flow('scroll-to-id', { ID: 'notify-master' });
        ctx.flow('tap-id', { ID: 'notify-master' });
        await ctx.until(async () => (await stubTokens()).length === 0, { ms: 60_000, what: 'the stub emptied' });
        await sleep(1500);
        const dark = save('3-off-dark');
        d.appearance('light');
        await sleep(2500);
        const light = save('4-off-light');
        return `stub tokens 0; line (dark): "${line(dark)}"; line (light): "${line(light)}"`;
      });
    } finally {
      d.appearance('light');
    }
  },
};
