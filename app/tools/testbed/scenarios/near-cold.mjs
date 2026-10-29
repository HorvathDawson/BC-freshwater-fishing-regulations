// (T264) THE TOWN TAP FIRST, TEN COLD STARTS: force-stop, launch, search "Cultus", tap the village row — the first
// read of the `near` file after the launch. Alternates offline / online. A run fails when the list says "Could not read
// what is near this town" OR when the app logged a failed near read at all (`near <place>: …`, useTown's console.error),
// so a failure something downstream papered over still counts.
//
//   node app/tools/testbed/run.mjs near-cold            (NEAR_COLD_RUNS=n to change the count; default 10)
import { coldStart } from './common.mjs';

const RUNS = Number(process.env.NEAR_COLD_RUNS ?? 10);
const FAILED = /Could not read what is near this town/;
const LISTED = /within \d+ km|^water near /i;

export const nearCold = {
  id: 'near-cold',
  title: `A town tapped first after a cold start, ${RUNS} times (offline and online)`,
  async run(ctx) {
    for (let i = 1; i <= RUNS; i++) {
      const mode = i % 2 ? 'offline' : 'wifi';
      await ctx.step(`cold start ${i}/${RUNS} (${mode}): town first, the waters near it`, async () => {
        await ctx.device.net(mode);
        ctx.device.logcatClear();
        await coldStart(ctx, { ms: 90_000 });
        ctx.flow('search', { Q: 'Cultus' });
        ctx.flow('tap-text', { TEXT: 'Village · every named water around it' });
        const end = Date.now() + 20_000;
        let w = [];
        while (Date.now() < end) {
          w = ctx.device.screenText();
          if (w.some((x) => FAILED.test(x) || LISTED.test(x))) break;
          await ctx.device.sleep(1000);
        }
        const js = ctx.device.logcatJs().split('\n').filter((l) => /\[T264\]|near \d+:|Error/.test(l))
          .map((l) => l.replace(/^.*ReactNativeJS:\s*/, '').slice(0, 400));
        const logged = js.filter((l) => /^near \d+:|FAILED|failed/.test(l));
        if (w.some((x) => FAILED.test(x)) || logged.length) {
          ctx.finding(`cold start ${i} (${mode}): ${logged.join(' / ').slice(0, 600) || 'said "Could not read…" with no log'}`);
          throw new Error(`near read failed: ${(logged[0] ?? 'Could not read what is near this town').slice(0, 300)}`);
        }
        const listed = w.find((x) => LISTED.test(x));
        if (!listed) throw new Error(`no list in 20 s (saw: ${w.slice(0, 10).join(' | ').slice(0, 300)})`);
        const timing = js.filter((l) => /water\(\) ok|watersNear ok/.test(l)).join(' · ');
        return `${listed}${timing ? ` — ${timing}` : ''}`;
      });
    }
  },
};
