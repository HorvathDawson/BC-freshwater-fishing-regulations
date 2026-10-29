// (T255) 5. REPORT A PROBLEM, OFFLINE (T237): sent with no network → "saved on this device"; the app is closed and
// reopened offline (the outbox is on disk); back online → the update service flushes it, and the STAGING feeds worker
// holds it (GET /reports with the run token: read only). The report's text carries a run marker so it is found exactly.
import fs from 'node:fs';
import path from 'node:path';
import { appUp, coldStart, toMap } from './common.mjs';

const WORKER = 'https://bc-fishing-feeds-staging.horvath-dawson.workers.dev';

async function stored(marker) {
  const tokFile = path.resolve(path.dirname(new URL(import.meta.url).pathname), '../../../workers/feeds/.run-token');
  const tok = (process.env.FEEDS_RUN_TOKEN ?? fs.readFileSync(tokFile, 'utf8')).trim();
  const since = new Date(Date.now() - 86_400_000).toISOString().slice(0, 10);
  const r = await fetch(`${WORKER}/reports?since=${since}`, { headers: { authorization: `Bearer ${tok}` } });
  if (!r.ok) throw new Error(`GET /reports: HTTP ${r.status}`);
  const all = await r.json();
  const list = Array.isArray(all) ? all : all.reports ?? [];
  return list.find((x) => JSON.stringify(x).includes(marker)) ?? null;
}

export const report = {
  id: 'report',
  title: 'Report a problem offline → queued → sent on reconnect',
  async run(ctx) {
    const marker = `TESTBED ${ctx.rec.id}-${Date.now().toString(36)}`;
    await ctx.step('offline, app up', async () => {
      await ctx.device.net('wifi');
      await appUp(ctx);
      await ctx.device.net('offline');
    });
    await ctx.step('send a report offline → "saved on this device"', async () => {
      ctx.flow('report-send', { WHAT: `${marker} - automated test bed report, please ignore` });
      return ctx.waitText(/saved on this device and will be sent/, 20_000);
    });
    await ctx.step('not on the server yet', async () => {
      if (await stored(marker)) throw new Error('the report reached the server while the device was offline');
      return 'absent, as it should be';
    });
    await ctx.step('closed and reopened offline (the outbox is on disk)', async () => {
      await coldStart(ctx, { ms: 60_000 });
    });
    await ctx.step('online → flushed by itself; the worker holds it', async () => {
      await ctx.device.net('wifi');
      const end = Date.now() + 120_000;
      let got = null;
      while (Date.now() < end && !(got = await stored(marker))) await ctx.device.sleep(5000);
      if (!got) throw new Error('not on the server 2 min after reconnect');
      return `stored ${got.id ?? ''} ${got.received ?? ''}`.trim();
    });
    await ctx.step('back to the map', async () => { await toMap(ctx); });
  },
};
