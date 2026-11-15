// (T723) A LOOK AT FIVE SHARED-UI FIXES ON iOS, light and dark: T729 the credits card (+ c099e9977 closes with the look),
// T728 the Conditions card's Flow / Level, T731 a step on a water's map, T717 Back from a water's page, T730 search on the
// phone. A walk that takes the shots and recordings and checks what the screen and the app's log can say; how each one
// LOOKS is judged from the shots (`shared-<item>-<look>-*.png`, recordings `*.mp4`) and written in T723.
//
//   node app/tools/testbed/run-ios.mjs shared-fixes          (SHARED_LOOKS=light,dark; SHARED_ITEMS=t729,t728,t731,t717,t730)
import path from 'node:path';

const LOOKS = (process.env.SHARED_LOOKS ?? 'light,dark').split(',');
const ITEMS = (process.env.SHARED_ITEMS ?? 't729,t728,t731,t717,t730').split(',');
// iOS search rows by their accessibility text (data names: kept out of the static check's literal scan, as regs-open's)
const ROW = { coquitlam: 'Show Coquitlam River,.*', cultus: 'Show Cultus Lake,.*Lindell Beach.*' };
const has = (ctx, id) => (ctx.device.screenText().ids ?? []).includes(id);
const box = (ctx, id) => ctx.device.nodes().find((n) => n.id === id)?.box ?? null;
const mid = (b) => [Math.round((b.x0 + b.x1) / 2), Math.round((b.y0 + b.y1) / 2)];
const taps = (ctx) => ctx.device.logcatJs().split('\n').filter((l) => l.includes('[tap] ')).length;

export const sharedFixesIos = {
  id: 'shared-fixes',
  inAll: false,
  title: `Shared-UI fixes on iOS (${ITEMS.join(', ')}; ${LOOKS.join(' + ')})`,
  async run(ctx) {
    const { coldStart, toMap, pageBack } = ctx.ios;
    const d = ctx.device, sleep = d.sleep;
    let look = 'light';
    const shot = (name) => d.screenshot(path.join(ctx.dir, `shared-${name}-${look}.png`));
    const record = async (name, act, ms = 3000) => {
      const file = path.join(ctx.dir, `shared-${name}-${look}.mp4`);
      const rec = d.recordVideo(file);
      await sleep(1500);
      const t0 = Date.now();
      await act();
      await sleep(ms);
      await rec.stop();
      return { file, actAt: (Date.now() - t0) };
    };
    const openWater = async (q, row) => {
      await toMap(ctx);
      ctx.flow('search', { Q: q });
      ctx.flow('open-result', { LINE: row });
      await ctx.until(() => has(ctx, 'water-back'), { ms: 20_000, what: `the page of ${q} (water-back)` });
    };
    const chip = async () => { ctx.flow('tap-id', { ID: 'map-credits-chip' }); await sleep(1200); return has(ctx, 'map-credits-card'); };

    for (const L of LOOKS) {
      look = L;
      // the first start may be a first open on a fresh simulator (TESTBED_SIM other than the baseline's): the release downloads
      await ctx.step(`${look}: appearance set, cold start`, async () => { d.appearance(look); await coldStart(ctx, { ms: 600_000 }); await sleep(3000); shot('map'); });

      if (ITEMS.includes('t729')) {
        await ctx.step(`${look} T729: the credits card on Map: opens, Close closes it, a tap outside closes it, the edge swipe`, async () => {
          await toMap(ctx);
          if (!(await chip())) throw new Error('the © chip opened no card (map-credits-card)');
          shot('t729-map');
          ctx.flow('tap-id', { ID: 'map-credits-close' }); await sleep(1000);
          const closeOk = !has(ctx, 'map-credits-card');
          await chip();
          const c = box(ctx, 'map-credits-card');
          ctx.flow('tap-point', { POINT: '50%,15%' }); await sleep(1000);        // outside the card (it is centred)
          const outsideOk = !has(ctx, 'map-credits-card');
          await chip();
          ctx.flow('edge-back'); await sleep(1500);
          const swipe = has(ctx, 'map-credits-card') ? 'stayed open' : 'closed';
          if (has(ctx, 'map-credits-card')) { ctx.flow('tap-id', { ID: 'map-credits-close' }); await sleep(800); }
          const note = `card box ${c ? JSON.stringify(c) : '?'} · Close: ${closeOk ? 'closed' : 'STAYED'} · tap outside: ${outsideOk ? 'closed' : 'STAYED'} · left-edge swipe: ${swipe}`;
          if (!closeOk || !outsideOk) throw new Error(note);
          return note;
        });
        await ctx.step(`${look} T729: satellite on, the card names Esri`, async () => {
          await toMap(ctx);
          ctx.flow('layers-satellite'); await sleep(800); ctx.flow('tap-text', { TEXT: 'Done' }); await sleep(3000);
          await chip();
          const w = d.screenText();
          shot('t729-satellite');
          ctx.flow('tap-id', { ID: 'map-credits-close' }); await sleep(800);
          ctx.flow('tap-text', { TEXT: 'Choose layers.*' }); await sleep(1000);
          ctx.flow('tap-id', { ID: 'option-ground\\.map' }); ctx.flow('tap-text', { TEXT: 'Done' }); await sleep(1500);
          const esri = w.find((x) => /Esri/.test(x));
          if (!esri) throw new Error(`no Esri line on the card with satellite on (saw: ${w.filter((x) => /©|credit|Map/i.test(x)).slice(0, 6).join(' | ')})`);
          return esri;
        });
        for (const tab of ['Conditions', 'Runs']) {
          await ctx.step(`${look} T729: the credits card on ${tab}`, async () => {
            await toMap(ctx);
            ctx.flow('tab', { TAB: tab }); await sleep(2500);
            if (d.screenText().includes('Skip')) ctx.flow('tap-text', { TEXT: 'Skip' });   // a first-use tour
            if (!(await chip())) throw new Error(`the © chip on ${tab} opened no card`);
            shot(`t729-${tab.toLowerCase()}`);
            if (tab === 'Conditions') { ctx.flow('tap-id', { ID: 'map-credits-close' }); await sleep(800); return 'opened, closed'; }
            // (c099e9977) left OPEN, the tab changed: the card and its scrim must go, and the next map tap must reach the map
            ctx.flow('tab', { TAB: 'Map' }); await sleep(2000);
            const left = has(ctx, 'map-credits-card') || has(ctx, 'map-credits-scrim');
            d.logcatClear();
            const n0 = taps(ctx);
            ctx.flow('ios-tap-at', { POINT: '201,450' }); await sleep(2500);
            const reached = taps(ctx) - n0;
            shot('t729-after-tab-change');
            const note = `after the tab change: card/scrim ${left ? 'STILL THERE' : 'gone'}; a map tap reached the map's press handler: ${reached ? 'yes' : 'NO'} ([tap] lines ${reached})`;
            if (d.screenText().includes('Regulations') && has(ctx, 'water-back')) await pageBack(ctx);
            if (left || !reached) throw new Error(note);
            return note;
          });
        }
      }

      if (ITEMS.includes('t728')) {
        await ctx.step(`${look} T728: Conditions map on Level; the card's Flow pick leaves it on Level`, async () => {
          await toMap(ctx);
          ctx.flow('tab', { TAB: 'Conditions' }); await sleep(2500);
          ctx.flow('tap-text', { TEXT: 'Choose layers.*' }); await sleep(1200);
          ctx.flow('tap-text', { TEXT: 'Level' }); await sleep(800);
          shot('t728-layers-before');
          const before = d.nodes().filter((n) => /^option-/.test(n.id)).map((n) => `${n.id}:${n.desc}`);
          ctx.flow('tap-text', { TEXT: 'Done' }); await sleep(1500);
          ctx.flow('search', { Q: 'Coquitlam River' });
          ctx.flow('open-result', { LINE: ROW.coquitlam });
          await ctx.until(() => has(ctx, 'water-back'), { ms: 20_000, what: 'the Coquitlam River page' });
          if (has(ctx, 'water-view-conditions')) ctx.flow('tap-id', { ID: 'water-view-conditions' });
          await sleep(3000);
          shot('t728-card-level');
          ctx.flow('tap-text', { TEXT: 'Flow' }); await sleep(1500);
          shot('t728-card-flow');
          await pageBack(ctx); await sleep(2000);
          ctx.flow('tap-text', { TEXT: 'Choose layers.*' }); await sleep(1200);
          shot('t728-layers-after');
          const after = d.nodes().filter((n) => /^option-/.test(n.id)).map((n) => `${n.id}:${n.desc}`);
          ctx.flow('tap-text', { TEXT: 'Done' }); await sleep(800);
          return `layers before: ${before.join(', ') || '(no option ids in the tree)'} · after: ${after.join(', ') || '(none)'} (judge from the shots)`;
        });
      }

      if (ITEMS.includes('t731')) {
        await ctx.step(`${look} T731: Coquitlam River's map, a step downstream and back up (recorded)`, async () => {
          await openWater('Coquitlam River', ROW.coquitlam);
          ctx.flow('tap-id', { ID: 'water-view-map' }); await sleep(3000);
          shot('t731-before');
          const bar0 = box(ctx, 'water-map-step-down'), up0 = box(ctx, 'water-map-step-up');
          if (!bar0) throw new Error('no step button (water-map-step-down) on the water map');
          const r = await record('t731-step', async () => { ctx.flow('tap-id', { ID: 'water-map-step-down' }); });
          shot('t731-after');
          const bar1 = box(ctx, 'water-map-step-down'), up1 = box(ctx, 'water-map-step-up');
          await record('t731-step-back', async () => { ctx.flow('tap-id', { ID: 'water-map-step-up' }); });
          await toMap(ctx);
          const moved = JSON.stringify([bar0, up0]) !== JSON.stringify([bar1, up1]);
          return `step buttons before ${JSON.stringify([bar0, up0])} after ${JSON.stringify([bar1, up1])}${moved ? ' (MOVED)' : ' (same place)'} · video ${path.basename(r.file)}`;
        });
      }

      if (ITEMS.includes('t717')) {
        await ctx.step(`${look} T717: Back from Cultus Lake's page (recorded), the map under the page takes no touch, memory on the page`, async () => {
          await openWater('Cultus', ROW.cultus);
          await sleep(2500);
          const mem = d.memory();
          shot('t717-page');
          // a tap on an empty part of the page: the hidden browse map under it must not hear it
          d.logcatClear();
          ctx.flow('ios-tap-at', { POINT: '200,90' }); await sleep(2000);   // the page's header row, between its back link and share
          const through = taps(ctx);
          const stillPage = has(ctx, 'water-back');
          const r = await record('t717-back', async () => { await pageBack(ctx); }, 3000);
          shot('t717-map-after');
          return `page footprint ${mem?.footprint ?? '?'} MB (malloc ${mem?.malloc ?? '?'}, RSS ${mem?.rss ?? '?'}) · tap on the page: map press handler ${through ? `HEARD it (${through})` : 'did not hear it'}, page ${stillPage ? 'stayed' : 'LEFT'} · video ${path.basename(r.file)}`;
        });
      }

      if (ITEMS.includes('t730')) {
        await ctx.step(`${look} T730: search: recents, swipe down with the keyboard up closes it`, async () => {
          await toMap(ctx);
          ctx.flow('tap-id', { ID: 'search-field' }); await sleep(1500);
          shot('t730-recents');
          const h = box(ctx, 'search-sheet-handle');
          if (!h) throw new Error('no search-sheet-handle in the tree');
          const [x, y] = mid(h);
          ctx.swipe([x, y], [x, y + 450], 350); await sleep(1500);
          const closed = !has(ctx, 'search-sheet-handle');
          shot('t730-after-swipe');
          if (!closed) ctx.flow('tap-id', { ID: 'search-panel-close' });
          return `handle at ${x},${y}; swipe down (keyboard up): ${closed ? 'closed' : 'STAYED OPEN'}`;
        });
        await ctx.step(`${look} T730: Back after a result stays on that water; search starts afresh`, async () => {
          await openWater('Cultus', ROW.cultus);
          await pageBack(ctx); await sleep(2500);
          shot('t730-back-on-water');
          ctx.flow('tap-id', { ID: 'search-field' }); await sleep(1500);
          const w = d.screenText();
          shot('t730-reopened');
          const kept = w.find((x) => /^Cultus/.test(x));
          ctx.flow('tap-id', { ID: 'search-panel-close' }); await sleep(800);
          return `search reopened: ${kept ? `still says "${kept}"` : 'empty (afresh)'} (judge the map from the shot)`;
        });
      }
    }
    d.appearance('light');
  },
};
