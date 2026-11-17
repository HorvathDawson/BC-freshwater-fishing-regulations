// (T826) THE CONDITIONS TAB ON iOS, against the owner's five parts (iPhone, build 36): the Conditions map's gauge markers
// and its flow bar ("Flow vs. past years"), the Chehalis (Region 2, gauge 08MG001) opened from search and its Conditions
// card, and the tap highlight after going back (the page's ‹ and the left-edge swipe). Shots and the screen's words for
// each step (`T826-*.png`, `T826-*.txt`); the app's JS log at the end (`T826-js.log`).
//
//   node app/tools/testbed/run-ios.mjs conditions
import fs from 'node:fs';
import path from 'node:path';

// the search row by its accessibility text (a data name: kept out of the static check's literal scan, as regs-open's)
const CHEHALIS = 'Show Chehalis River,.*Region 2.*';
const VEDDER = 'Show Vedder River,.*';

export const conditionsIos = {
  id: 'conditions',
  inAll: false,
  title: 'T826: Conditions on iOS (gauge markers, the flow bar, the Chehalis, the highlight after back)',
  async run(ctx) {
    const { coldStart, pageBack } = ctx.ios;
    const d = ctx.device, sleep = d.sleep;
    const save = (name) => {
      d.screenshot(path.join(ctx.dir, `T826-${name}.png`));
      const w = d.screenText();
      fs.writeFileSync(path.join(ctx.dir, `T826-${name}.txt`), [...w, '--- ids', ...(w.ids ?? [])].join('\n'));
      return w;
    };
    const openChehalis = async () => {
      ctx.flow('search', { Q: 'Chehalis' });
      await sleep(2500);
      ctx.flow('open-result', { LINE: CHEHALIS });
      await ctx.until(() => d.hasId('water-back'), { ms: 30_000, what: 'the Chehalis page (water-back)' });
    };

    await ctx.step('cold start; the Conditions tab', async () => {
      await coldStart(ctx, { ms: 600_000 });
      d.logcatClear();
      ctx.flow('tab', { TAB: 'Conditions' });
      await sleep(9000);
      const w = save('1-conditions-map');
      const bar = w.find((x) => /Flow vs\. past years/i.test(x));
      return `flow bar: ${bar ? 'shown' : 'NOT in the tree'} · words: ${w.slice(0, 14).join(' | ').slice(0, 300)}`;
    });

    await ctx.step('the Chehalis from search (its Map view), then its Conditions view: the gauge card', async () => {
      await openChehalis();
      await sleep(6000);
      save('2-chehalis-map-view');
      ctx.flow('tap-id', { ID: 'water-view-conditions' });
      await sleep(9000);
      const w = save('3-chehalis-conditions');
      const gauge = w.find((x) => /08MG001|Chehalis River Near Harrison Mills/i.test(x));
      ctx.flow('pan-up'); await sleep(2500);
      save('3b-chehalis-conditions-scrolled');
      return `gauge named: ${gauge ?? 'NONE'} · words: ${w.slice(0, 24).join(' | ').slice(0, 600)}`;
    });

    await ctx.step('back by the page\'s ‹: the highlight clears on the Conditions map', async () => {
      await pageBack(ctx);
      await sleep(4000);
      save('4-after-page-back');
    });

    await ctx.step('a tap on the Chehalis on the Conditions map: its Conditions, then back by the left-edge swipe', async () => {
      ctx.flow('tap-point', { POINT: '60%,58%' });
      await sleep(9000);
      const w = save('5-map-tap');
      const gauge = w.find((x) => /08MG001|Chehalis River Near Harrison Mills/i.test(x));
      if (d.hasId('water-back')) { ctx.flow('edge-back'); await sleep(4000); save('6-after-edge-back'); }
      if (d.hasId('water-back')) { await pageBack(ctx); await sleep(4000); save('6b-after-page-back'); }
      return `the tap opened: ${w.slice(0, 12).join(' | ').slice(0, 400)} · gauge named: ${gauge ?? 'NONE'}`;
    });

    await ctx.step('a water with a flow estimate (the Vedder): its Conditions view, the estimate card and its bar', async () => {
      await ctx.ios.toMap(ctx);
      ctx.flow('tab', { TAB: 'Conditions' }); await sleep(3000);
      ctx.flow('search', { Q: 'Vedder' }); await sleep(2500);
      ctx.flow('open-result', { LINE: VEDDER });
      await ctx.until(() => d.hasId('water-back'), { ms: 30_000, what: 'the Vedder page (water-back)' });
      await sleep(3000);
      ctx.flow('tap-id', { ID: 'water-view-conditions' });
      await sleep(9000);
      const w = save('7-vedder-conditions');
      ctx.flow('pan-up'); await sleep(2500);
      save('7b-vedder-conditions-scrolled');
      await pageBack(ctx); await sleep(3000);
      return `words: ${w.slice(0, 24).join(' | ').slice(0, 600)}`;
    });

    fs.writeFileSync(path.join(ctx.dir, 'T826-js.log'), d.logcatJs());
  },
};
