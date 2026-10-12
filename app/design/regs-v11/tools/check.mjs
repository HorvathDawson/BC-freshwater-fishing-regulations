// Headless checks for the regulations prototype (round 9): what the owner flagged on v10 must not come back.
//   node app/design/regs-v11/tools/check.mjs [page.html]      (default: app/design/regs-v11.html; exit 1 on any failure)
// Needs only Node 22+ and Chrome (CHROME=… to point at it; CI uses google-chrome). The page runs offline: every
// http(s) request is blocked. Each check reads the RENDERED page, so it judges what an angler would see.
//   no-hand-facts         no `data-src="design"` and no "pending" marker anywhere in the page's source
//   fishway-by-the-strip  the "no fishing within 23 m of a fishway / 100 m of a fish facility" lines sit with the
//                         open/closed strip (before the first fish group), never in or just above Gear & licence
//   one-sources-line      Gear & licence carries no source chip per line; every section has exactly ONE Sources line
//   no-barbless-box       no trailing "Single barbless hook ⓘ" box in Gear & licence
//   map-status-tokens     the water map paints each section with the APP's status token for its status
//                         (packages/map/style/themes/{light,dark}.json color.status.*), in light and in dark
//   map-from-pipeline     the map's data is the pipeline export's (water_map_export), with a labelled boundary
//   map-steps-and-back    ▲ Upstream moves to the next part; Escape returns to the regulations at the same scroll
import fs from 'node:fs'; import path from 'node:path'; import { fileURLToPath, pathToFileURL } from 'node:url';
import { open } from './cdp.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const APP = path.resolve(HERE, '../../..');
const PAGE = path.resolve(process.argv[2] || path.join(APP, 'design/regs-v11.html'));
const THEME = n => JSON.parse(fs.readFileSync(path.join(APP, 'packages/map/style/themes', `${n}.json`), 'utf8')).values;
const results = [];
const check = (name, ok, why) => { results.push([name, ok, why]); console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${ok ? '' : `  — ${why}`}`); };

async function page(hash, fn) {
  const b = await open(`${pathToFileURL(PAGE).href}#shot=1&${hash}`, { offline: true });
  try { await new Promise(r => setTimeout(r, 400)); return await fn(b); } finally { await b.close(); }
}

// 1. zero hand-written facts
const src = fs.readFileSync(PAGE, 'utf8');
const hand = (src.match(/data-src="design"/g) || []).length, pend = (src.match(/pending/gi) || []).length;
check('no-hand-facts', !hand && !pend, `${hand} data-src="design", ${pend} "pending" in ${path.basename(PAGE)}`);

// 2–4. on the Vedder, Oct 10, with the Line & hooks topic showing (v10 opens it with kit=line; v11 shows everything)
await page('w=vedder&d=1010&sec=gear&kit=line', async b => {
  const r = JSON.parse(await b.evaluate(`JSON.stringify((() => {
    const gear = document.getElementById('gear'), first = document.querySelector('.row.ledger');
    const fish = [...document.querySelectorAll('li, p, div, button')].filter(e => /fishway/i.test(e.textContent) && ![...e.children].some(c => /fishway/i.test(c.textContent)));
    const pos = e => first ? !!(e.compareDocumentPosition(first) & Node.DOCUMENT_POSITION_FOLLOWING) : false;
    const secs = gear ? [...gear.querySelectorAll('.gpart')] : [];
    return {
      fishway: fish.length, inGear: fish.filter(e => gear && gear.contains(e)).length, beforeFish: fish.filter(pos).length,
      chips: gear ? gear.querySelectorAll('.gsrc, .srcbtn:not([hidden]), [data-gearsrc]').length : -1,
      sections: secs.length, lines: secs.map(s => s.querySelectorAll('.gsources').length),
      barbless: gear ? [...gear.querySelectorAll('.kitnote, p, div')].filter(e => /^Single barbless hook\\s*ⓘ?$/.test(e.textContent.trim())).length : -1,
    }; })())`));
  check('fishway-by-the-strip', r.fishway > 0 && r.inGear === 0 && r.beforeFish === r.fishway,
    `${r.fishway} fishway lines: ${r.inGear} inside Gear & licence, ${r.fishway - r.beforeFish} after the fish groups (the top of Gear)`);
  check('one-sources-line', r.chips === 0 && r.sections > 0 && r.lines.every(n => n === 1),
    `${r.chips} source chips in Gear & licence; ${r.sections} sections with ${JSON.stringify(r.lines)} Sources lines`);
  check('no-barbless-box', r.barbless === 0, `${r.barbless} "Single barbless hook ⓘ" box(es)`);
});

// 5–6. the water map, light and dark
for (const theme of ['light', 'dark']) {
  const T = THEME(theme);
  await page(`w=vedder&d=1010&map=1&theme=${theme}`, async b => {
    const r = JSON.parse(await b.evaluate(`JSON.stringify((() => {
      const rgb = c => { const m = c.match(/\\d+(\\.\\d+)?/g); return m ? '#' + m.slice(0, 3).map(x => (+x).toString(16).padStart(2, '0')).join('') : c; };
      const els = [...document.querySelectorAll('#wmap [data-status]')].filter(e => e.dataset.status);
      return { n: els.length, statuses: [...new Set(els.map(e => e.dataset.status))],
        paint: els.map(e => { const cs = getComputedStyle(e); return [e.dataset.status, rgb(e.getAttribute('fill') !== 'none' && e.getAttribute('fill') ? cs.fill : cs.stroke)]; }),
        about: window.WATERMAP && WATERMAP.about, marks: document.querySelectorAll('#wmap .mk span').length }; })())`));
    const bad = r.paint.filter(([s, c]) => c.toLowerCase() !== String(T[`color.status.${s}`]).toLowerCase());
    check(`map-status-tokens (${theme})`, r.n > 0 && !bad.length,
      r.n ? `${bad.length} of ${r.n} painted off-token, e.g. ${JSON.stringify(bad[0])}` : 'no section on the map carries a status');
    if (theme === 'light') check('map-from-pipeline', r.about?.generated_by === 'python -m pipeline.tools.water_map_export' && r.marks > 0,
      `data from ${r.about?.generated_by || 'a hand-made file (no WATERMAP.about)'}; ${r.marks} labelled boundaries`);
  });
}

// 7. step upstream, then back to the same scroll
await page('w=vedder&d=1010&scroll=600', async b => {
  const r = JSON.parse(await b.evaluate(`(async () => {
    const wait = () => new Promise(r => setTimeout(r, 350)), sc = document.getElementById('sc'), before = sc.scrollTop;
    document.querySelector('[data-map]').click(); await wait();
    const name = () => document.querySelector('#wmap .mname')?.textContent || '';
    const n0 = name(), up = document.querySelector('#wmap [data-step="1"]'); up && up.click(); await wait();
    const n1 = name(), word = document.querySelector('#wmap .mst')?.textContent || '';
    document.dispatchEvent(new KeyboardEvent('keydown', {key: 'Escape'})); await wait();
    return JSON.stringify({before, after: document.getElementById('sc').scrollTop, n0, n1, word, open: document.getElementById('wmap').classList.contains('on')});
  })()`));
  check('map-steps-and-back', !!r.n0 && r.n1 && r.n1 !== r.n0 && !!r.word && !r.open && r.after === r.before,
    `upstream "${r.n0}" → "${r.n1}" (${r.word || 'no status words'}); map ${r.open ? 'still open' : 'closed'}; scroll ${r.before} → ${r.after}`);
});

const failed = results.filter(x => !x[1]).length;
console.log(`${results.length - failed} passed, ${failed} failed — ${path.relative(process.cwd(), PAGE)}`);
process.exit(failed ? 1 : 0);
