// Headless checks for the regulations prototype (round 9): what the owner flagged on v10 must not come back.
//   node app/design/regs-v11/tools/check.mjs [page.html]      (default: app/design/regs-v11.html; exit 1 on any failure)
// Needs only Node 22+ and Chrome (CHROME=… to point at it; CI uses google-chrome). The page runs offline: every
// http(s) request is blocked. Each check reads the RENDERED page, so it judges what an angler would see.
//   no-hand-facts         no `data-src="design"` and no "pending" marker anywhere in the page's source
//   nofish-quiet-note     (round 10, owner 1) the "no fishing within 23 m of a fishway / 100 m of a fish facility" lines
//                         sit BELOW the quota tables (after every fish group and Other fish), before Gear, as a quiet
//                         note: no ✕ glyph, not in the closed colour
//   pictures-whole        (owner 2) fish pictures never overlap, are never cropped (box = the photo's own aspect, no
//                         cover fit, no rounded clip), and every one with a photo opens its picture sheet (sp:CODE)
//   (round 11) place-in-header, gear-sentence, gear-no-icons, always-as-reference, bucket-number-big, bucket-note-once,
//   kind-row-one-line, inherited-line, braid-review: see each check's comment below
//   (round 13) header-one-shape, licence-others-in-dropdowns, bella-coola-front (compact-header-place retired); phase 2:
//   no-lift-box, bucket-replaces, not-kept-said, zero-kind-no-size, gear-no-any-hook, ape-creek-front (card-one-book-per-lift retired)
//   (round 14) licence-u16-you-need, nested-count-group, presence-in-section, week-in-words, not-kept-merged
//   (round 15) no-bare-zero, licence-says-residency, week-sheet-one-story; phase 2: size-group-box, bucket-between-big,
//   u16-by-residency, week-parts-decided
//   (round 12) sheet-book-words, bait-ways-compact, dated-rows, howtell-has-hatchery,
//   release-group-pictures (and place-in-header: no kind, the bare label on a full-width line of its own); phase 2 (rows 11,
//   gear 8): one-worked-list, buckets-own-first-sizes-whole, every-cap-once, gear-sentence-plain
//   gear-one-summary      (round 10, retired in round 11 by gear-sentence) gear and methods are ONE section holding one summary block with an icon per cell:
//                         lines, hooks, bait, then methods (gear first); the strip's dated changes are on it
//   boat-lines            (owner 3) the Lines cell says what the data says about a boat: the data's own words where it
//                         has them (Kootenay Lake), else the cited rule's own title when that rule names a boat
//   lines-says-and-cites  (gear v6) the Lines cell is the data's sentence ("… in a boat too" on a stream) with one book
//                         link per page of the rules it cites; Kootenay Lake Main Body's Lines cites 2 rules
//   licence-last          (owner 3) Licence is its own section and the LAST one
//   no-part-strip         (owner 5) no numbered part strip on the regulations tab (parts are chosen on the map)
//   map-way-obvious       (owner 5) a row right under the header says where these rules apply (the part's label from
//                         the data) and opens the map, whose title says it shows where these rules apply
//   one-sources-line      Gear & licence carries no source chip per line; every section has exactly ONE Sources line
//   no-barbless-box       no trailing "Single barbless hook ⓘ" box in Gear & licence
//   map-status-tokens     the water map paints each section with the APP's status token for its status
//                         (packages/map/style/themes/{light,dark}.json color.status.*), in light and in dark
//   map-from-pipeline     the map's data is the pipeline export's (water_map_export), with a labelled boundary
//   map-steps-and-back    ▲ Upstream moves to the next part; Escape returns to the regulations at the same scroll
//   card-* (answers 2.8, on the Vedder, Kootenay LWA, Cowichan part 2, Chilliwack and Wilkes Creek, a plain stream):
//   card-headline         each group's big number is the data's headline.n ("2 a day from this stream"), never its total
//   card-one-book-per-lift  each lift line carries one "Book p.N" link per distinct page of its lifting rules (each[].by)
//   card-picture-per-species  every kind row shows one picture per species it names
//   card-cap-once         no line on a group is repeated, and the big number never restates the total line's cap
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
  check('one-sources-line', r.chips === 0 && r.sections > 0 && r.lines.every(n => n === 1),
    `${r.chips} source chips in Gear & licence; ${r.sections} sections with ${JSON.stringify(r.lines)} Sources lines`);
  check('no-barbless-box', r.barbless === 0, `${r.barbless} "Single barbless hook ⓘ" box(es)`);
});

// round 10 (owner): where you can't fish is a quiet note below the quota tables
const quiet = [];
for (const hash of ['w=vedder&d=1010', 'w=cowichan&d=1010&p=0', 'w=klwa&d=1010']) await page(hash, async b => {
  quiet.push(JSON.parse(await b.evaluate(`JSON.stringify((() => {
    const keep = document.getElementById('keep'), gear = document.getElementById('gear');
    const fish = [...document.querySelectorAll('li, p, div, button')].filter(e => /fishway/i.test(e.textContent) && ![...e.children].some(c => /fishway/i.test(c.textContent)));
    const tables = [...keep.querySelectorAll('article.row.ledger, .olist')], last = tables[tables.length - 1];
    const after = e => !last || !!(last.compareDocumentPosition(e) & Node.DOCUMENT_POSITION_FOLLOWING) && !last.contains(e);
    const closed = getComputedStyle(document.documentElement).getPropertyValue('--closed').trim();
    const rgb = h => { const d = document.createElement('i'); d.style.color = h; document.body.append(d); const c = getComputedStyle(d).color; d.remove(); return c; };
    const red = rgb(closed);
    const loud = e => { for (let x = e; x && x !== keep; x = x.parentElement) { const bc = getComputedStyle(x, '::before').content;
      if (/✕/.test(bc) || getComputedStyle(x).color === red) return true; } return /✕/.test(e.textContent); };
    return {hash: location.hash, n: fish.length, notBelow: fish.filter(e => !after(e) || (gear && gear.contains(e))).length, loud: fish.filter(loud).length}; })())`)));
});
check('nofish-quiet-note', quiet.every(q => q.n > 0 && !q.notBelow && !q.loud),
  quiet.filter(q => !(q.n > 0 && !q.notBelow && !q.loud)).map(q => `${q.hash}: ${q.n} lines, ${q.notBelow} above the quota tables (or in Gear), ${q.loud} with a ✕ / closed colour`).join('; '));

// round 10 (owner 2): whole pictures, no overlap, every photo opens its sheet
const pic = {overlap: [], crop: [], dead: [], opened: null};
for (const hash of ['w=vedder&d=1010', 'w=klwa&d=1010', 'w=cowichan&d=1010&p=1', 'w=wilkes&d=1010']) await page(hash, async b => {
  const r = JSON.parse(await b.evaluate(`JSON.stringify((() => {
    const man = Object.fromEntries((FISHMANIFEST.images || []).map(i => [i.file, i.width / i.height]));
    const out = {overlap: [], crop: [], dead: []};
    document.querySelectorAll('#keep .krow, #keep .orow').forEach(row => {
      const imgs = [...row.querySelectorAll('img')], rs = imgs.map(i => i.getBoundingClientRect());
      for (let i = 0; i < rs.length; i++) for (let j = i + 1; j < rs.length; j++) {
        const a = rs[i], c = rs[j], w = Math.min(a.right, c.right) - Math.max(a.left, c.left), h = Math.min(a.bottom, c.bottom) - Math.max(a.top, c.top);
        if (w > 1 && h > 1) out.overlap.push(row.textContent.trim().slice(0, 40)); }
      imgs.forEach((im, i) => { const file = im.getAttribute('src').split('/').pop(), want = man[file], r = rs[i];
        let clip = false; for (let x = im; x && x !== row; x = x.parentElement) { const cs = getComputedStyle(x);
          if (parseFloat(cs.borderTopLeftRadius) > 2 && cs.overflow !== 'visible') clip = true;
          if (x !== im && cs.overflow !== 'visible') { const pr = x.getBoundingClientRect(); if (r.left < pr.left - .5 || r.right > pr.right + .5 || r.top < pr.top - .5 || r.bottom > pr.bottom + .5) clip = true; } }
        if (!want || getComputedStyle(im).objectFit === 'cover' || Math.abs(r.width / r.height - want) > want * .03 || clip) out.crop.push(file);
        const code = (FISHMANIFEST.images.find(x => x.file === file) || {}).code, op = im.closest('[data-open]');
        if (!op || op.dataset.open !== 'sp:' + code) out.dead.push(file); });
    });
    return out; })())`));
  for (const k of ['overlap', 'crop', 'dead']) pic[k].push(...r[k].map(x => `${hash.split('&')[0].slice(2)}: ${x}`));
});
await page('w=vedder&d=1010', async b => {
  pic.opened = JSON.parse(await b.evaluate(`(async () => { const t = document.querySelector('#keep .orow img')?.closest('[data-open^="sp:"]');
    if (!t) return JSON.stringify(null); t.click(); await new Promise(r => setTimeout(r, 300));
    return JSON.stringify({code: t.dataset.open, imgs: document.querySelectorAll('#sheet.on .sbody img').length}); })()`));
});
check('pictures-whole', !pic.overlap.length && !pic.crop.length && !pic.dead.length && pic.opened && pic.opened.imgs > 0,
  `${pic.overlap.length} overlapping rows (${pic.overlap[0] || ''}); ${pic.crop.length} cropped (${pic.crop[0] || ''}); ${pic.dead.length} that open no picture sheet (${pic.dead[0] || ''}); an Other fish picture opens ${pic.opened ? pic.opened.imgs + ' picture(s)' : 'nothing'}`);

// round 11 (owner 3): Gear & methods is the data's ONE gear sentence (gear 7 `summary.says`) with its dated changes as chips
// (`summary.changes`), an ⓘ per glossary term, and one book link per page of the rules it cites; no icons anywhere in
// Gear & licence; Bait and Ways to fish stay; Licence is the last section; the province-wide rules ("Always") are a
// folded reference AFTER it, not a section of their own
const gear = {};
for (const [name, hash] of [['vedder', 'w=vedder&d=1010'], ['klwa', 'w=klwa&d=1010'], ['klmb', 'w=klmb&d=1010'], ['vedder may', 'w=vedder&d=515'], ['cow part 2', 'w=cowichan&d=1010&p=1']]) await page(hash, async b => {
  gear[name] = JSON.parse(await b.evaluate(`JSON.stringify((() => {
    const g = document.getElementById('gear'), secs = [...g.querySelectorAll('.gpart')], st = SNAP.states[sid()], sm = st.summary;
    const norm = t => (t || '').replace(/\\s+/g, ' ').trim();
    const box = g.querySelector('[data-summary]'), say = box?.querySelector('.gsay');
    const pages = new Set((sm?.cites || []).flatMap(k => (SNAP.ruleInfo[k]?.book || []).map(b => b[0])));
    const chips = box ? [...box.querySelectorAll('[data-change]')].map(e => norm(e.textContent)) : [];
    const ref = g.querySelector('details[data-reference]');
    return {sm: !!sm, says: sm?.says || null, kind: st.kind, shown: say ? norm(say.textContent) : null, times: sm ? (norm(g.textContent).split(norm(sm.says)).length - 1) : 0,
      pages: pages.size, books: box ? (norm(box.querySelector('.gbooks')?.textContent).match(/Book p\\.\\d+/g) || []).length : 0,
      changes: (sm?.changes || []).map(c => [c.when, c.says]), chips,
      terms: (sm?.terms || []), tips: say ? [...say.querySelectorAll('[data-gl]')].flatMap(e => e.dataset.gl.split(',')) : [],
      icons: g.querySelectorAll('svg.gi, .gi').length, bait: !!g.querySelector('[data-k="bait"]'), ways: !!g.querySelector('[data-k="methods"]'),
      secs: secs.map(s => s.id), last: secs.length ? secs[secs.length - 1].id : null,
      always: secs.filter(s => /^always$/i.test(norm(s.querySelector('h3')?.textContent))).length,
      ref: ref ? {open: ref.open, after: !!secs.length && !!(secs[secs.length - 1].compareDocumentPosition(ref) & Node.DOCUMENT_POSITION_FOLLOWING), n: ref.querySelectorAll('[data-rule], .factbtn').length} : null,
      hasAlways: !!(st.panes && st.panes.always)}; })())`));
});
const sbad = Object.entries(gear).filter(([, r]) => !(r.sm && r.shown && r.shown.startsWith(r.says) && r.times === 1 && r.books === r.pages
  && r.changes.length === r.chips.length && r.changes.every(([w, t], i) => r.chips[i].includes(w) && r.chips[i].includes(t))
  && r.terms.every(t => r.tips.includes(t)) && (r.kind !== 'stream' || !/boat/.test(r.says))));
check('gear-sentence', !sbad.length, sbad.map(([w, r]) => `${w}: data ${JSON.stringify(r.says)} ${r.sm ? '' : '(no summary in the snapshot)'}, shown ${JSON.stringify(r.shown)} ×${r.times}; ${r.books} book links for ${r.pages} pages; changes ${JSON.stringify(r.changes)} vs chips ${JSON.stringify(r.chips)}; terms ${JSON.stringify(r.terms)} vs ⓘ ${JSON.stringify(r.tips)}`).join('; '));
const ibad = Object.entries(gear).filter(([, r]) => r.icons || !r.bait || !r.ways);
check('gear-no-icons', !ibad.length, ibad.map(([w, r]) => `${w}: ${r.icons} icons; bait ${r.bait}, ways to fish ${r.ways}`).join('; '));
const lbad = Object.entries(gear).filter(([, r]) => r.last !== 'g-licence' || r.secs.filter(s => s === 'g-licence').length !== 1);
check('licence-last', !lbad.length, lbad.map(([w, r]) => `${w}: sections ${JSON.stringify(r.secs)}`).join('; '));
const abad = Object.entries(gear).filter(([, r]) => r.always || (r.hasAlways && !(r.ref && !r.ref.open && r.ref.after && r.ref.n > 0)));
check('always-as-reference', !abad.length, abad.map(([w, r]) => `${w}: ${r.always} "Always" section(s); reference fold ${JSON.stringify(r.ref)}`).join('; '));

// round 11 (owner 4): the place is IN the header, on every state, from a FRESH load (v12 showed its row only after the
// part or water changed): water name, then "<kind> · <the part's label from the data>" (no section count), the
// "View on map ›" action (or "Other parts ›" without a map), the date control; no MAP button, no row under the header
const HS = [];
await page('w=vedder&d=1010', async b => {
  const list = JSON.parse(await b.evaluate(`JSON.stringify(Object.entries(WATERS).flatMap(([w, c]) => Object.entries(c.snap).flatMap(([d, x]) => (Array.isArray(x) ? x : [x]).map((id, p) => \`w=\${w}&d=\${d}&p=\${p}\`))))`));
  for (const hash of list) {
    await b.send('Page.navigate', { url: 'about:blank' });
    await b.send('Page.navigate', { url: `${pathToFileURL(PAGE).href}#shot=1&${hash}` });
    for (let t = 0; t < 200; t++) { if (await b.evaluate(`document.readyState === 'complete' && !!document.getElementById('hdr')`).catch(() => false)) break; await new Promise(r => setTimeout(r, 50)); }
    HS.push(JSON.parse(await b.evaluate(`(async () => {
      const norm = t => (t || '').replace(/\\s+/g, ' ').trim(), hdr = document.getElementById('hdr'), st = ST(), w = WATERS[S.w];
      const place = hdr.querySelector('[data-place]'), txt = norm(place?.textContent), hasMap = !!(w.map && WM.waters[w.map]);
      const many = Array.isArray(w.snap[S.d]) && w.snap[S.d].length > 1, o = st.parts.find(x => x.v === st.part);
      const uncut = x => x && !/…/.test(x) ? x : null;
      const labels = o ? [o.t.replace(/ · \\d+ sections?$/, ''), uncut(o.short)].filter(Boolean)
        : hasMap ? [uncut(WM.waters[w.map].parts[curPart()].short), WM.waters[w.map].parts[curPart()].name].filter(Boolean) : [];
      const act = hasMap ? hdr.querySelector('[data-map]') : many ? hdr.querySelector('[data-open="parts"]') : null;
      const r = place?.getBoundingClientRect(), hr = hdr.getBoundingClientRect();
      // round 12: the label is a full-width line of its own; the action sits below it, never beside it (measured before the
      // map opens, which re-renders the header)
      const own = !txt || (() => { const a = hdr.querySelector('.pgo') || act, w = hdr.querySelector('.ttl b').getBoundingClientRect();
        return Math.abs(r.left - w.left) < 2 && hr.right - r.right < 20 && (!a || a.getBoundingClientRect().top >= r.bottom - 1); })();
      let title = ''; if (hasMap && act) { act.click(); await new Promise(r => setTimeout(r, 300)); title = norm(document.querySelector('#wmap .mh')?.textContent); }
      return JSON.stringify({hash: location.hash.slice(8), kind: st.kind, name: norm(hdr.querySelector('.ttl b')?.textContent) === st.name, txt, labels,
        // round 12 (owner 1 + 4): no kind ("stream ·"): the place is the part's label itself, nothing for a whole water
        label: txt ? labels.includes(txt) : !labels.length || labels.includes(st.name), lead: txt !== st.kind && !txt.startsWith(st.kind + ' · '),
        own,
        count: /\\b\\d+ sections?\\b/.test(norm(hdr.textContent)), visible: !!r && r.height > 0 && r.top >= hr.top && r.bottom <= hr.bottom,
        act: hasMap ? !!act && /view on map/i.test(act.textContent) && /where these rules apply/i.test(title) : many ? !!act && /other parts/i.test(act.textContent) : true,
        date: !!hdr.querySelector('[data-open="date"]'), mapbtn: document.querySelectorAll('.mapbtn').length,
        row: [...document.querySelectorAll('#sc > .where, [data-map], [data-open="parts"]')].filter(e => !document.getElementById('hdr').contains(e) && !document.getElementById('wmap').contains(e) && !document.getElementById('sheet').contains(e) && !e.closest('.poster')).length,
        strip: document.getElementById('sc').querySelectorAll('.parts, [data-part]').length}); })()`)));
  }
});
const hbad = HS.filter(h => !(h.name && h.lead && h.label && h.own && !h.count && (h.visible || !h.txt) && h.act && h.date && !h.mapbtn && !h.row));
check('place-in-header', HS.length > 40 && !hbad.length, `${hbad.length} of ${HS.length} fresh loads: ` + hbad.slice(0, 3).map(h => `${h.hash}: place ${JSON.stringify(h.txt.slice(0, 90))} (labels ${JSON.stringify(h.labels)})${h.name ? '' : ', no name'}${h.lead ? '' : ', still leads with the kind'}${h.label ? '' : ', not the bare label'}${h.own ? '' : ', not a full-width line of its own (the action beside it)'}${h.count ? ', a section count' : ''}${h.visible ? '' : ', not visible in the header'}${h.act ? '' : ', no map / parts action'}${h.date ? '' : ', no date control'}${h.mapbtn ? ', a MAP button' : ''}${h.row ? ', a place row outside the header' : ''}`).join('; '));
check('no-part-strip', !HS.some(h => h.strip), HS.filter(h => h.strip).map(h => `${h.hash}: ${h.strip} part-strip elements`).join('; '));

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

// 8–11. the quota card (answers 2.8): what the data says, once
const CARDS = [['vedder', 'w=vedder&d=1010'], ['klwa', 'w=klwa&d=1010'], ['cow part 2', 'w=cowichan&d=1010&p=1'],
  ['chilliwack', 'w=chilliwack&d=1010&p=0'], ['wilkes', 'w=wilkes&d=1010']];
const card = {headline: [], book: [], pics: [], once: []};
for (const [name, hash] of CARDS) await page(hash, async b => {
  const r = JSON.parse(await b.evaluate(`JSON.stringify((() => {
    const out = []; if (!WATERS[S.w] || !WATERS[S.w].snap[S.d]) return [{missing: true}];
    const st = SNAP.states[sid()], norm = t => t.replace(/\\s+/g, ' ').trim();
    document.querySelectorAll('#keep article.row.ledger').forEach(art => {
      const g = (st.rows[+(art.dataset.row || 'r0').slice(1)] || {}).group; if (!g || !g.headline) return;
      const num = (art.querySelector('.rhead .num')?.firstChild?.textContent || '').trim();
      const lifts = (g.lifts || []).map(l => { const el = [...art.querySelectorAll('p, li, div')].reverse().find(e => norm(e.textContent).includes(norm(l.says).slice(0, 40)));
        // every lifting rule's page once (answers rows v9: each[] parts; older shape: the lift itself)
        const want = new Set((l.each && l.each.length ? l.each : [l]).flatMap(e => (SNAP.ruleInfo[(st.ruleKey || {})[e.by]]?.book || []).map(b => b[0])));
        return el ? [(norm(el.textContent).match(/Book p\\.\\d+/g) || []).length, Math.max(1, want.size)] : [-1, 1]; });
      const kinds = (g.buckets || []).flatMap(bk => bk.kinds).map(k => { const row = [...art.querySelectorAll('.krow')].find(e => e.textContent.includes(k.name));
        return [k.name, k.species.length, row ? row.querySelectorAll('img, svg').length : -1]; });
      const lines = [...art.querySelectorAll('p, li')].map(e => norm(e.textContent)).filter(Boolean);
      const dup = lines.filter((t, i) => lines.indexOf(t) !== i);
      out.push({head: g.headline.n, num, total: g.total, totalLine: !!g.total_line, lifts, kinds, dup});
    }); return out; })())`));
  for (const g of r) {
    if (g.missing) { card.headline.push([name, 'a state', 'not on this page']); continue; }
    if (g.head != null) card.headline.push([name, g.head, g.num]);
    g.lifts.forEach(n => card.book.push([name, n]));
    g.kinds.forEach(([k, want, got]) => card.pics.push([name, k, want, got]));
    card.once.push([name, g.dup, g.totalLine && g.total != null && g.head !== g.total && +g.num === g.total]);
  }
});
const badH = card.headline.filter(([, n, num]) => String(n) !== num);
check('card-headline', card.headline.length > 0 && !badH.length, `${badH.length} of ${card.headline.length} groups: e.g. ${JSON.stringify(badH[0])} [water, headline.n, shown]`);
// card-one-book-per-lift retired in round 13 (D): the lift box is gone; no-lift-box / bucket-replaces below
const badP = card.pics.filter(([, , want, got]) => want !== got);
check('card-picture-per-species', card.pics.length > 0 && !badP.length, `${badP.length} of ${card.pics.length} kind rows: e.g. ${JSON.stringify(badP[0])} [water, kind, species, pictures]`);
const badO = card.once.filter(([, dup, total]) => dup.length || total);
check('card-cap-once', !badO.length, `${badO.length} groups: e.g. ${JSON.stringify(badO[0])} [water, repeated lines, big number = the total line's cap]`);

// round 11 (owner 2): each bucket's shared number is big and obvious (the data's `cap`, found in its title), its scope
// (the title after " — ") a smaller line; the bucket's size note (rows 10 `notes[].says`) said once, under the title,
// before its kinds; each kind row reads on one line — name, detail, number — with its notes BEFORE its pictures
const BK = {big: [], note: [], line: []};
for (const [name, hash] of CARDS.concat([['vedder may', 'w=vedder&d=515'], ['iron', 'w=iron&d=720']])) await page(hash, async b => {
  const r = JSON.parse(await b.evaluate(`JSON.stringify((() => { if (!WATERS[S.w] || !WATERS[S.w].snap[S.d]) return {missing: true};
    const st = SNAP.states[sid()], norm = t => (t || '').replace(/\\s+/g, ' ').trim(), out = {big: [], note: [], line: []};
    document.querySelectorAll('#keep article.row.ledger').forEach(art => {
      const g = (st.rows[+(art.dataset.row || 'r0').slice(1)] || {}).group; if (!g || !g.headline) return;
      const secs = [...art.querySelectorAll('section.bkt')];
      (g.buckets || []).forEach((bk, i) => { const el = secs[i];
        if (bk.title && bk.cap != null && new RegExp('\\\\b' + bk.cap + '\\\\b').test(bk.title)) {
          const n = el?.querySelector('.bkn'), sc = el?.querySelector('.bks'), scope = bk.title.split(' — ')[1] || '';
          const fs = e => e ? parseFloat(getComputedStyle(e).fontSize) : 0;
          out.big.push([bk.title, !!n && norm(n.textContent) === String(bk.cap) && fs(n) >= 24 && (!scope || (!!sc && norm(sc.textContent).includes(scope) && fs(sc) < fs(n)))]); }
        /* round 15 (rows 14): a size cap is now a size group (sizes, drawn [data-sgroup]); it is held to the same rule */
        [...(bk.notes || []), ...(bk.sizes || [])].forEach(nt => { const all = norm(art.textContent).split(norm(nt.says)).length - 1, ne = el ? [...el.querySelectorAll('[data-bnote], [data-sgroup]')].find(e => norm(e.textContent).includes(norm(nt.says))) : null;
          const k1 = el?.querySelector('.krow'); out.note.push([nt.says, all === 1 && !!ne && (!k1 || !!(ne.compareDocumentPosition(k1) & Node.DOCUMENT_POSITION_FOLLOWING))]); });
      });
      art.querySelectorAll('.krow.k28').forEach(k => { const nm = k.querySelector('.nm'), kn = k.querySelector('.kn'), dt = k.querySelector('.kdet');
        const top = e => e.getClientRects()[0]?.top ?? 0, pics = k.querySelector('.kpics'), notes = [...k.querySelectorAll('.s2, .duty, [data-knote]')];
        const nr = nm.getClientRects(), lastTop = nr[nr.length - 1]?.top ?? 0;
        // the detail follows the name on its line, unless the name's last line has no room left for the detail's first word
        const room = () => { const kl = nm.parentElement.getBoundingClientRect(), w0 = document.createElement('span'); w0.className = dt.className;
          w0.textContent = dt.textContent.trim().split(' ')[0]; w0.style.whiteSpace = 'nowrap'; nm.parentElement.append(w0); const ww = w0.getBoundingClientRect().width; w0.remove();
          return kl.right - nr[nr.length - 1].right >= ww + 6; };
        const ok = (!kn || Math.abs(top(kn) - top(nm)) < 10) && (!dt || Math.abs(top(dt) - lastTop) < 6 || !room()) && (!pics || notes.every(e => e.getBoundingClientRect().bottom <= pics.getBoundingClientRect().top + 1));
        out.line.push([norm(nm.textContent), ok]); });
    }); return out; })())`));
  if (r.missing) { BK.big.push([name, 'missing', false]); return; }
  for (const k of ['big', 'note', 'line']) BK[k].push(...r[k].map(x => [name, ...x]));
});
const bb = BK.big.filter(x => !x[2]), bn = BK.note.filter(x => !x[2]), bl = BK.line.filter(x => !x[2]);
check('bucket-number-big', BK.big.length > 0 && !bb.length, `${bb.length} of ${BK.big.length} bucket titles: e.g. ${JSON.stringify(bb[0])} [water, title, big number + smaller scope]`);
check('bucket-note-once', BK.note.length > 0 && !bn.length, `${bn.length} of ${BK.note.length} bucket notes: e.g. ${JSON.stringify(bn[0])} [water, note, once under the title before the kinds]`);
check('kind-row-one-line', BK.line.length > 0 && !bl.length, `${bl.length} of ${BK.line.length} kind rows: e.g. ${JSON.stringify(bl[0])} [water, kind, name·detail·number on one line, notes before pictures]`);

// round 11 (A3): a lift another water's row carries here (display `inherited`) is a visible line on the Keep side, by the
// strip (before the quota tables), with the lifting rule's book page; none where the data has none
const inh = [];
for (const hash of ['w=iron&d=720', 'w=iron&d=1010', 'w=vedder&d=1010']) await page(hash, async b => {
  inh.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { if (!WATERS[S.w] || !WATERS[S.w].snap[S.d]) return {hash: location.hash, missing: true};
    const st = SNAP.states[sid()], norm = t => (t || '').replace(/\\s+/g, ' ').trim(), keep = document.getElementById('keep'), first = keep.querySelector('article.row.ledger');
    const els = [...keep.querySelectorAll('[data-inherited]')];
    return {hash: location.hash, want: (st.inherited || []).map(x => [x.says, (SNAP.ruleInfo[x.by]?.book || []).map(b => 'Book p.' + b[0])]),
      got: els.map(e => [norm(e.textContent), !first || !!(e.compareDocumentPosition(first) & Node.DOCUMENT_POSITION_FOLLOWING), e.getBoundingClientRect().height > 0])}; })())`)));
});
const ibd = inh.filter(x => x.missing || x.want.length !== x.got.length || x.want.some(([says, bks], i) => !(x.got[i][0].includes(says) && bks.every(k => x.got[i][0].includes(k)) && x.got[i][1] && x.got[i][2])));
check('inherited-line', inh.some(x => x.want && x.want.length) && !ibd.length, ibd.map(x => `${x.hash}: ${x.missing ? 'not in the snapshot' : `data ${JSON.stringify(x.want)}, shown ${JSON.stringify(x.got)}`}`).join('; ') || 'no state with an inherited line');

// round 11 (owner 6): braids the round-10 placement marked (bundle section_straddle) are SEEN before they are trusted: the
// water map export carries each (water, rule, its cut, placed side, share); the side panel lists every one; each opens
// a map where the braid is drawn dashed and labelled with its side and share
await page('w=vedder&d=1010', async b => {
  const r = JSON.parse(await b.evaluate(`(async () => { const B = (window.WATERMAP && WATERMAP.braids) || [], norm = t => (t || '').replace(/\\s+/g, ' ').trim();
    const rows = B.flatMap((x, i) => x.rows.map((rw, j) => ({i, j, water: x.water, rule: rw.rule, side: rw.side, share: rw.share})));
    const links = [...document.querySelectorAll('.poster [data-braid]')], seen = [];
    for (let k = 0; k < links.length; k++) { const l = document.querySelectorAll('.poster [data-braid]')[k]; l.click(); await new Promise(r => setTimeout(r, 250)); const [i, j] = l.dataset.braid.split('|').map(Number), rw = B[i]?.rows[j];
      const path = document.querySelector('#wmap path.braid'), lab = norm(document.querySelector('#wmap [data-braid-label]')?.textContent);
      seen.push({water: B[i]?.water, ok: !!rw && !!path && getComputedStyle(path).strokeDasharray !== 'none' && lab.includes(Math.round(rw.share * 100) + '%') && lab.includes(rw.words || (rw.side === 'in' ? 'inside' : 'outside')),
        lab: lab.slice(0, 120), linkText: norm(l.textContent).slice(0, 160)});
      document.dispatchEvent(new KeyboardEvent('keydown', {key: 'Escape'})); await new Promise(r => setTimeout(r, 150)); }
    return JSON.stringify({from: WATERMAP.about?.generated_by, waters: [...new Set(B.map(x => x.water))], rows: rows.length, links: links.length, bad: seen.filter(x => !x.ok),
      text: links.every((l, k) => { const rw = rows[k]; return rw && norm(l.textContent).includes(rw.water) && norm(l.textContent).includes(Math.round(rw.share * 100) + '%'); })}); })()`));
  check('braid-review', r.from === 'python -m pipeline.tools.water_map_export' && ['Dean River', 'Peace River'].every(w => r.waters.includes(w)) && r.rows > 0 && r.links === r.rows && r.text && !r.bad.length,
    `export ${r.from}; waters ${JSON.stringify(r.waters)}; ${r.rows} marked rows, ${r.links} review links${r.text ? '' : ' (a link without its water / share)'}; ${r.bad.length} maps without a dashed, labelled braid: ${JSON.stringify(r.bad[0] || '')}`);
});

// ---------- round 12 ----------
// (owner 1 + 4) compact-header-place retired in round 13: the header no longer changes on scroll (header-one-shape)

// (owner 3) an ⓘ sheet: the plain words first, the BOOK's words folded ("In the book's words", closed), and every printed
// page the term names a link to that page of the book's PDF (pdfPages: pipeline.common.synopsis_pages)
const GLS = [];
for (const k of ['bait_ban', 'single_barbless_hook', 'bait_ban,single_barbless_hook', 'daily_quota']) await page(`w=vedder&d=1010&sheet=gl:${k}`, async b => {
  GLS.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const sh = document.getElementById('sheet'), norm = t => (t || '').replace(/\\s+/g, ' ').trim();
    return {k: ${JSON.stringify(k)}, terms: ${JSON.stringify(k)}.split(',').map(id => { const t = SNAP.gloss[id]?.term; if (!t) return {id, none: true};
      const q = [...sh.querySelectorAll('blockquote')].find(e => norm(e.textContent) === norm(t.quote)), d = q?.closest('details');
      const links = [...sh.querySelectorAll('a[href*="fishing_synopsis.pdf#page="]')].map(a => [norm(a.textContent), +a.href.split('#page=')[1]]);
      return {id, folded: !!d && !d.open && /in the book.s words/i.test(d.querySelector('summary')?.textContent || ''),
        says: norm(sh.textContent).includes(norm(t.says)), pages: t.pages.map(p => links.some(([tx, n]) => tx.includes('p.' + p) && n === (SNAP.pdfPages || {})[p]))}; })}; })())`)));
});
const glb = GLS.filter(x => x.terms.some(t => t.none || !t.folded || !t.says || !t.pages.every(Boolean)));
check('sheet-book-words', !glb.length, glb.map(x => `${x.k}: ${JSON.stringify(x.terms)}`).join('; '));

// (owner 3) Bait and Ways to fish: the same items in no more room than v13 took; Bait as two columns side by side (✓ allowed |
// ✕ not), Ways to fish inline (each allowed way one line, the ones that are not one line)
const V13H = {'w=vedder&d=1010': 361, 'w=klwa&d=1010': 390, 'w=cowichan&d=1010&p=1': 352, 'w=vedder&d=515': 291};
const BW = [];
for (const hash of Object.keys(V13H)) await page(hash, async b => {
  BW.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const norm = t => (t || '').replace(/\\s+/g, ' ').trim(), st = ST(), out = {hash: ${JSON.stringify(hash)}, h: 0, cells: []};
    const P = h => { const d = document.createElement('div'); d.innerHTML = h || ''; return d; };
    const want = {bait: [...P(st.panes.bait).querySelectorAll('.bait')].map(e => [norm(e.querySelector('b')?.textContent), e.classList.contains('ok')]),
      methods: [...P(st.panes.ways).querySelectorAll('.wrow b')].map(e => [norm(e.textContent), true]).concat([...P(st.panes.ways).querySelectorAll('.nochip')].map(e => [norm(e.textContent.replace(/^✕/, '')), false]))};
    for (const k of ['bait', 'methods']) { const c = document.querySelector('[data-k="' + k + '"]'); if (!c) { out.cells.push({k, none: true}); continue; }
      out.h += c.getBoundingClientRect().height;
      const ok = c.querySelector('[data-col="ok"]'), no = c.querySelector('[data-col="no"]');
      const side = k === 'methods' ? !!(ok || no) : ok && no ? Math.abs(ok.getBoundingClientRect().top - no.getBoundingClientRect().top) < 4 && no.getBoundingClientRect().left > ok.getBoundingClientRect().left + 40 : !!(ok || no);
      const miss = want[k].filter(([n, y]) => !norm((y ? ok : no)?.textContent).includes(n));
      out.cells.push({k, side, miss: miss.map(x => x[0])}); }
    out.h = Math.round(out.h); return out; })())`)));
});
const bwb = BW.filter(x => x.h > V13H[x.hash] || x.cells.some(c => c.none || !c.side || c.miss.length));
check('bait-ways-compact', !bwb.length, bwb.map(x => `${x.hash}: ${x.h} px (v13 ${V13H[x.hash]}); ${JSON.stringify(x.cells)}`).join('; '));

// (owner 3) every dated change in Gear & methods (the sentence's other days, Bait's bait ban) is ONE row component: the
// date label, then the change, the dates in one column and the changes in another, in the data's words
const DR = [];
for (const hash of ['w=vedder&d=1010', 'w=cowichan&d=1010&p=1', 'w=vedder&d=515']) await page(hash, async b => {
  DR.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const norm = t => (t || '').replace(/\\s+/g, ' ').trim(), g = document.getElementById('g-gear'), st = ST();
    const rows = [...g.querySelectorAll('[data-dated]')].map(e => { const w = e.querySelector('.dw'), c = e.querySelector('.dc');
      return {w: norm(w?.textContent), c: norm(c?.textContent), wl: w ? Math.round(w.getBoundingClientRect().left) : -1, cl: c ? Math.round(c.getBoundingClientRect().left) : -1}; });
    const want = (st.summary?.changes || []).map(c => [c.when, c.says]);
    return {hash: location.hash.slice(8), rows, want, old: g.querySelectorAll('.gchip, .gxd').length}; })())`)));
});
const drb = DR.filter(x => x.old || !x.rows.length || x.rows.some(r => !r.w || !r.c) || new Set(x.rows.map(r => r.wl)).size > 1 || new Set(x.rows.map(r => r.cl)).size > 1
  || !x.want.every(([w, t]) => x.rows.some(r => r.w.includes(w) && r.c.includes(t))));
check('dated-rows', DR.some(x => x.rows.length > 1) && !drb.length, drb.map(x => `${x.hash}: ${x.old} old chips; rows ${JSON.stringify(x.rows)}; data ${JSON.stringify(x.want)}`).join('; '));

// (More) "Hatchery or wild" is explained INSIDE "How to tell" (its plain words; the book's words folded; Book p.13, p.80 as
// links), and no separate ⓘ sits beside "How to tell"
const HT = [];
for (const hash of ['w=vedder&d=1010', 'w=iron&d=720']) await page(hash, async b => {
  HT.push(JSON.parse(await b.evaluate(`(async () => { const norm = t => (t || '').replace(/\\s+/g, ' ').trim();
    const ht = document.querySelector('#keep [data-howtell]'), beside = ht ? [...ht.parentElement.querySelectorAll('[data-gl="hatchery_wild"]')].length : -1;
    ht && ht.click(); await new Promise(r => setTimeout(r, 300));
    const sh = document.getElementById('sheet'), t = SNAP.gloss.hatchery_wild?.term || {};
    const q = [...sh.querySelectorAll('blockquote')].find(e => norm(e.textContent) === norm(t.quote)), d = q?.closest('details');
    const links = [...sh.querySelectorAll('a[href*="fishing_synopsis.pdf#page="]')].map(a => [norm(a.textContent), +a.href.split('#page=')[1]]);
    return JSON.stringify({hash: location.hash.slice(8), howtell: !!ht, beside, open: sh.classList.contains('on'), says: norm(sh.textContent).includes(norm(t.says)),
      folded: !!d && !d.open, pages: (t.pages || []).map(p => links.some(([tx, n]) => tx.includes('p.' + p) && n === (SNAP.pdfPages || {})[p]))}); })()`)));
});
const htb = HT.filter(x => !(x.howtell && x.beside === 0 && x.open && x.says && x.folded && x.pages.length && x.pages.every(Boolean)));
check('howtell-has-hatchery', !htb.length, htb.map(x => JSON.stringify(x)).join('; '));

// (More) a release-only group (Iron River's "Char") lists its species with a picture and a name each, like a keep
// group's kind rows (the row's members)
const RG = [];
for (const hash of ['w=iron&d=720', 'w=iron&d=1010']) await page(hash, async b => {
  RG.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const st = ST(), out = [];
    document.querySelectorAll('#keep article.row.ledger').forEach(art => { const r = st.rows[+(art.dataset.row || 'r0').slice(1)] || {};
      if (r.group?.headline || r.kind !== 'release') return;
      const want = (r.members || []).filter(c => FISHPIC(c).length), rows = art.querySelectorAll('.krow');
      out.push({t: art.querySelector('.rtitle')?.textContent, want, rows: rows.length, got: want.filter(c => art.querySelector('.krow [data-open="sp:' + c + '"] .kcap'))}); });
    return {hash: location.hash.slice(8), out}; })())`)));
});
const rgb = RG.filter(x => !x.out.length || x.out.some(o => !o.rows || o.got.length !== o.want.length));
check('release-group-pictures', !rgb.length, rgb.map(x => `${x.hash}: ${JSON.stringify(x.out)}`).join('; ') || '');

// ---------- round 12, phase 2 (answers rows 11, gear 8) ----------
// ONE "How it adds up" list per group: the data's elsewhere.says, then every group.worked line (q, a) once; no "Fished
// elsewhere today?" row or sheet; a keep-here line reads "q →", the others are questions
// buckets in the data's order (a water's own rule first), every kind's size words whole (detail), every bucket note
// (several per group: Morley Lake, Atlin Lake) once, with its nested `then` caps
const W2 = [];
const CARDS2 = CARDS.concat([['vedder may', 'w=vedder&d=515'], ['morley', 'w=x12p&d=1010'], ['atlin', 'w=x3p&d=1010'], ['iron', 'w=iron&d=720'],
  ['wahleach', 'w=x34p&d=1010'], ['bennett', 'w=x35p&d=1010'], ['bella coola', 'w=bellacoola&d=1010&p=1'], ['bella coola may', 'w=bellacoola&d=501&p=1'], ['ape', 'w=ape&d=1010'], ['ape may', 'w=ape&d=501'], ['fraser', 'w=x15p&d=1010&p=0']]);
for (const [name, hash] of CARDS2) await page(hash, async b => {
  W2.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { if (!WATERS[S.w] || !WATERS[S.w].snap[S.d]) return {name: ${JSON.stringify(name)}, missing: true};
    const st = SNAP.states[sid()], norm = t => (t || '').replace(/\\s+/g, ' ').trim(), out = {name: ${JSON.stringify(name)}, fe: /fished elsewhere today/i.test(document.getElementById('keep').textContent), g: []};
    document.querySelectorAll('#keep article.row.ledger').forEach(art => { const raw = st.rows[+(art.dataset.row || 'r0').slice(1)] || {}, g = raw.group; if (!g || !g.headline) return;
      const lists = [...art.querySelectorAll('.worked')], wl = lists[0], wt = norm(wl?.textContent), at = norm(art.textContent);
      const el = raw.elsewhere?.says || '', worked = g.worked || [];
      const secs = [...art.querySelectorAll('section.bkt')];
      out.g.push({lists: lists.length, want: !!(el || worked.length), ask: (raw.elsewhere?.ask || []).length,
        el: !el || (wt.includes(norm(el)) && at.split(norm(el)).length === 2),
        lines: worked.map(w => wt.includes(norm(w.q)) && wt.includes(norm(w.a)) && at.split(norm(w.a)).length === 2 && (w.at !== 'here') === !wt.includes(norm(w.q) + ' →')),
        order: (g.buckets || []).map((bk, i) => !!secs[i] && bk.kinds.every(k => norm(secs[i].textContent).includes(norm(k.name)))),
        sizes: (g.buckets || []).flatMap((bk, i) => bk.kinds.map(k => !k.detail || [...(secs[i]?.querySelectorAll('.krow') || [])].some(r => norm(r.textContent).includes(norm(k.name)) && norm(r.querySelector('.kdet')?.textContent) === norm(k.detail)))),
        caps: (g.buckets || []).reduce((t, bk) => t + (bk.notes || []).length + (bk.sizes || []).length + (bk.groups || []).length + (bk.between ? 1 : 0), 0),
        notes: (g.buckets || []).flatMap((bk, i) => [...(bk.notes || []), ...(bk.sizes || [])].map(n => at.split(norm(n.says)).length === 2 && norm(secs[i]?.textContent).includes(norm(n.says))
          && (n.then || []).every(t => norm(n.says).includes(String(t.over_cm)) || [...secs[i].querySelectorAll('[data-then]')].some(e => norm(e.textContent).includes(String(t.over_cm)))))),
        first: (g.buckets || [])[0]?.why || null, whys: (g.buckets || []).map(bk => bk.why)}); });
    return out; })())`)));
});
const wb = W2.filter(x => x.missing || x.fe || x.g.some(g => g.ask || (g.want ? g.lists !== 1 : g.lists) || !g.el || !g.lines.every(Boolean)));
check('one-worked-list', W2.some(x => x.g && x.g.some(g => g.lines.length > 3)) && !wb.length, wb.map(x => `${x.name}: ${x.missing ? 'not in the snapshot' : `${x.fe ? '"Fished elsewhere today?" still shown; ' : ''}${JSON.stringify(x.g.map(g => [g.lists, g.ask, g.el, g.lines]))}`}`).join('; '));
const ob = W2.filter(x => !x.missing && x.g.some(g => !g.order.every(Boolean) || (g.whys.includes('rule') && g.first !== 'rule') || !g.sizes.every(Boolean)));
check('buckets-own-first-sizes-whole', !ob.length && W2.some(x => x.name === 'vedder' && x.g[0]?.first === 'rule'), ob.map(x => `${x.name}: ${JSON.stringify(x.g.map(g => [g.whys, g.order, g.sizes]))}`).join('; '));
const nb = W2.filter(x => !x.missing && x.g.some(g => !g.notes.every(Boolean)));
check('every-cap-once', !nb.length && W2.some(x => x.g && x.g.some(g => g.caps >= 3)), nb.map(x => `${x.name}: ${JSON.stringify(x.g.map(g => g.notes))}`).join('; ') || 'no group with several caps checked');
// gear 8: the sentence as the data says it (no em dash)
const gd = Object.entries(gear).filter(([, r]) => !r.shown || /—/.test(r.shown) || !r.shown.startsWith(r.says));
check('gear-sentence-plain', !gd.length, gd.map(([w, r]) => `${w}: ${JSON.stringify(r.shown)}`).join('; '));

// ---------- round 13 ----------
// (H) the header keeps ONE shape: the same height, words and classes at scroll 0 and scrolled; "View on map ›" shown in
// both on a water with a map; the two section tabs slim (one row no taller than 40 px)
const H13 = [];
for (const hash of ['w=vedder&d=1010', 'w=chilliwack&d=1010&p=0', 'w=x14p&d=1010&p=1', 'w=cowichan&d=1010&p=1']) await page(hash, async b => {
  H13.push(JSON.parse(await b.evaluate(`(async () => { const sc = document.getElementById('sc'), raf = () => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));
    const look = () => { const h = document.getElementById('hdr'), go = [...h.querySelectorAll('.pgo')].find(e => /View on map/.test(e.textContent));
      const tl = h.querySelector('[role=tablist]');
      return {h: Math.round(h.getBoundingClientRect().height), cls: h.className, text: h.textContent.replace(/\\s+/g, ' ').trim(),
        map: !!(go && go.offsetHeight > 0 && getComputedStyle(go).display !== 'none'), tabs: tl ? Math.round(tl.getBoundingClientRect().height) : -1}; };
    await raf(); const a = look(); sc.scrollTop = 900; sc.dispatchEvent(new Event('scroll')); await raf(); const z = look();
    const w = WATERS[S.w]; return JSON.stringify({hash: location.hash.slice(8), hasMap: !!(w.map && WM.waters[w.map]), a, z, scrolled: sc.scrollTop}); })()`)));
});
const hb13 = H13.filter(x => x.scrolled < 300 || x.a.h !== x.z.h || x.a.cls !== x.z.cls || x.a.text !== x.z.text || (x.hasMap && !(x.a.map && x.z.map)) || x.a.tabs > 40 || x.a.tabs < 0);
check('header-one-shape', H13.some(x => x.hasMap) && !hb13.length, hb13.map(x => `${x.hash}: top ${JSON.stringify(x.a)} vs scrolled ${JSON.stringify(x.z)}`).join('; '));

// (L) Licence: each tab shows only the default angler's (B.C. resident) answer; every other residency is a "Rules for
// other anglers" dropdown of its own, carrying that residency's 16-or-older AND under-16 answers (the snapshot's own words)
const L13 = [];
for (const age of ['adult', 'u16']) await page(`w=vedder&d=1010&age=${age}&at=%23g-licence`, async b => {
  L13.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const st = ST(), L = document.getElementById('g-licence'), norm = t => (t || '').replace(/\\s+/g, ' ').trim();
    const box = h => { if (!h) return ''; const d = document.createElement('div'); d.innerHTML = h; return norm((d.querySelector('.classbox') || d.querySelector('.needs li'))?.textContent); };
    const outside = L.cloneNode(true); outside.querySelectorAll('details').forEach(d => d.remove()); const out = norm(outside.textContent);
    const res = [['non_resident', 'u16_non', 'adult_non'], ['non_resident_alien', 'u16_alien', 'adult_alien']].map(([r, u, a]) => {
      const d = L.querySelector('details[data-res="' + r + '"]'), t = norm(d?.textContent);
      return {r, fold: !!d, closed: d ? !d.open : null, named: d ? /Rules for other anglers/.test(d.querySelector('summary')?.textContent || '') : false,
        u16: !!st.licence[u] && !!box(st.licence[u]) && t.includes(box(st.licence[u])), adult: !!st.licence[a] && !!box(st.licence[a]) && t.includes(box(st.licence[a])),
        leak: !!st.licence[u] && !!box(st.licence[u]) && box(st.licence[u]) !== box(st.licence[${JSON.stringify(age === 'u16' ? 'u16_res' : 'adult')}]) && out.includes(box(st.licence[u])) }; });
    const mine = box(st.licence[${JSON.stringify(age === 'u16' ? 'u16_res' : 'adult')}]);
    return {age: ${JSON.stringify(age)}, blocks: outside.querySelectorAll('.u16h').length, mine: !!mine && out.includes(mine), res}; })())`)));
});
const lb13 = L13.filter(x => x.blocks > 0 || !x.mine || x.res.some(r => !r.fold || !r.closed || !r.named || !r.u16 || !r.adult || r.leak));
check('licence-others-in-dropdowns', !lb13.length, lb13.map(x => JSON.stringify(x)).join('; '));

// (B) Bella Coola front and centre: snapshotted on Oct 10 AND May 1 (every part the page offers), the May 1 mainstem card
// is not Oct 10's (the Apr 1–May 31 exception), and the side panel's quick pills open both days
const B13 = await page('w=bellacoola&d=501&p=1', async b => JSON.parse(await b.evaluate(`JSON.stringify((() => { const w = WATERS.bellacoola;
  if (!w) return {missing: true};
  const days = Object.keys(w.snap).map(Number), ids = d => [].concat(w.snap[d] || []), names = days.flatMap(d => ids(d).map(i => SNAP.states[i]?.name));
  const main = d => ids(d).map(i => SNAP.states[i]).find(s => s && s.part === '0');
  const pills = [...document.querySelectorAll('#c-w [data-cw^="bellacoola|"]')].map(e => e.dataset.cw);
  return {days, parts: days.map(d => ids(d).length), names: [...new Set(names)], differ: !!(main(1010) && main(501) && main(1010).cardHTML !== main(501).cardHTML),
    pills, shown: document.querySelector('#hdr .ttl')?.textContent.trim()}; })())`)));
check('bella-coola-front', !B13.missing && [1010, 501].every(d => B13.days.includes(d)) && B13.parts.every(n => n >= 2) && B13.names.join() === 'Bella Coola River'
  && B13.differ && ['1010', '501'].every(d => B13.pills.some(p => p.split('|')[1] === d)) && B13.shown === 'Bella Coola River', JSON.stringify(B13));

// ---------- round 13 phase 2 (rows 12, gear 9, display 9) ----------
const C13 = [];
for (const [name, hash] of CARDS2) await page(hash, async b => {
  C13.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { if (!WATERS[S.w] || !WATERS[S.w].snap[S.d]) return {name: ${JSON.stringify(name)}, missing: true};
    const st = SNAP.states[sid()], norm = t => (t || '').replace(/\\s+/g, ' ').trim(), cap = t => t ? t[0].toUpperCase() + t.slice(1) : t, out = {name: ${JSON.stringify(name)}, lift: document.querySelectorAll('#keep .lift, #keep .glifts .lift').length, rep: [], nk: [], zero: []};
    document.querySelectorAll('#keep article.row.ledger').forEach(art => { const g = (st.rows[+(art.dataset.row || 'r0').slice(1)] || {}).group; if (!g || !g.headline) return;
      const secs = [...art.querySelectorAll('section.bkt')], at = norm(art.textContent);
      (g.buckets || []).forEach((bk, i) => { if (bk.replaces) { const r = secs[i]?.querySelector('[data-replaces]');
        out.rep.push({says: bk.replaces.says, shown: norm(r?.textContent), underTitle: !!r && !!r.previousElementSibling?.matches('.bkhd, .bkh'), once: at.split(norm(bk.replaces.says)).length === 2}); }
        bk.kinds.forEach(k => { if (k.n === 0) { const row = [...(secs[i]?.querySelectorAll('.krow') || [])].find(e => norm(e.querySelector('.nm')?.textContent) === norm(k.name));
          out.zero.push({kind: k.name, det: norm(row?.querySelector('.kdet')?.textContent), want: k.detail}); } }); });
      (g.not_kept || []).forEach(x => { const row = [...art.querySelectorAll('[data-notkept]')].find(e => norm(e.textContent).includes(norm(x.says)));
        const key = (st.ruleKey || {})[x.by], pages = (SNAP.ruleInfo[key]?.book || []).map(b => 'p.' + b[0]);
        out.nk.push({says: x.says, status: x.status, row: !!row, cls: row?.dataset.notkept, mark: norm(row?.querySelector('.kn')?.textContent), whose: !!row && norm(row.textContent).includes(norm(cap(x.whose))),
          book: !!row && pages.length > 0 && pages.every(p => norm(row.textContent).includes(p)), pics: row ? row.querySelectorAll('img').length : 0}); }); });
    return out; })())`)));
});
const lf = C13.filter(x => x.missing || x.lift);
check('no-lift-box', !lf.length, lf.map(x => `${x.name}: ${x.missing ? 'not in the snapshot' : x.lift + ' lift box(es)'}`).join('; '));
const rp = C13.filter(x => !x.missing && x.rep.some(r => r.shown !== r.says.replace(/\.?$/, '.') || !r.underTitle || !r.once));
check('bucket-replaces', C13.some(x => x.rep && x.rep.length) && !rp.length, rp.map(x => `${x.name}: ${JSON.stringify(x.rep)}`).join('; ') || 'no bucket with replaces checked');
const nkb = C13.filter(x => !x.missing && x.nk.some(n => !n.row || n.cls !== n.status || n.mark !== (n.status === 'closed' ? '✕' : '') || !n.whose || !n.book));
check('not-kept-said', C13.some(x => x.nk && x.nk.some(n => n.status === 'closed')) && C13.some(x => x.nk && x.nk.some(n => n.status === 'release')) && !nkb.length,
  nkb.map(x => `${x.name}: ${JSON.stringify(x.nk)}`).join('; ') || 'no closed AND release line checked');
const zb = C13.filter(x => !x.missing && x.zero.some(z => /\bcm\b/.test(z.det) || z.det !== z.want));
check('zero-kind-no-size', C13.some(x => x.zero && x.zero.length) && !zb.length, zb.map(x => `${x.name}: ${JSON.stringify(x.zero)}`).join('; ') || 'no 0-quota kind checked');
// gear 9: the sentence names no hook when nothing restricts it (the data's words, never "any hook")
const ah = Object.entries(gear).filter(([, r]) => /any hook/i.test(r.shown || ''));
check('gear-no-any-hook', !ah.length, ah.map(([w, r]) => `${w}: ${JSON.stringify(r.shown)}`).join('; '));
// Ape Creek (a Bella Coola tributary) on both days, with quick pills
const A13 = await page('w=ape&d=501', async b => JSON.parse(await b.evaluate(`JSON.stringify((() => { const w = WATERS.ape; if (!w) return {missing: true};
  return {days: Object.keys(w.snap).map(Number), name: document.querySelector('#hdr .ttl')?.textContent.trim(), pills: [...document.querySelectorAll('#c-w [data-cw^="ape|"]')].map(e => e.dataset.cw)}; })())`)));
check('ape-creek-front', !A13.missing && [1010, 501].every(d => A13.days.includes(d) && A13.pills.some(p => p.split('|')[1] === String(d))) && A13.name === 'Ape Creek', JSON.stringify(A13));

// ---------- round 14 ----------
// (2) Licence: the Under 16 tab shows the default angler's under-16 answer as the SAME open "You need" list as 16 or older
// (the page's own label and words), outside every fold; on the Vedder and on a classified water (whose classified box both
// ages share). In each other residency's dropdown the tab's own age comes first.
const CLW = await page('w=vedder&d=1010', async b => JSON.parse(await b.evaluate(`JSON.stringify(Object.entries(WATERS).flatMap(([w, x]) =>
  Object.entries(x.snap).map(([d, ids]) => [w, d, [].concat(ids)[0]])).find(([, , i]) => /class="classbox"/.test(SNAP.states[i]?.licence?.adult || '')) || null)`)));
const L14 = [];
for (const [w, d] of [['vedder', '1010'], ...(CLW ? [[CLW[0], CLW[1]]] : [])]) for (const age of ['adult', 'u16'])
  await page(`w=${w}&d=${d}&age=${age}&at=%23g-licence`, async b => {
  L14.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const st = ST(), L = document.getElementById('g-licence'), norm = t => (t || '').replace(/\\s+/g, ' ').trim();
    const mineH = st.licence[${JSON.stringify(age === 'u16' ? 'u16_res' : 'adult')}], ad = document.createElement('div'); ad.innerHTML = st.licence.adult;
    const label = norm(ad.querySelector('.needs .lbl')?.textContent), md = document.createElement('div'); md.innerHTML = mineH;
    const shared = new Set([...ad.querySelectorAll('.classbox')].map(e => norm(e.textContent)));
    const answer = [...md.querySelectorAll('.needs:not(.u16) li > b, .classbox > b')].filter(e => !shared.has(norm(e.parentElement.textContent))).map(e => norm(e.textContent));
    const lists = [...(L?.querySelectorAll('.needs:not(.u16sum)') || [])].filter(n => !n.closest('details'));
    const list = lists[0], items = list ? [...list.querySelectorAll('li > b')].map(e => norm(e.textContent)) : [];
    const order = [...(L?.querySelectorAll('details.others') || [])].map(f => norm(f.querySelector('.u16h')?.textContent));
    return {w: S.w, age: S.age, label, lists: lists.length, shownLabel: norm(list?.querySelector('.lbl')?.textContent), visible: !!list && list.getBoundingClientRect().height > 0,
      answer, items, order, want: [...ad.querySelectorAll('#who-age option')].find(o => o.value === (S.age === 'u16' ? 'under_16' : '16_plus'))?.textContent.trim()}; })())`)));
});
const lb14 = L14.filter(x => x.lists !== 1 || !x.label || x.shownLabel !== x.label || !x.visible || !x.answer.length || x.answer.join('|') !== x.items.join('|')
  || !x.order.length || x.order.some(o => o !== x.want));
check('licence-u16-you-need', L14.length === 4 && !lb14.length, lb14.map(x => JSON.stringify(x)).join('; ') || `checked ${L14.length} of 4 (classified water: ${JSON.stringify(CLW)})`);

// (3, rows 13) a count over SOME of a bucket's kinds is a nested group (`buckets[].groups`): drawn inside its bucket,
// BEFORE the bucket's size notes and its other kinds, the group's own words first, then each of its kinds with its own size
// (Bella Coola May 1, both parts, and the first other state the snapshot has with a group)
const NG = await page('w=vedder&d=1010', async b => JSON.parse(await b.evaluate(`JSON.stringify(Object.entries(WATERS).flatMap(([w, x]) =>
  Object.entries(x.snap).flatMap(([d, ids]) => [].concat(ids).map((i, p) => [w, d, p, i]))).filter(([, , , i]) => (SNAP.states[i]?.rows || []).some(r => (r.group?.buckets || []).some(bk => (bk.groups || []).length))))`)));
const NGW = [...NG.filter(x => x[0] === 'bellacoola' && x[1] === '501'), ...NG.filter(x => x[0] !== 'bellacoola').slice(0, 1)];
const N14 = [];
for (const [w, d, p] of NGW) await page(`w=${w}&d=${d}&p=${p}`, async b => {
  N14.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const st = ST(), norm = t => (t || '').replace(/\\s+/g, ' ').trim(), out = [];
    document.querySelectorAll('#keep article.row.ledger').forEach(art => { const g = (st.rows[+(art.dataset.row || 'r0').slice(1)] || {}).group; if (!g || !g.headline) return;
      const secs = [...art.querySelectorAll('section.bkt')];
      (g.buckets || []).forEach((bk, i) => (bk.groups || []).forEach((sg, j) => { const el = secs[i]?.querySelectorAll('[data-qgroup]')[j];
        const kids = el ? [...el.querySelectorAll('.krow')].map(r => ({nm: norm(r.querySelector('.nm')?.textContent), det: norm(r.querySelector('.kdet')?.textContent), n: norm(r.querySelector('.kn')?.textContent)})) : [];
        const later = el ? [...secs[i].querySelectorAll('.bnote, :scope > .krow')] : [];
        out.push({says: sg.says, el: !!el, first: !!el && norm(el.firstElementChild?.textContent).startsWith(norm(sg.says.split(':')[0])), shown: norm(el?.textContent).includes(norm(sg.says.split(':')[1] || sg.says)),
          kinds: sg.kinds.every(k => kids.some(x => x.nm === norm(k.name) && x.det === norm(k.detail).replace(/(\\d) cm/g, '$1 cm').replace(/ /g, ' ') && x.n === (k.n >= sg.n ? '' : String(k.n)))),
          before: later.every(e => el.compareDocumentPosition(e) & Node.DOCUMENT_POSITION_FOLLOWING),
          once: sg.kinds.every(k => [...art.querySelectorAll('.krow .nm')].filter(e => norm(e.textContent) === norm(k.name)).length === 1)}); })); });
    return {at: location.hash, out}; })())`)));
});
const nb14 = N14.filter(x => !x.out.length || x.out.some(o => !o.el || !o.first || !o.shown || !o.kinds || !o.before || !o.once));
check('nested-count-group', NGW.length >= 2 && !nb14.length, nb14.map(x => JSON.stringify(x)).join('; ') || `states checked: ${JSON.stringify(NGW)}`);

// (4, rows 13) the steelhead presence line sits INSIDE the steelhead's own section, once: its kind row, its not-kept line,
// or its own row (a sheet when that row is under Other fish); no card-level steelhead note but known_no_rules
const PRES = [['vedder', '1010', 0], ['vedder', '615', 0], ['bellacoola', '501', 1], ['ape', '1010', 0]];
const morePres = await page('w=vedder&d=1010', async b => JSON.parse(await b.evaluate(`JSON.stringify((() => { const all = Object.entries(WATERS).flatMap(([w, x]) =>
  Object.entries(x.snap).flatMap(([d, ids]) => [].concat(ids).map((i, p) => [w, d, p, SNAP.states[i]]))).filter(x => x[3]);
  const pick = f => { const x = all.find(([w, , , s]) => !['vedder', 'bellacoola', 'ape'].includes(w) && f(s)); return x ? [x[0], x[1], x[2]] : null; };
  return [pick(s => s.rows.some(r => (r.group?.buckets || []).some(bk => (bk.groups || []).some(g => g.kinds.some(k => k.presence))))),
    pick(s => s.rows.some(r => r.presence && !r.group?.headline && r.kind !== 'closed')), pick(s => s.steelLine === 'known_no_rules')].filter(Boolean); })())`)));
const P14 = [];
for (const [w, d, p] of [...PRES, ...morePres]) await page(`w=${w}&d=${d}&p=${p}`, async b => {
  P14.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const st = ST(), norm = t => (t || '').replace(/\\s+/g, ' ').trim(), K = document.getElementById('keep'), out = [];
    const arts = [...K.querySelectorAll('article.row.ledger')].filter(a => !a.closest('details'));
    const artOf = i => arts.find(a => a.dataset.row === 'r' + i);
    let placed = 0;
    st.rows.forEach((r, i) => { const g = r.group, art = artOf(i);
      (g?.buckets || []).forEach(bk => [...bk.kinds, ...(bk.groups || []).flatMap(s => s.kinds)].forEach(k => { if (!k.presence) return; placed++;
        const row = [...(art?.querySelectorAll('.krow') || [])].find(e => norm(e.querySelector('.nm')?.textContent) === norm(k.name));
        out.push({where: 'kind ' + k.name, ok: !!row && norm(row.textContent).includes(norm(k.presence.says))}); }));
      (g?.not_kept || []).forEach(x => { if (!x.presence) return; placed++;
        const row = [...(art?.querySelectorAll('[data-notkept]') || [])].find(e => norm(e.textContent).includes(norm(x.says)));
        out.push({where: 'not_kept ' + x.says, ok: !!row && norm(row.textContent).includes(norm(x.presence.says))}); });
      if (r.presence) { if (art) { placed++; out.push({where: 'row ' + i, ok: norm(art.textContent).includes(norm(r.presence.says))}); }
        else if (!arts.length && K.querySelector('.banner')) out.push({where: 'closed row ' + i + ' (closed card: no sections drawn)', ok: true, closed: true});
        else if ([...K.querySelectorAll('.krow')].some(e => { const fs = [...e.querySelectorAll('[data-open^="sp:"]')].map(x => x.dataset.open.slice(3));
            return fs.length === r.members.length && r.members.every(c => fs.includes(c)); })) { placed++;   // the row drawn as a kind of another card
          const e = [...K.querySelectorAll('.krow')].find(e => { const fs = [...e.querySelectorAll('[data-open^="sp:"]')].map(x => x.dataset.open.slice(3)); return fs.length === r.members.length && r.members.every(c => fs.includes(c)); });
          out.push({where: 'kind of another row ' + i, ok: norm(e.textContent).includes(norm(r.presence.says))}); }
        else { const o = Object.entries(OTHERS_).find(([id, v]) => id.startsWith(sid() + '|') && new RegExp('data-row="r' + i + '"').test(v.html));
          out.push({where: 'sheet row ' + i, ok: !!o && norm(new DOMParser().parseFromString(o[1].html, 'text/html').body.textContent).includes(norm(r.presence.says))}); } } });
    const words = [...new Set([...st.rows.flatMap(r => [r.presence, ...(r.group?.not_kept || []).map(x => x.presence), ...(r.group?.buckets || []).flatMap(bk => [...bk.kinds, ...(bk.groups || []).flatMap(s => s.kinds)].map(k => k.presence))]).filter(Boolean).map(p => norm(p.says))])];
    const said = words.reduce((n, w) => n + norm(K.textContent).split(w).length - 1, 0);
    return {at: location.hash, steelLine: st.steelLine, placed, said, out}; })())`)));
});
const pb14 = P14.filter(x => x.out.some(o => !o.ok) || x.said !== x.placed);
check('presence-in-section', P14.some(x => x.out.some(o => o.where.startsWith('not_kept'))) && P14.some(x => x.out.some(o => o.where.startsWith('kind'))) && !pb14.length,
  pb14.map(x => JSON.stringify(x)).join('; ') || JSON.stringify(P14.map(x => [x.at, x.out.length])));

// (5, rows 13) a weekday window is said in words (`rows[].week`), in place of the day's keep / release words: the
// Kootenay LWA kokanee sheet on Saturday and Monday shows the data's sentence, no date in it, no day's verdict beside it
const W14 = [];
for (const d of ['1010', '1012']) await page(`w=klwa&d=${d}`, async b => {
  W14.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const st = ST(), norm = t => (t || '').replace(/\\s+/g, ' ').trim(), out = [];
    st.rows.forEach((r, i) => { if (!r.week) return;
      const o = Object.entries(OTHERS_).find(([id, v]) => id.startsWith(sid() + '|') && new RegExp('data-row="r' + i + '"').test(v.html));
      const t = o ? norm(new DOMParser().parseFromString(sheetHtml('other:' + o[0]), 'text/html').body.textContent) : '';
      out.push({says: r.week.says, shown: t.includes(norm(r.week.says)), dates: (t.match(/\\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \\d{1,2}\\b/g) || []).length,
        verdict: /Release every one here|\\d+ a day here|\\buntil\\b|\\bfrom (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\b/.test(t), sheet: !!o}); });
    return {d: S.d, out}; })())`)));
});
const wb14 = W14.filter(x => !x.out.length || x.out.some(o => !o.sheet || !o.shown || o.dates || o.verdict));
check('week-in-words', W14.length === 2 && !wb14.length, wb14.map(x => JSON.stringify(x)).join('; '));

// (6, rows 13) not-kept lines: one per status and deciding row, in the data's order and words (Vedder May 15 "Rainbow trout
// and cutthroat trout: release every one"; Ape Creek's char named with its members)
const K14 = [];
for (const [w, d] of [['vedder', '515'], ['ape', '1010']]) await page(`w=${w}&d=${d}`, async b => {
  K14.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const st = ST(), norm = t => (t || '').replace(/\\s+/g, ' ').trim(), out = [];
    document.querySelectorAll('#keep article.row.ledger').forEach(art => { const g = (st.rows[+(art.dataset.row || 'r0').slice(1)] || {}).group; if (!g || !(g.not_kept || []).length) return;
      out.push({want: g.not_kept.map(x => norm(x.says)), shown: [...art.querySelectorAll('[data-notkept] .nm')].map(e => norm(e.textContent))}); });
    return {at: location.hash, out}; })())`)));
});
const kb14 = K14.filter(x => !x.out.length || x.out.some(o => o.want.join('|') !== o.shown.join('|')));
check('not-kept-merged', !kb14.length && K14.some(x => x.out.some(o => o.want.some(t => /rainbow trout and cutthroat trout/i.test(t)))), kb14.map(x => JSON.stringify(x)).join('; ') || JSON.stringify(K14));

// ---------- round 15 ----------
// (8) no bare big number outside a bucket's column of counts: a line nobody may keep says it in words, so no big 0 stands
// alone above "How it adds up" (Vedder May 15's rainbow / cutthroat release line sat a 0 over "Keep 2 more here")
const Z15 = [];
for (const hash of ['w=vedder&d=515', 'w=bellacoola&d=501&p=0', 'w=ape&d=1010', 'w=ape&d=501', 'w=vedder&d=1010']) await page(hash, async b => {
  Z15.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const norm = t => (t || '').replace(/\\s+/g, ' ').trim();
    const own = e => norm([...e.childNodes].filter(n => n.nodeType === 3).map(n => n.textContent).join(''));
    const bare = [...document.querySelectorAll('#keep .kn, #keep .num, #keep .bkn, #keep .qgn')].filter(e => own(e) === '0')
      .map(e => norm(e.closest('.krow, .rhead')?.textContent).slice(0, 70));
    const w = document.querySelector('#keep .worked'), prev = w?.previousElementSibling;
    // persona (round 15): a kind's number that only repeats its bucket's shared number (2+ kinds) is not drawn; the
    // not-kept lines come after "How it adds up"
    const st = ST(), rep = [], order = [];
    document.querySelectorAll('#keep article.row.ledger').forEach(art => { const g = (st.rows[+(art.dataset.row || 'r0').slice(1)] || {}).group; if (!g || !g.headline) return;
      const secs = [...art.querySelectorAll('section.bkt')], wk = art.querySelector('.worked'), nk = art.querySelector('.nkt');
      if (wk && nk) order.push(!!(wk.compareDocumentPosition(nk) & Node.DOCUMENT_POSITION_FOLLOWING));
      (g.buckets || []).forEach((bk, i) => { const n = bk.cap ?? +((bk.between || '').match(/\d+/) || [])[0]; if (!n || bk.kinds.length + (bk.groups || []).length < 2) return;
        [...(secs[i]?.querySelectorAll(':scope > .krow .kn') || [])].forEach(e => { if (+own(e) >= n) rep.push(norm(e.closest('.krow').textContent).slice(0, 50)); }); }); });
    return {at: location.hash, bare, rep, order: order.every(Boolean), aboveWorked: prev && !prev.matches('section.bkt') ? [...prev.querySelectorAll('.kn, .num')].map(own).filter(t => /^\\d+$/.test(t)) : []}; })())`)));
});
const zb15 = Z15.filter(x => x.bare.length || x.aboveWorked.length || x.rep.length || !x.order);
check('no-bare-zero', Z15.length === 5 && !zb15.length, zb15.map(x => JSON.stringify(x)).join('; '));

// (8) the licence tab says which residency (and age) the open answer is for: the page's own residency and age names, a
// label right above the answer, outside every fold, on both tabs
const R15 = [];
for (const age of ['adult', 'u16']) await page(`w=vedder&d=1010&age=${age}&at=%23g-licence`, async b => {
  R15.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const norm = t => (t || '').replace(/\\s+/g, ' ').trim(), L = document.getElementById('g-licence');
    const ad = document.createElement('div'); ad.innerHTML = ST().licence.adult;
    const res = norm(ad.querySelector('#who-residency option[value="resident"]')?.textContent), ag = norm(ad.querySelector('#who-age option[value="' + (S.age === 'u16' ? 'under_16' : '16_plus') + '"]')?.textContent);
    const lab = [...(L?.querySelectorAll('[data-for-res]') || [])].filter(e => !e.closest('details'));
    const ans = [...(L?.querySelectorAll('.needs, .classbox') || [])].find(e => !e.closest('details'));
    const pl = (ST().licLabel || {})[S.age === 'u16' ? 'u16_res' : 'adult'];   // licence 5: the data's own profile label
    return {age: S.age, want: pl ? pl : res && ag ? res + ' · ' + ag : '', shown: lab.map(e => norm(e.textContent)), before: !!lab[0] && !!ans && !!(lab[0].compareDocumentPosition(ans) & Node.DOCUMENT_POSITION_FOLLOWING),
      visible: !!lab[0] && lab[0].getBoundingClientRect().height > 0}; })())`)));
});
const rb15 = R15.filter(x => !x.want || x.shown.length !== 1 || x.shown[0] !== x.want || !x.before || !x.visible);
check('licence-says-residency', R15.length === 2 && !rb15.length, rb15.map(x => JSON.stringify(x)).join('; '));

// (3) a row that answers by weekday reads as ONE explanation in its sheet (Kootenay LWA kokanee, Sat + Mon): one status
// block first — the day, the data's weekday status, the data's weekday sentence — then "How this was decided" (whose
// day-parts week-parts-decided checks); no status drawn a second time; no rule line twice
const K15 = [];
for (const d of ['1010', '1012']) await page(`w=klwa&d=${d}`, async b => {
  K15.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const st = ST(), norm = t => (t || '').replace(/\\s+/g, ' ').trim(), out = [];
    st.rows.forEach((r, i) => { if (!r.week) return;
      const o = Object.entries(OTHERS_).find(([id, v]) => id.startsWith(sid() + '|') && new RegExp('data-row="r' + i + '"').test(v.html));
      const body = o ? new DOMParser().parseFromString(sheetHtml('other:' + o[0]), 'text/html').body : null;
      const box = body?.querySelector('[data-wkstat]'), dec = [...(body?.querySelectorAll('.lbl') || [])].find(e => /how this was decided/i.test(e.textContent));
      const parts = Object.values(r.week).filter(Array.isArray).flat().filter(p => p && typeof p === 'object' && p.says);
      const lines = [...(body?.querySelectorAll('.pline > span:first-child') || [])].filter(e => !e.closest('details')).map(e => norm(e.textContent));
      out.push({sheet: !!o, box: !!box, says: !!box && norm(box.textContent).includes(norm(r.week.says)),
        status: box ? box.querySelectorAll('.num, .pill, [data-wkv]').length : 0, outside: body ? [...body.querySelectorAll('.num, .pill')].filter(e => !box?.contains(e)).length : -1,
        order: !!box && !!dec && !!(box.compareDocumentPosition(dec) & Node.DOCUMENT_POSITION_FOLLOWING),
        parts: parts.every(p => norm(box?.textContent).includes(norm(p.says))), dup: lines.filter((t, j) => lines.indexOf(t) !== j)}); });
    return {d: S.d, out}; })())`)));
});
const kb15 = K15.filter(x => !x.out.length || x.out.some(o => !o.sheet || !o.box || !o.says || !o.status || o.outside || !o.order || o.dup.length));
check('week-sheet-one-story', K15.length === 2 && !kb15.length, kb15.map(x => JSON.stringify(x)).join('; '));

// ---------- round 15 phase 2 (rows 14, licence 5, display 11) ----------
// (1) a size cap is a boxed group (`buckets[].sizes`) in the SAME box as a count group: inside its bucket, the data's words
// with its number big, its rule's book page, a picture of every fish it binds; no size note left as a bare line
// (5) an own-rule bucket's shared cap (`buckets[].between`) is said big under its title, once
const S15 = [];
for (const hash of ['w=bellacoola&d=501&p=0', 'w=bellacoola&d=501&p=1', 'w=vedder&d=1010', 'w=vedder&d=515', 'w=ape&d=501']) await page(hash, async b => {
  S15.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const st = ST(), norm = t => (t || '').replace(/\\s+/g, ' ').trim(), sz = [], bw = [];
    document.querySelectorAll('#keep article.row.ledger').forEach(art => { const g = (st.rows[+(art.dataset.row || 'r0').slice(1)] || {}).group; if (!g || !g.headline) return;
      const secs = [...art.querySelectorAll('section.bkt')];
      (g.buckets || []).forEach((bk, i) => {
        (bk.sizes || []).forEach((z, j) => { const el = secs[i]?.querySelectorAll('[data-sgroup]')[j], key = (st.ruleKey || {})[z.by];
          const pages = (SNAP.ruleInfo[key]?.book || []).map(x => 'p.' + x[0]), fs = el ? [...el.querySelectorAll('[data-open^="sp:"]')].map(e => e.dataset.open.slice(3)) : [];
          sz.push({says: z.says, el: !!el, box: !!el && el.classList.contains('qgrp'), words: !!el && norm(el.textContent).includes(norm(z.says).replace(/(\\d) cm/g, '$1 cm').slice(0, 20)),
            big: norm(el?.querySelector('.qgn')?.textContent) === String(z.n), book: !!el && pages.length > 0 && pages.every(p => norm(el.textContent).includes(p)),
            pics: z.species.filter(c => (SNAP.fish[c] || FISHPIC(c).length)).every(c => fs.includes(c))}); });
        if (bk.between) { const el = secs[i]?.querySelector('[data-between]'), n = (bk.between.match(/\\d+/) || [])[0];
          bw.push({between: bk.between, el: !!el, big: norm(el?.querySelector('.bkn')?.textContent) === n, words: norm(el?.textContent) === norm(bk.between).replace(/\\s+/g, ' '),
            once: norm(art.textContent).split(norm(bk.between)).length <= 2}); } });
      art.querySelectorAll('.bnote').forEach(e => { if (/\\bcm\\b/.test(e.textContent)) sz.push({says: norm(e.textContent), el: false, bareNote: true}); }); });
    return {at: location.hash, sz, bw}; })())`)));
});
const sb15 = S15.filter(x => x.sz.some(z => !z.el || !z.box || !z.words || !z.big || !z.book || !z.pics));
check('size-group-box', S15.some(x => x.sz.length) && !sb15.length, sb15.map(x => JSON.stringify(x)).join('; ') || 'no size group checked');
const bb15 = S15.filter(x => x.bw.some(z => !z.el || !z.big || !z.words || !z.once));
check('bucket-between-big', S15.some(x => x.bw.length) && !bb15.length, bb15.map(x => JSON.stringify(x)).join('; ') || 'no between checked');

// (2) the Under 16 tab shows, OPEN and outside every fold, what each residency's under-16 angler needs (the frame's
// `under_16`): one item per residency, its label and its words, the tab's own residency marked; the 16-or-older tab does not
const U15 = [];
for (const age of ['adult', 'u16']) await page(`w=vedder&d=1010&age=${age}&at=%23g-licence`, async b => {
  U15.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const st = ST(), norm = t => (t || '').replace(/\\s+/g, ' ').trim(), L = document.getElementById('g-licence');
    const box = [...(L?.querySelectorAll('[data-u16sum]') || [])].filter(e => !e.closest('details')), inFold = L ? L.querySelectorAll('details [data-u16sum]').length : -1;
    const items = box[0] ? [...box[0].querySelectorAll('li')].map(li => ({t: norm(li.textContent), mine: li.classList.contains('mine')})) : [];
    return {age: S.age, want: (st.u16 || []).map(x => ({label: x.label, says: x.says, mine: x.residency === 'resident'})), boxes: box.length, inFold, items,
      visible: !!box[0] && box[0].getBoundingClientRect().height > 0}; })())`)));
});
const ub15 = U15.filter(x => !x.want.length || x.inFold || (x.age === 'u16' ? (x.boxes !== 1 || !x.visible || x.items.length !== x.want.length
  || x.want.some((w, i) => !x.items[i] || !x.items[i].t.includes(norm0(w.label)) || !x.items[i].t.includes(norm0(w.says)) || x.items[i].mine !== w.mine)) : x.boxes));
function norm0(t) { return (t || '').replace(/\s+/g, ' ').trim(); }
check('u16-by-residency', U15.length === 2 && !ub15.length, ub15.map(x => JSON.stringify(x)).join('; '));

// (3) the weekday sheet: the status box says the data's weekday status; "How this was decided" lists EVERY day-part with its
// days, its words and its rules' book pages (today's marked), then each restatement note once; the Other fish line
// says the split under the fish's name
const WK15 = [];
for (const d of ['1010', '1012']) await page(`w=klwa&d=${d}`, async b => {
  WK15.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const st = ST(), norm = t => (t || '').replace(/\\s+/g, ' ').trim(), out = [];
    st.rows.forEach((r, i) => { if (!r.week) return;
      const o = Object.entries(OTHERS_).find(([id, v]) => id.startsWith(sid() + '|') && new RegExp('data-row="r' + i + '"').test(v.html));
      const body = o ? new DOMParser().parseFromString(sheetHtml('other:' + o[0]), 'text/html').body : null, box = body?.querySelector('[data-wkstat]');
      const parts = [...(body?.querySelectorAll('[data-wkpart]') || [])];
      const pg = rs => rs.flatMap(x => (SNAP.ruleInfo[(st.ruleKey || {})[x]]?.book || []).map(b => 'p.' + b[0]));
      out.push({status: r.week.status, list: [...document.querySelectorAll('#keep [data-owk]')].some(e => norm(e.textContent) === norm(r.week.status)), inBox: !!box && (!r.week.status || norm(box.textContent).includes(norm(r.week.status))),
        parts: (r.week.parts || []).map((p, j) => ({days: p.days, ok: !!parts[j] && !box?.contains(parts[j]) && norm(parts[j].textContent).includes(norm(p.days)) && norm(parts[j].textContent).toLowerCase().includes(norm(p.says).toLowerCase())
          && pg(p.rules || []).every(x => norm(parts[j].textContent).includes(x)) && parts[j].classList.contains('today') === !!p.today})),
        restates: (r.restates || []).map(x => ({says: x.says, n: norm(body?.textContent).split(norm(x.says)).length - 1}))}); });
    return {d: S.d, out}; })())`)));
});
const wkb15 = WK15.filter(x => !x.out.length || x.out.some(o => !o.status || !o.list || !o.inBox || !o.parts.length || o.parts.some(p => !p.ok) || o.restates.some(r => r.n !== 1)));
check('week-parts-decided', WK15.length === 2 && !wkb15.length, wkb15.map(x => JSON.stringify(x)).join('; '));

const failed = results.filter(x => !x[1]).length;
console.log(`${results.length - failed} passed, ${failed} failed — ${path.relative(process.cwd(), PAGE)}`);
process.exit(failed ? 1 : 0);
