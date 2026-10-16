// Snapshot page v36 for regs-v11 (from regs-v10's snap.mjs; adds the parts the water map steps to): renders pipeline/deliver/answers/reference/page_v36.html in headless Chrome,
// drives the page's own controls for each state below, and copies the page's own HTML fragments, sheets and data.
// Nothing is written by hand: every string in the output is read off the rendered page or its embedded answers.
//   node app/design/regs-v11/tools/snap.mjs            -> app/design/regs-v11/v36snap.js
import fs from 'node:fs'; import path from 'node:path'; import { fileURLToPath, pathToFileURL } from 'node:url';
import { open } from './cdp.mjs';
import { execFileSync } from 'node:child_process';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(HERE, '../../../..');
const FROM = 'pipeline/deliver/answers/reference/page_v36.html';
const OUT = process.argv[2] ? path.resolve(process.argv[2]) : path.join(HERE, '..', 'v36snap.js');
const YEAR = 2026;   // the prototype's year (Oct 10 2026 is a Saturday: the Kootenay weekend rule)

// which page states the prototype shows: [snapshot id ({v} = each part the page offers), water name, month*100+day]
const SPECS = [
  ['vedder_1010', 'Vedder River', 1010], ['vedder_515', 'Vedder River', 515], ['vedder_615', 'Vedder River', 615],
  ['klwa_1010', 'Kootenay Lake — Lower West Arm', 1010], ['klwa_1012', 'Kootenay Lake — Lower West Arm', 1012],
  ['cow{v}_1010', 'Cowichan River', 1010],
  // v11: the other parts the water map steps to (pipeline/tools/water_map_export.py groups), so "Show regulations
  // for this part" has a page to show. One day each: the prototype's "today".
  ['chw{v}_1010', 'Chilliwack River', 1010],
  ['klmb_1010', 'Kootenay Lake — Main Body', 1010], ['kluwa_1010', 'Kootenay Lake — Upper West Arm', 1010],
  // round 2: plain waters (no row of their own: only zone + province rules). Page v36 offers 32 waters; these are its
  // only ones with no entries in the export. It offers no plain lake.
  ['anderson_1010', 'Anderson River', 1010], ['wilkes_1010', 'Wilkes Creek', 1010],
  // round 11 (A3): a Quinsam River tributary whose summer opening is the Quinsam's exemption (display `inherited`):
  // Jul 20, inside the exemption, and the prototype's today
  ['iron{v}_720', 'Iron River', 720], ['iron{v}_1010', 'Iron River', 1010],
  // every other water page v36 offers (owner 2026-10-15: "the same creeks as the old one"), each part, on the
  // prototype's today: ids x<page index>p<part>_<md>, or x<page index>p_<md> for a water with no part picker
  ['*', null, 1010],
];

// runs inside the page: uses the page's own globals (state, WATERS, renderAll, openRule, …)
function extract(SPECS, YR) {
  const $ = id => document.getElementById(id);
  const sheet = () => ({ title: $('shTitle').textContent, sub: $('shSub').textContent, body: $('shBody').innerHTML });
  const out = { states: {}, rules: {}, ruleInfo: {}, gloss: {}, sources: {}, fish: {}, howtell: null, strip: SEC.display.strip };
  const codes = new Set(), glIds = new Set();
  const scan = html => { const d = document.createElement('div'); d.innerHTML = html;
    d.querySelectorAll('[data-gl]').forEach(e => glIds.add(e.dataset.gl));
    d.querySelectorAll('[data-fish]').forEach(e => codes.add(e.dataset.fish));
    return [...d.querySelectorAll('[data-rule]')].map(e => e.dataset.rule).filter(Boolean); };
  const takeRules = keys => keys.forEach(k => { if (out.rules[k]) return; openRule(k); out.rules[k] = sheet();
    const i = DRULES[RIX[k]] || {}; out.ruleInfo[k] = { book: i.book ?? null, applies: i.applies ?? null, applies_trib: i.applies_trib ?? null }; });
  const shortOf = (water, pi) => { const p = DWATERS[water.id]?.parts?.[pi] || {}; const o = {};
    for (const [k, v] of Object.entries(p)) if (/short/i.test(k)) o[k] = v; return o; };
  const runsOf = () => { const runs = [];
    for (const md of DAYS) { const f = frameIx(PLACE, 'display', segOf(PLACE, md)), b = f == null ? null : SEC.display.frames[f].strip;
      const l = runs[runs.length - 1]; if (l && l[0] === b) l[1]++; else runs.push([b, 1]); } return runs; };
  const go = (wi, pi, md) => { state.wi = wi; state.pi = pi; YEAR = YR; state.md = md; state.kit = 'licence';
    state.who = { residency: 'resident', age: '16_plus', guidance: 'non_guided', status: 'none' }; renderAll(); };

  for (const [idT, name, md] of SPECS) {
    const wi = WATERS.findIndex(w => w.name === name); if (wi < 0) throw new Error('no water ' + name);
    go(wi, defaultPart(wi), md);
    const water = WATERS[wi], sel = $('part');
    const parts = sel ? [...sel.options].map(o => ({ v: o.value, t: o.textContent, ...shortOf(water, +o.value) })) : [];
    const each = idT.includes('{v}') && parts.length ? parts.map(p => p.v) : [null];
    for (const v of each) {
      if (v != null) go(wi, +v, md);
      const id = idT.replace('{v}', v ?? '');
      $('card').querySelectorAll('[data-src]').forEach(e => { e.dataset.src2 = `${id}|${e.dataset.src}`; });
      const card = $('card').innerHTML;
      const st = { name: water.name, kind: water.kind, sub: document.querySelector('[data-f="sub"]').textContent, parts,
        part: sel ? $('part').value : null, md, cardHTML: card, runs: runsOf(), rows: MODEL.rows.map(({ raw: r }) => ({ kind: st_(r.kind), members: r.members, daily: r.daily ?? null, group: r.group ?? null, elsewhere: r.elsewhere ?? null })) };
      // each row's kinds, in the page's order (its "Each kind" list): the fish codes of each, for the pictures
      st.kinds = MODEL.rows.map(row => { try { if (!row.conds) return null; let it = speciesItems(row, rowConds(row)); if (!it) return null;
        if (it.length === 1 && it[0].plain) it = []; return it.map(x => ({ key: x.key, members: [...x.members], static: !x.facts })); } catch (e) { return null; } });
      takeRules(scan(card));
      // gear v6 (answers W4): the Lines count row's own sentence (`says`, boat included) and every rule deciding it
      // (`cites`, display indices -> keys via D.rid), read off the page's gear frame for this day
      { const lp = gearAt(PLACE, md)?.counts?.lines_per_angler;
        st.lines = lp ? { says: lp.says ?? null, cites: (lp.cites || []).map(i => D.rid[i]).filter(Boolean) } : null;
        if (st.lines) takeRules(st.lines.cites); }
      // gear 7 (ROUND11 A2): the day's terminal gear as ONE sentence, its glossary terms, the rules it cites and the
      // other days whose sentence differs (cites: display indices -> keys via D.rid)
      { const sm = gearAt(PLACE, md)?.summary, key = i => D.rid[i];
        st.summary = sm ? { says: sm.says, terms: [...(sm.terms || [])], cites: (sm.cites || []).map(key).filter(Boolean),
          changes: (sm.changes || []).map(c => ({ when: c.when, says: c.says, cites: (c.cites || []).map(key).filter(Boolean) })) } : null;
        if (st.summary) { st.summary.terms.forEach(t => glIds.add(t)); takeRules([...st.summary.cites, ...st.summary.changes.flatMap(c => c.cites)]); } }
      // display 8 (ROUND11 A3): a lift another water's row carries here (the frame's `inherited`): its words and the
      // rule key behind its `by` (the lifting row's rule, whose book page the prototype cites)
      { const f = frameIx(PLACE, 'display', segOf(PLACE, md)), fr = f == null ? null : SEC.display.frames[f];
        st.inherited = (fr?.inherited || []).map(x => ({ says: x.says, by: D.rid[x.by] || null }));
        takeRules(st.inherited.map(x => x.by).filter(Boolean)); }
      // the rule key behind every `by` the quota card names (lifts, each part of a combined lift, buckets): page v36's
      // display index -> key (D.rid), so the prototype cites every lifting rule's book page
      { const bys = new Set(); MODEL.rows.forEach(({ raw: r }) => { const g = r.group; if (!g) return;
          (g.lifts || []).forEach(l => { bys.add(l.by); (l.each || []).forEach(e => bys.add(e.by)); });
          (g.buckets || []).forEach(b => bys.add(b.by)); });
        st.ruleKey = {}; for (const b of bys) if (b != null && D.rid[b]) st.ruleKey[b] = D.rid[b];
        takeRules(Object.values(st.ruleKey)); }
      (MODEL.rows || []).forEach(r => [...(r.raw?.members || []), ...(r.raw?.group?.kinds || [])].forEach(c => codes.add(c)));
      // the row sources sheets
      for (const s of new Set([...$('card').querySelectorAll('[data-src]')].map(e => e.dataset.src))) {
        openRowSources(s);
        // each source card's rule key, in the sheet's order (rowSources: the ones that decide it, then the overruled)
        const items = rowSources(MODEL.rows[+s.slice(1)]); $('shBody').querySelectorAll('.sc').forEach((e, i) => { if (items[i]) e.dataset.rule = items[i].r.key; });
        out.sources[`${id}|${s}`] = sheet(); takeRules(scan(out.sources[`${id}|${s}`].body)); }
      if (!out.howtell && $('card').querySelector('[data-howtell]')) { $('card').querySelector('[data-howtell]').click();
        const h = sheet(); h.body = h.body.replace(/<svg[\s\S]*?<\/svg>/g, ''); out.howtell = h; }
      // gear & licence: the tiles, the page's kit, and each tile's pane
      const kit = $('kit'), grid = kit.querySelector('.kgrid');
      st.kitTop = kit.innerHTML;
      if (grid) { st.kitTiles = grid.innerHTML; st.panes = {};
        for (const k of [...grid.querySelectorAll('[data-kit]')].map(b => b.dataset.kit)) { state.kit = k; renderKit();
          st.panes[k] = $('kit').querySelector('.kpane').innerHTML; takeRules(scan(st.panes[k])); }
        st.licence = {};
        for (const [lk, residency, age] of [['adult', 'resident', '16_plus'], ['u16_res', 'resident', 'under_16'], ['u16_non', 'non_resident', 'under_16'], ['u16_alien', 'non_resident_alien', 'under_16']]) {
          state.kit = 'licence'; state.who = { ...state.who, residency, age }; renderKit(); st.licence[lk] = $('kit').querySelector('.kpane').innerHTML; takeRules(scan(st.licence[lk])); }
      } else { st.kitTiles = ''; st.panes = {}; st.licence = { adult: '', u16_res: '', u16_non: '', u16_alien: '' }; }
      takeRules(scan(st.kitTop));
      out.states[id] = st;
    }
  }
  // round 12 (owner 3): each term's own fields too (its plain words, example, the book's words, PRINTED pages), so the
  // prototype can fold the book's words and link each page
  for (const g of glIds) { openGloss(g); const t = GLOSS.get(g) || {};
    out.gloss[g] = { ...sheet(), term: { says: t.says ?? null, example: t.example ?? null, quote: t.quote ?? null, pages: [...(t.pages || [])] } }; }
  // every fish the page names (v12: any species with a photo opens its picture sheet, not only FISHART's): its name, and
  // FISHART's look-for notes where it has them
  for (const c of codes) out.fish[c] = FISHART.has(c) ? { name: FISHART.SPECIES[c].name || spName(c), tips: FISHART.SPECIES[c].tips } : { name: spName(c), tips: [] };
  $('scrim').hidden = true;
  return out;
}

const page = path.join(REPO, FROM);
// the page reads the clock for "today": pin it to the prototype's today (Oct 10, YEAR) so every state is read as of then
const TODAY = `${YEAR}-10-10T12:00:00`;
const pin = `(() => { const T = new Date('${TODAY}').getTime(), D = Date; class P extends D { constructor(...a) { super(...(a.length ? a : [T])); } static now() { return T; } } window.Date = P; })();`;
const b = await open(pathToFileURL(page).href, { init: pin });
try {
  for (let t = 0; t < 300 && !(await b.evaluate(`typeof renderAll === 'function' && !!document.querySelector('#card *')`)); t++) await new Promise(r => setTimeout(r, 100));
  // the snapshot of every water is tens of MB: one evaluate result that size never arrives over the DevTools socket,
  // so it stays in the page as a string and is read back in slices
  // one water at a time (a hang names its water), each result read back in slices, merged here
  const names = await b.evaluate(`WATERS.map(w => w.name)`), named = new Set(SPECS.map(x => x[1]).filter(Boolean));
  const specs = SPECS.flatMap(sp => sp[0] !== '*' ? [sp]
    : names.map((n, wi) => named.has(n) ? null : [`x${wi}p{v}_${sp[2]}`, n, sp[2]]).filter(Boolean));
  const within = (p, ms, what) => Promise.race([p, new Promise((_, r) => setTimeout(() => r(new Error(`snap: ${what} took over ${ms / 1000} s`)), ms))]);
  const snap = { states: {}, rules: {}, ruleInfo: {}, gloss: {}, sources: {}, fish: {}, howtell: null };
  for (const sp of specs) {
    const len = await within(b.evaluate(`(window.__SNAP = JSON.stringify((${extract})(${JSON.stringify([sp])}, ${YEAR}))).length`), 120000, sp[1]);
    let raw = '';
    for (let i = 0; i < len; i += 4e6) raw += await b.evaluate(`window.__SNAP.slice(${i}, ${i + 4e6})`);
    const one = JSON.parse(raw);
    for (const k of ['states', 'rules', 'ruleInfo', 'gloss', 'sources', 'fish']) Object.assign(snap[k], one[k]);
    snap.howtell = snap.howtell || one.howtell; snap.strip = one.strip;
    process.stderr.write(`  ${sp[0]} ${sp[1]}: ${Object.keys(one.states).length} states\n`);
  }
  // pictures are drawn by the prototype (FISHART / the app's photos); page whitespace is collapsed
  const tidy = o => typeof o === 'string' ? o.replace(/<svg[\s\S]*?<\/svg>/g, '').replace(/\n\s*/g, ' ')
    : Array.isArray(o) ? o.map(tidy) : o && typeof o === 'object' ? Object.fromEntries(Object.entries(o).map(([k, v]) => [k, tidy(v)])) : o;
  Object.assign(snap, tidy(snap));
  // round 12: the ONE printed -> PDF page map (pipeline.common.synopsis_pages, read off the repo PDF's own footers), so a
  // printed page the data names (a glossary term's `pages`) links to the book's PDF page; nothing typed by hand
  snap.pdfPages = JSON.parse(execFileSync(path.join(REPO, '.venv/bin/python'), ['-c',
    'import json; from pipeline.common.synopsis_pages import printed_to_pdf; print(json.dumps(printed_to_pdf()))'], { cwd: REPO, encoding: 'utf8' }));
  if (!Object.keys(snap.pdfPages).length) throw new Error('snap: no printed -> PDF page map (is the synopsis PDF in data/?)');
  snap.read = new Date().toISOString(); snap.from = FROM;
  const head = `/* regs-v11: a snapshot of page v36, ${FROM}, read ${snap.read}.\n   Generated by regs-v11/tools/snap.mjs, which renders the page and copies its own HTML and data; nothing here is hand-written. */\n`;
  fs.writeFileSync(OUT, head + 'window.SNAP = ' + JSON.stringify(snap) + ';\n');
  console.log(`wrote ${path.relative(REPO, OUT)}: ${Object.keys(snap.states).length} states, ${Object.keys(snap.rules).length} rules, ${Object.keys(snap.gloss).length} glossary terms, ${Object.keys(snap.sources).length} source sheets, ${Object.keys(snap.fish).length} fish`);
} finally { await b.close(); }
