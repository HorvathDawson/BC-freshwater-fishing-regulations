// Golden strings for the app port: every string and structure the regulations prototype RENDERS, per state (water ×
// day × part), per tab, per licence profile, every sheet it can open, and the pin bar over a scroll of the Keep tab.
//   node app/design/regs-v11/port/golden.mjs [--ref HEAD|<sha>|WORKTREE] [--out dir] [--only <state-id regex>]
//   -> <out>/golden.json   { meta, states: {<id>: State}, sheets: {<global key>: Sheet} }
// The app's buildRegsPage() is tested against this file (handoff/PLAN/app-integration/tests.md). The extractor reads the
// RENDERED page through the prototype's own globals (S, go, render, sheetHtml, pinUpdate, WATERS, SNAP), so a design change
// lands in the goldens by re-running this script — not by editing it — unless the change renames a class it reads
// (then `meta.misses` lists the selectors that found nothing anywhere; a nonzero count is a signal to update EXTRACT below).
import fs from 'node:fs'; import path from 'node:path'; import { pathToFileURL } from 'node:url';
import { open } from '../tools/cdp.mjs';
import { stage, args } from './stage.mjs';

const A = args(); fs.mkdirSync(A.out, { recursive: true });
const S = stage(A.ref);

// Runs inside the page. Returns a JSON string. Kept as one function so it is easy to read against the prototype's classes.
function EXTRACT(onlyRe) {
  const norm = t => (t || '').replace(/\s+/g, ' ').trim();
  const BLOCK = 'P,LI,H1,H2,H3,H4,DIV,SECTION,ARTICLE,SUMMARY,BLOCKQUOTE,UL,OL,DETAILS,FIGURE,DT,DD';
  // every visible text line of an element, one per block (details' hidden content included: a fold is still data)
  const lines = el => { if (!el) return []; const c = el.cloneNode(true);
    c.querySelectorAll('svg, script, style').forEach(x => x.remove());
    c.querySelectorAll(BLOCK).forEach(x => x.prepend(document.createTextNode('\n')));
    c.querySelectorAll('span, small, em, button, a, i').forEach(x => x.prepend(document.createTextNode(' ')));
    c.querySelectorAll('br').forEach(x => x.replaceWith(document.createTextNode('\n')));
    return c.textContent.split('\n').map(norm).filter(Boolean); };
  const used = {}; const q = (el, sel) => { const r = el ? el.querySelector(sel) : null; used[sel] = (used[sel] || 0) + (r ? 1 : 0); return r; };
  const qa = (el, sel) => { const r = el ? [...el.querySelectorAll(sel)] : []; used[sel] = (used[sel] || 0) + r.length; return r; };
  const tx = (el, sel) => { const e = sel ? q(el, sel) : el; return e ? norm(e.textContent) : null; };
  const books = el => qa(el, 'a.book').map(a => ({ text: norm(a.textContent), pdf: +((a.getAttribute('href') || '').match(/#page=(\d+)/) || [])[1] || null }));
  const src = el => el ? { who: tx(el, '.srcs'), open: q(el, '.srcs')?.dataset.open || null, pages: books(el) } : null;
  const chg = el => qa(el, ':scope > .chg, :scope .chg').map(c => ({ now: c.classList.contains('now'), chip: tx(c, '.chgi'), text: tx(c, ':scope > span'), when: tx(c, 'span > b'), inForce: !!q(c, 'em') }));
  const pics = el => qa(el, ':scope > .kpics > button, :scope > .kb > .kpics > button').map(b => ({ code: (b.dataset.open || '').replace(/^sp:/, ''), caption: tx(b, '.kcap') }));
  const glOf = el => qa(el, '[data-gl]').map(b => b.dataset.gl);
  const sc = document.getElementById('sc');

  const header = () => { const h = document.getElementById('hdr'), pl = q(h, '[data-place]'), dot = q(h, '.hdate .dot'), go = q(h, '.hgo');
    const col = dot ? (dot.getAttribute('style') || '').match(/var\(--(\w+)\)/)?.[1] : null;
    return { name: tx(h, '.ttl b'), day: tx(h, '.hdate .hd2 b'), date: tx(h, '.hdate .hd2 > span'), dateAria: q(h, '.hdate')?.getAttribute('aria-label') || null,
      dot: col, place: pl ? { shown: norm(pl.textContent), full: pl.dataset.full || '', short: pl.dataset.short || '' } : null,
      action: go ? { kind: go.dataset.map ? 'map' : go.dataset.open === 'parts' ? 'parts' : 'other', label: tx(go, 'span'), aria: go.getAttribute('aria-label') } : null,
      tabs: qa(h, '.segm[role=tablist] button').map(b => ({ sec: b.dataset.sec, text: norm(b.textContent) })) }; };

  const strip = keep => { const w = q(keep, '.wstrip'); if (!w) return null; const bar = q(w, '.sbar'), today = q(bar, '.today');
    return { line: qa(w, '.wsline > span').map(s => ({ cls: s.className, text: norm(s.textContent) })),
      status: tx(w, '.wsstat'), statusClass: (q(w, '.wsstat')?.className || '').replace('wsstat', '').trim(),
      bar: bar ? [...bar.children].filter(i => !i.classList.contains('today')).map(i => ({ cls: i.className, flex: +((i.getAttribute('style') || '').match(/flex:(\d+)/) || [])[1] })) : [],
      today: today ? (today.getAttribute('style') || '') : null,
      months: qa(w, '.smonths > *').map(m => norm(m.textContent)), legend: qa(w, '.slegend > span').map(s => ({ swatch: q(s, 'i')?.className || '', text: norm(s.textContent) })),
      lines: lines(w) }; };

  const banner = keep => { const b = q(keep, '.banner'); if (!b) return null;
    return { big: qa(b, '.big').map(e => norm(e.textContent)), paras: qa(b, ':scope > p').map(e => norm(e.textContent)), golink: qa(b, '.golink').map(e => ({ text: norm(e.textContent), day: e.dataset.day || e.querySelector('[data-day]')?.dataset.day || null })),
      chain: qa(keep, '.closechain li').map(li => ({ text: tx(li, '.cd'), scope: tx(li, '.cd .muted'), src: tx(li, '.srcbtn') })), lines: lines(b) }; };

  const inherited = keep => { const i = q(keep, '.inh'); if (!i) return null;
    return { label: tx(i, '.ltag'), lines: qa(i, '[data-inherited]').map(p => ({ text: norm(p.textContent), pages: books(p) })), cautions: qa(i, '[data-caution]').map(p => norm(p.textContent)) }; };

  const kind = k => ({ t: 'kind', kind: k.dataset.kind || null, sp: (k.dataset.sp || '').split(' ').filter(Boolean), folded: !!k.dataset.folded, zero: k.classList.contains('kzero'),
    says: tx(k, ':scope > .kb > .ks'), n: tx(k, ':scope > .kb > .ks .kn'), howtell: !!q(k, '[data-howtell]'),
    size: tx(k, ':scope > .kb > .kmeta .szc'), q: tx(k, ':scope > .kb > .kmeta .kq'), pics: pics(k),
    sizeNote: tx(k, ':scope > .kb > .ksn'), also: qa(k, ':scope > .kb > .kac').map(e => norm(e.textContent)),
    duty: tx(k, ':scope > .kb > .duty'), presence: tx(k, ':scope > .kb > .kpres'), terms: glOf(k), lines: lines(k) });
  const tierNode = tn => { const t = q(tn, ':scope > .tier');
    return { t: 'tier', tier: tn.dataset.tnode, tid: tn.dataset.tid || null, kicker: tx(t, ':scope > .tkick'), line: tx(t, ':scope > .tl'),
      big: qa(t, ':scope > .tl .tn').map(b => ({ n: norm(b.textContent), second: b.classList.contains('tn2') })),
      pages: books(q(t, ':scope > .tl .tbk')), terms: glOf(q(t, ':scope > .tl')), sub: tx(t, ':scope > .tsub'),
      origin: (o => o ? { text: norm(o.textContent), says: tx(o, ':scope > span'), howtell: !!q(o, '[data-howtell]'), pages: books(o) } : null)(q(t, ':scope > .torigin')),
      children: [...(q(tn, ':scope > .tch')?.children || [])].map(node) }; };
  const node = el => el.matches('section.tnode') ? tierNode(el) : el.matches('.krow.kt') ? kind(el)
    : el.matches('.krel28') ? { t: 'release', names: tx(el, '.nm'), pics: pics(el) } : { t: 'other', cls: el.className, lines: lines(el) };

  const group = art => { const pin = q(art, ':scope > .ghd.gpin'), sub = q(art, ':scope > .ghd.gsub'), num = q(pin, '.num'), rt = q(pin, '.rtitle');
    const title = rt ? norm([...rt.childNodes].filter(n => !(n.nodeType === 1 && n.classList.contains('scope'))).map(n => n.textContent).join(' ')) : null;
    const tree = q(art, ':scope > .qtree');
    return { row: art.dataset.row || null, title, scope: tx(rt, '.scope'),
      head: num ? { form: num.dataset.tot || null, nolimit: num.classList.contains('nolimit'), rootTier: num.dataset.rootTier || null,
        lead: tx(num, '[data-share-lead]'), leadN: tx(num, '.gln'), big: tx(num, '.ghn'), ofTotal: tx(num, '[data-of-total]'),
        whyHere: tx(num, '[data-why-here]'), shareOf: tx(num, '[data-share-of]'), terms: glOf(num), text: norm(num.textContent) } : null,
      headLines: lines(pin), origin: sub ? { lines: lines(sub), tag: tx(sub, '.gtag'), howtell: !!q(sub, '[data-howtell]'), pages: books(sub) } : null,
      tree: tree ? [...tree.children].map(node) : null, treeRoot: tree?.dataset.root || null,
      buckets: tree ? null : qa(art, ':scope > section.bkt').map(b => lines(b)),
      notKept: qa(art, ':scope > .nkt > .nkr').map(r => ({ status: r.dataset.notkept, says: tx(r, '.nm'), whose: tx(r, '.s2'), mark: tx(r, ':scope > .kn'), presence: tx(r, '.kpres'), pics: pics(r), pages: books(r) })),
      worked: (w => w ? { says: tx(w, '.wsays'), lines: qa(w, 'li').map(li => ({ at: li.className === 'wh' ? 'here' : 'elsewhere', q: tx(li, 'span'), a: tx(li, 'b') })) } : null)(q(art, ':scope > .worked')),
      quiet: tx(art, ':scope > .gquiet'), more: tx(art, ':scope > .linkrow'), lines: lines(art) }; };

  const keepTab = () => { const keep = document.getElementById('keep');
    const others = [...keep.querySelectorAll('section.sec')].find(s => /Other fish/.test(tx(s, 'h2') || ''));
    return { order: [...keep.children].map(c => (c.tagName.toLowerCase() + '.' + [...c.classList].join('.'))),
      strip: strip(keep), inherited: inherited(keep), banner: banner(keep),
      groups: qa(keep, 'article.row.ledger').filter(a => a.classList.contains('g28')).map(group),
      plainRows: qa(keep, 'article.row').filter(a => !a.classList.contains('g28')).map(a => lines(a)),
      otherFish: others ? qa(others, '.orow').map(r => ({ thumb: q(r, '.th')?.dataset.open?.replace(/^sp:/, '') || (q(r, '.sil') ? null : null), name: tx(r, '.on'), week: tx(r, '.owk'), open: q(r, '.obtn')?.dataset.open || null, lines: lines(q(r, '.obtn')) })) : null,
      possession: (p => p ? { label: tx(p, '.on'), value: tx(p, 'b'), open: q(p, '[data-open]')?.dataset.open || null } : null)(q(keep, '.posrow')),
      whereNot: (n => n ? { heading: tx(n, 'h3'), scope: tx(n, 'h3 .scope'), items: qa(n, 'li').map(li => ({ text: norm(li.textContent), pages: books(li) })) } : null)(q(keep, '.nofish')),
      lines: lines(keep) }; };

  const yn = el => el ? qa(el, '.ync').map(c => ({ col: c.dataset.col, head: tx(c, '.ynh'), items: qa(c, 'li').map(li => ({ mark: tx(li, 'i'), name: tx(li, 'b'), note: tx(li, 'small') })) })) : null;
  const gearTab = () => { const g = document.getElementById('gear');
    const mode = q(g, '.gmodew');
    return { closed: tx(g, ':scope > .foot'), day: tx(g, '.gday'),
      mode: mode ? { value: tx(mode, '.gval'), terms: glOf(q(mode, '.gmode')), changes: chg(mode), src: src(q(mode, '.isrc')) } : null,
      lines: qa(g, '.glines > .gline').map(l => ({ k: l.dataset.gline, label: tx(l, '.glab'), values: qa(l, '.gval > b').map(b => norm(b.textContent)), terms: glOf(q(l, '.gval')), changes: chg(l), src: src(q(l, '.isrc')) })),
      bait: (b => b ? { label: tx(b, '.gxl'), notes: qa(b, ':scope > .gxs').map(e => norm(e.textContent)), dated: chg(b), list: yn(q(b, '.yn')), src: src(q(b, '.isrc')) } : null)(q(g, '.gxc[data-k=bait]')),
      lines_: lines(g) }; };
  const methodsTab = () => { const m = document.getElementById('methods');
    return { closed: tx(m, ':scope > .foot'),
      ways: (w => w ? { label: tx(w, '.gxl'), list: yn(q(w, '.yn')), src: src(q(w, '.isrc')) } : null)(q(m, '.gxc[data-k=methods]')),
      boats: (b => b ? { label: tx(b, '.gxl'), items: qa(b, 'li').map(li => norm(li.textContent)), src: src(q(b, '.isrc')) } : null)(q(m, '.gxc[data-k=boats]')),
      reference: (r => r ? { label: tx(r, 'summary .lab'), title: tx(r, 'summary b'), sub: tx(r, 'summary em'), open: r.open,
        groups: qa(r, ':scope > .hh').map(h => ({ head: norm(h.textContent), items: [...(h.nextElementSibling?.matches('ul') ? h.nextElementSibling.querySelectorAll('li') : [])].map(li => norm(li.textContent)) })), src: src(q(r, '.isrc')) } : null)(q(m, '.gref')),
      lines: lines(m) }; };
  const licenceTab = () => { const l = document.getElementById('licence'); if (!l) return null; const ch = q(l, '.lchoose');
    const ans = q(l, '#g-licence'), body = ans ? [...ans.children].filter(c => !c.matches('h3, .lchoose, .resfor, .isrc')) : [];
    return { question: tx(ch, ':scope > .lab'), questionTerms: glOf(q(ch, ':scope > .lab')),
      axes: qa(ch, '[role=radiogroup]').map(g => ({ axis: g.dataset.axis, aria: g.getAttribute('aria-label'), options: qa(g, '[role=radio]').map(b => ({ text: norm(b.textContent), checked: b.getAttribute('aria-checked') === 'true', age: b.dataset.age || null, res: b.dataset.res || null })) })),
      label: tx(l, '.resfor'),
      needs: qa(l, '.needs').map(n => ({ label: tx(n, '.lbl'), items: qa(n, 'li').map(li => ({ text: norm(li.textContent), pages: books(li) })) })),
      blocks: body.map(b => ({ cls: b.className, lines: lines(b) })),
      src: (s => s ? { who: tx(s, '.srcs'), open: q(s, '.srcs')?.dataset.open || null } : null)(q(l, '[data-isrc-lic]')), lines: lines(l) }; };

  // the sheets this state can open: every data-open / data-gl / data-rule / data-howtell key on the page
  const sheetKeys = () => { const ks = new Set(), root = sc;
    root.querySelectorAll('[data-open]').forEach(e => { if (!['date'].includes(e.dataset.open)) ks.add(e.dataset.open); });
    root.querySelectorAll('[data-gl]').forEach(e => ks.add('gl:' + e.dataset.gl));
    root.querySelectorAll('[data-rule]').forEach(e => ks.add('rule:' + e.dataset.rule));
    root.querySelectorAll('[data-howtell]').forEach(() => ks.add('hatch'));
    root.querySelectorAll('[data-src]').forEach(e => ks.add('src:' + sid() + '|' + e.dataset.src));
    return [...ks]; };
  const sheetOf = k => { let h = ''; try { h = sheetHtml(k); } catch (e) { return { error: String(e) }; } if (!h) return null;
    const d = document.createElement('div'); d.innerHTML = h;
    return { title: tx(d, '.shead h2'), sub: tx(d, '.shead > p'), folds: qa(d, 'details').map(x => ({ summary: tx(x, 'summary'), open: x.open })),
      pages: books(d), lines: lines(q(d, '.sbody')), nested: [...new Set([...d.querySelectorAll('[data-open],[data-rule],[data-gl]')].map(e => e.dataset.open || (e.dataset.rule ? 'rule:' + e.dataset.rule : 'gl:' + e.dataset.gl)))] }; };
  const GLOBAL = /^(rule:|gl:|sp:|hatch$)/;

  // the pin bar over a scroll of the Keep tab: the anchors (content offsets at scrollTop 0) and every distinct bar
  const pins = () => { const hdr = document.getElementById('hdr'), keep = document.getElementById('keep'), bar = document.querySelector('#pins .pinbar');
    sc.scrollTop = 0; const top0 = sc.getBoundingClientRect().top, Y = e => Math.round(e.getBoundingClientRect().top - top0 + sc.scrollTop), H = e => Math.round(e.getBoundingClientRect().height);
    const anchors = qa(keep, 'article.row.ledger.g28').map(a => ({ row: a.dataset.row, top: Y(a), height: H(a),
      pin: (p => p ? { top: Y(p), height: H(p) } : null)(q(a, ':scope > .ghd.gpin')),
      tiers: qa(a, 'section.tnode').map(tn => ({ tid: tn.dataset.tid, tier: tn.dataset.tnode, top: Y(tn), height: H(tn), head: (t => t ? { top: Y(t), height: H(t) } : null)(q(tn, ':scope > .tier')) })),
      worked: (w => w ? { top: Y(w), height: H(w) } : null)(q(a, ':scope > .worked')) }));
    const end = keep.offsetTop + keep.offsetHeight, samples = []; let last = null;
    let error = null;
    for (let y = 0; y <= end; y += 8) { sc.scrollTop = y; try { pinUpdate(); } catch (e) { error = error || { y, error: String(e) }; continue; }
      const ls = bar.hidden ? [] : [...bar.querySelectorAll('.pl')].map(p => { const c = p.cloneNode(true), s = c.querySelector('.pls'); const sub = s ? norm(s.textContent) : null; s?.remove();
        return { cls: p.className.replace(/^pl\s*/, ''), text: norm(c.textContent), sub, big: norm(p.querySelector('b')?.textContent) }; });
      const key = JSON.stringify(ls); if (key !== last) { samples.push({ y: sc.scrollTop, lines: ls, height: bar.hidden ? 0 : Math.round(bar.getBoundingClientRect().height) }); last = key; } }
    sc.scrollTop = 0; try { pinUpdate(); } catch (e) {}
    return { error, viewport: { width: sc.clientWidth, height: sc.clientHeight, header: hdr.offsetHeight, LH: 26, DROP: 110 }, anchors, samples }; };

  const PROFILES = [['adult', 'resident'], ['adult', 'non_resident'], ['adult', 'non_resident_alien'], ['u16', 'resident'], ['u16', 'non_resident'], ['u16', 'non_resident_alien']];
  const out = { sheets: {} };
  const list = []; for (const [w, W] of Object.entries(WATERS)) for (const [d, x] of Object.entries(W.snap)) [].concat(x).forEach((id, p) => { if (id && SNAP.states[id]) list.push({ id, w, d: +d, p }); });
  const one = ({ id, w, d, p }) => {
    go({ w, d, p, sec: 'keep', sheet: '', map: 0, age: 'adult', res: 'resident', tot: '', chg: '', theme: '' });
    const st = SNAP.states[id];
    const rec = { id, w, d, p, water: st.name, waterKind: st.kind, part: st.part, md: st.md, sub: st.sub, parts: st.parts,
      header: header(), keep: keepTab(), gear: gearTab(), methods: methodsTab(), licence: {}, sheets: {}, pins: pins() };
    const keys = new Set(sheetKeys());
    for (const [age, res] of PROFILES) { go({ age, res }, true); const L = licenceTab(); if (L) rec.licence[`${age}|${res}`] = L; sheetKeys().forEach(k => keys.add(k)); }
    go({ age: 'adult', res: 'resident' }, true);
    for (const k of [...keys].sort()) { if (GLOBAL.test(k)) { if (!(k in out.sheets)) out.sheets[k] = sheetOf(k); rec.sheets[k] = '@'; } else {
      const m = k.match(/^licsrc:lic\|[^|]+\|(\w+)$/); if (m) { const P = { adult: ['adult', 'resident'], adult_non: ['adult', 'non_resident'], adult_alien: ['adult', 'non_resident_alien'], u16_res: ['u16', 'resident'], u16_non: ['u16', 'non_resident'], u16_alien: ['u16', 'non_resident_alien'] }[m[1]];
        if (P) go({ age: P[0], res: P[1] }, true); }
      rec.sheets[k] = sheetOf(k); if (m) go({ age: 'adult', res: 'resident' }, true); } }
    return rec; };
  window.__PORT = { list: list.filter(x => !onlyRe || new RegExp(onlyRe).test(x.id)), one: i => JSON.stringify(one(window.__PORT.list[i])), sheets: () => JSON.stringify(out.sheets), used: () => JSON.stringify(used) };
  return window.__PORT.list.length;
}

const b = await open(`${pathToFileURL(S.page).href}#shot=1`, { offline: true });
try {
  await new Promise(r => setTimeout(r, 600));
  const t0 = Date.now();
  const n = await b.evaluate(`(${EXTRACT.toString()})(${JSON.stringify(A.only || '')})`);
  const out = { states: {} };
  for (let i = 0; i < n; i++) { const r = JSON.parse(await b.evaluate(`window.__PORT.one(${i})`)); out.states[r.id] = r;
    if (i % 10 === 0) process.stderr.write(`  ${i + 1}/${n} ${r.id} ${Date.now() - t0} ms\n`); }
  out.sheets = JSON.parse(await b.evaluate('window.__PORT.sheets()')); out.used = JSON.parse(await b.evaluate('window.__PORT.used()'));
  const misses = Object.entries(out.used).filter(([, n]) => n === 0).map(([s]) => s);
  delete out.used;
  const meta = { generator: 'app/design/regs-v11/port/golden.mjs', ref: S.ref, sha: S.sha, version: S.version, states: Object.keys(out.states).length,
    sheets: Object.keys(out.sheets).length, profiles: 6, misses, ms: Date.now() - t0 };
  const file = path.join(A.out, 'golden.json');
  fs.writeFileSync(file, JSON.stringify({ meta, ...out }, null, 1));
  console.log(`golden: ${meta.states} states, ${meta.sheets} global sheets, regs ${meta.version} @ ${meta.sha} -> ${path.relative(process.cwd(), file)} (${(fs.statSync(file).size / 1e6).toFixed(1)} MB, ${meta.ms} ms)`);
  if (misses.length) console.log(`selectors that matched nothing in any state (expected for features no snapshot state has; a NEW entry after a design change means EXTRACT needs a look):\n  ${misses.join('\n  ')}`);
} finally { await b.close(); S.cleanup(); }
