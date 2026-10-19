/* regs-diagram gallery: five more drawings of the same limits — V Venn/Euler, J jars, F flow, T decision tree, K card
   deck — all from ONE model built out of each group's tree + caps + table in data.js (page v36's own numbers and words).
   The page loads this before its own script and calls GALLERY.render(w, formId) for the picked form.
   Nothing drawn here is hand-written except the form names, the one-line how-to-read under each, and the structural
   words in WORDS below (the page's footer lists them). Set geometry is never per-water: every region is laid out from
   the model (CSS grid for containment, a measured SVG overlay for the sets that cut across it). */
window.GALLERY = (() => {
'use strict';
const D = window.DIAG;
const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
const fishName = c => D.fish[c] || c;
const WORDS = { release: 'release', closed: 'closed', nolimit: 'no daily limit', outside: 'Outside every limit — these go back', counts: 'counts toward', which: 'Which fish?', long: 'How long?', origin: 'Hatchery or wild?', keep: 'keep', wild: 'wild', catch: 'A fish on your line', also: 'Also on this water', ownLimit: 'its own limit' };
const book = (w, by) => { const r = w.rules[by]; if (!r || !r.book.length) return ''; return `<span class="book" title="${esc(r.title)}">Rule book p.${r.book.map(b => b[0]).join(', ')}</span>`; };
const lower = (nm, i) => i && !/^(Dolly|Arctic)/.test(nm) ? nm[0].toLowerCase() + nm.slice(1) : nm; // the data's own style: "Brook trout, brown trout and cutthroat trout"
const joinNames = list => { const n = list.map(fishName).map(lower); return n.length <= 1 ? n.join('') : n.slice(0, -1).join(', ') + ' and ' + n[n.length - 1]; };
const bandText = (lo, hi) => hi == null ? (lo ? `over ${lo} cm` : 'any size') : lo ? `${lo}–${hi} cm` : `${hi} cm or less`; // fallback only; the table's own band words win
const num = n => n == null ? '∞' : n;

/* ---------- the model: sets (limits) and chips (fish kinds × size band), from one group's tree ---------- */
function groupModel(w, r) {
  const g = r.group, T = g.table, caps = g.caps || [];
  const sets = [], chips = [], byKey = new Map(); let sid = 0, cid = 0;
  const mkSet = o => { const s = Object.assign({ id: 'S' + (++sid), members: new Set(), items: [], parent: null, depth: 0 }, o); sets.push(s); return s; };
  const shortOf = (says, n) => says.replace(/^(?:only|no more than|up to)\s+/i, '').replace(new RegExp('^' + n + '\\s*'), '').replace(/\.$/, '');
  // an also_counts entry becomes one set, shared by every kind that names it; its words come from the matching cap when one does
  const alsoSet = a => {
    const key = JSON.stringify([[...a.species].sort(), a.n, a.over_cm || 0]);
    if (byKey.has(key)) return byKey.get(key);
    let cap = a.by ? caps.find(c => c.by === a.by && c.take === a.n) : null;
    if (!cap) { const names = a.species.map(s => fishName(s).toLowerCase()); const hits = caps.filter(c => c.take === a.n && names.every(nm => c.says.toLowerCase().includes(nm)) && (!a.over_cm || c.says.includes(a.over_cm + ' cm'))); if (hits.length === 1) cap = hits[0]; }
    const s = mkSet({ kind: a.over_cm > 0 ? 'size' : 'also', n: a.n, over_cm: a.over_cm || 0, species: a.species, says: cap ? cap.says : a.says, short: shortOf(a.over_cm > 0 || !cap ? a.says : cap.says, a.n), by: a.by ?? cap?.by ?? null, overlay: true });
    byKey.set(key, s); return s;
  };
  const lookup = (species, lo, hi) => { // the table's cell for this band: how many, origin, the band's words
    let best = null;
    for (const sp of species) { const bands = T?.bands?.[sp]; if (!bands) continue;
      let i = bands.findIndex(b => b.from === lo && (b.to ?? null) === (hi ?? null)); if (i < 0) i = bands.findIndex(b => b.from <= lo && (b.to == null || (hi != null && hi <= b.to))); if (i < 0) continue;
      const c = T.cells[sp][i]; if (!best || (c.n != null && (best.n == null || c.n < best.n))) best = { n: c.n, origin: c.origin, says: bands[i].says, as: c.as, why: c.why }; }
    return best || {};
  };
  function kindChips(k, container, sizeChain) {
    const applying = sizeChain.map(s => ({ set: s, sp: new Set([...s.species, ...(s.extra ? s.extra.species : [])]), cut: s.over_cm, extraSp: new Set(s.extra ? s.extra.species : []) }));
    for (const a of (k.also_counts || [])) { const s = alsoSet(a); applying.push({ set: s, sp: new Set(a.species), cut: a.over_cm || 0, extraSp: new Set() }); }
    const groups = new Map(); // species of one kind split by which cuts and groups apply to them
    for (const sp of k.species) { const sig = applying.map((a, i) => a.sp.has(sp) ? i : '').join('|'); if (!groups.has(sig)) groups.set(sig, []); groups.get(sig).push(sp); }
    for (const [, species] of groups) {
      const apps = applying.filter(a => a.sp.has(species[0]));
      const cuts = [...new Set(apps.filter(a => a.cut > 0).map(a => a.cut))].sort((a, b) => a - b);
      const name = species.length === k.species.length ? k.name : joinNames(species);
      const column = { name, species, chips: [], kind: k }; container.items.push({ type: 'col', col: column });
      const ranges = k.sizes?.cm || [[0, null]], pieces = [];
      for (const [lo, hi] of ranges) { let a = lo; for (const c of cuts) { if (a < c && (hi == null || c < hi)) { pieces.push([a, c]); a = c; } } pieces.push([a, hi]); }
      for (const [lo, hi] of pieces) {
        const cell = lookup(species, lo, hi);
        const ch = { id: 'C' + (++cid), species, name, lo, hi, band: cell.says || bandText(lo, hi), detail: pieces.length === 1 ? k.detail : (cell.says || bandText(lo, hi)), n: cell.n ?? k.n, origin: cell.origin || null, memberOf: new Set(), kind: k, column, container, extra: null, status: 'keep' };
        for (let c = container; c; c = c.parent) { ch.memberOf.add(c.id); c.members.add(ch.id); }
        for (const a of apps) if (a.cut === 0 || lo >= a.cut) { ch.memberOf.add(a.set.id); a.set.members.add(ch.id); if (a.extraSp.has(species[0]) && a.set.extra) ch.extra = a.set.extra; }
        chips.push(ch); column.chips.push(ch);
      }
    }
  }
  function walk(node, container, sizeChain) {
    if (node.node === 'kind') return kindChips(node, container, sizeChain);
    if (node.tier === 'size') { // a size cap is not a box: its kinds belong to the box above it and it becomes a set that cuts across them
      const [main, extraText] = node.says.split(' — ');
      const s = mkSet({ kind: 'size', n: node.n, over_cm: node.over_cm, species: node.species, says: node.says, short: (main.match(/over \d+ cm \([^)]*\)/) || [main])[0], by: node.by, overlay: true,
        extra: node.extra ? { n: node.extra.n, species: node.extra.species, says: extraText || '', by: node.extra.by } : null });
      node.children.forEach(c => walk(c, container, [...sizeChain, s])); return;
    }
    const lift = (g.lifts || []).find(l => l.by === node.by);
    const s = mkSet({ kind: node.tier, n: node.n, says: node.says, by: node.by, whose: node.whose || null, line: lift ? lift.says : (node.replaces || null), parent: container, depth: container ? container.depth + 1 : 0, overlay: false });
    if (container) container.items.push({ type: 'set', set: s });
    node.children.forEach(c => walk(c, s, sizeChain));
  }
  g.tree.forEach(t => walk(t, null, []));
  for (const s of sets) s.short = s.short ?? s.says.split(' — ')[0].split(' (part')[0];
  // outside every set: the table's bands nobody may keep (grouped by why + band), the not-kept rows, the origin rule
  const outside = [], seen = new Map();
  const nk = new Set((g.not_kept || []).flatMap(x => x.species));
  if (T) for (const sp of Object.keys(T.bands)) { if (nk.has(sp)) continue;
    T.bands[sp].forEach((b, i) => { const c = T.cells[sp][i]; if (c.n !== 0 || c.as) return; const key = b.says + '|' + c.why; if (!seen.has(key)) { seen.set(key, { species: [], band: b.says, why: c.why, status: c.why === 'closed' ? 'closed' : 'release' }); outside.push(seen.get(key)); } seen.get(key).species.push(sp); }); }
  for (const o of outside) o.name = joinNames(o.species);
  for (const x of (g.not_kept || [])) outside.push({ species: x.species, name: joinNames(x.species), band: '', why: x.says, status: x.status, by: x.by });
  const roots = sets.filter(s => !s.overlay && !s.parent);
  return { row: r, g, sets, chips, roots, outside, origin: g.origin || null };
}
function waterModel(w) {
  if (w.__model) return w.__model;
  const groups = w.rows.filter(r => r.group?.tree).map(r => groupModel(w, r));
  const plain = w.rows.filter(r => !r.group?.tree && r.words);
  groups.forEach(index);
  return (w.__model = { groups, plain });
}
// a container's body in data order, consecutive kind columns merged into one grid; columns that share a cross-cutting
// set with a neighbouring box are moved next to it so the set can be drawn as one region
function bodyItems(s) {
  const out = []; let grid = null;
  for (const it of s.items) { if (it.type === 'col') { if (!grid) { grid = { type: 'grid', cols: [] }; out.push(grid); } grid.cols.push(it.col); } else { grid = null; out.push(it); } }
  const overlaySets = ch => [...ch.memberOf].filter(id => id.startsWith('S')).map(id => s.__all?.get(id)).filter(x => x && x.overlay && x.kind === 'also');
  out.forEach((it, i) => { if (it.type !== 'grid') return;
    const before = out.slice(0, i).filter(x => x.type === 'set').map(x => x.set), after = out.slice(i + 1).filter(x => x.type === 'set').map(x => x.set);
    const shares = (col, sets) => col.chips.some(ch => overlaySets(ch).some(o => sets.some(st => [...o.members].some(m => st.members.has(m)))));
    const score = col => shares(col, before) ? -1 : shares(col, after) ? 1 : 0;
    it.cols = it.cols.map((c, j) => [score(c), j, c]).sort((a, b) => a[0] - b[0] || a[1] - b[1]).map(x => x[2]);
    const sizeSets = [...new Set(it.cols.flatMap(c => c.chips.flatMap(ch => [...ch.memberOf])))].map(id => s.__all.get(id)).filter(x => x && x.overlay && x.over_cm > 0);
    const cuts = [...new Set(sizeSets.map(x => x.over_cm))].sort((a, b) => a - b);
    it.rows = cuts.length + 1;
    it.cols.forEach((c, ci) => c.chips.forEach(ch => { ch.col = ci; ch.row = cuts.filter(x => x <= ch.lo).length; }));
    // room above the first row when a ring's tag will sit there (a ring whose members start on row 0, or any ring on a phone)
    it.room = it.cols.some(c => c.chips.some(ch => [...ch.memberOf].some(id => s.__all.get(id)?.overlay)));
  });
  return out;
}
const index = m => { const all = new Map(m.sets.map(s => [s.id, s])); m.sets.forEach(s => s.__all = all); return all; };

/* ---------- the measured overlay: sets that cut across the boxes (size caps, shared counts) as hulls + tags ---------- */
function boxOf(rs) { const x = Math.min(...rs.map(r => r.x)), y = Math.min(...rs.map(r => r.y)); return { x, y, w: Math.max(...rs.map(r => r.x + r.w)) - x, h: Math.max(...rs.map(r => r.y + r.h)) - y }; }
function cluster(mem, foreign, pad) {
  const hits = b => foreign.some(f => f.x < b.x + b.w + pad - 3 && f.x + f.w > b.x - pad + 3 && f.y < b.y + b.h + pad - 3 && f.y + f.h > b.y - pad + 3);
  const all = boxOf(mem); if (!hits(all)) return [all];
  const sorted = [...mem].sort((a, b) => a.y - b.y || a.x - b.x), out = []; let cur = [sorted[0]];
  for (const r of sorted.slice(1)) { if (!hits(boxOf([...cur, r]))) cur.push(r); else { out.push(boxOf(cur)); cur = [r]; } }
  out.push(boxOf(cur)); return out;
}
const overlap = (a, b, m = 0) => a.x < b.x + b.w - m && a.x + a.w > b.x + m && a.y < b.y + b.h - m && a.y + a.h > b.y + m;
function drawOverlay(root) {
  const svg = root.querySelector(':scope > svg.eover'), tags = root.querySelector(':scope > .etags'), models = root.__models || []; if (!svg || !tags) return;
  const R = root.getBoundingClientRect(), rel = el => { const b = el.getBoundingClientRect(); return { x: b.left - R.left, y: b.top - R.top, w: b.width, h: b.height }; };
  const chipEls = [...root.querySelectorAll('[data-chip]')], rects = new Map(chipEls.map(e => [e.dataset.chip, rel(e)]));
  const obstacles = [...root.querySelectorAll('.ehead, .jlabel, .eout, .ekey')].map(rel), placed = [];
  svg.setAttribute('viewBox', `0 0 ${R.width} ${R.height}`); svg.setAttribute('width', R.width); svg.setAttribute('height', R.height);
  let out = ''; tags.innerHTML = '';
  for (const m of models) {
    const sets = m.sets.filter(s => s.overlay && s.members.size).sort((a, b) => b.members.size - a.members.size);
    for (const s of sets) {
      const subs = sets.filter(o => o !== s && o.members.size < s.members.size && [...o.members].every(x => s.members.has(x))).length, pad = 7 + subs * 6;
      const mem = [...s.members].map(id => rects.get(id)).filter(Boolean); if (!mem.length) continue;
      const foreign = chipEls.filter(e => !s.members.has(e.dataset.chip)).map(e => rects.get(e.dataset.chip));
      const tag = document.createElement('div'); tag.className = `etag etag-${s.kind}`; tag.title = s.says;
      tag.innerHTML = `<b>${num(s.n)}</b><span>${esc(s.short)}</span>${s.extra && s.extra.says ? `<em>· ${esc(s.extra.says)}</em>` : ''}`; tags.appendChild(tag);
      const tw = tag.offsetWidth, th = tag.offsetHeight, top = pad + th / 2 + 3; // the hull keeps room under its top edge for the tag that straddles it
      const hulls = cluster(mem, foreign, pad);
      hulls.forEach((h, i) => { out += `<rect class="hull hull-${s.kind}" x="${h.x - pad}" y="${h.y - top}" width="${h.w + 2 * pad}" height="${h.h + pad + top}" rx="${12 + subs * 3}"/>`;
        if (i) { const p = hulls[i - 1]; out += `<line class="hlink hull-${s.kind}" x1="${p.x + p.w / 2}" y1="${p.y + p.h / 2}" x2="${h.x + h.w / 2}" y2="${h.y + h.h / 2}"/>`; } });
      const h = hulls[0], H = { x: h.x - pad, y: h.y - top, w: h.w + 2 * pad, h: h.h + pad + top };
      const cands = [[H.x + 10, H.y - th / 2], [H.x + H.w - tw - 10, H.y - th / 2], [H.x + 10, H.y + H.h - th / 2], [H.x + H.w - tw - 10, H.y + H.h - th / 2], [H.x + H.w + 6, H.y + H.h / 2 - th / 2], [H.x - tw - 6, H.y + H.h / 2 - th / 2]]
        .map(([x, y]) => ({ x: Math.max(0, Math.min(R.width - tw, x)), y, w: tw, h: th }));
      const others = chipEls.filter(e => !s.members.has(e.dataset.chip)).map(e => rects.get(e.dataset.chip));
      const free = c => ![...others, ...obstacles, ...placed].some(o => overlap(c, o, 2)) && c.y >= 0;
      const at = cands.find(free) || cands[0]; placed.push(at);
      tag.style.left = at.x + 'px'; tag.style.top = at.y + 'px';
    }
  }
  svg.innerHTML = out;
}
const ro = new ResizeObserver(entries => { for (const e of entries) requestAnimationFrame(() => drawOverlay(e.target)); });
function mountOverlay(el) { el.__pending = true; requestAnimationFrame(() => drawOverlay(el)); ro.observe(el); if (document.fonts) document.fonts.ready.then(() => drawOverlay(el)); }

/* ---------- V: Venn / Euler ---------- */
const chipHTML = ch => `<div class="chip${ch.n === 0 ? ' k0' : ''}" data-chip="${ch.id}" style="--c:${(ch.col ?? 0) + 1};--r:${(ch.row ?? 0) + 1}" title="${esc(ch.kind.sentence || '')}"><b class="cn">${num(ch.n)}</b><span class="cname">${esc(ch.name)}</span><span class="cband">${esc(ch.detail)}</span>${ch.origin ? `<span class="corg">${esc(ch.origin)}</span>` : ''}${ch.extra ? `<span class="cextra" title="${esc(ch.extra.says)}">+${ch.extra.n}</span>` : ''}</div>`;
const leafCols = s => s.items.reduce((t, it) => t + (it.type === 'col' ? 1 : leafCols(it.set)), 0);
function setHTML(w, s, g = 1) {
  const body = bodyItems(s).map(it => it.type === 'set' ? setHTML(w, it.set, leafCols(it.set)) : `<div class="egrid${it.rows > 1 ? ' cut' : ''}${it.room ? ' room' : ''}" style="--k:${it.cols.length};--rr:${it.rows};--g:${it.cols.length}">${it.cols.flatMap(c => c.chips).map(chipHTML).join('')}</div>`).join('');
  return `<section class="eset e-${s.kind}" data-set="${s.id}" style="--g:${g}"><div class="ehead"><b class="en">${num(s.n)}</b><div class="esay">${esc(s.says)} ${book(w, s.by)}${s.line ? `<span class="eline">${esc(s.line)}</span>` : ''}</div></div><div class="ebody">${body}</div></section>`;
}
const outsideHTML = m => (m.outside.length || m.origin) ? `<div class="eout"><div class="lab">${WORDS.outside}</div><div class="ochips">${m.outside.map(o => `<div class="chip oc ${o.status}"><span class="cname">${esc(o.name)}</span>${o.band ? `<span class="cband">${esc(o.band)}</span>` : ''}<span class="owhy">${esc(o.why)}</span></div>`).join('')}${m.origin ? `<p class="onote">${esc(m.origin)}</p>` : ''}</div></div>` : '';
const keyHTML = (w, m) => `<ul class="ekey">${m.sets.map(s => `<li><i class="sw sw-${s.kind}"></i>${s.n != null && s.says.startsWith(String(s.n)) ? '' : `<b>${num(s.n)}</b> `}${esc(s.says)} ${book(w, s.by)}</li>`).join('')}</ul>`;
function formV(w) {
  const M = waterModel(w);
  const groups = M.groups.map(m => { index(m); return `<article class="grp"><header class="ghead"><h3>${esc(m.row.words?.title || '')}${m.row.words?.scope ? `<span class="scope">${esc(m.row.words.scope)}</span>` : ''}</h3></header>
    <div class="euler" data-group="${m.row.words?.title || ''}">${m.roots.map(s => setHTML(w, s)).join('')}${outsideHTML(m)}<svg class="eover" aria-hidden="true"></svg><div class="etags"></div></div>${keyHTML(w, m)}</article>`; });
  const plain = M.plain.length ? `<div class="eplain"><div class="lab">${WORDS.also}</div><div class="ochips">${M.plain.map(r => r.kind === 'keep' ? `<div class="eset e-plain"><div class="ehead"><b class="en">${r.daily}</b><div class="esay">${esc(r.words.num || '')}</div></div><div class="chip oc keep"><span class="cname">${esc(r.words.title)}</span>${r.words.scope ? `<span class="cband">${esc(r.words.scope)}</span>` : ''}</div></div>`
    : `<div class="chip oc ${r.kind}"><span class="cname">${esc(r.words.title)}</span><span class="owhy">${esc(r.words.pill || (r.kind === 'nolimit' ? WORDS.nolimit : r.kind))}${r.words.sum ? ' — ' + esc(r.words.sum) : ''}</span></div>`).join('')}</div></div>` : '';
  return { html: groups.join('') + plain, after: root => root.querySelectorAll('.euler').forEach((el, i) => { el.__models = [M.groups[i]]; mountOverlay(el); }) };
}

/* ---------- J: jars — a limit is a jar; a jar inside a jar is "part of"; a jar's width is its share of the one it sits in ---------- */
const FISH = '<svg class="fg" viewBox="0 0 44 24" aria-hidden="true"><path d="M2 12c7-9 22-9 31 0-9 9-24 9-31 0z"/><path d="M31 12l11-8v16z"/><circle cx="10" cy="10" r="1.8" class="eye"/></svg>';
const fishHTML = ch => `<div class="fish${ch.n === 0 ? ' k0' : ''}" data-chip="${ch.id}" title="${esc(ch.kind.sentence || '')}">${FISH}<b>×${num(ch.n)}</b><span class="fname">${esc(ch.name)}</span><span class="fband">${esc(ch.detail)}${ch.origin ? ` · ${esc(ch.origin)}` : ''}</span>${ch.extra ? `<span class="cextra" title="${esc(ch.extra.says)}">+${ch.extra.n}</span>` : ''}</div>`;
function jarHTML(w, s, frac) {
  const n = s.n == null ? 0 : Math.min(s.n, 25);
  const ticks = `<div class="ticks" aria-hidden="true">${Array.from({ length: n }, (_, i) => `<i style="bottom:${(i + 1) / n * 100}%"></i>`).join('')}</div>`;
  const body = bodyItems(s).map(it => it.type === 'set' ? jarHTML(w, it.set, s.n == null || it.set.n == null ? 1 : Math.min(1, it.set.n / s.n)) : `<div class="school">${it.cols.flatMap(c => c.chips).sort((a, b) => a.row - b.row || a.col - b.col).map(fishHTML).join('')}</div>`).join('');
  return `<div class="jar j-${s.kind}" style="--f:${frac}"><div class="jlabel"><b>${num(s.n)}</b><span>${esc(s.short)}</span>${book(w, s.by)}</div><div class="jbody">${ticks}${body}</div></div>`;
}
function formJ(w) {
  const M = waterModel(w);
  const groups = M.groups.map(m => { index(m); return `<article class="grp"><header class="ghead"><h3>${esc(m.row.words?.title || '')}${m.row.words?.scope ? `<span class="scope">${esc(m.row.words.scope)}</span>` : ''}</h3></header>
    <div class="jars euler">${m.roots.map(s => jarHTML(w, s, 1)).join('')}${outsideHTML(m)}<svg class="eover" aria-hidden="true"></svg><div class="etags"></div></div>${keyHTML(w, m)}</article>`; });
  return { html: groups.join(''), after: root => root.querySelectorAll('.jars').forEach((el, i) => { el.__models = [M.groups[i]]; mountOverlay(el); }) };
}

/* ---------- F: flow — a fish you might keep → the limit it fills → the limit that one is part of → the day's total ---------- */
function flowSVG(w, M, width) {
  const UNIT = 3, MINH = 34, GAP = 8, PAD = 4;
  const all = []; M.groups.forEach(m => { index(m); all.push(...m.sets); });
  const maxDepth = Math.max(0, ...all.filter(s => !s.overlay).map(s => s.depth)), cols = maxDepth + 2;
  const nodeW = Math.max(120, Math.min(180, width * (cols > 3 ? 0.19 : 0.23))), ribW = (width - nodeW * cols) / (cols - 1);
  const colNodes = Array.from({ length: cols }, () => []), links = [], nodes = [];
  const mk = o => { const n = Object.assign({ h: MINH + (o.n == null ? 2 : Math.min(o.n, 20)) * UNIT, into: [] }, o); nodes.push(n); colNodes[n.col].push(n); return n; }; // h may be given
  for (const m of M.groups) {
    const byId = new Map();
    const dfs = s => { const nd = mk({ col: maxDepth - s.depth + 1, n: s.n, set: s, kind: s.kind, label: s.short, full: s.says, by: s.by }); byId.set(s.id, nd);
      for (const it of bodyItems(s)) { if (it.type === 'set') { dfs(it.set); links.push({ a: byId.get(it.set.id), b: nd, v: it.set.n ?? 2, kind: s.kind }); } else for (const ch of it.cols.flatMap(c => c.chips).sort((a, b) => a.row - b.row || a.col - b.col)) { const cn = mk({ col: 0, n: ch.n, chip: ch, kind: 'chip', label: `${ch.name} · ${ch.detail}${ch.origin ? ' · ' + ch.origin : ''}`, full: ch.kind.sentence }); links.push({ a: cn, b: nd, v: ch.n ?? 2, kind: s.kind }); } } };
    m.roots.forEach(dfs);
    m.outside.forEach(o => mk({ col: 0, n: 0, kind: 'out', status: o.status, h: 50, label: `${o.name}${o.band ? ' · ' + o.band : ''} — ${o.why}` }));
  }
  const outs = nodes.filter(n => n.kind === 'out');
  let sink = null; if (outs.length) { sink = mk({ col: cols - 1, n: null, kind: 'sink', label: WORDS.release, h: Math.max(MINH, outs.length * 14) }); outs.forEach(o => links.push({ a: o, b: sink, v: 1, kind: 'out' })); }
  for (const l of links) l.b.into.push(l);
  // y: column 0 stacked in order; every other node sits at the centre of what feeds it, then pushed apart
  let y = 0; for (const n of colNodes[0]) { n.y = y; y += n.h + GAP; }
  for (let c = 1; c < cols; c++) { const list = colNodes[c];
    for (const n of list) { const src = n.into.map(l => l.a).filter(a => a.y != null); n.y = src.length ? src.reduce((t, a) => t + a.y + a.h / 2, 0) / src.length - n.h / 2 : 0; }
    list.sort((a, b) => a.y - b.y); let last = 0; for (const n of list) { n.y = Math.max(n.y, last); last = n.y + n.h + GAP; } }
  const H = Math.max(...nodes.map(n => n.y + n.h)) + 2;
  const x = c => c * (nodeW + ribW);
  const col = { total: 'var(--ink)', own: 'var(--accent)', share: 'var(--ink)', count: 'var(--ink)', out: 'var(--restricted)' };
  let svg = '';
  for (const n of nodes) { const tot = n.into.reduce((t, l) => t + l.v, 0); let off = 0;
    for (const l of n.into) { const sh = l.v / tot * n.h, ax = x(l.a.col) + nodeW, bx = x(n.col), mx = (ax + bx) / 2, ay0 = l.a.y, ay1 = l.a.y + l.a.h, by0 = n.y + off, by1 = by0 + sh; off += sh;
      svg += `<path class="rib rib-${l.kind}" d="M${ax},${ay0}C${mx},${ay0} ${mx},${by0} ${bx},${by0}L${bx},${by1}C${mx},${by1} ${mx},${ay1} ${ax},${ay1}Z"/>`; } }
  for (const n of nodes) svg += `<g class="fnode fn-${n.kind}"><rect x="${x(n.col)}" y="${n.y}" width="${nodeW}" height="${n.h}" rx="7"/><foreignObject x="${x(n.col)}" y="${n.y}" width="${nodeW}" height="${n.h}"><div xmlns="http://www.w3.org/1999/xhtml" class="fl" title="${esc(n.full || '')}">${n.kind === 'sink' ? '' : `<b>${n.kind === 'out' ? '✕' : num(n.n)}</b>`}<span>${esc(n.label)}</span>${n.by != null ? book(w, n.by) : ''}</div></foreignObject></g>`;
  // hoops: a cross-cutting set rings the ribbons of its members just after they leave the fish
  const chipRect = n => ({ x: x(0) + nodeW + 10, y: n.y, w: 22, h: n.h });
  for (const m of M.groups) for (const s of m.sets.filter(s => s.overlay && s.members.size)) {
    const mem = nodes.filter(n => n.chip && s.members.has(n.chip.id)).map(chipRect), foreign = nodes.filter(n => n.col === 0 && !(n.chip && s.members.has(n.chip.id))).map(chipRect); if (!mem.length) continue;
    const hulls = cluster(mem, foreign, PAD);
    hulls.forEach((h, i) => { svg += `<rect class="hull hull-${s.kind}" x="${h.x - PAD}" y="${h.y - PAD}" width="${h.w + 2 * PAD}" height="${h.h + 2 * PAD}" rx="11"/>`; if (i) { const p = hulls[i - 1]; svg += `<line class="hlink hull-${s.kind}" x1="${p.x + p.w / 2}" y1="${p.y + p.h}" x2="${h.x + h.w / 2}" y2="${h.y}"/>`; } });
    const h = hulls[0], tw = Math.min(360, width - h.x + PAD + 6), above = h.y - PAD >= 30;
    svg += `<foreignObject x="${h.x - PAD - 6}" y="${above ? h.y - PAD - 30 : h.y + h.h + PAD + 4}" width="${tw}" height="60"><div xmlns="http://www.w3.org/1999/xhtml" class="tagwrap"><div class="etag etag-${s.kind} inflow" title="${esc(s.says)}"><b>${num(s.n)}</b><span>${esc(s.short)}</span>${s.extra?.says ? `<em>· ${esc(s.extra.says)}</em>` : ''}</div></div></foreignObject>`;
  }
  return `<svg class="flow" viewBox="0 0 ${width} ${H}" width="${width}" height="${H}">${svg}</svg>`;
}
function formF(w) {
  const M = waterModel(w);
  return { html: `<div class="flowwrap"></div>`, after: root => { const el = root.querySelector('.flowwrap'); const draw = () => { const wd = Math.max(680, Math.floor(el.clientWidth)); if (wd && el.__w !== wd) { el.__w = wd; el.innerHTML = flowSVG(w, M, wd); } }; draw(); new ResizeObserver(draw).observe(el); } };
}

/* ---------- T: decision tree — which fish → how long → hatchery or wild → keep (and what it counts toward) or back ---------- */
function formT(w) {
  const M = waterModel(w);
  const tags = ch => ch ? `<span class="ttags">${[...ch.memberOf].map(id => ch.container.__all.get(id)).filter(Boolean).sort((a, b) => a.depth - b.depth).map(s => `<i class="tg tg-${s.kind}" title="${esc(s.says)}"><b>${num(s.n)}</b> ${esc(s.short)}</i>`).join('')}</span>` : '';
  const findChip = (m, sp, b) => m.chips.find(ch => ch.species.includes(sp) && ch.lo <= b.from && (ch.hi == null || (b.to != null && b.to <= ch.hi)));
  const leafKeep = (m, sp, b, c, ch) => `<span class="leaf keep"><b>${WORDS.keep}</b> · ${num(c.n)}${c.as ? ` <small>= ${esc(fishName(c.as))}</small>` : ''}</span>${tags(ch)}${ch?.extra ? `<span class="leaf more">+${ch.extra.n} · ${esc(ch.extra.says)}</span>` : ''}${ch?.kind.duties?.length ? `<span class="duties">${ch.kind.duties.map(esc).join(' · ')}</span>` : ''}`;
  const band = (m, sp, b, c) => {
    const ch = findChip(m, sp, b) || (c.as ? findChip(m, c.as, b) : null);
    let leaf;
    if (c.n === 0 || c.n == null && !c.as) leaf = `<span class="leaf ${c.why === 'closed' ? 'closed' : 'rel'}">${esc(c.why || WORDS.release)}${c.as ? ` <small>= ${esc(fishName(c.as))}</small>` : ''}</span>`;
    else if (c.origin) leaf = `<ul><li><span class="dn q">${WORDS.origin}</span><ul><li><span class="dn a">${esc(c.origin)}</span>${leafKeep(m, sp, b, c, ch)}</li><li><span class="dn a">${WORDS.wild}</span><span class="leaf rel">${esc(m.origin || WORDS.release)}</span></li></ul></li></ul>`;
    else leaf = leafKeep(m, sp, b, c, ch);
    return `<li><span class="dn b">${esc(b.says)}</span>${leaf}</li>`;
  };
  const species = (m, sp) => { const bands = m.g.table.bands[sp], nk = m.outside.find(o => o.by && o.species.includes(sp));
    const inner = nk ? `<span class="leaf ${nk.status}">${esc(nk.why)}</span>` : bands.length === 1 ? band(m, sp, bands[0], m.g.table.cells[sp][0]).replace(/^<li>/, '').replace(/<\/li>$/, '') : `<ul><li><span class="dn q">${WORDS.long}</span><ul>${bands.map((b, i) => band(m, sp, b, m.g.table.cells[sp][i])).join('')}</ul></li></ul>`;
    return `<li class="sp"><span class="dn sp">${esc(fishName(sp))}<small>${esc(m.row.words?.scope || '')}</small></span>${inner}</li>`; };
  const groups = M.groups.map(m => Object.keys(m.g.table?.bands || {}).map(sp => species(m, sp)).join('')).join('');
  const plain = M.plain.map(r => `<li class="sp"><span class="dn sp">${esc(r.words.title)}<small>${esc(r.words.scope || '')}</small></span><span class="leaf ${r.kind === 'keep' ? 'keep' : r.kind === 'closed' ? 'closed' : 'rel'}">${r.kind === 'keep' ? `<b>${WORDS.keep}</b> · ${esc(r.words.num || r.daily)}` : esc(r.words.pill || (r.kind === 'nolimit' ? WORDS.nolimit : r.kind))}</span>${r.words.sum ? `<span class="duties">${esc(r.words.sum)}</span>` : ''}</li>`).join('');
  return { html: `<div class="dt"><span class="dn root">${WORDS.catch}</span><ul class="l1"><li><span class="dn q">${WORDS.which}</span><ul class="spl">${groups}${plain}</ul></li></ul></div>` };
}

/* ---------- K: card deck — one tile per fish kind × size, with the limits it counts toward as stamps ---------- */
function formK(w) {
  const M = waterModel(w);
  const stamp = s => `<i class="tg tg-${s.kind}" title="${esc(s.says)}"><b>${num(s.n)}</b> ${esc(s.short)}</i>`;
  const tile = (m, ch) => `<div class="tile keep" title="${esc(ch.kind.sentence || '')}"><div class="tt">${esc(ch.name)}</div><div class="tb">${esc(ch.detail)}</div><div class="tn">${num(ch.n)}${ch.extra ? `<span class="cextra" title="${esc(ch.extra.says)}">+${ch.extra.n}</span>` : ''}</div>${ch.origin ? `<div class="to">${esc(ch.origin)}</div>` : ''}<div class="lab">${WORDS.counts}</div><div class="ttags">${[...ch.memberOf].map(id => m.sets.find(s => s.id === id)).filter(Boolean).sort((a, b) => a.depth - b.depth).map(stamp).join('')}</div></div>`;
  const otile = o => `<div class="tile ${o.status}"><div class="tt">${esc(o.name)}</div>${o.band ? `<div class="tb">${esc(o.band)}</div>` : ''}<div class="tn">✕</div><div class="to">${esc(o.why)}</div></div>`;
  const groups = M.groups.map(m => `<article class="grp"><header class="ghead"><h3>${esc(m.row.words?.title || '')}${m.row.words?.scope ? `<span class="scope">${esc(m.row.words.scope)}</span>` : ''}</h3>${m.origin ? `<p class="hsays">${esc(m.origin)}</p>` : ''}</header><div class="deck">${m.chips.map(ch => tile(m, ch)).join('')}${m.outside.map(otile).join('')}</div></article>`).join('');
  const plain = M.plain.length ? `<article class="grp"><header class="ghead"><h3>${WORDS.also}</h3></header><div class="deck">${M.plain.map(r => `<div class="tile ${r.kind === 'keep' ? 'keep' : r.kind === 'closed' ? 'closed' : 'release'}"><div class="tt">${esc(r.words.title)}</div><div class="tb">${esc(r.words.scope || '')}</div><div class="tn">${r.kind === 'keep' ? r.daily : r.kind === 'nolimit' ? '∞' : '✕'}</div><div class="to">${esc(r.kind === 'keep' ? r.words.num : (r.words.pill || WORDS.nolimit))}${r.words.sum ? ' — ' + esc(r.words.sum) : ''}</div></div>`).join('')}</div></article>` : '';
  return { html: groups + plain };
}

const FORMS = [
  { id: 'V', tab: 'Venn / Euler', name: 'Venn / Euler — the limits as overlapping sets', how: 'Each box is one limit with its number; a dashed ring is a cap that cuts across boxes. A fish sits in every box and ring that counts it, and only those — so a fish outside a box is not part of that limit.' },
  { id: 'J', tab: 'Jars', name: 'Jars — a limit is a jar, part-of is a jar inside a jar', how: 'A jar holds its number; a jar inside another is "part of" it, as wide as its share. Fish swim in the innermost jar that counts them; a dashed ring is a cap on some of them.' },
  { id: 'F', tab: 'Flow', name: 'Flow — from the fish to the day\'s total', how: 'Left to right: a fish you may keep → the limit it fills → the limit that one is part of → the day\'s total. Band thickness is the number; a dashed ring is a cap the fish passes through; fish that go back flow to the bottom. Scrolls sideways on a phone.' },
  { id: 'T', tab: 'Decision tree', name: 'Decision tree — which fish, how long, hatchery or wild', how: 'Follow one fish down: its kind, its length band, then (where it matters) hatchery or wild, to keep — with the limits it counts toward — or back in the water.' },
  { id: 'K', tab: 'Card deck', name: 'Card deck — one tile per fish kind and size', how: 'One tile per kind and length band: how many, and stamps for every limit that fish counts toward. Crossed tiles go back, with the reason.' },
];
const R = { V: formV, J: formJ, F: formF, T: formT, K: formK };
function render(w, id, mount) {
  const f = FORMS.find(x => x.id === id) || FORMS[0], out = R[f.id](w);
  mount.innerHTML = `<header class="fh"><h2><span class="fletter">${f.id}</span>${esc(f.name)}</h2><p class="why">${esc(f.how)}</p></header><div class="fbody f-${f.id}">${out.html}</div>`;
  if (out.after) out.after(mount);
  return f;
}
return { FORMS, WORDS, render, waterModel };
})();
