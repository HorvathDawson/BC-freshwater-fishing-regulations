
/* ================= DATA =================
   Page v36 reads every DECISION from the answers file (ui-rules-answers.json, format answers/2, sliced to
   the waters below by pipeline/deliver/answers/reference/page_data.py): open or closed, the ladder, the
   card's rows, gear, licence and the part picker. The export pair (#data) is read only for words: rule
   labels, verbatims, sources, species names, entry names. The page decides nothing: where the answers
   file has no value, the page says so in a dashed red box ("Missing from the answers file"). */
const D = JSON.parse(document.getElementById('data').textContent);
const A = JSON.parse(document.getElementById('answers').textContent);
const CASES = JSON.parse(document.getElementById('cases').textContent);
// ANSWERS-SPEC §1: one bundle, one rule list, one format, or nothing is shown
const PAIR_ERR = A.about.format !== 'answers/2' ? `answers format is ${A.about.format}, not answers/2`
  : JSON.stringify(A.about.bundle) !== JSON.stringify(D.about.bundle) ? 'the answers file and the export come from different bundles'
  : JSON.stringify(A.about.bundle) !== JSON.stringify(CASES.about.bundle) ? 'the answers file and the guide come from different bundles' : '';
const SEC = A.sections, DRULES = SEC.display.rules, DWATERS = SEC.display.waters;
const MON = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const DIM = [31,28,31,30,31,30,31,31,30,31,30,31];
const DAYS = []; DIM.forEach((n,m)=>{for(let d=1;d<=n;d++)DAYS.push((m+1)*100+d);});
// the answers' calendar: day 1..366 on the leap calendar (Feb 29 = 60, Mar 1 = 61 in every year)
const LEAP0 = [0,31,60,91,121,152,182,213,244,274,305,335];
const dayOf = md => LEAP0[Math.floor(md/100)-1] + md%100;
const RANK = {'-1':{t:'Federal / parks',c:'--r-1'},'0':{t:'This water',c:'--r0'},'1':{t:'From downstream',c:'--r1'},'2':{t:'Named area',c:'--r2'},'3':{t:'Region',c:'--r3'},'4':{t:'Province',c:'--r4'}};
const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const fmtMd = md => MON[Math.floor(md/100)-1] + ' ' + (md%100);
// TODAY is the viewer's own date (the browser clock), never a fixed demo day (user test 2026-10-08,
// item 8). Feb 29 is answered as Feb 28 (the page's year has 365 days; the answers' Feb 29 = Feb 28).
const NOW = new Date();
const TODAY = (() => { const md = (NOW.getMonth() + 1) * 100 + NOW.getDate(); return md === 229 ? 228 : md; })();
const cap = s => s ? s.charAt(0).toUpperCase() + s.slice(1) : s;
const clean = s => String(s || '').replace(/\*+/g, '').replace(/\s+/g, ' ').trim();
// a value the answers file should hold and does not: shown, never guessed
const missing = what => `<div class="missing"><b>Missing from the answers file:</b> ${esc(what)}</div>`;
/* ---------- "What does this mean?": the answers file's glossary (answers 2.2, generated from the data) ---------- */
const GLOSS = new Map(((A.glossary || {}).terms || []).map(t => [t.id, t]));
// a small ⓘ beside a term; nothing where the glossary has no such term
const gi = id => GLOSS.has(id) ? `<button class="gl" type="button" data-gl="${id}" aria-label="What does “${esc(GLOSS.get(id).term)}” mean?" title="What does this mean?">ⓘ</button>` : '';
// the region a row's region-wide limit comes from (its entry `z6:…` -> glossary term `region_6`)
const regionGi = row => { const m = /^z(\d\w?):/.exec(row?.scope?.entry || ''); return m ? gi('region_' + m[1]) : ''; };
function openGloss(id){ const t = GLOSS.get(id); if (!t) return;
  openSheet(t.term, `What does this mean? · Book p.${t.pages.join(', p.')}`, `<p class="glsays">${esc(t.says)}</p>${t.example ? `<p class="glex"><b>For example:</b> ${esc(t.example)}</p>` : ''}<div class="lbl">In the book’s words</div><blockquote class="exact">${esc(t.quote)}</blockquote>`); }

/* ---------- species (names only) ---------- */
const FISH = D.species.fish, GROUPS = D.species.groups;
const expand = codes => { const out = new Set(); (codes || []).forEach(c => { const g = GROUPS[c]; if (g) (g.members || []).forEach(m => out.add(m)); else out.add(c); }); return [...out]; };
const PROT = D.species.protected || {}, isProt = c => !!PROT[c];
const SALM = D.species.salmon || {};
const GAME = GROUPS.ALL_GAME_FISH.members;
const spName = c => FISH[c]?.name || GROUPS[c]?.name || PROT[c]?.name || SALM[c]?.name || c;
const lc = s => /^(Dolly|Arctic|Nooksack|Salish|Cultus|Enos|Coastal|Westslope|Misty|Vananda|Paxton|Hadley|Morrison|Rocky|Speckled|Charlotte)/.test(s) ? s : s.charAt(0).toLowerCase() + s.slice(1);
function groupFor(codes){ const s = new Set(codes); if (s.size < 2) return null; for (const [g, v] of Object.entries(GROUPS)){ if (g === 'SA' || v.open) continue; if (v.members.length === s.size && v.members.every(m => s.has(m))) return v.name; } return null; }
const join = (n, conj=' and ') => n.length < 2 ? n.join('') : n.slice(0,-1).join(', ') + conj + n[n.length-1];
const lcNames = (codes, conj, max=Infinity) => { const g = groupFor(codes); if (g) return lc(g); const n = codes.map(c => lc(spName(c))); if (n.length > max) return n.slice(0, max-1).join(', ') + ` + ${n.length-max+1} more`; return join(n, conj); };
const Names = (codes, conj, max) => cap(lcNames(codes, conj, max));
const writtenName = (r, conj) => { const w = r.f.species || []; if (w.length === 1 && w[0] === 'TROUT_CHAR' && (r.f.species_except || []).includes('CHAR')) return 'trout'; return w.length ? join(w.map(c => lc(spName(c))), conj) : 'fish'; };

/* ---------- time (words only) ---------- */
const whenDates = w => (w?.dates || []).map(d => [d.from_month*100 + d.from_day, d.to_month*100 + d.to_day]);
const rangeTxt = (a, b) => a === b ? fmtMd(a) : Math.floor(a/100) === Math.floor(b/100) ? `${fmtMd(a)}–${b%100}` : `${fmtMd(a)}–${fmtMd(b)}`;
function timeCond(w){
  if (!w) return '';
  const bits = [];
  if (w.weekdays?.length) bits.push(w.weekdays.map(d => d.slice(0,3)).join(', '));
  if (w.hours){ const t = x => x.at ? x.at : `${x.offset_min ? Math.abs(x.offset_min) + ' min ' + (x.offset_min < 0 ? 'before ' : 'after ') : ''}${x.solar}`; bits.push(`${t(w.hours.start)}–${t(w.hours.end)}`); }
  return bits.join(', ');
}
const winTxt = r => r.wins.length ? ', ' + r.wins.map(([a, b]) => rangeTxt(a, b)).join(', ') : '';

/* ---------- rules: the export's words; kind, closure, bands and plain sentence from answers display.rules ---------- */
const RULES = D.rules;
const RIX = {}; for (const [ix, k] of Object.entries(D.rid)) RIX[k] = +ix;
for (const [k, r] of Object.entries(RULES)){
  const f = r.f = r.fields;
  r.key = k; r.ix = RIX[k]; r.baseRank = r.prov.rank;
  r.dr = DRULES[r.ix] || null;
  r.k = r.dr ? r.dr.kind : null;
  r.species = expand(f.species); r.speciesExcept = expand(f.species_except);
  r.wins = whenDates(f.when); r.tcond = timeCond(f.when);
  r.part = f.undrawn_part || null;
  r.notes = [];
  if (r.prov.uncertain) r.notes.push('Uncertain placement: ' + (r.prov.why || 'no reason given'));
  if (f.review_reason) r.notes.push('Curator still to settle: ' + f.review_reason);
}
// a rule the answers name that the page's data lacks: a visible stand-in, never a guess
const NORULE = ix => ({ key:'#' + ix, ix, f:{}, fields:{}, prov:{ rank:4 }, baseRank:4, rank:4, species:[], speciesExcept:[], wins:[], tcond:'', notes:['Missing from the page’s data: rule ' + ix], label:`(rule ${ix}: missing from the page’s data)`, verbatim:`(rule ${ix}: missing from the page’s data)`, k:null, dr:null });
const RX = ix => RULES[D.rid[ix]] || NORULE(ix);
const closedGate = r => r.k === 'gate' && !!r.dr?.closure;
const covers = (r, S) => r.species.includes(S) && !r.speciesExcept.includes(S);

/* size bands: answers display.rules[].bands, [[from_cm, to_cm|null, take|null]] */
function bands(r){ return (r.dr?.bands || []).map(([a, b, take]) => ({ a, b: b == null ? Infinity : b, take })); }
const bandTxt = (a, b) => a === 0 ? `under ${b} cm` : b === Infinity ? `over ${a} cm` : `${a}–${b} cm`;
// a rule as a line of words (the kind is the answers'; the words are the page's)
function describe(r){
  const f = r.f, sp = writtenName(r), o = f.origin ? f.origin + ' ' : '', w = f.water ? (f.water === 'stream' ? ' from streams' : ' from lakes') : '';
  const t = winTxt(r) + (r.tcond ? ` (${r.tcond})` : '');
  const bs = bands(r);
  switch (r.k){
    case 'gate': return (closedGate(r) ? `No fishing for ${o}${sp.replace(/^all game fish$/, 'any game fish')}` : `Release every ${o}${sp}`) + w + t;
    case 'pool': return (f.unlimited ? `No limit on ${sp}${w}` : `Keep ${f.take} ${o}${sp}${w} a day${r.species.length > 1 ? ', shared' : ''}`) + bs.filter(s => s.take === 0).map(s => `, none ${bandTxt(s.a, s.b)}`).join('') + t;
    case 'subcap': return `Inside the day’s total: at most ${f.take} ${o}${writtenName(r, ' or ')}${w}` + t;
    case 'sizecap': return 'Inside the day’s total: ' + bs.filter(s => s.take != null).map(s => s.take === 0 ? `none ${bandTxt(s.a, s.b)}` : `at most ${s.take} ${bandTxt(s.a, s.b)}`).join(', ') + ` (${sp})` + t;
    case 'size': return `${cap(o + sp)}${w}: ` + bs.filter(s => s.take === 0).map(s => `release any ${bandTxt(s.a, s.b)}`).join(', ') + t;
    case 'annual': { const c = bs.find(s => s.take > 0); return `${f.take} ${o}${sp}${c && c.a > 0 ? ' ' + bandTxt(c.a, c.b) : ''} a ${f.period === 'monthly' ? 'month' : 'year'}${c && c.a > 0 ? '. Smaller fish don’t count' : ''}`; }
    // possession (user ruling Q15) and a snagged fish (Q38): the answers' sentence, never the page's
    case 'possession_cap': case 'possession': case 'caught': return r.dr?.plain ? r.dr.plain.replace(/\.$/, '') : `(no words in the answers for ${r.key})`;
    default: return clean(r.verbatim);
  }
}
const cutWords = (t, n) => t.length <= n ? t : t.slice(0, n).replace(/[\s,;:·–-]+\S*$/, '') + '…';

/* ---------- a place: one export part of one water, and its part key in the answers file ---------- */
const WATERS = D.waters;
function makePlace(wi, pi){
  const water = WATERS[wi], p = water.parts[pi], k = A.parts[water.id]?.[pi] ?? null;
  const via = new Map((p?.rules || []).map(([ix, v]) => [ix, v]));
  const cache = new Map();
  // a rule as this part ranks it: a water rule reached by the tributary walk ranks 1 (the reader's place)
  const R = ix => { if (cache.has(ix)) return cache.get(ix); const base = RX(ix), c = Object.create(base); c.via = via.get(ix) || 'reach'; c.rank = c.via === 'trib' && base.baseRank >= 0 ? 1 : base.baseRank; cache.set(ix, c); return c; };
  const cands = (p?.rules || []).map(([ix]) => R(ix));
  return { water, part:p, pi, k, key: k == null ? null : A.keys[k], name:water.name, kind:water.kind, R, cands, disp: DWATERS[water.id]?.parts?.[pi] || null, _seg:{} };
}
const segStarts = w => A.segments[w.key[9]];
// MOMENTS (answers 2.1): where a weekday or hours rule makes a day read two ways, the day's start repeats,
// one segment per moment ({weekdays, hours: null | {start, end, in}}); null where the part has none
const momentsOf = w => w.key && w.key[10] != null ? A.segment_moments[w.key[10]].map(i => A.moments[i]) : null;
// the page's year is the viewer's (its weekdays: a weekday rule decides on the date's weekday)
let YEAR = NOW.getFullYear(); const WEEK = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'];
const weekdayOf = md => WEEK[(new Date(Date.UTC(YEAR, Math.floor(md/100) - 1, md % 100)).getUTCDay() + 6) % 7];
// the day's segments: every segment sharing the start on or before it (one unless the part has moments)
function daySegs(w, md){ const s = segStarts(w), d = dayOf(md); let i = 0; for (let j = 0; j < s.length; j++) if (s[j] <= d) i = j;
  const out = []; for (let j = s.indexOf(s[i]); j <= i; j++) out.push(j); return out; }
// the segment holding a day (ANSWERS-SPEC §2): at the date's weekday, outside any hours window unless `inside`
function segAt(w, md, wd, inside){ const ss = daySegs(w, md), M = momentsOf(w); if (!M || ss.length === 1) return ss[ss.length - 1];
  const j = ss.find(j => M[j].weekdays.includes(wd) && (!M[j].hours || M[j].hours.in === !!inside)); return j == null ? -1 : j; }
const segOf = (w, md) => segAt(w, md, weekdayOf(md), false);
// the segment's first day as MMDD (no Feb 29 in the page's year)
const mdOfDay = d => { let m = 11; while (m > 0 && LEAP0[m] >= d) m--; const md = (m + 1) * 100 + (d - LEAP0[m]); return md === 229 ? 301 : md; };
const frameIx = (w, name, s) => { const at = SEC[name].at[w.k]; return at ? at[s] : undefined; };
const isTidal = w => !!(w.key && w.key[8]);

/* ---------- the ladder frame: per fish and origin, every rule's state (answers ladder) ---------- */
function verdictList(ref){
  const [speaks, beside, shown, nym, partly, lost] = SEC.ladder.verdicts[ref], out = new Map();
  [['speaks', speaks], ['beside', beside], ['shown', shown], ['not_yet_mapped', nym]].forEach(([st, l]) => l.forEach(i => out.set(i, { state:st, partly:null })));
  partly.forEach(([i, by]) => { const x = out.get(i); if (x) x.partly = by; });
  lost.forEach(([i, r, by]) => out.set(i, { state:SEC.ladder.reason_state[r], reason:SEC.ladder.reasons[r], by }));
  return out;
}
// {fish code: {none, hatchery, wild: Map(rule ix -> {state, partly, reason, by})}}
function ladderAt(w, s){
  const ck = 'L' + s; if (w._seg[ck] !== undefined) return w._seg[ck];
  const f = frameIx(w, 'ladder', s); if (f == null) return (w._seg[ck] = null);
  const [common, rows] = SEC.ladder.frames[f], base = verdictList(common), out = {};
  for (const row of rows){ const own = row.length === 2 ? [row[1], row[1], row[1]] : row.slice(1);
    out[A.fish[row[0]]] = Object.fromEntries(['none', 'hatchery', 'wild'].map((o, i) => [o, new Map([...base, ...verdictList(own[i])])])); }
  return (w._seg[ck] = out);
}
// what the ladder says of the place today, over every fish it asks (origin not known): the boxes above the card read this
function settle(w, md){
  const s = segOf(w, md), ck = 'C' + s; if (w._seg[ck]) return w._seg[ck];
  const L = ladderAt(w, s), st = new Map(), lifted = new Map();
  if (L) for (const S of Object.keys(L)) for (const [ix, v] of L[S].none){ if (v.state === 'lifted') lifted.set(ix, v.by); else if (!st.has(ix) || v.state === 'speaks') st.set(ix, v.state); }
  const with_ = state => [...st].filter(([, v]) => v === state).map(([ix]) => w.R(ix));
  const speaks = with_('speaks'), beside = with_('beside'), shown = with_('shown');
  // undrawn parts: every one the key holds over the year, today's marked
  if (!w._nym){ w._nym = new Map(); segStarts(w).forEach((_, i) => { const Li = ladderAt(w, i); if (Li) for (const S of Object.keys(Li)) for (const [ix, v] of Li[S].none) if (v.state === 'not_yet_mapped') w._nym.set(ix, true); }); }
  const nymNow = new Set(with_('not_yet_mapped').map(r => r.ix));
  const inpart = [...w._nym.keys()].map(ix => Object.assign(Object.create(w.R(ix)), { now: nymNow.has(ix) }));
  // the closures that close the water today: the answers' decided `closing` (display frame, gap G1) — those closing
  // every game fish, else every one listed; the page infers nothing from the ladder
  const disp = frameIx(w, 'display', s), status = disp == null ? null : SEC.display.frames[disp];
  const closing = (status?.closing || []).map(([ix, fs]) => [w.R(ix), fs]);
  // the one closing the most game fish first (every one, where one does), then by rank
  const broad = closing.slice().sort((a, b) => b[1].length - a[1].length || a[0].rank - b[0].rank).map(([r]) => r);
  return (w._seg[ck] = { w, md, s, L, status, speaks, beside, shown, inpart, lifted, broad,
    timed: beside.filter(r => r.tcond), side: beside.filter(r => r.f.side), standing: shown.filter(r => r.k === 'standing'),
    anglerclosed: speaks.filter(r => r.k === 'anglerclosure') });
}
const statusAt = (w, md) => settle(w, md).status?.status ?? null;   // base | own | closed | tidal (answers display)
const isClosedAt = (w, md) => statusAt(w, md) === 'closed';

/* ---------- the card: answers rows, per fish and origin and per shared limit ---------- */
const KEEPISH = s => s === 'keep' || s === 'nolimit';
const st_ = s => s === 'no_limit' ? 'nolimit' : s;
function hydLine(w, l){
  const o = { ...l, r: w.R(l.r) };
  if ('b' in l) o.b = l.b == null ? Infinity : l.b;
  if ('outer' in l) o.outer = w.R(l.outer);
  if ('daily' in l) o.daily = l.daily == null ? Infinity : l.daily;
  if (l.status) o.status = st_(l.status);
  return o;
}
function hydRes(w, d, S, o){
  if (!d) return null;
  return { S, o, status:st_(d.status), win:w.R(d.win), daily: d.daily == null ? (d.status === 'no_limit' ? Infinity : null) : d.daily,
    narrow: d.narrow == null ? null : w.R(d.narrow), lines:d.lines.map(l => hydLine(w, l)),
    roles:new Map(d.roles.map(([r, role, by]) => [w.R(r).key, { role, by: by == null ? null : w.R(by).key }])),
    liftNotes:d.lift_notes.map(([by, q]) => ({ by:w.R(by), q })) };
}
function hydFact(w, l){ const o = hydLine(w, l); o.members = new Set(l.members); o.rules = (l.rules || [l.r]).map(w.R);
  if (l.carve_of) o.carveOf = { a:l.carve_of[0], b: l.carve_of[1] == null ? Infinity : l.carve_of[1], take:l.carve_of[2] }; return o; }
function hydRow(w, x){
  const kind = st_(x.kind);
  return { kind, pool: x.pool == null ? null : w.R(x.pool), win: x.win == null ? null : w.R(x.win), members:x.members, allMembers:x.all_members,
    daily: x.daily == null ? (kind === 'nolimit' ? Infinity : null) : x.daily, narrow: x.narrow == null ? null : w.R(x.narrow),
    everyone:x.everyone.map(l => hydFact(w, l)), groups:x.groups.map(g => ({ members:g.members, facts:g.facts.map(l => hydFact(w, l)) })),
    prot:x.prot, wins:(x.wins || []).map(w.R), liftNotes:x.lift_notes.map(([by, q]) => ({ by:w.R(by), q })), scope:x.scope,
    conds:x.conds ? x.conds.map(c => ({ ...c, r: c.r == null ? null : w.R(c.r), b: c.b === null ? Infinity : c.b })) : null,
    items:x.items || null, real:x.real_daily ?? null, raw:x };
}
// the card on one segment: {spp, R: {fish: {H, W}}, rows, line} — or null where the answers file holds no rows frame
function rowsAt(w, s){
  const ck = 'R' + s; if (w._seg[ck] !== undefined) return w._seg[ck];
  const f = frameIx(w, 'rows', s); if (f == null) return (w._seg[ck] = null);
  const [spp, fish, rows, line] = SEC.rows.frames[f], dec = SEC.rows.decided;
  const R = {}; for (const [S, [h, wd]] of Object.entries(fish)) R[S] = { H: hydRes(w, h == null ? null : dec[h], S, 'hatchery'), W: hydRes(w, wd == null ? null : dec[wd], S, 'wild') };
  return (w._seg[ck] = { spp, R, rows: rows.map(i => hydRow(w, SEC.rows.rows[i])), line });
}
const mainRes = (H, W) => (H && KEEPISH(H.status)) ? H : (W && KEEPISH(W.status)) ? W : (H || W);
// the page's MODEL for one day: the answers' rows, the closures if the water is closed
function buildModel(w, md){ const c = settle(w, md), r = rowsAt(w, c.s); return { R: r?.R || {}, rows: r?.rows || [], spp: r?.spp || [], line: r?.line ?? null, missingRows: !r, broad: c.status?.status === 'closed' ? c.broad : [], status: c.status?.status ?? null, ctx: c }; }

/* ---------- through the year: per day, read from the segment holding it ---------- */
const RANKSTAT = { keep:3, nolimit:3, release:2, closed:1 };
function dayStatus(w, md, members){
  const r = rowsAt(w, segOf(w, md)); let best = 'none', bv = 0; if (!r) return 'none';
  for (const S of members){ const m = r.R[S] ? mainRes(r.R[S].H, r.R[S].W) : null; if (m && RANKSTAT[m.status] > bv){ bv = RANKSTAT[m.status]; best = m.status === 'nolimit' ? 'keep' : m.status; } }
  return best;
}
function strip(w, members){
  const segs = []; let cur = null, len = 0;
  for (const md of DAYS){ const best = dayStatus(w, md, members); if (best === cur) len++; else { if (cur) segs.push([cur, len]); cur = best; len = 1; } }
  segs.push([cur, len]); return segs;
}
function segBar(segs, md, cls, label){
  const idx = DAYS.indexOf(md);
  return `<div class="sbar ${cls}">${segs.map(([s, n]) => `<i class="s-${s}${n <= 14 ? ' short' : ''}" style="flex:${n} 0 0"></i>`).join('')}<span class="today" style="left:calc(${(idx/365*100).toFixed(2)}% - 1px)">${label ? `<em>${label}</em>` : ''}</span></div>`;
}
function shortWindows(segs){ let d = 0; const out = []; segs.forEach(([s, n]) => { if (s === 'keep' && n <= 14 && segs.length > 1) out.push(rangeTxt(DAYS[d], DAYS[d + n - 1])); d += n; }); return out.length ? `<div class="swin">Keeping allowed only ${out.join(' and ')}</div>` : ''; }
const MONTHS_ROW = `<div class="smonths">${MON.map(m => `<span>${m[0]}</span>`).join('')}</div>`;
function stripHtml(w, members, md){
  const segs = strip(w, members), has = new Set(segs.map(s => s[0]));
  const lab = { keep:'can keep', release:'release only', closed:'closed', none:'no rule' };
  const rt = runTxt(segs, md);
  return `<div class="strip"><div class="lbl">Through the year</div>${rt ? `<p class="swin">${esc(rt)}</p>` : ''}${segBar(segs, md, '')}${shortWindows(segs)}${MONTHS_ROW}<div class="slegend">${[...has].map(s => `<span><i class="s-${s}"></i>${lab[s]}</span>`).join('')}</div></div>`;
}
// only the fish that can be kept here belong in an "only N can be …" line
const keepNames = (codes, r) => { const k = codes.filter(S => KEEPISH(mainRes(MODEL.R[S]?.H, MODEL.R[S]?.W)?.status)); return k.length && k.length < codes.length ? lcNames(k, ' or ') : r ? writtenName(r, ' or ') : lcNames(codes, ' or '); };
/* ================= fish illustrations: drawn here, no outside images =================
   One parametric side-view fish, tuned per species. Numbered marks point at the features that tell it apart;
   the numbers match the "What to look for" list under the picture. */
const FISHART = (() => {
  const VB_W = 400, VB_H = 150, CY = 76;
  // a smooth closed path through points (Catmull-Rom to cubic Bezier)
  const smooth = (pts, closed = true) => {
    const p = closed ? [pts[pts.length - 1], ...pts, pts[0], pts[1]] : [pts[0], ...pts, pts[pts.length - 1]];
    let d = `M${pts[0][0].toFixed(1)} ${pts[0][1].toFixed(1)}`;
    for (let i = 1; i < p.length - 2; i++){
      const [p0, p1, p2, p3] = [p[i - 1], p[i], p[i + 1], p[i + 2]];
      const c1 = [p1[0] + (p2[0] - p0[0]) / 6, p1[1] + (p2[1] - p0[1]) / 6], c2 = [p2[0] - (p3[0] - p1[0]) / 6, p2[1] - (p3[1] - p1[1]) / 6];
      d += ` C${c1[0].toFixed(1)} ${c1[1].toFixed(1)} ${c2[0].toFixed(1)} ${c2[1].toFixed(1)} ${p2[0].toFixed(1)} ${p2[1].toFixed(1)}`;
    }
    return d + (closed ? 'Z' : '');
  };
  // seeded random so each species always draws the same spots
  const rng = seed => () => { seed = (seed * 16807) % 2147483647; return (seed - 1) / 2147483646; };

  function body(c){
    const x0 = c.x0 ?? 28, xp = c.xp ?? 318, L = xp - x0, h = c.h, ph = c.ph ?? h * 0.24, b = c.belly ?? 1, s = c.snout ?? 0.1;
    const t = c.top || [[0, s * h], [0.04, -0.44], [0.15, -0.86], [0.32, -1], [0.52, -0.9], [0.72, -0.6], [0.9, -0.34]];
    const u = c.bot || [[0.9, 0.32], [0.74, 0.52], [0.54, 0.84], [0.34, 0.96], [0.17, 0.84], [0.05, 0.48]];
    const pts = [...t.map(([f, y], i) => [x0 + f * L, CY + (i === 0 ? y : y * h)]), [xp, CY - ph], [xp, CY + ph], ...u.map(([f, y]) => [x0 + f * L, CY + y * h * b])];
    return { d:smooth(pts), x0, xp, L, h, ph, at:(f, yf) => [x0 + f * L, CY + yf * h] };
  }
  function tail(c, B){
    const tl = c.tailLen ?? 58, th = c.tailH ?? B.h * 0.95, fork = c.fork ?? 0.45, xp = B.xp, ph = B.ph, xt = xp + tl;
    if (c.tailShape === 'round') return `M${xp} ${CY - ph} C${xp + tl * 0.6} ${CY - th} ${xt + 6} ${CY - th * 0.5} ${xt} ${CY} C${xt + 6} ${CY + th * 0.5} ${xp + tl * 0.6} ${CY + th} ${xp} ${CY + ph}Z`;
    if (c.tailShape === 'shark') return `M${xp - 4} ${CY - ph} Q${xp + tl * 0.5} ${CY - th * 0.7} ${xt + 14} ${CY - th * 1.25} Q${xp + tl * 0.62} ${CY - th * 0.1} ${xp + tl * 0.5} ${CY + th * 0.35} Q${xp + tl * 0.2} ${CY + th * 0.2} ${xp - 4} ${CY + ph}Z`;
    const notch = xp + tl * (1 - fork);
    return `M${xp - 2} ${CY - ph} Q${xp + tl * 0.45} ${CY - ph * 1.1} ${xt} ${CY - th} Q${xt - tl * 0.12} ${CY - th * 0.45} ${notch} ${CY} Q${xt - tl * 0.12} ${CY + th * 0.45} ${xt} ${CY + th} Q${xp + tl * 0.45} ${CY + ph * 1.1} ${xp - 2} ${CY + ph}Z`;
  }
  // a fin as a curved polygon from base start to base end, with its tip offset
  const fin = (a, b, tip, bulge = 0.25) => { const mx = (a[0] + b[0]) / 2, my = (a[1] + b[1]) / 2; return `M${a[0]} ${a[1]} Q${a[0] + (tip[0] - a[0]) * 0.2} ${tip[1] + (a[1] - tip[1]) * bulge} ${tip[0]} ${tip[1]} Q${b[0] - (b[0] - tip[0]) * 0.1} ${tip[1] + (b[1] - tip[1]) * 0.55} ${b[0]} ${b[1]}Z`; };
  const rays = (a, b, tip, n = 6) => { let s = ''; for (let i = 1; i < n; i++){ const f = i / n, bx = a[0] + (b[0] - a[0]) * f, by = a[1] + (b[1] - a[1]) * f, tx = tip[0] + (b[0] - tip[0]) * f * 0.8, ty = tip[1] + (b[1] - tip[1]) * f * 0.8; s += `M${bx.toFixed(1)} ${by.toFixed(1)} L${tx.toFixed(1)} ${ty.toFixed(1)}`; } return s; };

  function draw(key, opts = {}){
    const c = SPECIES[key]; if (!c) return '';
    const id = 'f' + key + (opts.variant || '') + Math.random().toString(36).slice(2, 6);
    const B = body(c), R = rng(c.seed || 7);
    const P = [], D = [], F = [];
    // colours
    const grad = `<linearGradient id="${id}g" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="${c.back}"/><stop offset="0.42" stop-color="${c.side}"/><stop offset="0.78" stop-color="${c.belly2 || c.side}"/><stop offset="1" stop-color="${c.bellyC || '#f4f1ea'}"/></linearGradient>
      <linearGradient id="${id}s" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#fff" stop-opacity=".0"/><stop offset=".35" stop-color="#fff" stop-opacity=".28"/><stop offset=".6" stop-color="#fff" stop-opacity="0"/></linearGradient>
      <clipPath id="${id}c"><path d="${B.d}"/></clipPath>`;
    const finC = c.fin || c.back, finO = c.finO ?? 0.85;
    // fins behind the body
    const dorsal = c.dorsal || [0.42, 0.6, 1.0];            // start, end (fraction of body), height (× h)
    const dA = B.at(dorsal[0], -0.97), dB = B.at(dorsal[1], -0.8), dT = [dA[0] + (dB[0] - dA[0]) * (c.dorsalLean ?? 0.3), CY - B.h * (0.97 + dorsal[2])];
    if (c.spiny){ // spiny front dorsal, soft rear
      const sA = B.at(c.spiny[0], -0.96), sB = B.at(c.spiny[1], -0.99); let sp = `M${sA[0]} ${sA[1]}`;
      const n = c.spinyN || 9; for (let i = 0; i <= n; i++){ const f = i / n, x = sA[0] + (sB[0] - sA[0]) * f, y = sA[1] + (sB[1] - sA[1]) * f; sp += ` L${x - 3} ${y - B.h * c.spiny[2] * (0.7 + 0.3 * Math.sin(Math.PI * f))} L${x + 2} ${y - 2}`; }
      F.push(`<path d="${sp} L${sB[0]} ${sB[1]}Z" fill="${c.spinyC || finC}" opacity="${finO}"/>`);
    }
    if (c.dorsal !== false){
      if (c.sail) { const sa = B.at(dorsal[0], -0.95), sb = B.at(dorsal[1], -0.85); F.push(`<path d="M${sa[0]} ${sa[1]} C${sa[0] + 5} ${sa[1] - B.h * 1.5} ${sb[0] - 20} ${sb[1] - B.h * 1.6} ${sb[0] + 6} ${sb[1] - 4} L${sb[0]} ${sb[1]}Z" fill="${c.sailC || finC}" opacity=".9"/>`); for (let i = 0; i < 16; i++) F.push(`<circle cx="${sa[0] + 8 + R() * (sb[0] - sa[0] - 10)}" cy="${sa[1] - 6 - R() * B.h * 1.1}" r="1.8" fill="${c.sailSpot || '#3b6ea8'}" opacity=".8"/>`); }
      else { F.push(`<path d="${fin(dA, dB, dT)}" fill="${finC}" opacity="${finO}"/><path d="${rays(dA, dB, dT)}" stroke="#000" stroke-opacity=".12" stroke-width=".7" fill="none"/>`); }
    }
    const ADI = [];
    if (c.adipose !== false && c.adipose !== undefined){
      const env = f => 1 - (f - 0.32) * 1.1, aA = B.at(c.adipose[0], -env(c.adipose[0]) + 0.04), aB = B.at(c.adipose[0] + 0.055, -env(c.adipose[0] + 0.055) + 0.04);
      const F = ADI;
      if (opts.clipped) F.push(`<path d="M${aA[0]} ${aA[1] + 1} Q${(aA[0] + aB[0]) / 2} ${aA[1] - 3} ${aB[0]} ${aB[1] + 1}" stroke="#8b5a4a" stroke-width="2.2" fill="none" stroke-linecap="round"/>`);
      else F.push(`<path d="M${aA[0]} ${aA[1] + 1} Q${aA[0] + 5} ${aA[1] - B.h * 0.36} ${aB[0] + 4} ${aB[1] - 2} L${aB[0]} ${aB[1] + 1}Z" fill="${c.adiC || finC}" stroke="rgba(0,0,0,.25)" stroke-width=".6"/>`);
    }
    const anal = c.anal || [0.66, 0.76, 0.55];
    const nA = B.at(anal[0], 0.7 * (c.belly ?? 1)), nB = B.at(anal[1], 0.52 * (c.belly ?? 1)), nT = [nA[0] + (nB[0] - nA[0]) * 0.35, CY + B.h * (0.72 + anal[2])];
    F.push(`<path d="${fin(nA, nB, nT)}" fill="${c.lowFin || finC}" opacity="${finO}"/>`);
    if (c.dorsal2){ const a = B.at(c.dorsal2[0], -0.72), b = B.at(c.dorsal2[1], -0.45); F.push(`<path d="M${a[0]} ${a[1]} Q${(a[0] + b[0]) / 2} ${a[1] - B.h * c.dorsal2[2]} ${b[0]} ${b[1]}Z" fill="${finC}" opacity="${finO}"/>`); }
    const pel = c.pelvic || [0.44, 0.52, 0.42], pA = B.at(pel[0], 0.95 * (c.belly ?? 1)), pB = B.at(pel[1], 0.9 * (c.belly ?? 1)), pT = [pA[0] + 12, CY + B.h * (0.95 + pel[2])];
    F.push(`<path d="${fin(pA, pB, pT)}" fill="${c.lowFin || finC}" opacity="${finO}"/>`);
    if (c.whiteEdge) F.push(`<path d="M${pA[0]} ${pA[1]} L${pT[0]} ${pT[1]} M${nA[0]} ${nA[1]} L${nT[0]} ${nT[1]}" stroke="#fff" stroke-width="2.4" stroke-linecap="round"/>`);
    // body
    const tr = []; if (!c.tailShape){ const tl = c.tailLen ?? 58, th = c.tailH ?? B.h * 0.95; for (let i = -4; i <= 4; i++) if (i) tr.push(`M${B.xp + 2} ${CY + i * 1.2} L${B.xp + tl * 0.82} ${CY + i * th * 0.2}`); }
    const bodyShapes = [`<path d="${tail(c, B)}" fill="${c.tailC || finC}" opacity="${c.tailO ?? 0.95}"/><path d="${tr.join('')}" stroke="#000" stroke-opacity=".12" stroke-width=".7"/>`, `<path d="${B.d}" fill="url(#${id}g)" stroke="${c.edge || 'rgba(0,0,0,.35)'}" stroke-width="1"/>`];
    // patterns, clipped to the body
    const inBody = (x, y) => { const t = (x - B.x0) / B.L; if (t < 0.02 || t > 1) return false; const half = B.h * (t < 0.32 ? 0.35 + 2 * t : 1 - (t - 0.32) * 1.1); return Math.abs(y - CY) < Math.max(half, B.ph); };
    const scatter = (n, fx0, fx1, fy0, fy1, fn) => { for (let i = 0; i < n; i++){ const x = B.x0 + (fx0 + R() * (fx1 - fx0)) * B.L, y = CY + (fy0 + R() * (fy1 - fy0)) * B.h; if (inBody(x, y)) P.push(fn(x, y, R())); } };
    if (c.band){ const a = B.x0 + B.L * 0.1, m = B.x0 + B.L * 0.45, e = B.xp + 8, w = B.h * 0.3; P.push(`<path d="M${a} ${CY + 2} C${a + 30} ${CY - w} ${m} ${CY - w * 1.1} ${e} ${CY - w * 0.35} L${e} ${CY + w * 0.4} C${m} ${CY + w * 1.2} ${a + 30} ${CY + w} ${a} ${CY + 2}Z" fill="${c.band}" opacity="${c.bandO || 0.45}"/>`); }
    (c.patterns || []).forEach(p => {
      if (p.t === 'spots') scatter(p.n, p.x[0], p.x[1], p.y[0], p.y[1], (x, y, r) => `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="${(p.r * (0.7 + r * 0.6)).toFixed(2)}" fill="${p.c}" opacity="${p.o ?? 0.9}"/>`);
      if (p.t === 'halo') scatter(p.n, p.x[0], p.x[1], p.y[0], p.y[1], (x, y, r) => `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="${(p.r * 1.9).toFixed(2)}" fill="${p.h}" opacity=".85"/><circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="${p.r.toFixed(2)}" fill="${r < (p.mix || 0) ? p.c2 : p.c}"/>`);
      if (p.t === 'worms') scatter(p.n, p.x[0], p.x[1], p.y[0], p.y[1], (x, y, r) => `<path d="M${x.toFixed(1)} ${y.toFixed(1)} q${4 + r * 5} ${-3 + r * 6} ${9 + r * 6} ${r * 3}" stroke="${p.c}" stroke-width="${p.w || 2.2}" fill="none" stroke-linecap="round" opacity=".85"/>`);
      if (p.t === 'bars'){ for (let i = 0; i < p.n; i++){ const x = B.x0 + B.L * (p.x[0] + (p.x[1] - p.x[0]) * i / (p.n - 1)); P.push(`<path d="M${x} ${CY - B.h * 1.1} Q${x + 4} ${CY} ${x - 2} ${CY + B.h * (p.down || 0.45)}" stroke="${p.c}" stroke-width="${p.w || 8}" stroke-opacity="${p.o || 0.55}" fill="none" stroke-linecap="round"/>`); } }
      if (p.t === 'stripe') P.push(`<path d="M${B.x0 + B.L * 0.18} ${CY} L${B.xp + 4} ${CY}" stroke="${p.c}" stroke-width="${p.w || 7}" stroke-opacity="${p.o || 0.7}" stroke-dasharray="${p.dash || ''}" fill="none"/>`);
      if (p.t === 'scales'){ for (let x = B.x0 + B.L * 0.24; x < B.xp; x += 9) for (let y = CY - B.h; y < CY + B.h; y += 7){ const yy = y + ((x / 9) % 2) * 3.5; if (inBody(x, yy)) P.push(`<path d="M${x - 4} ${yy} q4 5 8 0" stroke="${p.c}" stroke-opacity=".35" stroke-width=".8" fill="none"/>`); } }
      if (p.t === 'blotch') scatter(p.n, p.x[0], p.x[1], p.y[0], p.y[1], (x, y, r) => `<ellipse cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" rx="${(p.r * (1 + r)).toFixed(1)}" ry="${(p.r * 0.6).toFixed(1)}" fill="${p.c}" opacity="${p.o || 0.5}"/>`);
      if (p.t === 'scutes'){ for (let i = 0; i < 13; i++){ const f = 0.18 + i * 0.058, [x, y] = B.at(f, -0.78 + (f > 0.6 ? (f - 0.6) * 0.8 : 0)); P.push(`<path d="M${x - 5} ${y + 2} L${x} ${y - 4} L${x + 5} ${y + 2}Z" fill="#e9ecee" stroke="#6b7479" stroke-width=".7"/>`); } for (let i = 0; i < 16; i++){ const [x, y] = B.at(0.2 + i * 0.05, 0.1); P.push(`<path d="M${x - 3} ${y + 1} L${x} ${y - 2.5} L${x + 3} ${y + 1}Z" fill="#d7dcdf" opacity=".9"/>`); } }
    });
    // lateral line, gill cover, eye, mouth
    const eye = c.eye || [0.075, -0.18], er = B.h * (c.eyeR ?? 0.17), [ex, ey] = B.at(eye[0], eye[1]);
    const gx = B.x0 + B.L * (c.gill ?? 0.19);
    D.push(`<path d="M${gx} ${CY - B.h * 0.62} Q${gx + 10} ${CY} ${gx - 2} ${CY + B.h * 0.72}" stroke="rgba(0,0,0,.28)" stroke-width="1.3" fill="none"/>`);
    D.push(`<path d="M${B.x0 + B.L * 0.21} ${CY - B.h * 0.18} Q${B.x0 + B.L * 0.6} ${CY - B.h * 0.14} ${B.xp} ${CY - 1}" stroke="rgba(0,0,0,.2)" stroke-width=".8" fill="none"/>`);
    const mouth = c.mouth ?? 0.08, [mx, my] = B.at(mouth, c.mouthY ?? 0.12);
    D.push(`<path d="M${B.x0 + 1} ${CY + (c.snout ?? 0.1) * B.h * 0.8} Q${(B.x0 + mx) / 2} ${my + 1} ${mx} ${my}" stroke="rgba(0,0,0,.55)" stroke-width="1.3" fill="none" stroke-linecap="round"/>`);
    if (c.slash) D.push(`<path d="M${B.x0 + B.L * 0.035} ${CY + B.h * 0.42} Q${B.x0 + B.L * 0.08} ${CY + B.h * 0.55} ${B.x0 + B.L * 0.13} ${CY + B.h * 0.5}" stroke="#d6362e" stroke-width="3" fill="none" stroke-linecap="round"/>`);
    if (c.barbel) (Array.isArray(c.barbel) ? c.barbel : [[0.05, 0.3, 10]]).forEach(([f, y, l]) => { const [bx, by] = B.at(f, y); D.push(`<path d="M${bx} ${by} q-2 ${l * 0.6} -4 ${l}" stroke="#5a4e3e" stroke-width="1.4" fill="none" stroke-linecap="round"/>`); });
    D.push(`<circle cx="${ex}" cy="${ey}" r="${er}" fill="${c.eyeC || '#d9c27a'}" stroke="rgba(0,0,0,.5)" stroke-width=".8"/><circle cx="${ex + er * 0.1}" cy="${ey}" r="${er * 0.6}" fill="#111"/><circle cx="${ex - er * 0.25}" cy="${ey - er * 0.3}" r="${er * 0.22}" fill="#fff" opacity=".85"/>`);
    // pectoral fin, over the body
    const [px, py] = B.at((c.gill ?? 0.19) + 0.02, 0.45);
    D.push(`<path d="M${px} ${py} Q${px + 22} ${py + 2} ${px + 30} ${py + 14} Q${px + 12} ${py + 12} ${px} ${py + 6}Z" fill="${c.lowFin || finC}" opacity=".8"/>`);
    // numbered marks
    const marks = (opts.marks === false ? [] : (c.marks || [])).map(([f, yf], i) => { const [mx2, my2] = B.at(f, yf); return `<g><circle cx="${mx2}" cy="${my2}" r="8.5" fill="#0e5a73" stroke="#fff" stroke-width="2"/><text x="${mx2}" y="${my2 + 3.6}" text-anchor="middle" font-size="10.5" font-weight="700" fill="#fff" font-family="system-ui,sans-serif">${i + 1}</text></g>`; });
    const adiMark = opts.adiMark && c.adipose ? (() => { const [ax, ay] = B.at(c.adipose[0] + 0.025, -0.62); return `<circle cx="${ax}" cy="${ay}" r="13" fill="none" stroke="#0e8fb3" stroke-width="2.2"/><path d="M${ax} ${ay - 13} L${ax} ${ay - 28}" stroke="#0e8fb3" stroke-width="1.6"/>`; })() : '';
    return `<svg viewBox="0 0 ${VB_W} ${VB_H}" class="fishsvg" role="img" aria-label="${c.name}${opts.clipped ? ', adipose fin clipped' : ''}"><defs>${grad}</defs>
      <ellipse cx="${(B.x0 + B.xp) / 2 + 20}" cy="${CY + B.h * 1.25 + 10}" rx="${B.L * 0.5}" ry="5" fill="#000" opacity=".07"/>${F.join('')}${bodyShapes.join('')}<g clip-path="url(#${id}c)">${P.join('')}<path d="${B.d}" fill="url(#${id}s)"/></g>${ADI.join('')}${D.join('')}${adiMark}${marks.join('')}</svg>`;
  }

  /* per species: shape, colours, pattern, and marks (anchor points for the numbered tips, in body fractions) */
  const SPECIES = {
    RB:{ name:'Rainbow trout', h:30, back:'#4e6b52', side:'#c3cbc0', belly2:'#e6e4dc', band:'#e0708a', bandO:.5, adipose:[0.8], fork:.3, seed:11,
      patterns:[{ t:'spots', n:95, x:[.08, 1], y:[-1, -0.05], r:1.5, c:'#1a1a1a' }, { t:'spots', n:22, x:[.55, 1], y:[0, .5], r:1.2, c:'#1a1a1a' }],
      tips:['Pink or red band along the side','Small black spots, mostly above the middle of the side','Rows of black spots on the tail'], marks:[[0.45, 0.05], [0.4, -0.6], [1.12, -0.35]] },
    ST:{ name:'Steelhead', h:31, back:'#526e7a', side:'#d5dcdf', belly2:'#eef0ef', band:'#e0808f', bandO:.18, adipose:[0.8], fork:.3, seed:13, xp:322,
      patterns:[{ t:'spots', n:55, x:[.1, 1], y:[-1, -0.3], r:1.3, c:'#1a1a1a' }],
      tips:['A rainbow trout that went to sea: bright silver, 50 cm or longer','Faint spots, mostly on the back and tail','Hatchery steelhead are missing this small fin (the adipose fin)'], marks:[[0.5, 0.2], [0.35, -0.7], [0.83, -0.72]] },
    CT:{ name:'Cutthroat trout', h:29, back:'#5d6537', side:'#c8b683', belly2:'#e2d7b3', adipose:[0.8], fork:.25, slash:true, mouth:.14, seed:17,
      patterns:[{ t:'spots', n:150, x:[.06, 1], y:[-1, .8], r:1.35, c:'#1c1c14' }],
      tips:['Red or orange slash under the lower jaw (may be faint)','Large mouth, reaching well past the eye','Black spots all over, more toward the tail'], marks:[[0.075, 0.62], [0.14, 0.15], [0.78, 0.35]] },
    GB:{ name:'Brown trout', h:30, back:'#5c4526', side:'#c7985a', belly2:'#e8cf8e', bellyC:'#f3e2b2', adipose:[0.8], adiC:'#c7743a', fork:.08, tailC:'#7a6038', seed:19,
      patterns:[{ t:'halo', n:60, x:[.08, .95], y:[-.95, .45], r:1.9, c:'#1b140c', c2:'#c4362b', mix:.3, h:'#efe0c0' }],
      tips:['Black and red spots, many with pale halos','Tail with few or no spots','Orange-rimmed adipose fin'], marks:[[0.5, -0.2], [1.12, 0], [0.83, -0.72]] },
    KO:{ name:'Kokanee', h:28, back:'#2d5b86', side:'#cfd8df', belly2:'#eef1f3', adipose:[0.8], fork:.5, anal:[0.64, 0.8, 0.5], seed:23,
      patterns:[], tips:['No black spots on the sides','Dark blue back, bright silver sides','Long fin under the tail end (13 or more rays)'], marks:[[0.45, 0.2], [0.35, -0.75], [0.72, 1.05]] },
    DV:{ name:'Dolly Varden/bull trout', h:27, back:'#4d5a4c', side:'#8f9a87', belly2:'#c9cbbf', adipose:[0.8], fork:.15, whiteEdge:true, lowFin:'#b3695b', seed:29,
      patterns:[{ t:'spots', n:70, x:[.1, 1], y:[-0.9, .6], r:1.8, c:'#f1d7cc', o:.85 }, { t:'spots', n:18, x:[.2, .95], y:[-.2, .5], r:1.4, c:'#e57a74', o:.8 }],
      tips:['Pale pink or cream spots, smaller than the pupil','No worm-like markings on the back fin','White front edges on the lower fins','Bull trout: big, broad, flat head'], marks:[[0.5, -0.4], [0.5, -1.35], [0.5, 1.3], [0.09, -0.55]] },
    LT:{ name:'Lake trout', h:30, back:'#46544d', side:'#7f8c84', belly2:'#c6ccc4', adipose:[0.8], fork:.62, tailLen:62, seed:31,
      patterns:[{ t:'spots', n:170, x:[.06, 1], y:[-1, .7], r:1.9, c:'#e6e3cf', o:.85 }],
      tips:['Pale spots all over a dark body, no red','Worm-like light markings on the back and back fin','Deeply forked tail'], marks:[[0.45, 0.3], [0.4, -0.75], [1.14, -0.5]] },
    EB:{ name:'Brook trout', h:30, back:'#3f4a28', side:'#7c7f47', belly2:'#c98548', bellyC:'#e4974f', adipose:[0.8], fork:.05, whiteEdge:true, lowFin:'#d9683a', tailC:'#5d6034', seed:37,
      patterns:[{ t:'worms', n:70, x:[.08, .95], y:[-1, -0.45], c:'#c9c083' }, { t:'spots', n:40, x:[.15, .95], y:[-.4, .5], r:2, c:'#e8d27a' }, { t:'halo', n:14, x:[.2, .9], y:[-.25, .45], r:1.6, c:'#d8372c', h:'#5c86c9' }],
      tips:['Worm-like markings on the back and back fin','Red spots with blue halos','Orange lower fins with white front edges'], marks:[[0.3, -0.8], [0.55, 0.15], [0.5, 1.3]] },
    GR:{ name:'Arctic grayling', h:26, back:'#4f566b', side:'#a7acb8', belly2:'#dcdde2', adipose:[0.82], fork:.45, sail:true, dorsal:[0.3, 0.6, 1], sailC:'#5a4b6f', sailSpot:'#58a3c9', seed:41,
      patterns:[{ t:'spots', n:14, x:[.18, .4], y:[-.5, .2], r:1.6, c:'#1f1f25' }],
      tips:['Very tall, sail-like back fin with blue-purple spots','A few black spots near the head','Small mouth'], marks:[[0.45, -2.1], [0.3, -0.1], [0.06, 0.25]] },
    LW:{ name:'Whitefish', h:27, back:'#5f6448', side:'#c9ccc2', belly2:'#e9eae4', adipose:[0.8], fork:.5, mouth:.05, snout:.25, seed:43,
      patterns:[{ t:'scales', c:'#3f4636' }],
      tips:['Large scales, silver sides','Small mouth under a blunt snout','Has an adipose fin; no spots'], marks:[[0.5, 0.1], [0.04, 0.3], [0.83, -0.72]] },
    WSG:{ name:'White sturgeon', h:20, back:'#6d7479', side:'#b2b8bb', belly2:'#dde0e1', dorsal:[0.8, 0.88, 0.7], anal:[0.8, 0.87, 0.5], tailShape:'shark', tailLen:60, x0:18, xp:322, snout:.1,
      top:[[0, 0.1], [0.07, -0.55], [0.25, -0.95], [0.5, -1], [0.75, -0.75], [0.9, -0.45]], bot:[[0.9, 0.45], [0.7, 0.7], [0.45, 0.9], [0.2, 0.8], [0.05, 0.45]], barbel:[[0.05, 0.45, 9], [0.07, 0.5, 9]], eyeR:.12, eye:[0.09, -0.3], mouth:.1, mouthY:.62, seed:47,
      patterns:[{ t:'scutes' }], tips:['Rows of bony plates, no scales','Barbels under a pointed snout','Upper lobe of the tail much longer'], marks:[[0.4, -1.05], [0.06, 1.15], [1.2, -1.5]] },
    BB:{ name:'Burbot', h:18, back:'#5d4a2f', side:'#9a8057', belly2:'#cdbb95', dorsal:[0.48, 0.97, 0.35], dorsal2:[0.28, 0.42, 0.9], anal:[0.5, 0.97, 0.3], tailShape:'round', tailLen:34, adipose:false, x0:22, xp:338,
      top:[[0, 0.15], [0.05, -0.6], [0.18, -0.95], [0.45, -1], [0.75, -0.85], [0.92, -0.55]], bot:[[0.92, 0.55], [0.72, 0.85], [0.42, 1], [0.18, 0.95], [0.05, 0.6]], barbel:[[0.06, 0.55, 11]], seed:53,
      patterns:[{ t:'blotch', n:60, x:[.05, 1], y:[-1, .8], r:4, c:'#3a2c18', o:.5 }],
      tips:['One barbel under the chin','Two back fins, the second very long','Long, eel-like mottled body'], marks:[[0.06, 1.5], [0.7, -1.6], [0.6, 0.2]] },
    NP:{ name:'Northern pike', h:21, back:'#3f5130', side:'#78875a', belly2:'#d9dcc4', dorsal:[0.8, 0.9, 0.9], anal:[0.8, 0.9, 0.7], fork:.35, snout:0.02, mouth:.13, mouthY:.1, x0:16, xp:326, lowFin:'#8a7a4a', finO:.9,
      top:[[0, 0.02], [0.08, -0.35], [0.2, -0.75], [0.45, -0.95], [0.7, -0.95], [0.9, -0.6]], bot:[[0.9, 0.6], [0.7, 0.9], [0.45, 0.95], [0.2, 0.75], [0.07, 0.3]], eye:[0.11, -0.35], seed:59,
      patterns:[{ t:'blotch', n:80, x:[.15, 1], y:[-.9, .7], r:3.2, c:'#e7e6b8', o:.75 }],
      tips:['Long, flat, duck-bill snout','Back fin set far back, near the tail','Rows of pale bean-shaped spots'], marks:[[0.04, -0.2], [0.85, -1.9], [0.5, 0.2]] },
    WP:{ name:'Walleye', h:26, back:'#5f5a2e', side:'#b8a55f', belly2:'#e2d9b0', spiny:[0.3, 0.5, 0.9], spinyC:'#6b6337', dorsal:[0.55, 0.74, 0.7], anal:[0.68, 0.8, 0.5], fork:.4, eyeR:.2, eyeC:'#e8e2c4', mouth:.14, seed:61, tailC:'#8a8154',
      patterns:[{ t:'bars', n:5, x:[.3, .85], c:'#3a3417', w:9, o:.45, down:.1 }],
      tips:['Big, glassy eye','Spiny front back fin','White tip on the lower tail lobe','Sharp, fang-like teeth'], marks:[[0.075, -0.6], [0.4, -2.1], [1.18, 0.9], [0.12, 0.45]] },
    YP:{ name:'Yellow perch', h:30, back:'#4c5a23', side:'#d6c24a', belly2:'#efe5a8', spiny:[0.28, 0.5, 0.9], spinyC:'#7b7a3a', dorsal:[0.55, 0.72, 0.7], anal:[0.68, 0.79, 0.5], fork:.35, lowFin:'#e78a2e', finO:.95, seed:67,
      patterns:[{ t:'bars', n:7, x:[.22, .9], c:'#2e3312', w:8, o:.6, down:.6 }],
      tips:['Six to nine dark bars down the sides','Orange lower fins','Spiny front back fin; no fang-like teeth'], marks:[[0.5, 0.1], [0.45, 1.3], [0.38, -2.1]] },
    BCB:{ name:'Black crappie', h:44, back:'#43533f', side:'#b9c0ad', belly2:'#e2e4d8', dorsal:[0.4, 0.7, 0.6], anal:[0.45, 0.75, 0.5], fork:.2, tailH:40, ph:12, mouth:.12, mouthY:-.05, snout:-0.1,
      top:[[0, -0.1], [0.08, -0.55], [0.25, -0.95], [0.45, -1], [0.66, -0.8], [0.86, -0.42]], bot:[[0.86, 0.42], [0.66, 0.85], [0.42, 1], [0.2, 0.85], [0.06, 0.4]], x0:60, xp:300, seed:71,
      patterns:[{ t:'blotch', n:120, x:[.05, 1], y:[-1, 1], r:2.6, c:'#1d2419', o:.7 }],
      tips:['Deep, flat body covered in black speckles','Large, rounded back and bottom fins','Mouth reaches the front of the eye'], marks:[[0.5, 0.2], [0.55, -1.5], [0.12, -0.05]] },
    SMB:{ name:'Smallmouth bass', h:31, back:'#4a4226', side:'#9c8650', belly2:'#dccf9f', spiny:[0.3, 0.52, 0.65], spinyC:'#5f5634', dorsal:[0.52, 0.75, 0.65], anal:[0.66, 0.8, 0.5], fork:.2, eyeC:'#c9432e', mouth:.12, seed:73,
      patterns:[{ t:'bars', n:9, x:[.25, .9], c:'#2c2612', w:6, o:.5, down:.5 }],
      tips:['Bronze-brown with faint vertical bars','Jaw reaches about the middle of the eye','Red or orange eye'], marks:[[0.55, 0.15], [0.12, 0.3], [0.075, -0.55]] },
    LMB:{ name:'Largemouth bass', h:31, back:'#3e5226', side:'#9fb07a', belly2:'#e2e6cd', spiny:[0.3, 0.48, 0.6], spinyC:'#4d5d31', dorsal:[0.53, 0.75, 0.65], anal:[0.66, 0.8, 0.5], fork:.2, mouth:.17, seed:79,
      patterns:[{ t:'stripe', c:'#1f2a12', w:9, o:.7, dash:'12 5' }],
      tips:['Dark stripe along the side','Jaw reaches past the back of the eye','Front and back fins almost separate'], marks:[[0.6, 0.02], [0.17, 0.3], [0.5, -1.5]] },
    GE:{ name:'Goldeye', h:32, back:'#5a6b76', side:'#d9dee0', belly2:'#f0f1ef', dorsal:[0.62, 0.72, 0.6], anal:[0.55, 0.8, 0.45], fork:.55, eyeC:'#e8b93a', eyeR:.2, seed:83,
      patterns:[], tips:['Large golden eye','Deep, flat silver body','Back fin set behind the middle'], marks:[[0.075, -0.6], [0.4, 0.2], [0.67, -1.6]] },
    IN:{ name:'Inconnu', h:28, back:'#5b6a6a', side:'#d0d6d4', belly2:'#eef0ee', adipose:[0.8], fork:.5, mouth:.12, mouthY:.05, snout:.25, seed:89,
      patterns:[{ t:'scales', c:'#3d4a4a' }], tips:['Large mouth with a jutting lower jaw','Big silver scales','Has an adipose fin'], marks:[[0.06, 0.35], [0.5, 0.1], [0.83, -0.72]] },
  };
  SPECIES.MW = { ...SPECIES.LW, name:'Mountain whitefish', h:24, snout:.3, seed:97 };
  return { draw, SPECIES, has: k => !!SPECIES[k] };
})();
/* ================= shared UI ================= */
const state = { wi:0, pi:0, md:TODAY, open:new Set(), dd:new Set(), all:false, tab:'card', kit:'licence', who:{ residency:'resident', age:'16_plus', guidance:'non_guided', status:'none' } };
let PLACE = null, MODEL = null, REOPEN = null;
function rowTitle(row){
  if (row.pool){ const w = row.pool.f.species || []; if (w.length === 1 && GROUPS[w[0]]) return w[0] === 'TROUT_CHAR' && (row.pool.f.species_except || []).includes('CHAR') ? 'Trout' : GROUPS[w[0]].name; return Names(row.members, ' and '); }
  if (row.prot) return 'Protected species';
  if (!row.members.length && row.win) return cap(spName((row.win.f.species || [])[0]).toLowerCase());
  return Names(row.members, ' and ');
}
// the next day the answers file does not call the water closed
function nextOpen(w, md){ const i = DAYS.indexOf(md); for (let k = 1; k < 366; k++){ const d = DAYS[(i + k) % 365]; if (!isClosedAt(w, d)) return d; } return null; }
function waterSegs(w){ if (w._wsegs) return w._wsegs; const segs = []; let cur = null, len = 0;
  for (const d of DAYS){ const s = isClosedAt(w, d) ? 'closed' : 'keep'; if (s === cur) len++; else { if (cur) segs.push([cur, len]); cur = s; len = 1; } }
  segs.push([cur, len]); return (w._wsegs = segs); }
// tidal water is a documented state (answers display frame, D12): the freshwater rules do not apply
// the words are the export's tidal note (angler words only since answers 2.1, review B17), as v35 showed them
function tidalHtml(w, md){
  const t = settle(w, md).status || {};
  if (t.status !== 'tidal') return missing(`the tidal state on ${fmtMd(md)} (display frame for this part key and segment)`);
  return `<div class="banner"><div class="big">Tidal water</div><p>${esc(w.water.tidal?.guide || t.note || '')}</p></div>`;
}
function waterStrip(w, md){
  if (isTidal(w)) return tidalHtml(w, md);
  if (statusAt(w, md) == null) return missing(`open or closed on ${fmtMd(md)} (display frame for this part key and segment)`);
  return waterStrip0(w, md);
}
function waterStrip0(w, md){
  const segs = waterSegs(w), ctx = settle(w, md), openNow = ctx.status.status !== 'closed', has = new Set(segs.map(s => s[0]));
  // "Open, except some parts": a retention rule of an undrawn part in force today (answers ladder not_yet_mapped)
  const partShut = openNow && ctx.inpart.some(r => r.now && r.family === 'retention');
  return `<div class="wstrip"><div class="wsline"><span class="wsstat ${openNow ? 'open' : 'closed'}">${openNow ? (partShut ? 'Open, except some parts' : 'Open') : 'Closed'}</span><span class="muted small">${md === TODAY ? 'today' : 'on ' + fmtMd(md)}${momentsOf(w) && daySegs(w, md).length > 1 ? ` (${weekdayOf(md).slice(0, 3)})` : ''}</span>${(() => { if (!openNow){ const n = nextOpen(w, md); return n ? `<span class="wsoon open">Opens ${fmtMd(n)}</span>` : ''; } const i = DAYS.indexOf(md); for (let k = 1; k <= 21; k++){ const d = DAYS[(i + k) % 365]; if (isClosedAt(w, d)) return `<span class="wsoon">Closed from ${fmtMd(d)}</span>`; }
      const tr = (MODEL && w === PLACE) ? MODEL.rows.find(r => !r.pool && ['closed','release'].includes(r.kind) && r.members.some(S => ['RB','CT','EB','GB','LT','DV'].includes(S))) : null;
      if (tr){ const ru = runOf(strip(w, tr.members), md); return `<span class="wsoon">Trout ${tr.kind === 'closed' ? 'closed' : 'release only'}${ru.all ? ' all year' : ` until ${fmtMd(ru.until)}`}</span>`; }
      return ''; })()}</div>
    ${segBar(segs, md, 'big', fmtMd(md))}${MONTHS_ROW}${has.size > 1 ? `<div class="slegend"><span><i class="s-keep"></i>open</span><span><i class="s-closed"></i>closed: no fishing for any game fish</span></div>` : ''}</div>`;
}
function miniStrip(w, members, md){
  const segs = strip(w, members); if (segs.length < 2) return '';
  const toDays = ss => ss.flatMap(([s, n]) => Array(n).fill(s)); const a = toDays(segs), b = toDays(waterSegs(w)), base = segs.find(s => s[0] !== 'closed')?.[0] || 'closed';
  if (a.every((s, i) => s === (b[i] === 'closed' ? 'closed' : base))) return '';
  return segBar(segs, md, 'mini') + shortWindows(segs);
}
// the part picker's choices (answers display.waters picker)
const choicesOf = water => DWATERS[water.id]?.picker?.choices || [];
const groupOf = (water, pi) => choicesOf(water).find(g => g.parts.includes(pi));
const partLabel = (water, pi) => DWATERS[water.id]?.parts?.[pi]?.label;
// when several stretches closed all year are one choice, list them with the closure that closes each
function mergedHtml(){
  const water = WATERS[state.wi], g = groupOf(water, state.pi);
  if (!g || !g.closed || g.parts.length < 2) return '';
  return `<div class="merged"><div class="lbl">Closed all year on ${g.parts.length} stretches (${g.sections} sections)</div><ul>${g.parts.map(i => { const pl = makePlace(state.wi, i), b = pl.k == null ? null : settle(pl, state.md).broad[0];
    return `<li><span>${esc(partLabel(water, i) || '')} <span class="muted">· ${water.parts[i].sections} section${water.parts[i].sections > 1 ? 's' : ''}</span></span>${b ? ` <button class="srcbtn inline" type="button" data-rule="${esc(b.key)}">${esc(RANK[String(b.rank)].t)}</button>` : ''}</li>`; }).join('')}</ul></div>`;
}

// how long today's state lasts, and what comes next (back-to-back closures read as one run)
function runOf(segs, md){
  const days = segs.flatMap(([st, n]) => Array(n).fill(st)), i = DAYS.indexOf(md), now = days[i];
  let a = i, b = i, g = 0; while (days[(a + 364) % 365] === now && g++ < 365) a = (a + 364) % 365;
  g = 0; while (days[(b + 1) % 365] === now && g++ < 365) b = (b + 1) % 365;
  if (g >= 365) return { now, all:true };
  return { now, from:DAYS[a], until:DAYS[b], next:DAYS[(b + 1) % 365], then:days[(b + 1) % 365] };
}
function closedRuns(segs){
  let d = 0; const runs = []; segs.forEach(([st, n]) => { if (st === 'closed') runs.push([d, d + n - 1]); d += n; });
  if (!runs.length) return '';
  if (runs.length === 1 && runs[0][0] === 0 && runs[0][1] === 364) return 'All year.';
  // a closure running over New Year is one closure
  if (runs.length > 1 && runs[0][0] === 0 && runs[runs.length - 1][1] === 364){ const last = runs.pop(); runs[0] = [last[0], runs[0][1]]; }
  const wsg = waterSegs(PLACE).flatMap(([st, n]) => Array(n).fill(st));
  return runs.map(([a, b]) => { const whole = wsg[a] === 'closed' && wsg[(b + 365) % 365] === 'closed'; return `${whole ? 'The whole water is closed' : 'Closed'} ${rangeTxt(DAYS[a], DAYS[(b + 365) % 365])}.`; }).join(' '); }
const nextYr = (d, md) => DAYS.indexOf(d) < DAYS.indexOf(md) ? ' next year' : '';
function runUntil(segs, md){ const r = runOf(segs, md); return r.all ? '' : fmtMd(r.until) + nextYr(r.until, md); }
function runTxt(segs, md, tail){
  const r = runOf(segs, md); if (r.all) return tail ? '' : r.now === 'keep' ? '' : 'All year.';
  const NOW = { release:'Release only', closed:'Closed', keep:'You can keep them', none:'No rule' }, THEN = { release:'release only', closed:'closed', keep:'you can keep them', none:'no rule' };
  if (r.now === 'keep') return `You can keep them until ${fmtMd(r.until)}. Then ${THEN[r.then]} from ${fmtMd(r.next)}.`;
  let out = tail ? '' : `${NOW[r.now]} until ${fmtMd(r.until)}${nextYr(r.until, md)}.`, cur = r, k = 0;
  const wsg = waterSegs(PLACE), wClosed = d => runOf(wsg, d).now === 'closed';
  while (cur.then !== 'keep' && k++ < 3){ const nx = runOf(segs, cur.next); if (nx.all) break; out += nx.now === 'closed' && wClosed(nx.from) ? ` Then the whole water is closed ${rangeTxt(nx.from, nx.until)}.` : ` Then ${THEN[nx.now]} ${rangeTxt(nx.from, nx.until)}.`; cur = nx; if (nx.next === r.from) break; }
  if (cur.then === 'keep') out += ` You can keep them from ${fmtMd(cur.next)}${nextYr(cur.next, md)}.`; else if (!segs.some(([st]) => st === 'keep')) out += ' No keeping at any time of year.';
  return out.trim();
}
// the closures that close the water on a day: of the decided `closing` (display frame), those closing the most game
// fish — every one where a single closure does; a closure of one fish (white sturgeon) beside them closes nothing more
function closersAt(w, md){ const st = settle(w, md).status; const cl = (st?.status === 'closed' ? st.closing : []) || [];
  const top = Math.max(0, ...cl.map(([, fs]) => fs.length)), all = GAME.filter(S => cl.some(([, fs]) => fs.includes(S))).length;
  return cl.filter(([, fs]) => fs.length === top || top < all && !cl.some(([, gs]) => gs.length > fs.length && fs.every(S => gs.includes(S)))).map(([ix]) => w.R(ix)); }
function closedBanner(what){
  if (MODEL.status !== 'closed') return '';
  if (!MODEL.broad.length) return `<div class="banner"><div class="big">Closed</div>${missing('the closure that closes it (no closure in the ladder speaks for the game fish today)')}${mergedHtml()}</div>`;
  const b = MODEL.broad[0];
  // one closure can run straight into the next: list each one between today and the day it reopens (answers ladder per segment)
  // the run shown ("Jul 4–Nov 15") is made of every closure that closes it: the answers' decided `closing` of each
  // segment of the run, first day to last, merged across back-to-back segments (user test 2026-10-08, item 10)
  const run = REOPEN ? runOf(waterSegs(PLACE), state.md) : null;
  // a closure printed "unless opened" whose proviso is the answer there (answers display frame `unless_opened`,
  // user ruling Z12/Q41: a national park — never a park reserve, which is plainly closed)
  const unl = new Set(), mark = st => (st?.unless_opened || []).forEach(ix => unl.add(PLACE.R(ix)));
  mark(MODEL.ctx?.status);
  const chain = []; if (run && !run.all){ let d = run.from, g = 0; while (g++ < 366){ mark(settle(PLACE, d).status); closersAt(PLACE, d).forEach(r => { if (!chain.includes(r)) chain.push(r); }); if (d === run.until) break; d = DAYS[(DAYS.indexOf(d) + 1) % 365]; } }
  const closeTxt = r => unl.has(r) ? (r.dr?.unless_opened || `(no words in the answers for ${r.key})`) : describe(r) + '.';
  // two rules printing the same closure (the same words, the same source) are one line
  const list = (chain.length ? chain : [b]).filter((r, i, a) => a.findIndex(x => describe(x) === describe(r) && x.rank === r.rank) === i);
  return `<div class="banner"><div class="big">${REOPEN ? `Closed · opens again ${fmtMd(REOPEN)}` : 'Closed all year'}</div>
    ${REOPEN ? `<p><b>No fishing for any game fish ${esc(rangeTxt(run.from, run.until))}.</b>${list.length > 1 ? ` Together these closures cover it:` : ''}</p>` : ''}
    <ul class="closechain">${list.map(r => `<li>${esc(closeTxt(r))} <span class="muted small">${esc(RANK[String(r.rank)].t)}</span> <button class="srcbtn inline" type="button" data-rule="${esc(r.key)}">Source</button></li>`).join('')}</ul>
    ${REOPEN ? `<button class="golink" type="button" data-md="${REOPEN}">Opens again ${fmtMd(REOPEN)}. See the ${what || 'rules'} from then ›</button>` : ''}${mergedHtml()}</div>`;
}
// days of the week in words: "Mon–Fri", "Sat–Sun", "Fri–Sun", "Sat"
function daysTxt(wds){ const ix = wds.map(d => WEEK.indexOf(d)).sort((a, b) => a - b), runs = [];
  ix.forEach(i => { const r = runs[runs.length - 1]; if (r && r[1] === i - 1) r[1] = i; else runs.push([i, i]); });
  const sh = i => WEEK[i].slice(0, 3); return runs.map(([a, b]) => a === b ? sh(a) : `${sh(a)}–${sh(b)}`).join(', '); }
const hoursTxt = h => timeCond({ hours:h });
function momentTxt(m){ if (!m) return ''; const d = m.weekdays.length === 7 ? '' : daysTxt(m.weekdays);
  return [d, m.hours ? (m.hours.in ? hoursTxt(m.hours) : 'other hours') : ''].filter(Boolean).join(', '); }
const answerTxt = d => !d ? 'no rule' : d.status === 'keep' ? `${d.daily} a day` : d.status === 'no_limit' ? 'no limit' : d.status === 'release' ? 'release' : 'closed';
// AT CERTAIN TIMES (answers 2.1): the day reads differently by weekday or hour. The card is the date's own moment
// (its weekday, outside any hours window); each other moment of the day is named with what it decides: a closure
// of some hours or days (its decided `closing`), a fish whose answer differs ("Kokanee: 5 a day Sat–Sun; release
// Mon–Fri"), an angler closure of some days. Every word is a lookup in the answers frames.
function timedHtml(ctx){
  const w = ctx.w, M = momentsOf(w); if (!M) return '';
  const ss = daySegs(w, ctx.md); if (ss.length < 2) return '';
  const cur = ctx.s, items = [], dispOf = j => { const f = frameIx(w, 'display', j); return f == null ? null : SEC.display.frames[f]; };
  // a closure of some hours or some days
  ss.filter(j => j !== cur && dispOf(j)?.status === 'closed' && dispOf(cur)?.status !== 'closed').forEach(j => {
    const cl = (dispOf(j).closing || []).map(([ix, fs]) => [w.R(ix), fs]), asked = GAME.filter(S => A.fish.includes(S));
    // as the banner reads them (`settle`): those closing every game fish, else every one listed — less any that
    // closes at the day's own moment too (it is on the card already)
    const all = cl.filter(([, fs]) => asked.every(S => fs.includes(S))), pick = (all.length ? all : cl.slice().sort((a, b) => b[1].length - a[1].length).slice(0, 1)).map(([r]) => r);
    const whole = pick.filter(r => !(dispOf(cur).closing || []).some(([ix]) => ix === r.ix));
    whole.forEach(r => items.push(`<li><button class="factbtn" type="button" data-rule="${esc(r.key)}"><b>${esc(cap(describe(r).replace(/ \(.*\)$/, '')))}</b><span class="where">${esc(momentTxt(M[j]))}</span></button></li>`));
    if (!whole.length) items.push(`<li>${missing('the closure that closes it ' + momentTxt(M[j]))}</li>`); });
  // a fish whose answer differs between the day's moments
  // (a moment the water is closed at is said once, above, never fish by fish)
  const rs = ss.filter(j => j === cur || dispOf(j)?.status !== 'closed').map(j => [j, rowsAt(w, j)]), spp = [...new Set(rs.flatMap(([, r]) => r ? r.spp : []))];
  for (const S of spp){
    const by = rs.map(([j, r]) => [j, r && r.R[S] ? mainRes(r.R[S].H, r.R[S].W) : null]);
    const words = by.map(([j, d]) => [j, answerTxt(d)]); if (new Set(words.map(([, t]) => t)).size < 2) continue;
    const grp = new Map(); words.forEach(([j, t]) => (grp.get(t) || grp.set(t, []).get(t)).push(j));
    const line = [...grp].map(([t, js]) => `${t} ${daysTxt([...new Set(js.flatMap(j => M[j].weekdays))])}${js.every(j => M[j].hours) ? (js.every(j => M[j].hours.in) ? ' ' + hoursTxt(M[js[0]].hours) : ' other hours') : ''}`).join('; ');
    items.push(`<li><button class="factbtn" type="button" data-fish="${esc(S)}"><b>${esc(spName(S))}: ${esc(line.replace(/ Mon–Sun/g, ''))}</b><span class="where">${esc('today (' + momentTxt(M[cur]) + '): ' + answerTxt(by.find(([j]) => j === cur)?.[1]))}</span></button></li>`); }
  // an angler closure of some days
  const spk = j => { const L = ladderAt(w, j), out = new Set(); if (L) for (const S of Object.keys(L)) for (const [ix, v] of L[S].none) if (v.state === 'speaks' && w.R(ix).k === 'anglerclosure') out.add(ix); return out; };
  const sets = ss.map(j => [j, spk(j)]), all = new Set(sets.flatMap(([, x]) => [...x]));
  for (const ix of all){ const on = sets.filter(([, x]) => x.has(ix)).map(([j]) => j); if (on.length === ss.length || on.includes(cur)) continue;
    const r = w.R(ix); items.push(`<li><button class="factbtn" type="button" data-rule="${esc(r.key)}"><b>${esc(cap(describe(r).replace(/ \(.*\)$/, '')))}</b><span class="where">${esc(r.tcond || daysTxt([...new Set(on.flatMap(j => M[j].weekdays))]))}</span></button></li>`); }
  return items.length ? `<div class="caveats top"><div class="lbl">⚠ At certain times</div><ul>${items.join('')}</ul></div>` : '';
}
function anglerClosures(ctx){
  const a = ctx.anglerclosed;
  return a.length ? `<div class="caveats top"><div class="lbl">⚠ Closed to some anglers</div><ul>${a.map(r => `<li><button class="factbtn" type="button" data-rule="${esc(r.key)}"><b>${esc(r.label)}</b></button></li>`).join('')}</ul></div>` : '';
}
function uncertainHtml(kinds){
  const u = (WATERS[state.wi].uncertain || []).map(RX).filter(r => r && kinds.includes(r.family));
  return u.length ? `<div class="caveats top check"><div class="lbl">? Check: rules for parts of this water that couldn’t be placed on the map</div><ul>${u.map(r => `<li><button class="factbtn" type="button" data-rule="${esc(r.key)}"><b>${esc(clean(r.label))}</b><span class="where">${esc(friendlyWhy(r.prov.why || ''))}</span></button></li>`).join('')}</ul></div>` : '';
}
function inPartHtml(ctx, fams){
  const u = ctx.inpart.filter(r => fams.includes(r.family));
  if (!u.length) return '';
  const now = u.filter(r => r.now), later = u.filter(r => !r.now);
  const boat = r => r.family === 'vessel';
  const hot = now.some(r => !boat(r)), onlyBoat = now.length && now.every(boat);
  const where = r => { const p = cap(r.part || '').replace(/\(map ([A-Z])\)/, '(map $1 in the regulations)'); return /various|do not identify|buoys and signs/i.test(p) ? 'Where: some spots, marked by buoys and signs. Look for signs.' : 'Where: ' + p; };
  const li = r => `<li><button class="factbtn" type="button" data-rule="${esc(r.key)}"><b>${esc(clean(r.parts?.what || r.label).replace(/ — in part:.*$/, ''))}${r.wins.length ? `<span class="muted"> · ${esc(winTxt(r).slice(2))}</span>` : ''}</b><span class="where">${esc(where(r))}</span></button></li>`;
  const title = hot ? 'Fishing is different in some spots today' : onlyBoat ? 'Boat rules in some spots' : 'Some spots have their own rules';
  const calm = hot ? 'Everywhere else, follow this page.' : onlyBoat ? `Fishing rules are the same everywhere on this ${PLACE.kind === 'lake' ? 'lake' : 'river'}.` : 'Not in force today. The map can’t draw these places yet, so check where you are.';
  return `<details class="inpart${hot ? ' hot' : ''}"${hot ? ' open' : ''}><summary><span class="ic">ⓘ</span>${title}</summary><p class="calm">${calm}</p><ul class="plain">${(now.length ? now : later).map(li).join('')}</ul>${now.length && later.length ? `<details class="inl later"><summary>Other dates (${later.length})</summary><ul class="plain later">${later.map(li).join('')}</ul></details>` : ''}</details>`;
}
function sideHtml(ctx, fams){
  const u = ctx.side.filter(r => fams.includes(r.family));
  return u.length ? `<div class="caveats top"><div class="lbl">⚠ On one side of the channel only</div><ul>${u.map(r => `<li><button class="factbtn" type="button" data-rule="${esc(r.key)}"><b>${esc(clean(r.parts?.what || r.label))}</b><span class="where">${esc(cap(r.parts?.side || r.f.side + ' half only'))}${r.parts?.where ? ' · ' + esc(r.parts.where) : ''}</span></button></li>`).join('')}</ul></div>` : '';
}
function standingHtml(ctx){
  const s = ctx.standing;
  return s.length ? `<div class="caveats"><div class="lbl">Anywhere in B.C.</div><ul>${s.map(r => `<li><button class="factbtn" type="button" data-rule="${esc(r.key)}">No fishing: ${esc(lc(clean(r.verbatim).replace(/\.$/, '')))}</button></li>`).join('')}</ul></div>` : '';
}
// the possession footnote: a rule the answers give the role "possession"
function possessionHtml(){
  const keys = new Set(); Object.values(MODEL.R).forEach(x => [x.H, x.W].forEach(r => r && r.roles.forEach((v, k) => { if (v.role === 'possession') keys.add(k); })));
  const p = [...keys].map(k => PLACE.cands.find(r => r.key === k) || RULES[k]).filter(Boolean);
  if (!p.length) return '';
  const main = p.find(r => !(r.f.species || []).length) || p.slice().sort((a, b) => b.f.per_daily - a.f.per_daily)[0], ex = p.filter(r => r !== main && r.f.per_daily !== main.f.per_daily);
  // the words are the answers' (display.rules[].plain: "Possession: twice the daily quota (fish at home don’t count).", user ruling Q15)
  const said = [main, ...ex].map(r => say(r));
  if (said.some(t => !t)) return missing('the possession sentence (display.rules[].plain)');
  return `<div class="foot">${gi('possession_quota')} ${said.map(esc).join(' ')} <button class="srcbtn inline" type="button" data-rule="${esc(main.key)}">Source</button></div>`;
}
// whose count a pool is: answers rows[].scope {of: water | area | region | bc, entry, share, apart}
const scopeOf = row => { const s = row?.scope; if (!s) return null; if (s.of === 'bc') return 'B.C.'; if (s.of === 'region' || s.of === 'area') return D.entries[s.entry]?.name || row.pool?.prov?.entry_name || (s.of === 'area' ? 'the area' : 'the region'); return null; };
// a rule's own reach, for the words of an "outer" line (export provenance)
const ruleScope = r => r.rank === 4 ? 'B.C.' : r.rank === 3 ? (r.prov.entry_name || 'the region') : r.rank === 2 ? (r.prov.entry_name || 'the area') : null;
function scopeBadge(row){ if (!row.pool || row.kind !== 'keep') return ''; if (!row.scope) return `<span class="scope here">${esc('scope missing')}</span>`; const s = scopeOf(row); if (s && row.scope.share && row.narrow) return `<span class="scope wide">${esc(s)} · ${esc(row.narrow.f.water)}s</span>`; return s ? `<span class="scope wide">${esc(s)}-wide</span>` : `<span class="scope here">This water</span>`; }

/* ---------- plain facts ---------- */
function fact(l, row){
  const r = l.r;
  switch (l.t){
    case 'origin': return { m:'✕', c:'rel', long: l.o === 'wild' ? 'Hatchery fish only. Carefully release every wild one.' : 'Wild fish only. Carefully release every hatchery one.', short:`${cap(l.keepO)} only` };
    case 'subcap': return { m:r.f.take, c:'cap', long:`At most ${r.f.take} of your ${row.daily} can be ${lcNames([...l.members], ' or ')}`, short:`Max ${r.f.take}` };
    case 'cap': return l.carveOf ? { m:l.take, c:'cap', long:`Up to ${l.take} ${bandTxt(l.a, l.b)} (instead of ${l.carveOf.take})`, short:`${l.take} ${bandTxt(l.a, l.b)}` } : { m:l.take, c:'cap', long:`At most ${l.take} ${bandTxt(l.a, l.b)}`, short:`only ${l.take} ${bandTxt(l.a, l.b)}` };
    case 'rel': return { m:'✕', c:'rel', long:`Release any ${bandTxt(l.a, l.b)}`, short: l.a === 0 ? `${l.b} cm minimum` : l.b === Infinity ? `${l.a} cm maximum` : `none ${l.a}–${l.b} cm` };
    case 'annual': { const c = bands(r).find(s => s.take > 0); const co = c && c.a > 0; return { m:'yr', c:'cal', long:`Yearly limit: ${r.f.take}${co ? ` ${bandTxt(c.a, c.b)}. Smaller ones don’t count toward it` : ' a year'}${r.f.origin ? ` (${r.f.origin} fish)` : ''}`, short:`${r.f.take}${co ? ' ' + bandTxt(c.a, c.b) : ''} a year` }; }
    case 'possession_cap': return { m:'P', c:'cal', long:(say(r) || `(no words in the answers for ${r.key})`).replace(/\.$/, ''), short:`${r.f.take} in possession` };
    case 'record': { const mn = (r.f.lengths || []).find(x => x.min_cm)?.min_cm; return { m:'!', c:'cal', long:`Record each one you keep${mn ? ` over ${mn} cm` : ''} on your licence`, short: mn ? `record ones over ${mn} cm` : 'record it' }; }
    case 'duty': return { m:'!', c:'cal', long:clean(r.verbatim), short: r.type === 'stop_fishing_after_quota' ? 'stop fishing once you have your limit' : /record/i.test(r.parts?.duty || r.verbatim || '') ? 'record it' : clean(r.parts?.duty || r.parts?.what || 'see rule').toLowerCase().slice(0, 48) };
    case 'exc': return { m:'✕', c: l.status === 'closed' ? 'closed' : 'rel', long:(l.status === 'closed' ? 'Closed. Don’t fish for them' : 'Release every one') + winTxt(r), short: l.status === 'closed' ? 'Closed' : 'Release' };
    case 'xref': return { m:'→', c:'cap', long:`Has its own limit of ${l.daily === Infinity ? 'no limit' : l.daily + ' a day'} (its own row), and still counts toward this ${row.daily}`, short:`own limit ${l.daily === Infinity ? 'none' : l.daily}` };
    case 'outer': { const s = ruleScope(r); return { m:'⊂', c:'cap', long:`Also counts toward ${s ? s + '’s' : 'the'} total of ${r.f.take} ${writtenName(r)} a day${s ? ', all waters there together' : ''}. It never lets you keep more than the number here`, short:`counts toward ${r.f.take} ${writtenName(r)}` }; }
    case 'also': return l.capped ? { m:r.f.unlimited ? '∞' : r.f.take, c:'cap', long:`${r.rank <= 1 ? 'This water’s own limit' : 'Its own limit'} is ${r.f.unlimited ? 'no limit' : r.f.take + ' a day'}, but the ${row.daily} total caps it`, short:`own ${r.f.unlimited ? 'no limit' : r.f.take}, capped` } : { m:r.f.take, c:'cap', long:`Another limit also applies: ${describe(r)}`, short:`also max ${r.f.take}` };
    case 'origin2': return { m:l.daily === Infinity ? '∞' : l.daily, c:'cap', long:`${cap(l.o)} fish: ${l.daily === Infinity ? 'no limit' : l.daily + ' a day'}${l.min ? `, none under ${l.min} cm` : ''}${l.max ? `, none over ${l.max} cm` : ''} (${clean(l.r.parts?.what || l.r.label)})`, short:`${l.o} ${l.daily === Infinity ? 'no limit' : l.daily}` };
    case 'orphan': return { m:r.f.take, c:'cap', long:`${cap(describe(r).replace(/^Inside the day’s total: /, ''))}`, short:`max ${r.f.take}` };
    case 'partly': return { m:'~', c:'cal', long:`Partly lifted here: “${clean(r.parts?.what || r.label)}” still holds, except for the fish or anglers a lift names`, short:null };
    case 'caution': return { m:'?', c:'cal', long:`Unclear in the book: this limit ${l.says}`, short:'unclear: size' };
    case 'tnote': return { m:'i', c:'cal', long: l.only ? 'This regulation lists char separately, so “trout” here means trout only (rainbow, steelhead, cutthroat, brown). Char follow their own lines.' : 'Trout includes char (Dolly Varden/bull trout, lake trout, brook trout) unless the regulation lists char separately.', short:null };
    case 'steel': return { m:'ST', c:'cal', long:'Over 50 cm, a rainbow here counts as a steelhead: the steelhead rules apply to it', short:'over 50 cm = steelhead' };
    case 'outercap': return { m:r.f.take, c:'cap', long:`In that ${l.outer.f.take}, at most ${r.f.take} can be ${writtenName(r, ' or ')}`, short:null };
    case 'outersize': return { m:l.take, c:'cap', long:`In that ${l.outer.f.take}, at most ${l.take} ${bandTxt(l.a, l.b)}`, short:null };
  }
  return { m:'·', c:'cal', long:clean(r.verbatim), short:null };
}
function liftNoteHtml(row){ return (row.liftNotes || []).map(n => `<div class="flag"><b>!</b><span>${esc(`Lifted only ${n.q.when_targeting ? 'when fishing for ' + lcNames(expand(n.q.when_targeting)) : n.q.while ? 'while ' + n.q.while.join(', ').replace(/_/g, ' ') : n.q.lengths ? 'for fish ' + n.q.lengths.map(x => bandTxt(x.min_cm ?? 0, x.max_cm ?? Infinity)).join(', ') : 'for ' + lcNames(expand(n.q.species))}: “${clean(n.by.verbatim)}”`)}</span></div>`).join(''); }

/* ---------- today's card ---------- */
function valueHtml(row, big){
  if (row.kind === 'keep') return big ? `<div class="num">${row.daily}<small>${row.members.length > 1 ? 'a day, shared' : 'a day'}</small></div>` : `<span class="gv">${row.daily}<small> a day</small></span>`;
  if (row.kind === 'nolimit') return `<span class="pill nolimit">No limit</span>`;
  return `<span class="pill ${row.kind}">${row.kind === 'closed' ? 'Closed' : 'Release'}</span>${gi(row.kind === 'closed' ? 'no_fishing_for' : 'catch_and_release')}`;
}
const narrowAll = row => !!row.narrow && row.members.every(S => (row.narrow.species || []).includes(S) && !(row.narrow.speciesExcept || []).includes(S));
// fish names, a whole book group said by its name ("char" for Dolly Varden, lake trout and brook trout)
function groupedNames(codes, conj){
  let rest = [...codes]; const parts = [];
  for (const [g, v] of Object.entries(GROUPS)){ if (v.open || g === 'ALL_GAME_FISH' || g === 'TROUT_CHAR' || v.members.length < 2) continue; if (v.members.every(m => rest.includes(m))){ parts.push(lc(v.name)); rest = rest.filter(m => !v.members.includes(m)); } }
  return join([...parts, ...rest.map(c => lc(spName(c)))], conj);
}
// a stream (or lake) share of a regional total holds here (answers scope.share) and binds every fish of the row
const nar0 = row => !!(row.pool && row.scope?.share && narrowAll(row));
const ddOpen = k => state.dd.has(k) ? ' open' : '';
// trout and char first, in full; every other fish folds into one short list (readers: "70% of the scroll")
const TROUTISH = ['RB','CT','EB','GB','LT','DV','ST','BT','CT_ws','CT_c'];
function rowsHtml(){
  const all = MODEL.rows.map((r, i) => [r, i]);
  const main = all.filter(([r]) => (r.allMembers || r.members).some(S => TROUTISH.includes(S)));
  const rest = all.filter(x => !main.includes(x));
  if (!main.length || rest.length < 2) return all.map(([r, i]) => rowHtml(r, i)).join('');
  const short = r => r.kind === 'keep' ? `${r.daily} a day` : r.kind === 'nolimit' ? 'no limit' : r.kind === 'closed' ? 'closed' : 'release';
  return main.map(([r, i]) => rowHtml(r, i)).join('') +
    `<details class="others" data-dd="others"${ddOpen('others')}><summary><span class="otitle">Other fish</span><span class="olist">${rest.map(([r]) => `<span class="orow"><span class="on">${esc(rowTitle(r))}</span><b class="k-${r.kind}">${esc(short(r))}</b></span>`).join('')}</span></summary>${rest.map(([r, i]) => rowHtml(r, i)).join('')}</details>`;
}
function renderCard(){
  const el = document.getElementById('card');
  if (PLACE.k == null){ el.innerHTML = `<p class="foot">This part of the water is outside B.C.: no B.C. rules apply.</p>`; return; }
  if (isTidal(PLACE)){ el.innerHTML = waterStrip(PLACE, state.md); return; }
  const ctx = settle(PLACE, state.md);
  let h = waterStrip(PLACE, state.md) + closedBanner();
  if (MODEL.status !== 'closed' && ctx.status){
    h += inPartHtml(ctx, ['retention','access']) + sideHtml(ctx, ['retention','access']) + anglerClosures(ctx) + timedHtml(ctx);
    if (MODEL.missingRows) h += missing(`the card’s rows on ${fmtMd(state.md)} (rows frame for this part key and segment)`);
    else h += rowsHtml();
    h += uncertainHtml(['retention','access']);
    if (!MODEL.missingRows && !MODEL.rows.length) h += `<p class="foot">No quota rules reach this part of the water.</p>`;
    h += standingHtml(ctx) + possessionHtml();
  }
  el.innerHTML = h;
}
const adiposeHtml = () => `<div class="adipose2"><figure class="fishart"><figcaption><b>Wild</b> · adipose fin there</figcaption>${FISHART.draw('RB', { marks:false, adiMark:true })}</figure><figure class="fishart"><figcaption><b>Hatchery</b> · clipped, a healed scar instead</figcaption>${FISHART.draw('RB', { marks:false, adiMark:true, clipped:true })}</figure></div><p class="small muted">Look on the back, just in front of the tail. A small fin there means wild. Only a scar means hatchery. If you can’t tell, treat the fish as wild.</p>`;

/* ---------- the conditions on keeping, in words: one per answers rows[].conds entry, in its order ---------- */
function rowConds(row){
  const n = row.daily, conds = [];
  const groupWord = lc(rowTitle(row)).replace(/ and /g, ' or ');
  const po = row.pool.f.origin ? row.pool.f.origin + ' ' : '';
  const s = scopeOf(row);
  const steelWater = isSteelRiver();
  for (const c of row.conds || []){
    const who = c.who || null;
    if (c.c === 'origin') conds.push({ c:'origin', r:c.r, raw:c, t: c.o === 'wild' ? 'Hatchery fish only. Carefully release every wild one.' : 'Wild fish only. Carefully release every hatchery one.' });
    else if (c.c === 'size'){ const nn = po + (row.members.length > 1 ? groupWord.replace(/ or /g, ' and ') : lcNames(row.members)); conds.push({ c:'size', r:c.r, raw:c, t: c.a === 0 ? `Keep only ${nn} ${c.b} cm or longer.` : c.b === Infinity ? `Keep only ${nn} ${c.a} cm or shorter.` : `Carefully release any ${c.a}–${c.b} cm.` }); }
    else if (c.c === 'back'){ const names = cap(lcNames(who, ' and ')), dt = c.r?.wins.length ? ` (${winTxt(c.r).slice(2)})` : '', cl = c.status === 'closed' ? ' (closed: don’t fish for them)' : '';
      conds.push({ c:'back', who, r:c.r, raw:c, ph:`carefully release every one${dt}${cl}`, t:`${names}: carefully release every one${dt}${cl}.` }); }
    else if (c.c === 'group'){ const names = cap(lcNames(who, ' and ')), parts = [];
      (c.keep || []).forEach(p => { if (p.origin) parts.push(p.origin === 'hatchery' ? 'hatchery only (carefully release wild ones)' : 'wild only (carefully release hatchery ones)'); else parts.push(p.a === 0 ? `${p.b} cm or longer` : p.b == null ? `${p.a} cm or shorter` : `not ${p.a}–${p.b} cm`); });
      const p0 = parts.slice(); if (c.sub != null) parts.push(who.length > 1 ? `only ${c.sub} in total of these together` : `only ${c.sub}`);
      conds.push({ c:'group', who, r:c.r, raw:c, ph:p0.join(', '), sub:c.sub, t:`${names}: ${join(parts, ', and ')}.` }); }
    else if (c.c === 'cap' && c.r == null){ // a general size cap with carve-outs for some fish (they follow as their own entries)
      conds.push({ c:'cap', raw:c, sub:c.sub, t:`Only ${c.take} fish ${bandTxt(c.a, c.b)} that ${c.take > 1 ? "aren't" : "isn't a"} ${lcNames(c.except || [], ' or ')}.` }); }
    else if (c.c === 'cap' && who){ conds.push({ c:'cap', who, r:c.r, raw:c, sub:c.sub, ph:`${bandTxt(c.a, c.b)}: up to ${c.take}`, t:`${cap(lcNames(who, ' and '))} ${bandTxt(c.a, c.b)}: up to ${c.take}.` }); }
    else if (c.c === 'cap'){ const of = c.of && c.of.length ? ` (${lcNames(c.of, ' and ')})` : '';
      conds.push({ c:'cap', r:c.r, raw:c, sub:c.sub, t:`Only ${c.take} can be ${bandTxt(c.a, c.b)}${of}.`, except:c.except || null }); }
    else if (c.c === 'origin2') conds.push({ c:'origin2', r:c.r, raw:c, sub:c.sub, t:`${cap(c.o)} ${who ? lcNames(who, ' and ') : groupWord}: only ${c.daily == null ? 'unlimited' : c.daily} a day${c.min ? `, none under ${c.min} cm` : ''}${c.max ? `, none over ${c.max} cm` : ''}.` });
    else if (c.c === 'subcap') conds.push({ c:'subcap', r:c.r, raw:c, sub:c.sub, t:`Only ${c.r.f.take} of them can be ${keepNames(c.members || [])}.` });
    else if (c.c === 'outercap') conds.push({ c:'outercap', r:c.r, raw:c, sub:c.sub, t:`Only ${c.r.f.take} can be ${keepNames(c.r.species || [], c.r)}.` });
    else if (c.c === 'outersize') conds.push({ c:'outersize', r:c.r, raw:c, sub:c.sub, t:`Only ${c.take} can be ${bandTxt(c.a, c.b)}.` });
    else if (c.c === 'streamcap'){ const r = c.r, tr = r.species.filter(x => !r.speciesExcept.includes(x));
      const lifted = o => S => MODEL.R[S]?.[o]?.roles.get(r.key)?.role === 'lifted';
      const keepO = (S, o) => KEEPISH(MODEL.R[S]?.[o]?.status);
      const whoL = tr.filter(S => !(lifted('H')(S) && lifted('W')(S))).filter(S => lifted('H')(S) ? keepO(S, 'W') : lifted('W')(S) ? keepO(S, 'H') : (keepO(S, 'H') || keepO(S, 'W'))).map(S => lifted('H')(S) ? 'wild ' + lc(spName(S)) : lifted('W')(S) ? 'hatchery ' + lc(spName(S)) : lc(spName(S)));
      const L = whoL.length > 1 ? whoL.slice(0, -1).join(', ') + ' or ' + whoL[whoL.length - 1] : whoL[0];
      conds.push({ c:'streamcap', r, raw:c, sub:null, t:`Only ${r.f.take} can be a ${L}${r.wins.length ? ', ' + winTxt(r).slice(2) : ''}. This rule is for ${r.f.water}s.` }); }
    else conds.push({ c:c.c, r:c.r, raw:c, t:`(${c.c}: no words for this condition)` });
  }
  const allf = [...row.everyone, ...row.groups.flatMap(g => g.facts)];
  const whoN = l => !l.members || l.general || l.members.size === row.members.length && row.members.every(S => l.members.has(S)) ? null : [...l.members];
  const extra = allf.filter(l => ['annual','possession_cap','outer'].includes(l.t)).map(l => l.t === 'annual' ? (() => { const m = fact(l, row).short.replace(/ a year$/, '').match(/^(\d+)(.*)$/) || ['', '', '']; return `Per licence year: ${m[1]} ${whoN(l) ? lcNames(whoN(l), ' and ') : writtenName(l.r)}${m[2]}`; })()
    : l.t === 'possession_cap' ? (say(l.r) || `(no words in the answers for ${l.r.key})`).replace(/\.$/, '')
    : `Also counts toward ${ruleScope(l.r) ? ruleScope(l.r) + '’s' : 'a'} total of ${l.r.f.take} ${writtenName(l.r)} a day, all waters there together. It never lets you keep more than ${n} here.`);
  const hw = (row.conds || []).some(c => c.c === 'origin' || (c.c === 'group' && (c.keep || []).some(p => p.origin)));
  const keepLine = row.kind === 'nolimit' ? 'Keep as many as you like' : row.members.length > 1 ? `Keep up to ${n}${po ? ' ' + po.trim() : ''}, any mix` : `Keep up to ${n} ${po}${lcNames(row.members)}`;
  return { conds, hw, keepLine, extra, n, s, steelWater };
}
/* ---------- fish pictures ---------- */
const FAMS = D.species.families || {};
const hasPic = S => FISHART.has(S);
function openFish(S){
  const c = FISHART.SPECIES[S], fam = Object.values(FAMS).find(f => f.members.includes(S));
  openSheet(spName(S), fam ? `${fam.name}${S === 'DV' ? ' · a bull trout is counted as a Dolly Varden' : ''}` : '',
    c ? `<figure class="fishart">${FISHART.draw(S)}</figure><div class="idlist"><div class="lbl">What to look for</div><ol>${c.tips.map(t => `<li>${esc(t)}</li>`).join('')}</ol></div>` : `<p class="muted">No drawing for this fish yet.</p>`);
}
function openFishGroup(codes){
  openSheet('What they look like', '', codes.map(S => FISHART.has(S) ? `<div class="fgpic"><div class="lbl">${esc(spName(S))}</div><figure class="fishart">${FISHART.draw(S)}</figure><ol class="small">${FISHART.SPECIES[S].tips.map(t => `<li>${esc(t)}</li>`).join('')}</ol></div>` : `<div class="fgpic"><div class="lbl">${esc(spName(S))}</div><p class="muted small">No drawing yet.</p></div>`).join(''));
}
const namedIn = r => { const m = clean(r.verbatim).match(/includ\w*:?\s*(.+)$/i); return m ? m[1].replace(/\s*·\s*/g, ', ').replace(/\.$/, '') : ''; };

/* ---------- sources: the answers' roles, the export's words ---------- */
const ROLE = { governs:'Sets the number', contains:'The group total this counts toward', narrows:'Lowers the number', limit:'A limit inside the total', floor:'Size limit', season:'Yearly limit', possession_cap:'Possession limit', duty:'Something you must do', possession:'Possession limit',
  also:'Also applies (a different limit)', replaced:'Beaten by', agrees:'Says the same as', falls:'Falls away with', moot:'Doesn’t matter today because of', lifted:'Lifted by' };
const WINS = ['also','governs','contains','narrows','limit','floor','season','possession_cap','duty','possession'];
const RK = k => PLACE.cands.find(x => x.key === k) || RULES[k];
function rowSources(row){
  const ctx = settle(PLACE, state.md), agg = new Map();
  for (const S of row.allMembers){ const R = MODEL.R[S]; if (!R) continue;
    for (const res of [R.H, R.W]) if (res) for (const [k, v] of res.roles){ const key = k + '|' + v.role + '|' + (v.by || ''); if (!agg.has(key)) agg.set(key, { r:RK(k), role:v.role, by:v.by, spp:new Set() }); agg.get(key).spp.add(S); } }
  for (const [ix, by] of ctx.lifted){ const r = PLACE.R(ix); if (r.species.some(s => row.allMembers.includes(s))) agg.set(r.key + 'lift', { r, role:'lifted', by:PLACE.R(by).key, spp:new Set() }); }
  const best = new Map(); [...agg.values()].sort((a, b) => WINS.includes(b.role) - WINS.includes(a.role)).forEach(it => { if (!best.has(it.r.key)) best.set(it.r.key, it); });
  return [...best.values()].sort((a, b) => (WINS.includes(b.role) - WINS.includes(a.role)) || (a.r.rank ?? a.r.baseRank) - (b.r.rank ?? b.r.baseRank));
}
function srcCard(r, roleTxt, won, extra){
  const rk = RANK[String(r.rank ?? r.baseRank ?? r.prov?.rank ?? 4)] || RANK['4'];
  return `<div class="sc${won === false ? ' lost' : ''}"><span class="badge" style="background:var(${rk.c})">${rk.t}${r.via === 'trib' ? ' · via tributary' : ''}</span>${r.via === 'trib' || rk === RANK['1'] ? gi('tributaries') : ''}<div class="scq">“${esc(clean(r.verbatim))}”</div>
    <div class="muted small">${esc(r.prov?.who || r.prov?.entry_name || '')}</div>${roleTxt ? `<div class="scrole ${won ? 'won' : ''}">${esc(roleTxt)}</div>` : ''}${(r.notes || []).map(n => `<div class="note">⚑ ${esc(n)}</div>`).join('')}${extra || ''}</div>`;
}
const fieldsPre = f => `<details class="fraw"><summary>Fields</summary><pre class="gjson">${esc(JSON.stringify(f, null, 1))}</pre></details>`;
const scrim = document.getElementById('scrim');
function openSheet(title, sub, body){ document.getElementById('shTitle').textContent = title; document.getElementById('shSub').textContent = sub; document.getElementById('shBody').innerHTML = body; scrim.hidden = false; document.getElementById('shClose').focus(); }
function openRowSources(id){
  const row = MODEL.rows[+id.slice(1)]; if (!row) return;
  const items = rowSources(row), won = items.filter(i => WINS.includes(i.role)), lost = items.filter(i => !WINS.includes(i.role));
  const card = it => { let t = ROLE[it.role] || it.role; if (it.role === 'governs' && it.r.k === 'gate') t = closedGate(it.r) ? 'Closes it' : 'Release every one'; if (it.by && RULES[it.by]) t += ` “${clean(RULES[it.by].verbatim)}”`; if (it.spp.size && it.spp.size < row.allMembers.length) t += ` · ${(it.r.dr?.subsets || []).find(x => sameSet(x.fish, [...it.spp]))?.for || '(no “for …” in the answers)'}`; return srcCard(it.r, t, WINS.includes(it.role), fieldsPre(it.r.f)); };
  openSheet(rowTitle(row), `${PLACE.name} · ${fmtMd(state.md)}. The more specific rule speaks first: this water, then from downstream, a named area, the region, then the province.`,
    `<div class="lbl">These decide it (${won.length})</div>${won.map(card).join('')}` + (lost.length ? `<details class="lostlist"><summary class="lbl">Overruled (${lost.length})</summary>${lost.map(card).join('')}</details>` : ''));
}
/* ================= Today’s card: ledger layout =================
   One row per daily limit. Top: the group limit (how many, how the count works, rules for every fish).
   Then, on a rail, each fish’s own rules. Bottom: "How this was decided", a short ladder of the rules that set each part. */
const levelTag = r => { const k = r.rank ?? r.baseRank ?? 4; if (k === 4) return 'B.C. rule'; if (k === 3){ const m = (r.entry_id || '').match(/^z(\d+)/); return m ? `Region ${m[1]} rule` : 'Region rule'; } if (k === 2) return 'Named area rule'; if (k === 1) return 'Downstream rule'; if (k === -1) return 'Federal or park rule'; return 'This water’s rule'; };
const levelHead = k => k === 4 ? 'All of B.C.' : k === 3 ? 'The region' : k === 2 ? 'A named area' : k === 1 ? 'Downstream waters' : k === -1 ? 'Federal law' : PLACE.name || 'This water';
const ICON = {
  fin:'<svg class="ic" viewBox="0 0 20 20" aria-hidden="true"><path d="M2 11c3-4 8-5 12-3l4-3v10l-4-3c-4 2-9 1-12-1z" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linejoin="round"/><path d="M11 7.5l1.5-2.5" stroke="currentColor" stroke-width="1.6"/></svg>',
  ruler:'<svg class="ic" viewBox="0 0 20 20" aria-hidden="true"><rect x="1.5" y="6.5" width="17" height="7" rx="1.5" fill="none" stroke="currentColor" stroke-width="1.6"/><path d="M5 6.5v3M8 6.5v2M11 6.5v3M14 6.5v2" stroke="currentColor" stroke-width="1.4"/></svg>',
  cal:'<svg class="ic" viewBox="0 0 20 20" aria-hidden="true"><rect x="2.5" y="4" width="15" height="13" rx="2" fill="none" stroke="currentColor" stroke-width="1.6"/><path d="M2.5 8h15M7 2.5v3M13 2.5v3" stroke="currentColor" stroke-width="1.6"/></svg>',
  pen:'<svg class="ic" viewBox="0 0 20 20" aria-hidden="true"><path d="M4 16l1-4 8-8 3 3-8 8z" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linejoin="round"/></svg>',
  hand:'<svg class="ic" viewBox="0 0 20 20" aria-hidden="true"><path d="M6 10V5a1.3 1.3 0 012.6 0v4V3.5a1.3 1.3 0 012.6 0V9V4.5a1.3 1.3 0 012.6 0V11c0 4-2 6-5 6s-4.5-2-6-5l-1-2a1.2 1.2 0 012-1.2z" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linejoin="round"/></svg>',
  water:'<svg class="ic" viewBox="0 0 20 20" aria-hidden="true"><path d="M2 8c2.5-2 5.5 2 8 0s5.5 2 8 0M2 13c2.5-2 5.5 2 8 0s5.5 2 8 0" fill="none" stroke="currentColor" stroke-width="1.6"/></svg>',
};
// "Brook trout, brown trout, cutthroat trout and rainbow trout" -> "Brook, brown, cutthroat and rainbow trout"
function shortNames(members){
  const ns = members.map(S => spName(S).replace(/\//g, ', '));
  if (ns.length < 2) return ns[0] || '';
  const last = w => w.split(' ').pop(), tail = last(ns[0]);
  const same = ns.every(n => n.includes(' ') && last(n) === tail && !n.includes(','));
  const parts = same ? ns.map((n, i) => i === ns.length - 1 ? lc(n) : lc(n.replace(new RegExp(' ' + tail + '$'), ''))) : ns.map(lc);
  const out = parts.length > 1 ? parts.slice(0, -1).join(', ') + ' and ' + parts[parts.length - 1] : parts[0];
  return cap(out);
}
// a condition sentence split into a short head, chips (dates, water) and one muted sub-line
function condParts(t){
  let x = t.trim(), sub = '', chips = [];
  const nm = x.match(/^Only (\d+) can be (over|under) (\d+) cm \((.+?)\)\.$/);
  if (nm) return { head:`Only ${nm[1]} ${nm[4].replace(/ and /g, ' or ')} ${nm[2]} ${nm[3]} cm`, sub:'', chips, ic:'ruler' };
  let m = x.match(/^Only (\d+) fish (over|under) (\d+) cm that (?:isn't|isn’t) an? (.+?)\.$/);
  if (m) return { head:`Only ${m[1]} ${m[2]} ${m[3]} cm`, sub: /steelhead/i.test(m[4]) ? 'Steelhead don’t count' : `Not counting ${m[4]}`, chips, ic:'ruler' };
  const cov = x.match(/It doesn[’']t cover (.+?): they have their own bag\.?/); if (cov){ sub = `${cap(cov[1])} don’t count toward this`; x = x.replace(cov[0], ''); }
  const wat = x.match(/This rule is for (streams|lakes)\.?/); if (wat){ chips.push({ ic:'water', t:`${cap(wat[1])} only` }); x = x.replace(wat[0], ''); }
  const dt = x.match(/, ((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d+[–-](?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)?[a-z]* ?\d+)/); if (dt){ chips.push({ ic:'cal', t:dt[1] }); x = x.replace(dt[0], ''); }
  x = x.replace(/\s+/g, ' ').trim().replace(/\.$/, '');
  return { head:x, sub, chips, ic: /cm/.test(x) ? 'ruler' : null };
}
const chipHtml = cs => cs.length ? `<ul class="chips">${cs.map(c => `<li>${c.ic ? ICON[c.ic] : ''}${esc(c.t)}</li>`).join('')}</ul>` : '';
function extraChips(extra){
  return extra.map(t => /^(hatchery|wild) only/i.test(t) ? { ic:'fin', t:cap(t.match(/^(hatchery|wild) only/i)[0]) } : /a year/.test(t) ? { ic:'cal', t:cap(t.replace(/^(\d+) over (\d+) cm a year$/, '$1 a year over $2 cm')) } : /record/i.test(t) ? { ic:'pen', t: /over \d+/.test(t) ? cap(t.replace('record ones', 'Record ones')) + ' on your licence' : 'Record it on your licence' } : /stop fishing/i.test(t) ? { ic:'hand', t:'Stop fishing at limit' } : { ic:null, t:cap(t) });
}
// rule labels shortened for the ladder; the exact words stay in the rule sheet
function shortLabel(label, row){
  let x = clean(label || '');
  const t = rowTitle(row); if (t && x.toLowerCase().startsWith(t.toLowerCase() + ' — ')) x = x.slice(t.length + 3);
  return cap(x.replace(/ per day/g, ' a day').replace(/, all species combined/, '').replace(/\(none under (\d+) cm\)/, '$1 cm+').replace(/\(no more than (\d+) over (\d+) cm\)/, ': max $1 over $2 cm').replace(/possession quota is 2 daily quotas/, 'hold up to 2 days’ limits').replace(/ — /g, ': ').replace(/\s+:/g, ':').replace(/\s+/g, ' ').trim());
}
const friendlyWhy = s => !s ? '' : /^[a-z_]+:/.test(s) ? 'the regulations don’t say exactly where' : s;

// the fish of a keep row, one item per kind (answers rows[].items), with their own conditions in words
const COVER = ['cap','rel','subcap','origin','outercap','outersize','tnote','partly'];
function speciesItems(row, rc){
  if (!row.items) return null;
  const gfacts = row.groups.flatMap(g => g.facts);
  return row.items.map((x, i) => {
    const cs = x.conds.map(j => rc.conds[j]).filter(Boolean);
    const facts = gfacts.filter(l => x.members.some(S => l.members.has(S)));
    const it = { members:x.members, cs, facts: facts.length ? facts : null, key:'i' + i, x, sub:x.sub };
    it.back = cs.find(c => c.c === 'back');
    const others = [...new Set(facts.filter(l => !cs.length || !COVER.includes(l.t)).map(l => fact(l, row).short).filter(Boolean).filter(t => t !== 'must-do'))];
    const org = cs.find(c => /hatchery only|wild only/.test(c.ph || ''));
    it.extra = [...(org ? [cap((org.ph.match(/(hatchery|wild) only[^,]*/) || [''])[0])] : []), ...(it.back ? [] : others.map(cap))];
    it.lines = [...new Set([...cs.map(c => c.ph != null ? c.ph : c.t).filter(Boolean).map(cap), ...(it.back ? [] : others.map(cap))])];
    if (it.sub != null) it.lines.push(`Only ${it.sub} of your ${rc.n}${it.members.length > 1 ? ' can be these' : ''}`);
    if (!it.lines.length){ it.plain = true; it.lines = ['No extra rules']; it.facts = null; }
    return it;
  });
}
// an item's keep range split into bands with their own number (answers items[].bands)
const bandWords = (a, b, first) => a === 0 && b === Infinity ? 'any size' : a === 0 ? `up to ${b} cm` : b === Infinity ? (first ? `${a} cm or longer` : `over ${a} cm`) : `${a}–${b} cm`;
const itemBands = it => it.x.bands ? it.x.bands.map(([a, b, mx], i) => { const bb = b == null ? Infinity : b; return { a, b:bb, mx, t:bandWords(a, bb, i === 0) }; }) : null;
// per origin, when hatchery and wild are kept differently (answers items[].origins, a fish with its own row)
const itemOrigins = it => (it.x.origins || []).map(x => x.rel ? { o:cap(x.o), rel:true } : { o:cap(x.o), n: x.n == null ? Infinity : x.n, lo:x.lo || 0, hi: x.hi == null ? Infinity : x.hi });
// the number a fish with its own row counts toward here (answers items[].against)
const againstOf = (it, n) => it.x.against === 'unlimited' ? Infinity : it.x.against ?? n;
const VAR = (() => { try { return new URLSearchParams(location.search).get('v') || 'C'; } catch (e) { return 'C'; } })();
// the general "only K fish over X cm" of a row (answers conds: a cap for no named fish, from X cm up)
const generalCap = rc => rc.conds.find(c => c.c === 'cap' && !c.who && !(c.raw.of && c.raw.of.length) && c.raw.a > 0 && c.raw.b === Infinity);
function groupBlock(row, rc){
  const s = scopeOf(row), n = row.daily, facts = [];
  let ex = '';
  const tot = row.kind === 'keep' && n > 1 && row.allMembers.length > 1 ? `<b>${n} a day in total, all kinds together.</b> ` : '';
  if (row.kind === 'nolimit') facts.push(`<div class="gf"><div class="gdt"><b>∞</b></div><div class="gdd">No daily limit</div></div>`);
  else if (!row.scope) facts.push(missing('whose count this limit is (rows scope)'));
  else if (nar0(row)){
    const wk = row.narrow.f.water, other = wk === 'stream' ? 'lakes' : 'streams', b = row.pool.f.take;
    facts.push(`<p class="gline">${tot}${tot ? gi('daily_quota') + ' ' : ''}Shared with every ${esc(s)} ${regionGi(row)} ${wk} ${wk === 'stream' ? gi('stream') : ''} you fish today.</p>`);
    ex = `<ul class="exs"><li><span>Kept ${n} on another ${wk}?</span> <strong>Keep 0 here</strong></li><li class="sep"><span>Kept some at a ${other.replace(/s$/, '')}?</span> <strong>Those don’t count here, but stop at ${b} for the whole day</strong></li></ul>`;
  } else if (s){ const lakeToo = !row.pool.f.water; facts.push(`<p class="gline">${tot}${tot ? gi('daily_quota') + ' ' : ''}Shared with everywhere in ${esc(s)} ${regionGi(row)} you fish today${lakeToo ? ', lakes and streams alike' : ''}.</p>`);
    if (row.kind === 'keep' && n > 1){ const k = n > 2 ? 2 : 1; ex = `<ul class="exs"><li><span>Kept ${k} elsewhere today?</span> <strong>Keep ${n - k} more here</strong></li></ul>`; } }
  // a water's own daily limit still counts the fish kept elsewhere today (answers rows fix F9)
  else facts.push(`<p class="gline">${tot}This ${PLACE.kind === 'stream' ? 'river' : 'lake'}’s own limit. Fish you kept elsewhere today count toward it too.${row.scope.apart ? ' These fish are counted apart from the region’s total: they don’t use it up.' : ''}</p>`);
  const ec = row.real;
  if (ec && ec.all) facts.push(`<p class="gline warnline"><b>Really ${ec.sum} a day here.</b> ${esc(s || 'The region')} allows ${n}, but each kind below has its own smaller limit.</p>`);
  else if (ec && ec.capped_sum < n && ec.rb){ const big = (ec.shared_cap || []).map(c => `Every one you could keep is over ${c.over_cm} cm, and only ${c.take} fish over ${c.over_cm} cm ${c.take > 1 ? 'are' : 'is'} allowed.`).join(' ');
    const op = ec.open.length > 3 ? 'other trout or char' : groupedNames(ec.open, ' or ');
    // a count several kinds share counts once for all of them (answers real_daily.shared_count, rows F11)
    const shc = (ec.shared_count || []).map(c => `${cap(groupedNames(c.members, ', '))} share one limit of ${c.take}${row.narrow && row.narrow.f.take === c.take && row.narrow.f.water ? ` from ${row.narrow.f.water}s` : ''}.`).join(' ');
    facts.push(`<p class="gline warnline"><b>Only ${ec.capped_sum} of the ${n} can be ${esc(groupedNames(ec.capped, ' or '))}.</b><span class="wsub">${esc([shc, big].filter(Boolean).join(' '))} The other ${n - ec.capped_sum} would have to be ${esc(op)}.</span></p>`); }
  const conds = [], oth = [];
  const org = rc.conds.find(c => c.c === 'origin' && !c.who);
  const H = org && /^Hatchery/.test(org.t);
  if (org && VAR === 'B') conds.push(`<li>${ICON.fin}<div><b>${H ? 'Hatchery fish only' : 'Wild fish only'}</b> <span class="in">· release every ${H ? 'wild' : 'hatchery'} one</span> <button class="linkbtn" type="button" data-howtell="1">How to tell ›</button> ${gi('hatchery_wild')}</div></li>`);
  if (org && VAR === 'C') conds.push(`<li><i class="ck">✓</i><div><b>${H ? 'Hatchery only' : 'Wild only'}</b><span class="in">, release ${H ? 'wild' : 'hatchery'} ones</span> <button class="linkbtn" type="button" data-howtell="1">How to tell ›</button> ${gi('hatchery_wild')}</div></li>`);
  // which fish a general cap does not cover: the answers' `except`, else the fish whose answers give it no role
  // the fish a general cap does not count: the answers' `except` (rows decision F12), nothing inferred here
  const exemptOf = c => ['cap','outersize'].includes(c.c) ? (c.raw.except || []) : [];
  const seenT = new Set();
  rc.conds.filter(c => !c.who && c.c !== 'origin').forEach(c => { if (seenT.has(c.t)) return; seenT.add(c.t); const t = c.t; const p = condParts(t);
    const exm = exemptOf(c); if (exm.length && !p.sub) p.sub = `Not counting ${lcNames(exm, ' or ')}`;
    if (p.sub === 'Steelhead don’t count') p.sub = 'Not counting steelhead';
    // on a steelhead river every rainbow over 50 cm is a steelhead, which has its own rule
    if (/^Not counting /.test(p.sub || '')) p.sub = p.sub.replace(/^Not counting (rainbow trout or steelhead|steelhead or rainbow trout)$/, 'Not counting steelhead').replace(/^Not counting (.+)$/, '$1 don’t count');
    const sub = [p.chips.map(c => c.t).join(' · '), p.sub].filter(Boolean).join('. ');
    const hd = p.head.replace(/^Only (\d+) (can be )?over/, 'Only $1 fish over');
    const gen = c.c === 'cap' && !(c.raw.of && c.raw.of.length);
    (gen || VAR === 'B' ? conds : oth).push(VAR === 'B' ? `<li>${p.ic ? ICON[p.ic] : '<span class="ic"></span>'}<div><b>${esc(hd)}</b>${sub ? `<span class="in">, ${esc(sub)}</span>` : ''}</div></li>`
      : `<li>${gen ? '<i class="ck">✓</i>' : p.ic ? ICON[p.ic] : '<span class="ic"></span>'}<div><b>${esc(gen && /^Only \d+ fish over/.test(hd) && !/ (total|all)/.test(hd) ? hd.replace(/ fish over (\d+ cm)$/, ' fish over $1 in total') : !gen && !/don’t count$/.test(sub) && /^\(?(.+?)\)?$/.test(sub) && /^Only \d+ fish over/.test(hd) ? hd.replace(/ fish over/, ' ' + sub.replace(/[()]/g, '') + ' over') : hd)}</b>${sub && !(!gen && !/don’t count$/.test(sub) && /^Only \d+ fish over/.test(hd)) ? `<span class="in">${/don’t count$/.test(sub) ? ` (${esc(sub)})` : ', ' + esc(sub)}</span>` : ''}</div></li>`); });
  const apart = MODEL.rows.filter(r => r !== row && r.pool && row.pool && r.members.length && r.members.every(S => { const m = mainRes(MODEL.R[S]?.H, MODEL.R[S]?.W); return m && m.roles.get(row.pool.key)?.role === 'lifted'; }));
  apart.forEach(r => (VAR === 'B' ? conds : oth).push(`<li><span class="ic"></span><div><b>${esc(rowTitle(r))}: own limit of ${r.daily} a day</b><span class="sub">Not part of these ${n}. See its row below.</span></div></li>`));
  const sp = (row.allMembers.includes('ST') || (row.allMembers.includes('RB') && !MODEL.rows.some(r => r.allMembers.includes('ST')))) ? steelPresence() : '';
  return `<section class="gblock">${facts.join('')}${ex}${conds.length ? (VAR === 'B' ? `<ul class="gconds">${conds.join('')}</ul>` : `<div class="gfor"><div class="lbl">${VAR === 'C' ? 'For every kind' : 'Across all kinds'}</div><ul class="gconds v2">${conds.join('')}</ul></div>`) : ''}${oth.length ? `<div class="gfor">${conds.length ? '<div class="lbl">Also</div>' : ''}<ul class="gconds v2 oth">${oth.join('')}</ul></div>` : ''}${sp ? `<p class="gquiet">${esc(sp)} ${gi('steelhead')}</p>` : ''}</section>`;
}
// the keep sentence: "Keep 1 of the 2 · 60 cm or longer · hatchery only"
const sent = (num, of, segs, o) => { const sg = segs.filter(Boolean); return `<div class="sl">${o ? `<span class="o">${o}</span>` : ''}<span class="k">Keep${of && num >= of ? ' up to' : ''}</span> <b class="n">${num === Infinity ? 'any number' : num}</b>${of && of !== Infinity && num < of ? ` <span class="of">of your ${of}</span>` : ''}${sg.length ? `<span class="segs">${sg.map(x => `<span class="seg">${esc(x)}</span>`).join(' · ')}</span>` : ''}</div>`; };
const rngTxt = (lo, hi) => lo && hi !== Infinity ? `${lo}–${hi} cm` : lo ? `${lo} cm or longer` : hi !== Infinity ? `up to ${hi} cm` : 'any size';
// the smallest any fish of a cross-referenced item may be kept at: each fish's own keep range (answers items[].ranges,
// rows 5, gap G4; `origins` for a one-fish item) — Infinity when none is kept
function xrefLo(it){ if (it.members.length > 1) return it.x.ranges ? Math.min(Infinity, ...it.x.ranges.map(([, a]) => a)) : NaN;
  return Math.min(Infinity, ...(it.x.origins || []).filter(o => !o.rel).map(o => o.lo || 0)); }
// a rainbow over 50 cm is a steelhead on this part (answers key: steelhead_water)
function isSteelRiver(){ return !!(PLACE.key && PLACE.key[2]); }
// how sure we are that steelhead are here (answers rows steelhead_line)
const STEEL_LINE = { possible_with_rules:'Steelhead rules apply here; steelhead may not be present in this water.', known_with_rules:'Steelhead are known to be in this water.', known_no_rules:'Steelhead have been recorded here, but no steelhead rule applies: treat any rainbow, however big, as a rainbow trout.' };
function steelPresence(){ const l = MODEL.line; return l == null ? '' : STEEL_LINE[l] || `(steelhead line “${l}”: no words for it)`; }
// "Only 2 at 30–50 cm. Only 1 over 50 cm." reads as 3: a cap that runs on to the top is "of those"
function capLines(q, topN){
  const out = [], used = new Set();
  q.forEach((x, i) => { if (x.mx >= topN || used.has(i)) return; const y = q[i + 1];
    if (y && x.b !== Infinity && y.a === x.b && y.b === Infinity && y.mx < x.mx){ out.push(`Only ${x.mx} over ${x.a} cm, and only ${y.mx} of those over ${y.a} cm.`); used.add(i + 1); return; }
    out.push(x.mx === 0 ? `None ${/^(over|up to)/.test(x.t) ? x.t : 'at ' + x.t}: release them.` : `Only ${x.mx} ${/^(over|up to)/.test(x.t) ? x.t : 'at ' + x.t}.`); });
  return out;
}
function speciesBlock(row, rc, id){
  let items = speciesItems(row, rc);
  if (items == null) return missing('the kinds of fish of this row (rows items)');
  // fish with no rules of their own add nothing when they're the whole list (bass, whitefish…)
  if (items.length === 1 && items[0].plain) items = [];
  if (!items.length) return '';
  const qs = items.map(itemBands);
  const org = rc.conds.find(c => c.c === 'origin' && !c.who), gOrigin = org ? (/^Hatchery/.test(org.t) ? 'hatchery only' : 'wild only') : '';
  const n = rc.n, many = n > 1 && row.kind === 'keep' && items.length > 1;
  const gc = generalCap(rc), gK = gc ? gc.raw.take : null, gX = gc ? gc.raw.a : null;
  const body = items.map((it, i) => {
    const stOnly = it.members.length === 1 && it.members[0] === 'ST' && isSteelRiver();
    const q = qs[i], xref = (it.facts || []).find(l => l.t === 'xref');
    let main = '', s2 = [], chips = extraChips((it.extra || []).filter(t => !/^own limit/i.test(t) && !/^(hatchery|wild) only/i.test(t))).filter(c => ['cal','pen','hand'].includes(c.ic));
    const itOrigin = (it.extra || []).find(t => /^(hatchery|wild) only/i.test(t));
    const both = q && it.members.every(S => ['H', 'W'].every(o => KEEPISH(MODEL.R[S]?.[o]?.status)));
    const origin = gOrigin || (itOrigin ? lc(itOrigin.match(/^(hatchery|wild) only/i)[0]) : both && !gOrigin ? 'wild or hatchery' : '');
    const N = xref ? againstOf(it, n) : n;
    if (q){
      const lo = q[0].a, hi = q[q.length - 1].b, topN = Math.max(...q.map(x => x.mx));
      main = sent(topN, topN < n || many ? n : null, [stOnly && lo === 50 && hi === Infinity ? 'over 50 cm' : rngTxt(lo, hi), gOrigin && VAR === 'B' ? '' : origin]);
      capLines(q, topN).forEach(t => s2.push(t));
      if (VAR !== 'B' && !stOnly && gc && lo >= gX && (!gc.r || it.members.some(S => covers(gc.r, S)))) s2.push(`All are over ${gX} cm: each one uses your ${gK} fish over ${gX} cm.`);
      // on a steelhead river a fish under 50 cm is a rainbow, not a small steelhead: no "release under 50"
      if (stOnly && lo <= 50) s2.push('Under 50 cm, it’s a rainbow trout.');
    } else if (xref){
      const S = it.members[0], ol = it.members.length === 1 ? itemOrigins(it) : [];
      const keep = ol.filter(x => !x.rel);
      if (keep.length > 1 && keep.some(x => x.n !== keep[0].n || x.lo !== keep[0].lo || x.hi !== keep[0].hi)) main = keep.map(x => sent(Math.min(x.n, N), N, [rngTxt(x.lo, x.hi)], x.o)).join('');
      else { const x = keep[0]; main = sent(Math.min(xref.daily, N), N, [x ? (S === 'ST' && isSteelRiver() && x.lo === 50 && x.hi === Infinity ? 'over 50 cm' : rngTxt(x.lo, x.hi)) : (lo0 => Number.isNaN(lo0) ? '(no keep range in the answers: items[].ranges)' : lo0 && lo0 !== Infinity ? `${lo0} cm or longer` : '')(xrefLo(it)), keep.length === 1 && ol.length === 2 ? lc(x.o) + ' only' : keep.length === 2 ? 'wild or hatchery' : '']); }
      const other = MODEL.rows.find(r => r !== row && r.pool && it.members.every(S => r.members.includes(S)));
      if (other){ // only the other row's duties that are about every fish of this item
        const of = [...other.everyone, ...other.groups.flatMap(g => g.facts)].filter(l => it.members.every(S => l.members.has(S)));
        chips = extraChips([...new Set(of.map(l => fact(l, other).short).filter(t => t && /a year|possession|record|stop fishing/i.test(t)))]).filter(c => ['cal','pen','hand'].includes(c.ic)); }
      if (VAR !== 'B' && !(S === 'ST' && isSteelRiver()) && gc){ const klo = keep.length ? Math.min(...keep.map(x => x.lo || 0)) : xrefLo(it);
        if (klo !== Infinity && klo >= gX) s2.push(`All are over ${gX} cm: each one uses your ${gK} fish over ${gX} cm.`); }
      if (ol.some(x => x.rel) && !(keep.length === 1 && ol.length === 2)) s2.push(`<span class="rl">Release:</span> ${ol.filter(x => x.rel).map(x => lc(x.o)).join(', ')} ones.`);
    } else if (it.back){ const sg = strip(PLACE, it.members), never = !sg.some(([st]) => st === 'keep'), un = never ? '' : runUntil(sg, state.md), rt = never ? ('No keeping at any time of year. ' + closedRuns(sg)).trim() : runTxt(sg, state.md, true); if (rt) s2.push(rt);
      main = `<div class="sl"><span class="rl">${it.members.every(S => mainRes(MODEL.R[S]?.H, MODEL.R[S]?.W)?.status === 'closed') ? 'Closed: don’t fish for them' : 'Release every one'}${un ? ' until ' + un : ''}</span>${it.members.includes('ST') && isSteelRiver() ? ' <span class="of">· includes any rainbow over 50 cm</span>' : ''}</div>`; }
    else main = `<div class="sl">${esc(it.lines.join('. '))}.</div>`;
    const nw = row.narrow, share = nw && row.scope?.share && !nar0(row) && it.members.some(S => covers(nw, S) && ['H', 'W'].some(o => KEEPISH(MODEL.R[S]?.[o]?.status) && MODEL.R[S][o].roles.get(nw.key)?.role !== 'lifted'));
    if (share){ const txt = rc.conds.find(c => c.c === 'streamcap'); const hd = txt ? condParts(txt.t).head.replace(/^Only (\d+) can be an? /, 'only $1 in all: ') : ''; const k = (txt ? condParts(txt.t).head : '').match(/^Only (\d+)/); const own = q ? Math.max(...q.map(x => x.mx)) : null; if (k && own === +k[1]) {} else if (k) s2.push(`Wild ones share the limit of ${k[1]} above.`); else if (hd) s2.push(`Shares ${hd}.`); }
    const topK = q ? Math.max(...q.map(x => x.mx)) : xref ? Math.min(xref.daily, N) : null;
    chips = chips.map(c => c.ic === 'hand' && topK ? { ic:'hand', t:`Stop fishing for the day after ${topK}` } : c);
    const stM = mainRes(MODEL.R.ST?.H, MODEL.R.ST?.W), steelWater = isSteelRiver();
    if (steelWater && it.members.includes('RB') && !(it.notes || []).some(t => /steelhead/.test(t)) && !(it.lines || []).some(t => /counts as a steelhead/.test(t)))
      (it.notes = it.notes || []).push(stM && KEEPISH(stM.status) ? 'Rainbows over 50 cm count as steelhead' : 'Rainbows over 50 cm count as steelhead: release them');
    if (steelWater && it.members.includes('RB')){ const rest = it.members.filter(S => S !== 'RB');
      s2 = s2.map(t => t.replace(/^Only (\d+) over 50 cm\.$/, (m, k) => rest.length ? `Only ${k} ${lc(shortNames(rest)).replace(/ and ([^ ]+ trout)$/, ' or $1')} over 50 cm.` : '')).filter(Boolean); }
    const notes = [...(it.notes || [])].map(t => `<p class="s2">${esc(t)}.</p>`).join('');
    const head = `<b class="nm">${esc(shortNames(it.members))}</b>${main}${s2.length ? `<p class="s2">${s2.join(' ')}</p>` : ''}${notes}${chips.length ? `<ul class="mini">${[...new Set(chips.map(c => c.t))].filter((t, _, a) => !(/^\d+ a year$/.test(t) && a.some(u => u !== t && u.startsWith(t + ' ')))).map(t => `<li>${esc(t)}</li>`).join('')}</ul>` : ''}`;
    if (!it.facts) return `<div class="sp2 static">${head}</div>`;
    const resTop = q ? Math.max(...q.map(x => x.mx)) : 0, rLo = q ? q[0].a : 0, rHi = q ? q[q.length - 1].b : Infinity;
    const res = q ? `Keep up to ${resTop}, ${stOnly && rLo === 50 ? 'over 50 cm' : rngTxt(rLo, rHi)}${q.filter(x => x.mx < resTop).map(x => `. Only ${x.mx} ${/^(over|up to)/.test(x.t) ? x.t : 'at ' + x.t}`).join('')}. They count toward your ${n}` : null;
    const ladRow = xref ? (MODEL.rows.find(r => r !== row && r.pool && it.members.every(S => r.members.includes(S))) || row) : row;
    const res2 = res || (it.back ? (it.members.every(S => mainRes(MODEL.R[S]?.H, MODEL.R[S]?.W)?.status === 'closed') ? 'Closed here: don’t fish for them' : 'Release every one here') : xref ? (() => { const ol = it.members.length === 1 ? itemOrigins(it).filter(x => !x.rel) : []; const ST1 = it.members[0] === 'ST' && isSteelRiver();
      const rg = x => ST1 && x.lo === 50 ? 'over 50 cm' : rngTxt(x.lo, x.hi), nn = x => Math.min(x.n, N) === Infinity ? 'any number' : Math.min(x.n, N);
      if (ol.length === 2 && ol[0].n === ol[1].n && rg(ol[0]) === rg(ol[1])) return `Keep up to ${nn(ol[0])}, ${rg(ol[0])}, wild or hatchery. Each one counts toward your ${N}`;
      if (ol.length === 2){ const [a, b] = ol[0].n >= ol[1].n ? ol : [ol[1], ol[0]]; return `Keep up to ${nn(a)}: ${lc(a.o)} ${rg(a)}. Only ${b.n} can be ${lc(b.o)} (${rg(b)}). Each one counts toward your ${N}`; }
      return ol.length ? `Keep up to ${nn(ol[0])}, ${rg(ol[0])}${itemOrigins(it).length === 2 ? `, ${lc(ol[0].o)} only` : ''}. Each one counts toward your ${N}` : `Each one counts toward your ${N}`; })() : null);
    return `<details class="sp2" data-dd="${id}${it.key}"${ddOpen(id + it.key)}><summary>${head}</summary><div class="minidec"><div class="lbl">How this was decided</div>${ladderHtml(ladRow, id, it.members, res2)}</div></details>`;
  }).join('');
  return `<section class="sblock"><div class="lbl">Each kind${many && items.every(it => it.x.against == null || it.x.against === n) ? ` <span class="lbl2">· every fish counts toward the ${n}</span>` : ''}${VAR === 'D' && gOrigin ? ` <button class="linkbtn lblbtn" type="button" data-howtell="1">Hatchery or wild? ›</button>` : ''}</div><div class="rail">${body}</div></section>`;
}
// "How this was decided": one short ladder per question, from B.C. down to this water
const SAYS = { governs:'Daily limit', contains:'Daily limit', narrows:'Lower daily limit', also:'Also applies', floor:'Size', limit:'Extra cap', season:'Yearly limit', possession_cap:'Possession limit', duty:'Must do', possession:'Carry limit' };
const QORDER = ['governs','contains','narrows','also','floor','limit','season','possession_cap','duty','possession'];
const LOSES = { replaced:'Replaced', agrees:'Same as another rule', falls:'Not needed here', moot:'Doesn’t matter today', lifted:'Replaced' };
const byName = k => { const b = RULES[k]; if (!b) return 'a rule above'; const rk = b.rank ?? b.baseRank ?? b.prov?.rank ?? 4; return rk === 0 ? `this ${PLACE.kind === 'lake' ? 'lake' : 'river'}’s rule` : rk === 3 ? `the ${b.prov?.entry_name || 'region'} rule` : rk === 4 ? 'a B.C.-wide rule' : 'a rule above'; };
// a regulation line as a plain sentence: answers display.rules[].plain (null: the label, shortened)
const say = r => r.dr?.plain || '';
function ladderHtml(row, id, members, res){
  const items = rowSources(row).filter(it => !members || !it.spp.size || [...it.spp].some(S => members.includes(S)));
  if (!items.length && row.win) items.push({ r:row.win, role:'governs', spp:new Set() });
  items.forEach(it => { if (it.r.f?.record_retention && WINS.includes(it.role)) it.role = 'duty'; });
  const Q = [['What you can keep', ['governs','contains','narrows','also']], ['Size', ['floor']], ['Other limits', ['limit']], ['Other rules', ['season','possession_cap','duty','possession']]];
  // the fish a line is listed for when it is fewer than the row's (or the item's) — its sentence for them and the
  // "for …" are the answers' (display.rules[].subsets, gap G3), never written here
  const forFish = it => it.spp.size && it.spp.size < row.allMembers.length && !(members && [...it.spp].every(S => members.includes(S))) && !(members && sameSet([...it.spp].filter(S => members.includes(S)), members)) ? [...it.spp].filter(S => !members || members.includes(S)) : null;
  const subsetOf = (r, fish) => (r.dr?.subsets || []).find(x => sameSet(x.fish, fish)) || null;
  const line = (it, won) => { let st = won ? SAYS[it.role] || it.role : (LOSES[it.role] || 'Doesn’t apply');
    if (!won && LOSES[it.role] === 'Replaced') st = `Replaced by ${byName(it.by)}`;
    if (won && it.r.f?.record_retention) st = 'Record it';
    if (won && it.r.type === 'stop_fishing_after_quota') st = 'Stop fishing';
    if (won && it.role === 'governs' && it.r.k === 'gate') st = closedGate(it.r) ? 'Closed' : 'Release';
    const fish = forFish(it), ss = fish ? subsetOf(it.r, fish) : null;
    if (fish && !ss) return `<li>${missing(`the sentence for ${it.r.key} said for ${fish.join(', ')} (display.rules[].subsets)`)}</li>`;
    return `<li class="${won ? 'won' : 'lost'}"><button class="pline pl2" type="button" data-rule="${esc(it.r.key)}"><span>${esc(ss?.plain || say(it.r) || shortLabel(it.r.label, row))}${ss && !ss.plain ? `<small>${esc(ss.for)}</small>` : ''}</span><em>${esc(st)}</em></button></li>`; };
  const steps = list => { const by = new Map(); list.sort((a, b) => (b.r.rank ?? b.r.baseRank ?? 4) - (a.r.rank ?? a.r.baseRank ?? 4)).forEach(it => { const k = it.r.rank ?? it.r.baseRank ?? 4; (by.get(k) || by.set(k, []).get(k)).push(it); });
    return [...by].map(([k, its]) => `<li class="step"><span class="lvl">${esc(k === 3 ? (its[0].r.prov?.entry_name || 'The region') : k === 2 && (its[0].r.prov?.entry_name || '').length < 40 ? its[0].r.prov.entry_name : levelHead(k))}</span><ul>${its.map(it => line(it, true)).join('')}</ul></li>`).join(''); };
  const s = scopeOf(row);
  const result = {
    'What you can keep': res ? res : row.kind === 'nolimit' ? 'No daily limit here.' : row.pool ? `${row.daily} a day here${s && row.pool.f.take !== row.daily ? `, and they count toward ${row.pool.f.take} a day in ${s}` : s ? `, for all of ${s} together` : ''}.` : (row.kind === 'closed' ? 'Closed here.' : 'Release every one here.'),
  };
  let h = '';
  if (members){
    const won = items.filter(i => WINS.includes(i.role) && i.role !== 'possession').sort((a, b) => QORDER.indexOf(a.role) - QORDER.indexOf(b.role));
    h += `${res ? `<div class="lres">✓ ${esc(res)}.</div>` : ''}<ol class="ladder2">${steps(won)}</ol>`;
  } else for (const [q, roles] of Q){ const its = items.filter(i => roles.includes(i.role)); if (!its.length) continue;
    h += `<div class="lq"><div class="lbl">${q}</div>${result[q] ? `<div class="lres">✓ ${esc(result[q])}</div>` : ''}<ol class="ladder2">${steps(its)}</ol></div>`; }
  const wonSay = new Set(items.filter(i => WINS.includes(i.role)).map(i => say(i.r)).filter(Boolean));
  const lost = members ? [] : items.filter(i => !WINS.includes(i.role) && i.role !== 'agrees' && !wonSay.has(say(i.r)) && !(i.by && RULES[i.by] && shortLabel(RULES[i.by].label, row) === shortLabel(i.r.label, row)));
  if (lost.length) h += `<details class="inl"><summary>Rules that don’t apply here (${lost.length})</summary><ul class="plines">${lost.map(it => line(it, false)).join('')}</ul></details>`;
  return h + (members ? '' : `<button class="linkrow" type="button" data-src="${id}">See every regulation line ›</button>`);
}
const sameSet = (a, b) => a && b && a.length === b.length && a.every(x => b.includes(x));

function summaryHtml(row, id){
  const decided = `<details class="decide" data-dd="${id}d"${ddOpen(id + 'd')}><summary>How this was decided</summary><div class="ddbody">${ladderHtml(row, id)}</div></details>`;
  if (!row.pool){ const named = row.prot ? row.prot.map(spName).join(', ') : !row.members.length ? namedIn(row.win) : ''; const sg = row.members.length ? strip(PLACE, row.members) : null, never = sg && !sg.some(([st]) => st === 'keep'), un = sg && !never ? runUntil(sg, state.md) : '', rt = sg ? (never ? ((row.kind === 'closed' ? '' : 'No keeping at any time of year. ') + closedRuns(sg)).trim() : runTxt(sg, state.md, true)) : '';
    return `<p class="sum">${row.kind === 'closed' ? `Don’t fish for them${un ? ' until ' + un : ''}. If one bites, carefully release it.` : `You can fish for them, but release every one${un ? ' until ' + un : ''}.`}${rt ? ' ' + esc(rt) : un ? '' : row.win.wins.length ? ' ' + esc(cap(winTxt(row.win).slice(2))) + '.' : ''}</p>${named ? (nl => nl.length > 5 ? `<details class="names"><summary>Which fish (${nl.length})</summary><p class="names">${esc(nl.join(' · '))}</p></details>` : `<p class="names">${esc(nl.join(' · '))}</p>`)(row.prot ? row.prot.map(spName) : named.split(/,\s*/)) : ''}${miniStrip(PLACE, row.members, state.md)}${decided}`; }
  if (!row.conds) return missing('the conditions on keeping of this row (rows conds)') + decided;
  const rc = rowConds(row);
  const pics = row.allMembers.filter(hasPic);
  const photos = pics.length ? `<details class="looks" data-dd="${id}p"${ddOpen(id + 'p')}><summary>${pics.length > 1 ? `What they look like <span class="lc">${pics.length} fish</span>` : `What ${/^[aeiou]/i.test(spName(pics[0])) ? 'an' : 'a'} ${esc(spName(pics[0]).toLowerCase())} looks like`}</summary><div class="lstrip">${pics.map(S => `<button class="lthumb" type="button" data-fish="${S}">${FISHART.draw(S, { marks:false })}<span>${esc(spName(S))}</span></button>`).join('')}</div></details>` : '';
  const more = [liftNoteHtml(row), miniStrip(PLACE, row.members, state.md) ? stripHtml(PLACE, row.members, state.md) : ''].join('');
  const sb = speciesBlock(row, rc, id);
  const srcLink = `<button class="linkrow" type="button" data-src="${id}">See every regulation line ›</button>`;
  const s = scopeOf(row);
  let single = '';
  if (!sb && row.kind === 'keep' && row.members.length && row.items && row.items.length){
    const facts = [...row.everyone, ...row.groups.flatMap(g => g.facts)];
    const q = itemBands({ x:row.items[0] });
    const both = row.members.every(S => ['H', 'W'].every(o => KEEPISH(MODEL.R[S]?.[o]?.status)));
    const org = rc.conds.find(c => c.c === 'origin' && !c.who);
    const chips = extraChips([...new Set(facts.map(l => fact(l, row).short).filter(t => t && /a year|possession|record|stop fishing/i.test(t)))]).filter(c => ['cal','pen','hand'].includes(c.ic));
    if (q){ const lo = q[0].a, hi = q[q.length - 1].b, topN = Math.max(...q.map(x => x.mx)), s2 = [];
      capLines(q, topN).forEach(t => s2.push(t));
      const rel = [lo ? `under ${lo} cm` : '', hi !== Infinity ? `over ${hi} cm` : ''].filter(Boolean); if (rel.length) s2.push(`<span class="rl">Release:</span> ${rel.join(', ')}.`);
      single = `<div class="sp2 static solo">${sent(topN, null, [rngTxt(lo, hi), org ? (/^Hatchery/.test(org.t) ? 'hatchery only' : 'wild only') : both ? 'wild or hatchery' : ''])}${s2.length ? `<p class="s2">${s2.join(' ')}</p>` : ''}${chipHtml(chips)}</div>`; }
  }
  const simple = !sb && !rc.conds.length && !nar0(row) && row.kind === 'keep';
  if (simple && single && /class="seg"/.test(single) && !/class="s2"|class="chips"/.test(single)){
    const segs = [...single.matchAll(/<span class="seg">· ([^<]+)<\/span>/g)].map(m => m[1]);
    return `<p class="gsub">${segs.length ? esc(cap(segs.join(', '))) + '.' : ''}${s ? `${segs.length ? ' ' : ''}Shared with all of ${esc(s)} today.` : ''}</p>` + photos + more + decided; }
  const gb = simple ? (s ? `<p class="gsub">Shared with all of ${esc(s)} today.</p>` : '') : groupBlock(row, rc);
  return gb + single + sb + photos + more + (sb ? srcLink : decided);
}
function parentRow(row){ return row.pool ? MODEL.rows.find(r => r !== row && r.pool && row.members.every(S => r.allMembers.includes(S) || r.groups.some(g => g.members.includes(S))) && r.groups.some(g => g.facts.some(l => l.t === 'xref' && row.members.some(S => g.members.includes(S))))) : null; }
function rowHtml(row, i){
  const id = 'r' + i;
  const P0 = parentRow(row);
  const A0 = !P0 && row.pool ? MODEL.rows.find(r => r !== row && r.pool && row.members.every(S => { const m = mainRes(MODEL.R[S]?.H, MODEL.R[S]?.W); return m && m.roles.get(r.pool.key)?.role === 'lifted'; })) : null;
  if (P0) return '';
  const ec = row.real;
  return `<article class="row ledger" data-row="${id}"><div class="rhead"><span class="rtitle">${esc(rowTitle(row))}${A0 ? `<span class="scope inside">Separate from your ${A0.daily} ${esc(lc(rowTitle(A0)))}</span>` : ''}${A0 ? '' : (nar0(row) ? scopeBadge(row).replace(/>([^<]+) · streams</, (m, x) => `>All ${x} streams<`) : scopeBadge(row).replace(/>([^<]+) · streams</, (m, x) => `>${x}-wide<`))}</span>${(() => { const v = valueHtml(row, !!row.pool && row.kind === 'keep'); return ec && ec.all ? v.replace(`<div class="num">${row.daily}<`, `<div class="num">${ec.sum}<`) : v; })().replace('a day, shared', 'a day').replace('>Release<', '><span aria-hidden="true">↩ </span>Release<').replace('>Closed<', '><span aria-hidden="true">⊘ </span>Closed<')}</div>
    <div class="rsum">${summaryHtml(row, id)}</div></article>`;
}
// a single rule, in plain words first; the legal text and technical detail below
function openRule(key){
  const r = PLACE.cands.find(x => x.key === key) || RULES[key] || LIC[key]; if (!r) return;
  openSheet(clean(r.label || 'Rule'), `${levelTag(r)}${r.prov?.entry_name ? ' · ' + r.prov.entry_name : ''}`,
    `<div class="lbl">The exact words</div><blockquote class="exact">${esc(clean(r.verbatim || ''))}</blockquote>${r.dr?.plain ? `<p class="small">${esc(r.dr.plain)}</p>` : ''}${(r.notes || []).map(n => `<div class="note">⚑ ${esc(n)}</div>`).join('')}
     <details class="fraw"><summary>Technical detail</summary><div class="mono">${esc(key)}</div><pre class="gjson">${esc(JSON.stringify(r.fields || r.f, null, 1))}</pre></details>`);
}
/* ================= GEAR: the answers gear frame (gear.resolve, from the reader's rules in force) =================
   Which clause answers each slot and element, the hook, the bait per element and the ways to fish are the
   answers'; the page writes the words. A clause is [rule ref, clause index into the export rule's `gear`]. */
const SLOT_LABEL = { lines_per_angler:'Lines', terminal_attachments_per_line:'Hook, lure or fly per line', hooks_per_line:'Hooks per line', flies_per_line:'Flies per line',
  points_per_hook:'Hook points', hook_gap_mm:'Hook gap', weight_per_line_kg:'Weight on the line', bait_possession_kg:'Bait you can carry', light_to_hook_mm:'Light to hook' };
const ELEM = { roe:'Roe (fish eggs)', invertebrate:'Water insects and crayfish', worms:'Worms', fin_fish:'Fish or fish parts', dead_fin_fish:'dead fish', live_fin_fish:'live fish', artificial_fly:'flies', artificial_lure:'lures',
  angling:'Angling (rod and line)', fly_fishing:'Fly fishing', ice_fishing:'Ice fishing', set_lining:'Set lines', spear_fishing:'Spear or bow', crayfish_trapping:'Crayfish traps', netting:'Nets', snagging:'Snagging', chumming:'Chumming', downrigger:'Downrigger', light:'Light to attract fish' };
const WHILE_TXT = { set_lining:'set lining', ice_fishing:'ice fishing', spear_fishing:'spearing', crayfish_trapping:'trapping crayfish', snagging:'snagging', downrigger:'using a downrigger', light:'using a light', alone_in_boat:'alone in a boat', in_boat:'in a boat', in_powered_boat:'in a powered boat', from_shore:'fishing from shore', downrigger_weight:'the weight is a downrigger’s' };
const MUST = { quick_release_to_line:'attached to your line by a quick-release', submerged:'submerged', attached_to_line:'attached to the line' };
const GROUPWORD = { ALL_GAME_FISH:'game fish', SALMON:'salmon', PROTECTED_SPECIES:'protected species', ALL_FIN_FISH:'any fish' };
const tgtTxt = codes => { const a = codes.map(c => GROUPWORD[c] || lcNames(expand([c]))); return a.length > 1 ? a.slice(0, -1).join(', ') + ' or ' + a[a.length - 1] : a[0]; };
const clauseOf = ref => { const r = PLACE.R(ref[0]), c = (r.f.gear || [])[ref[1]]; return { r, c: c || null }; };
const circTxt = e => e.targeting ? `when fishing for ${tgtTxt(e.targeting)}` : e.note ? e.note : 'while ' + (e.while || []).map(x => WHILE_TXT[x] || x.replace(/_/g, ' ')).join(', ');
function countTxt(slot, c){
  if (!c) return '(clause missing)';
  if (c.unlimited) return 'no limit';
  const n = c.max ?? c.min, pre = 'min' in c ? 'at least ' : '';
  switch (slot){
    case 'points_per_hook': return n === 1 ? 'Single point only (no trebles)' : `${pre}${n} points`;
    case 'hook_gap_mm': return `${pre}${n / 10} cm`;
    case 'weight_per_line_kg': case 'bait_possession_kg': return `${pre || 'up to '}${n} kg`;
    case 'light_to_hook_mm': return `within ${n / 1000} m of the hook`;
    default: return `${pre}${n}`;
  }
}
const gSrc = r => `<button class="src gsrc" type="button" data-rule="${esc(r.key)}">${RANK[String(r.rank)].t}</button>`;
function gRow(label, val, sub, r, strict){ return `<div class="grow${strict ? ' strict' : ''}"><div class="glabel">${label}</div><div class="gval"><b>${val}</b>${sub ? `<span>${sub}</span>` : ''}${r ? ' ' + gSrc(r) : ''}</div></div>`; }
const gearAt = (w, md) => { const f = frameIx(w, 'gear', segOf(w, md)); return f == null ? null : SEC.gear.frames[f]; };
const MOMENT_HEAD = { release:'When you release a fish', keep:'When you keep a fish', carry:'Carrying fish', never:'Never', also:'Also' };
function gearParts(){
  const w = PLACE, md = state.md, P = {};
  if (MODEL.status === 'closed') return null;
  const G = gearAt(w, md); if (!G) return { missing:true };
  if (G.tidal) return { tidal:G };
  const top = s => G.counts[s] ? { ...clauseOf(G.counts[s].by), also:G.counts[s].also || [] } : null;
  const E = G.elements, el = k => E[k] ? { ...clauseOf(E[k].by), s:E[k].verdict } : null;
  const tags = [], L = top('lines_per_angler');
  if (L) tags.push(`${countTxt('lines_per_angler', L.c)} line${L.c?.max > 1 ? 's' : ''}${L.also.map(e => ` (${countTxt('lines_per_angler', clauseOf(e.clause).c)} ${circTxt(e).replace('while ', '')})`).join('')}`);
  const HOOK = { single_barbless:'Single barbless hook', single:'Single hook', any_barbless:'Any barbless hook', trebles_and_barbs:'Trebles and barbs OK' };
  tags.push(HOOK[G.hook] || `(hook “${G.hook}”: no words for it)`);
  tags.push(G.bait_ban ? 'Bait ban' : 'Some bait OK');
  if (G.fly === 'artificial_fly_only') tags.push('Artificial fly only: floats and sinkers OK');
  if (G.fly === 'fly_fishing_only') tags.push('Fly fishing only: no float or sinker');
  const timed = G.timed.map(PLACE.R);
  let h = (timed.length ? `<div class="caveats top"><div class="lbl">⚠ At certain times</div><ul>${timed.map(r => `<li><button class="factbtn" type="button" data-rule="${esc(r.key)}"><b>${esc(r.label)}</b><span class="where">${esc(r.tcond)}</span></button></li>`).join('')}</ul></div>` : '');
  h += uncertainHtml(['gear_and_method']) + inPartHtml(settle(w, md), ['gear_and_method','vessel','conduct']);
  P.pre = h; P.tags = tags;
  const row = (slot, sub) => { const t = top(slot); if (!t) return ''; return gRow(SLOT_LABEL[slot] || slot, countTxt(slot, t.c), [sub ? sub(t) : '', t.c?.members ? 'one ' + t.c.members.map(m => m.replace(/_/g, ' ').replace('artificial ', '')).join(', or one ') : '', t.c?.unless ? 'not when ' + t.c.unless.map(u => WHILE_TXT[u.gear_in_use] || JSON.stringify(u)).join(', ') : '', ...t.also.map(e => `${countTxt(slot, clauseOf(e.clause).c)} ${circTxt(e)}`)].filter(Boolean).join(' · '), t.r, slot === 'points_per_hook'); };
  let line = row('lines_per_angler') + row('terminal_attachments_per_line');
  line += top('points_per_hook') ? row('points_per_hook', t => t.r.f.water ? `on ${t.r.f.water}s` : '') : gRow('Hook points', 'Trebles allowed', 'any hook');
  const barbed = el('barb:barbed');
  line += barbed?.s === 'ban' ? gRow('Barbs', 'Barbless only', (barbed.r.f.water ? `on ${barbed.r.f.water}s · ` : '') + 'pinch barbs flat or buy barbless', barbed.r, true) : gRow('Barbs', 'Barbs allowed', '');
  const lureNo = el('lure:artificial_lure');
  line += lureNo?.s === 'ban' ? gRow('Lures', 'Artificial flies only', 'floats and sinkers may be on the line', lureNo.r, true) : gRow('Lures', 'Any lure or fly', '');
  line += (top('terminal_attachments_per_line') ? '' : row('flies_per_line')) + row('weight_per_line_kg') + row('hooks_per_line');
  P.line = `<div class="gsec">${line}</div>${/single|barbless/.test(G.hook) ? `<p class="kitnote">${esc(HOOK[G.hook])} ${gi('single_barbless_hook')}</p>` : ''}`;
  P.baitBan = G.bait_ban;
  const WHY = { bait_ban:'bait ban', on_streams:'on streams', on_lakes:'on lakes', not_on_streams:'not on streams', not_on_lakes:'not on lakes', banned_roe_ok:'banned · roe is still OK', allowed:'allowed' };
  P.baitList = G.bait.map(b => ({ e:b.element, ok:b.ok }));
  P.bait = `<div class="gsec">${P.baitBan ? `<p class="kitnote">No natural bait of any kind while the ban runs. ${gi('bait_ban')}</p>` : ''}<div class="baitgrid">${G.bait.map(b => {
    const r = b.by ? PLACE.R(b.by[0]) : null, sub = b.by ? (b.why ? (WHY[b.why] || b.why) : '') : 'allowed';
    return `<button class="bait ${b.ok ? 'ok' : 'no'}" type="button" data-rule="${r ? esc(r.key) : ''}"><i>${b.ok ? '✓' : '✕'}</i><b>${ELEM[b.element] || b.element}</b><span>${esc([sub, b.carry_kg != null ? `carry up to ${b.carry_kg} kg` : ''].filter(Boolean).join(' · '))}</span>${(b.also_allowed || []).map(c => `<small>✓ dead fish ${esc(circTxt(c))}</small>`).join('')}</button>`; }).join('')}</div></div>`;
  // ways to fish: the answers' verdict per method against the province's lawful methods
  const WAYWHY = { fly_fishing_only:'fly fishing only: nothing but the fly on the line, no float, sinker or attractor', not_allowed_here:'not allowed here', not_lawful:'not a lawful way to sport fish', fly_fishing_only_here:'fly fishing only here' };
  const chips = G.ways.map(x => {
    const r = x.by ? PLACE.R(x.by[0]) : null;
    if (!x.allowed) return { k:x.method, allow:false, r, info: WAYWHY[x.why] || '' };
    if (x.why === 'fly_fishing_only') return { k:x.method, allow:true, r, info:WAYWHY[x.why] };
    const bits = [];
    if (x.not_for) bits.push('not for ' + tgtTxt(x.not_for));
    if (x.for) bits.push(tgtTxt(x.for) + ' allowed');
    (x.while || []).forEach(e => { const c = clauseOf(e.clause).c; if (!c) return; bits.push(c.must_be ? `must be ${c.must_be.map(m => MUST[m] || m.replace(/_/g, ' ')).join(', ')}` : `${(SLOT_LABEL[c.slot] || c.slot).toLowerCase()}: ${countTxt(c.slot, c)}`); });
    (x.conduct || []).forEach(a => bits.push(D.conduct[a] || a));
    (x.while_rules || []).map(PLACE.R).forEach(r2 => bits.push(/other than crayfish/i.test(r2.label) ? 'release anything in the trap that isn’t a crayfish' : clean(r2.label)));
    for (const [d, v] of Object.entries(x.devices || {})) bits.push(`with a ${d}: ${(v.must_be || []).map(m => MUST[m] || m.replace(/_/g, ' ')).join(', ')}${v.within_mm != null ? ', ' + countTxt('light_to_hook_mm', { max:v.within_mm }) : ''}`);
    return { k:x.method, allow:true, r, info:[...new Set(bits)].join(' · ') };
  });
  P.waysNo = chips.filter(o => !o.allow);
  P.ways = `<div class="gsec"><div class="lbl">Allowed</div><div class="wlist">${chips.filter(o => o.allow).map(o => `<button class="wrow" type="button" data-rule="${o.r ? esc(o.r.key) : ''}"><i class="${/not for game fish/.test(o.info || '') ? 'lim' : 'ok'}">${/not for game fish/.test(o.info || '') ? '!' : '✓'}</i><span><b>${esc(ELEM[o.k] || o.k)}${/not for game fish/.test(o.info || '') ? ' — not for trout or other game fish' : ''}</b>${o.k === 'set_lining' ? ' ' + gi('set_line') : ''}${o.info ? `<small>${esc(o.info)}</small>` : ''}</span></button>`).join('')}</div>${chips.some(o => !o.allow) ? `<div class="lbl">Not allowed</div><div class="wno">${chips.filter(o => !o.allow).map(o => `<button class="nochip" type="button" data-rule="${o.r ? esc(o.r.key) : ''}"><i>✕</i>${esc(cap(lc(ELEM[o.k] || o.k)))}</button>`).join('')}</div>` : ''}</div>`;
  // handling rules by moment (answers conduct; the short phrases are the answers' static moments)
  const PHRASE = new Map(SEC.gear.moments.flatMap(([, acts]) => acts.map(([a, p]) => [a, p])));
  const moments = Object.entries(G.conduct);
  // a duty for a fish CAUGHT some way (answers gear `caught`, user ruling Q38): the answers' sentence
  const caught = (G.caught || []).map(PLACE.R);
  const caughtCard = !G.caught ? missing('the duties for a snagged fish (gear `caught`)') : caught.length ? `<div class="hcard"><div class="hh">When you catch a fish</div><ul>${caught.map(r => `<li><button class="factbtn" type="button" data-rule="${esc(r.key)}">${esc(say(r) || `(no words in the answers for ${r.key})`)}</button></li>`).join('')}</ul></div>` : '';
  P.alwaysN = moments.reduce((n, [, acts]) => n + acts.length, 0) + caught.length;
  P.always = P.alwaysN ? `<div class="hcards">${caughtCard}${moments.map(([m, acts]) => `<div class="hcard"><div class="hh">${esc(MOMENT_HEAD[m] || m)}</div><ul>${acts.map(([a, rs]) => `<li><button class="factbtn" type="button" data-rule="${esc(PLACE.R(rs[0]).key)}">${esc(PHRASE.get(a) || SEC.gear.conduct_means[a] || D.conduct[a] || a)}</button></li>`).join('')}</ul></div>`).join('')}</div>` : '';
  P.waysOk = chips.filter(o => o.allow).length;
  P.waysNames = chips.filter(o => o.allow).map(o => ELEM[o.k] || o.k);
  const boats = [...G.vessel.active, ...G.vessel.timed].map(PLACE.R);
  P.boats = boats.length ? `<div class="gsec"><ul class="plain">${boats.map(r => `<li><button class="factbtn" type="button" data-rule="${esc(r.key)}">${esc(clean(r.label))} <span class="src">${RANK[String(r.rank)].t}</span></button></li>`).join('')}</ul></div>` : '';
  P.boatFirst = G.vessel.active.map(PLACE.R).map(r => clean(r.parts?.what || r.label).replace(/ — .*$/, ''))[0] || '';
  return P;
}
function openGearAll(){
  const G = gearAt(PLACE, state.md); if (!G || G.tidal) return;
  const won = G.decides.map(PLACE.R), rep = G.repeats.map(PLACE.R), over = G.overruled.map(o => ({ r:PLACE.R(o.rule), o }));
  openSheet('Gear sources', `${PLACE.name} · ${fmtMd(state.md)}`, `<div class="lbl">These decide it (${won.length})</div>${won.sort((a, b) => a.rank - b.rank).map(r => srcCard(r, r.label, true, fieldsPre(r.f))).join('')}` +
    (rep.length || over.length ? `<details class="lostlist"><summary class="lbl">Overruled or repeated (${rep.length + over.length})</summary>${over.map(({ r, o }) => srcCard(r, `${r.label} · ${o.state}${o.by != null ? ' by “' + clean(PLACE.R(o.by).verbatim) + '”' : ''}`, false, fieldsPre(r.f))).join('')}${rep.map(r => srcCard(r, `${r.label} · repeats a closer rule`, false, fieldsPre(r.f))).join('')}</details>` : ''));
}
/* ================= LICENCES: the answers licence frame, per angler profile =================
   Licensing never opens or closes a water. Every requirement in force, every document to buy, every
   exemption is the answers'; the page asks who is fishing and writes the words. */
const LIC = D.licensing;
for (const [k, l] of Object.entries(LIC)){ l.key = k; l.f = l.fields; l.wins = whenDates(l.f.when); l.prov = l.prov || {}; l.verbatim = l.verbatim || ''; }
const LX = ix => LIC[D.lid[ix]] || { key:'#L' + ix, f:{}, fields:{}, prov:{}, label:`(licensing record ${ix}: missing from the page’s data)`, verbatim:'', kind:'?' };
const DOC = d => D.licences[d]?.name || d.replace(/_/g, ' ');
const AXES = { residency:[['resident','B.C. resident'],['non_resident','Canadian, other province'],['non_resident_alien','From outside Canada']], age:[['16_plus','16 or older'],['under_16','Under 16']], guidance:[['non_guided','Not guided'],['guided','Guided']], status:[['none','No status'],['indian_bc_resident','Status First Nations, living in B.C.'],['metis','Métis'],['disabled','Disabled'],['aged_65_plus','65 or older']] };
const whoTxt = who => !who ? 'every angler' : Object.entries(who).map(([a, l]) => (Array.isArray(l) ? l : [l]).map(v => { const t = String(AXES[a]?.find(x => x[0] === v)?.[1] || v); return a === 'status' ? (v === 'indian_bc_resident' ? 'a status First Nations person living in B.C.' : t) : t.replace(/^[A-Z](?=[a-z])/, c => c.toLowerCase()); }).join(' or ')).join(', ');
function actTxt(d){
  const sp = (d.species ? lcNames(expand(d.species)) || d.species.map(c => (GROUPS[c]?.name || c).toLowerCase()).join(', ') : '') || 'fish', len = d.lengths?.length ? ' ' + d.lengths.map(x => x.min_cm != null && x.max_cm != null ? `${x.min_cm}–${x.max_cm} cm` : x.min_cm != null ? `over ${x.min_cm} cm` : `under ${x.max_cm} cm`).join(', ') : '';
  switch (d.act){ case 'fishing': return 'To fish here'; case 'targeting': return `To fish for ${sp}`; case 'retaining': return `To keep ${sp}${len}`; case 'retaining_recorded': return 'When you keep a fish you must record'; case 'guiding': return 'To guide anglers'; default: return d.act; }
}
// the angler profile's index: mixed radix over profile_dims (answers licence static)
const profileIx = who => SEC.licence.profiles.indexOf(SEC.licence.profile_dims.map(([d]) => who[d]).join('/'));
function licAt(w, md){ const f = frameIx(w, 'licence', segOf(w, md)); if (f == null) return null; const [h, d] = SEC.licence.frames[f]; return { holds:SEC.licence.holds[h], profiles:SEC.licence.documents[d].map(i => SEC.licence.answers[i]) }; }
const priceTxt = p => [p.year != null ? `$${p.year.toFixed(2)} a year` : '', ...(p.day || []).map(x => `$${x.toFixed(2)} a day`), ...(p.eight_days || []).map(x => `$${x.toFixed(2)} for 8 days`)].filter(Boolean).join(' · ');
function pathHtml(p){
  const bits = [];
  if ('need' in p) bits.push(p.need.length ? p.need.map(d => `<span class="doc">${esc(DOC(d))}</span>`).join(' + ') : `<span class="doc ok">Nothing to buy: you’re exempt</span>`);
  if (p.accompanied_by) bits.push(`be with someone who is ${esc(whoTxt(p.accompanied_by.who || p.accompanied_by))} and holds what this fishing needs${p.quota === 'counts_to_companion' ? ' <em>(fish you keep count toward their limit)</em>' : ''}`);
  if (p.as) bits.push(`meet the rules as ${esc(whoTxt(p.as.who || p.as))}`);
  return bits.join(' ') + (p.alt != null ? ` <span class="muted small">(also accepted here)</span>` : '');
}
const srcOf = l => l.placement === 'province' ? 'Province' : l.placement === 'on_designation' ? 'Classified water' : l.f.authority === 'superior' ? 'Federal / parks' : 'This water';
function reqCard(x, holds){
  const l = LX(x.req), also = (holds.also_printed || {})[String(x.req)] || [];
  const docs = x.paths.length ? x.paths.map(p => `<div class="lpath">${pathHtml(p)}</div>`).join('<div class="lor">or</div>') : '';
  const cond = (l.f.conduct || []).map(c => `<div class="lpath">${esc(D.conduct[c] || c)}</div>`).join('');
  const terms = (x.terms || []).map(LX).map(t => `<li><button class="factbtn" type="button" data-lic="${esc(t.key)}">${esc(t.label)}</button></li>`).join('');
  const by = x.presumes_by != null ? LX(x.presumes_by) : null;
  return `<div class="lreq${x.displaced_by ? ' lost' : ''}"><button class="lhead" type="button" data-lic="${esc(l.key)}"><span>${esc(l.label)}</span><span class="src">${srcOf(l)}${also.length ? ' +' + also.length : ''}</span></button>
    ${x.displaced_by ? `<div class="muted small">Not valid here: a federal or park authority’s requirement replaces provincial licences.</div>` : ''}${docs}${x.presumes_freed ? `<div class="doc ok">Not for you: you don’t need the licence this is about</div>` : cond}${!x.presumes_freed && by ? `<div class="muted small">Not needed if you are ${esc(whoTxt(by.f.who).replace(/\.$/, ''))}.</div>` : ''}${terms ? `<ul class="plain small lterms">${terms}</ul>` : ''}</div>`;
}
// another angler's (or a guide's) requirement: how it is met as the record prints it — the answers' `others` /
// `guiding` {req, paths} (licence 4, gap G2), not this angler's answer
function otherCard(o){
  const l = LX(o.req), paths = o.paths;
  return `<div class="muted small">For ${esc(whoTxt(l.f.who))}${l.f.who_except ? ` (not ${esc(whoTxt(l.f.who_except))})` : ''}:</div><div class="lreq"><button class="lhead" type="button" data-lic="${esc(l.key)}"><span>${esc(l.label)}</span><span class="src">${srcOf(l)}</span></button>${paths.map(p => `<div class="lpath">${pathHtml(p)}</div>`).join('<div class="lor">or</div>')}</div>`;
}
function profileHtml(){
  return `<div class="profile"><div class="lbl">Who is fishing? <span class="lblgl">Under 16 ${gi('under_16')} · Guided ${gi('guided')}</span></div>${Object.entries(AXES).map(([a, opts]) => `<label class="psel"><span class="sr">${a}</span><select data-axis="${a}" id="who-${a}">${opts.map(([v, t]) => `<option value="${v}"${state.who[a] === v ? ' selected' : ''}>${t}</option>`).join('')}</select></label>`).join('')}</div>`;
}
function licParts(){
  const water = WATERS[state.wi], md = state.md;
  if (MODEL.status === 'closed') return null;
  const F = licAt(PLACE, md); if (!F) return { missing:true, buy:[], none:false, body: profileHtml() + missing(`the licence answer on ${fmtMd(md)} (licence frame for this part key and segment)`) };
  const H = F.holds, pix = profileIx(state.who), P = F.profiles[pix];
  if (H.tidal) return { tidal:H.tidal, buy:[], none:false, body: profileHtml() + `<div class="classbox"><b>Tidal water</b><span>${esc(H.tidal.licence || H.tidal.note || '')}</span></div>` };
  if (!P) return { missing:true, buy:[], none:false, body: profileHtml() + missing('the answer for this angler profile') };
  let h = profileHtml();
  const des = (H.designations || []).map(LX);
  const stampDates = des.filter(d => d.f.steelhead_stamp_during).map(d => whenDates(d.f.steelhead_stamp_during.when).map(([a, b]) => rangeTxt(a, b)).join(', '))[0] || '';
  if (des.length) h += des.map(d => `<div class="classbox"><b>${esc(d.period?.says || `Class ${d.f.classified} Classified Water`)} ${gi('classified_waters')}</b><span>Licence unit: ${esc(d.f.unit_name || d.f.unit)}${d.f.steelhead_stamp_during ? `. Steelhead Stamp needed ${esc(whenDates(d.f.steelhead_stamp_during.when).map(([a, b]) => rangeTxt(a, b)).join(', '))}, whatever you fish for${H.stamp_period ? ' (in force today)' : ''}; at any other time only to fish for steelhead` : ''}${d.stamp_waiver ? '. ' + d.stamp_waiver.says.replace(/\.$/, '') : d.f.steelhead_stamp_waived ? '. Steelhead Stamp not needed here unless you fish for steelhead' : ''}.</span> <button class="srcbtn inline" type="button" data-lic="${esc(d.key)}">Source</button></div>`).join('');
  if (H.contested) h += `<div class="flag"><b>?</b><span>Check: part of this water is also marked “not a Classified Water”.</span></div>`;
  (DWATERS[water.id]?.unresolved_licensing || []).map(LX).forEach(l => { h += `<div class="flag"><b>?</b><span>Check: “${esc(l.label)}” couldn’t be placed on the map (${esc(friendlyWhy(l.prov.why || l.f.review_reason || '') || 'the place isn’t clear')}).</span></div>`; });
  if (P.exempt) h += `<div class="scopenote wide">You’re exempt from: ${esc(P.exempt.from.map(DOC).join(', '))}. <button class="srcbtn inline" type="button" data-lic="${esc(LX(P.exempt.by[0]).key)}">Source</button></div>`;
  if (P.none_needed) h += `<div class="classbox"><b>No licence needed to fish here</b><span>${state.who.age === 'under_16' ? 'Anglers under 16 who live in B.C. don’t need a basic angling licence.' : P.exempt ? 'You’re exempt from the basic angling licence.' : 'No licence rule applies to this angler.'} The fishing rules and limits still apply.</span></div>`;
  const whenTxt = (w) => { const t = actTxt(w); const perSays = des.map(d => d.period?.says).filter(Boolean)[0];
    return w.on === 'classified_period' && perSays ? `Needed while it’s classified: ${lc(perSays.replace(/^Classified \(Class (I+)\)/, 'Class $1'))}` : w.on === 'steelhead_period' ? `Needed to fish at all${stampDates ? ' ' + stampDates : ''}, whatever you fish for` : t === 'To fish here' ? 'Needed to fish at all' : t.startsWith('To fish for') ? 'Only if you ' + lc(t.replace(/^To /, '')) + ', even to release them' : t.startsWith('To keep') ? 'Only if you ' + lc(t.replace(/^To /, '')) : t; };
  // the answers' documents: `when` the broadest need, `also_when` the narrower ones (licence L10), `or` another
  // way to satisfy the same need (L9: "B.C. or Yukon licence", never both)
  const alsoTxt = w => { const t = actTxt(w); return t.startsWith('To fish for') ? 'at any other time only if you ' + lc(t.replace(/^To /, '')) : lc(whenTxt(w)); };
  const docGi = d => d === 'steelhead_stamp' ? gi('steelhead_stamp') : d === 'classified_waters_licence' ? gi('classified_waters') : /conservation surcharge/i.test(DOC(d)) ? gi('conservation_surcharge') : '';
  const buy = P.documents.map(b => { const alts = (b.or || []).map(o => o.need.map(DOC).join(' + '));
    return { name:cap([DOC(b.doc), ...alts].join(' or ')), docs:[b.doc], alts, when:[whenTxt(b.when), ...(b.also_when || []).map(alsoTxt)].join('; ') + (alts.length ? '. Either one does' : ''), base:b.base, price:priceTxt(b.prices || {}),
      tile:[DOC(b.doc), ...alts].map(n => n.replace(/^basic angling licence$/i, alts.length ? 'B.C. licence' : 'Basic licence').replace(/^(.+?) angling licence$/i, '$1 licence')).join(' or ').replace(/^(\S+) licence or (\S+) licence$/, '$1 or $2 licence') }; });
  // UNDER 16 (user test 2026-10-08, item 2): no document of their own, and a fishing requirement met by fishing
  // with a licensed adult (`accompanied_by`) or as an adult (`as`): say so on top, in "You need" and on the tile
  const youthReq = !P.documents.length && P.requirements.find(x => x.when.act === 'fishing' && x.paths.some(p => p.accompanied_by || p.as));
  let youth = null;
  if (youthReq){
    const asP = youthReq.paths.find(p => p.as), acc = youthReq.paths.find(p => p.accompanied_by);
    const adult = asP ? F.profiles[profileIx({ ...state.who, ...Object.fromEntries(Object.entries(asP.as).map(([k, v]) => [k, (Array.isArray(v) ? v : [v]).includes(state.who[k]) ? state.who[k] : (Array.isArray(v) ? v[0] : v)])) })] : null;
    const adocs = (adult?.documents || []).filter(b => b.base).map(b => cap([DOC(b.doc), ...(b.or || []).map(o => o.need.map(DOC).join(' + '))].join(' or ')));
    youth = { acc:!!acc, adocs, counts: acc?.quota === 'counts_to_companion' };
    h += `<div class="classbox youth"><b>Under 16: no licence needed if you fish with a licensed adult ${gi('under_16')}</b><span>${acc ? `Fish with someone ${esc(whoTxt(acc.accompanied_by.who || {}))} who holds what this fishing needs${adocs.length ? ` (${esc(adocs.join(', '))})` : ''}.${youth.counts ? ' What you keep counts toward their limit.' : ''}` : ''}${asP ? ` Or buy ${adocs.length ? 'the ' + esc(adocs.join(' + ')) : 'what a 16+ angler needs'} yourself to have your own limit.` : ''}</span></div>`;
    buy.push({ name:'Nothing, if you fish with a licensed adult (16 or older)', when: youth.counts ? 'What you keep counts toward their limit' : 'They must hold what this fishing needs', base:true, price:'', youth:true });
    if (asP && adocs.length) buy.push({ name:adocs.join(' + '), when:'Or, for your own limit: what a 16+ angler buys', base:false, price:priceTxt(adult.documents.find(b => b.base)?.prices || {}), youth:true });
  }
  if (buy.length) h += `<div class="needs"><div class="lbl">You need</div><ul>${buy.map(b => `<li><b>${esc(b.name)} ${(b.docs || []).map(docGi).join('')}</b><span>${esc(b.when)}</span>${b.price ? `<span class="price">${esc(b.price)}</span>` : ''}</li>`).join('')}</ul>${buy.some(b => b.price) ? '<p class="pricenote">Prices as printed for 2025–2027, before tax. Today’s prices: gov.bc.ca/fish-licence</p>' : ''}</div>`;
  // the paper licence: the record duties that reach this water (answers display.waters part paper_licence)
  const paperReq = P.requirements.find(x => x.when.act === 'retaining_recorded');
  const paperRules = (PLACE.disp?.paper_licence || []).map(PLACE.R);
  if (paperReq && paperRules.length){
    const fishOf = r => { const f = r.f, min = (f.lengths || []).find(l => l.min_cm != null); return `${f.life_stage === 'adult' ? 'adult ' : ''}${f.origin ? f.origin + ' ' : ''}${lcNames(expand(f.species || []), ' or ')}${min ? ` over ${min.min_cm} cm` : ''}`; };
    const fish = [...new Set(paperRules.map(fishOf))];
    h += `<p class="paper">Keep ${/^[aeiou]/.test(fish[0]) ? 'an' : 'a'} ${esc(join(fish, ' or '))}? <b>Carry your paper licence</b> (16 and over) and record it right away.</p>`; }
  (H.not_yet_mapped || []).map(LX).forEach(l => { h += `<div class="nymbox"><div class="lbl">In one part of this water</div><p><b>${esc(cap(l.parts?.in_part || l.not_yet_mapped?.part || ''))}:</b> you need ${esc(l.parts?.need || DOC((l.f.satisfied_by || [])[0]?.hold?.[0] || ''))} ${esc(l.parts?.doing || 'to fish')}.</p><p class="muted small">That part isn’t drawn on the map yet. It doesn’t apply to the rest of the water.</p></div>`; });
  const groups = new Map(); P.requirements.forEach(x => { const t = actTxt(x.when); if (!groups.has(t)) groups.set(t, []); groups.get(t).push(x); });
  const orderAct = t => t === 'To fish here' ? 0 : t.startsWith('To fish for') ? 1 : t.startsWith('To keep') ? 2 : 3;
  h += `<details class="lostlist"><summary class="lbl">Where each one comes from</summary>`;
  [...groups.entries()].sort((a, b) => orderAct(a[0]) - orderAct(b[0])).forEach(([t, list]) => { h += `<div class="gsec"><div class="lbl">${esc(t)}</div>${list.map(x => reqCard(x, H)).join('')}</div>`; });
  h += `</details>`;
  if (!P.requirements.length && !P.none_needed) h += `<p class="muted">Nothing is required of this angler here.</p>`;
  if ((P.guiding || []).length) h += `<details class="lostlist"><summary class="lbl">If you are guiding other anglers</summary>${P.guiding.map(otherCard).join('')}</details>`;
  if ((P.others || []).length) h += `<details class="lostlist"><summary class="lbl">Rules for other anglers (${P.others.length})</summary>${P.others.map(otherCard).join('')}</details>`;
  h += `<button class="srcbtn" type="button" data-licsrc="1">All licence sources</button>`;
  return { buy, none:P.none_needed, youth, body:h };
}
function openLic(key){ const l = LIC[key]; if (!l) return; openSheet('Licence source', l.prov.entry_name || '', srcCard({ ...l, prov:{ who:l.placement === 'province' ? 'Provincial · province-wide' : l.placement === 'on_designation' ? 'Wherever a classified designation is in force' : l.placement === 'not_placed' ? 'Not bound to a place' : l.prov.entry_name } , rank:l.placement === 'province' ? 4 : 0, notes:[l.f.review_reason && 'Curator still to settle: ' + l.f.review_reason, l.prov.uncertain && 'Unresolved: ' + l.prov.why].filter(Boolean) }, `${l.kind}: ${l.label}`, true, `<div class="muted small">${esc(key)} · placement: ${esc(l.placement)}</div>` + fieldsPre(l.f))); }
function openLicAll(){
  const F = licAt(PLACE, state.md); if (!F || F.holds.tidal) return;
  const all = (F.holds.considered || []).map(LX);
  const card = l => srcCard({ ...l, prov:{ who:l.placement }, rank:l.placement === 'province' ? 4 : 0, notes:[] }, `${l.kind}: ${l.label}`, true, fieldsPre(l.f));
  openSheet('Licence sources', `${PLACE.name} · ${fmtMd(state.md)}`, `<div class="lbl">Records considered (${all.length})</div>${all.map(card).join('')}`);
}
/* ---------- Gear & licence: one view. A row of answer tiles; tap one to see its detail. ---------- */
const KIT_TABS = [
  ['licence', 'Licence'], ['line', 'Line & hooks'], ['bait', 'Bait'], ['ways', 'Ways to fish'], ['boats', 'Boats'], ['always', 'Always']
];
const shortDoc = n => n.replace(/^Conservation Surcharge Stamp for (.+)$/i, '$1 stamp').replace(/^(.+?) Conservation Surcharge Stamp$/i, (m, x) => lc(x) + ' stamp').replace(/^Conservation Surcharge Stamp for Kootenay Lake rainbow trout.*$/i, 'Kootenay rainbow stamp');
function renderKit(){
  const el = document.getElementById('kit'); if (!el) return;
  if (PLACE.k == null){ el.innerHTML = `<p class="foot">This part of the water is outside B.C.: no B.C. rules apply.</p>`; return; }
  let h = waterStrip(PLACE, state.md);
  if (isTidal(PLACE)){ const G = gearAt(PLACE, state.md), L = licAt(PLACE, state.md);
    h += (G?.tidal ? '' : missing('the gear answer for tidal water')) + (L?.holds?.tidal ? '' : missing('the licence answer for tidal water'));
    el.innerHTML = h; return; }
  if (MODEL.status === 'closed'){
    h += closedBanner('rules') + `<p class="muted small">No gear may go in the water and no licence is needed while fishing is closed.</p>`;
    el.innerHTML = h; return;
  }
  const G = gearParts(), L = licParts();
  if (G.missing){ el.innerHTML = h + missing(`the gear answer on ${fmtMd(state.md)} (gear frame for this part key and segment)`) + L.body; return; }
  const base = L.buy.filter(b => b.base), extra = L.buy.filter(b => !b.base);
  const tiles = {
    licence: L.missing ? { v:'Missing', s:'no licence answer' } : L.youth ? { v: L.youth.acc ? 'None with a licensed adult' : 'An adult’s licence', s: L.youth.acc ? (L.youth.counts ? 'Under 16: fish count toward theirs' : 'Under 16') + (L.youth.adocs.length ? ' · or buy your own' : '') : 'Under 16' } : { v: L.none ? 'None needed' : base.length ? base.map(b => cap(shortDoc(b.tile || b.name))).join(' + ') : 'Basic licence', s: extra.length ? '+ ' + shortDoc(extra[0].name) + (extra.length > 1 ? ` and ${extra.length - 1} more` : '') + ' if needed' : 'Nothing extra' },
    line: { v: G.tags[1], s: [G.tags[0], ...G.tags.slice(3)].filter(Boolean).join(' · ') },
    bait: { v: G.baitBan ? 'Bait ban' : 'Some bait OK', s: G.baitBan ? 'Lures and flies only' : (G.baitList || []).map(b => `${({ worms:'Worms', roe:'Roe', invertebrate:'Insects', fin_fish:'Fish parts' })[b.e] || b.e} ${b.ok ? '✓' : '✕'}`).join(' · '), html: !G.baitBan && (G.baitList || []).length ? (G.baitList || []).map(b => `<span class="bk ${b.ok ? 'ok' : 'no'}">${esc(({ worms:'Worms', roe:'Roe', invertebrate:'Insects', fin_fish:'Fish parts' })[b.e] || b.e)} <i>${b.ok ? '✓' : '✕'}</i></span>`).join('') : null },
    ways: { v: G.waysNames[0] ? cap(lc(G.waysNames[0]).replace(/ \(rod and line\)/, '')) : 'None', s: G.waysNames.length > 1 ? 'Also ' + G.waysNames.slice(1).map(x => lc(x)).join(', ') : 'Nothing else' },
    boats: G.boats ? { v: G.boatFirst ? cap(G.boatFirst) : 'Rules apply', s: 'Tap for all boat rules' } : null,
    always: G.alwaysN ? { v: 'Handling fish', s: `${G.alwaysN} rules that apply everywhere` } : null,
  };
  const tabs = KIT_TABS.filter(([k]) => tiles[k]);
  if (!tiles[state.kit]) state.kit = 'licence';
  h += G.pre;
  h += `<div class="kgrid" role="tablist" aria-label="Gear and licence">${tabs.map(([k, t]) => `<button class="ktile${state.kit === k ? ' on' : ''}" role="tab" aria-selected="${state.kit === k}" type="button" data-kit="${k}"><span class="kl">${t}</span><b class="kv">${esc(tiles[k].v)}</b><span class="ks">${tiles[k].html || esc(tiles[k].s)}</span></button>`).join('')}</div>`;
  const body = { licence: L.body, line: G.line, bait: G.bait, ways: G.ways, boats: G.boats, always: G.always }[state.kit];
  const ti = tabs.findIndex(([k]) => k === state.kit), prev = tabs[ti - 1], next = tabs[ti + 1];
  h += `<section class="kpane" role="tabpanel"><div class="khead"><h3>${KIT_TABS.find(([k]) => k === state.kit)[1]}</h3><span class="knav">${prev ? `<button type="button" data-kit="${prev[0]}">‹ ${prev[1]}</button>` : ''}${next ? `<button type="button" data-kit="${next[0]}">${next[1]} ›</button>` : ''}</span></div>${body}${state.kit !== 'licence' ? '<button class="srcbtn" type="button" data-gearsrc="1">All gear sources</button>' : ''}</section>`;
  el.innerHTML = h;
}
/* ================= Checks: the export's sample cases and ours, read from the answers ladder =================
   Each case names a water, a date and a fish; the page finds the export part whose rule set the case names
   (page_data.py), looks up that part key's ladder frame on that day for the fish (origin not known) and
   compares each rule's state with the case's. The page runs no ladder of its own. */
const SHOWN = ['speaks', 'beside', 'shown', 'not_yet_mapped'];
const caseRule = ix => CASES.rules[String(ix)] || { id:'#' + ix, label:`(rule ${ix}: missing from the cases block)`, family:'?', rank:4 };
function caseLadder(c){
  if (c.key == null) return null;
  const w = { k:c.key, key:A.keys[c.key], _seg:{} }, [m, d] = c.date.split('-').map(Number);
  // a case asked at a moment (answers 2.1: `at`, a weekday class and inside / outside an hours window)
  const s = c.at ? segAt(w, m * 100 + d, c.at.weekdays[0], !!c.at.hours?.in) : segOf(w, m * 100 + d);
  const L = ladderAt(w, s);
  return L ? (L[c.fish] ? L[c.fish].none : 'nofish') : null;
}
function runCase(c){
  if (!c.ruleset) return { c, rows:[], ok: Object.keys(c.expect).length === 0, outside:true };
  const v = caseLadder(c);
  if (v == null || v === 'nofish') return { c, rows:[], ok:false, gap: v == null ? 'no ladder frame for this case’s part key and day' : `the ladder frame does not answer ${c.fish}` };
  const listed = c.only_listed ? Object.keys(c.expect).map(k => [+k, 'reach']) : c.members;
  const rows = listed.map(([ix, via]) => { const r = caseRule(ix), e = c.expect[String(ix)] ?? '—', g = v.get(ix);
    const gs = g && SHOWN.includes(g.state) ? g.state + (g.partly ? ' · partly lifted' : '') : '—';
    const why = g && !SHOWN.includes(g.state) ? `${g.state} (${g.reason}) by ${caseRule(g.by).label}` : '';
    return { r, ix, via, es:e, gs, why, same: e === gs }; });
  return { c, rows, ok: rows.every(x => x.same) };
}
let CASE_RESULTS = null;
function renderCases(){
  const el = document.getElementById('cases-list'); if (!el) return;
  if (!CASE_RESULTS) CASE_RESULTS = CASES.cases.map(runCase);
  const all = document.getElementById('casesAll')?.checked;
  const n = CASE_RESULTS.filter(x => x.ok).length, no = CASE_RESULTS.filter(x => x.c.src === 'ours').length, ng = CASE_RESULTS.length - no;
  const okG = CASE_RESULTS.filter(x => x.ok && x.c.src === 'guide').length, okO = n - okG;
  let h = `<div class="casesum"><b>${n} of ${CASE_RESULTS.length}</b> cases agree with the answers file’s ladder: ${okG} of ${ng} of the export’s sample waters (<code>guide.cases</code>: sample waters to build against, not a test oracle; their expected states come from the same reader the answers file stores, so a ✕ here means the two files disagree) and ${okO} of ${no} of ours, written from rulings made during review (see Reference notes). Each case is one water, one day and one fish, read on the export part whose rule set it names.</div>`;
  h += `<label class="small muted"><input type="checkbox" id="casesAll"${all ? ' checked' : ''}> Show gear, conduct and province-wide rules too</label>`;
  h += CASE_RESULTS.map(({ c, rows, ok, outside, gap }) => {
    const fams = ['retention', 'access'];
    const vis = rows.filter(x => all || (fams.includes(x.r.family) && !x.r.id.startsWith('zp:')) || !x.same);
    const inPlay = vis.filter(x => x.gs !== '—' || x.es !== '—');
    const quiet = vis.filter(x => x.gs === '—' && x.es === '—');
    const rk = x => RANK[String(x.via === 'trib' && x.r.rank >= 0 ? 1 : x.r.rank)]?.t || '';
    const rowHtml = x => `<tr class="${x.same ? '' : 'miss'}"><td><span class="cst ${x.gs.split(' ')[0].replace('—', 'none')}">${esc(x.gs)}</span>${x.same ? '' : `<div class="small">case: ${esc(x.es)}</div>`}</td><td>${esc(rk(x))}</td><td>${esc(x.r.label)}${x.why ? `<div class="small muted">${esc(x.why)}</div>` : ''}</td></tr>`;
    const [m, d] = c.date.split('-').map(Number);
    return `<details class="case${ok ? '' : ' bad'}"><summary><span class="cok">${ok ? '✓' : '✕'}</span>${c.src === 'ours' ? '<span class="ourtag">ours</span>' : ''}<span><b>${esc(cap(c.shows || ''))}</b><span class="muted small"> · ${esc(c.water.name)} · ${esc(fmtMd(m * 100 + d))} · ${esc(FISH[c.fish]?.name || c.fish)}</span></span></summary>
      <p class="small">${esc(c.what || '')}</p>${c.ruling ? `<p class="small muted">Ruling: ${esc(c.ruling)}</p>` : ''}
      ${outside ? '<p class="small muted">Outside B.C.: no rules at all.</p>' : gap ? missing(gap) : `<table class="ctab"><tbody>${inPlay.map(rowHtml).join('')}</tbody></table>
      ${quiet.length ? `<details class="small"><summary class="muted">${quiet.length} rules here that don’t speak for this fish on this day</summary><table class="ctab"><tbody>${quiet.map(rowHtml).join('')}</tbody></table></details>` : ''}`}
    </details>`;
  }).join('');
  el.innerHTML = h;
}
document.addEventListener('change', e => { if (e.target.id === 'casesAll') renderCases(); });
document.getElementById('tabs').addEventListener('click', e => { if (e.target.closest('[data-v="cases"]')) renderCases(); });
/* ================= chrome ================= */
// open on the stretch that reaches the river's mouth (where most people fish), else the first open choice
function defaultPart(wi){
  const water = WATERS[wi], ch = choicesOf(water).filter(g => !g.closed), parts = water.parts;
  const open = ch.flatMap(g => g.parts).sort((a, b) => (parts[b]?.sections || 0) - (parts[a]?.sections || 0));
  if (open.length && !open.some(i => (parts[i]?.runs || []).some(r => r.to === 'mouth' && !r.branch))) return open[0];
  const mouth = open.find(i => (parts[i]?.runs || []).some(r => r.to === 'mouth' && !r.branch));
  if (mouth != null) return mouth;
  if (ch.length) return ch[0].parts[0];
  const any = parts.findIndex(p => p); return any < 0 ? 0 : any;
}
// date chips: the days the answers' segments start (where some rule's or lift's dates begin)
function dateChips(){
  if (PLACE.k == null) return `<button class="dchip now" data-md="${TODAY}" data-today="1" type="button">Today</button>`;
  // a part whose year is never cut has no date to offer (as v35: no window, no chip)
  const s0 = [...new Set(segStarts(PLACE).map(mdOfDay))], s = s0.length === 1 ? [] : s0;
  const arr = s.filter(x => x !== 101 || s.length < 4).sort((a, b) => a - b).slice(0, 6);
  return `<button class="dchip now" data-md="${TODAY}" data-today="1" type="button">Today</button>` + arr.map(md => `<button class="dchip" data-md="${md}" type="button">${fmtMd(md)}</button>`).join('');
}
function renderParts(){
  const water = WATERS[state.wi], el = document.getElementById('partrow');
  const G = choicesOf(water), n = water.parts.filter(Boolean).length;
  if (n < 2 && !water.outside_bc){ el.innerHTML = ''; return; }
  if (!G.length){ el.innerHTML = missing('the part picker (display waters picker)'); return; }
  const cur = groupOf(water, state.pi), headed = DWATERS[water.id].picker.headed;
  const opt = g => `<option value="${g.parts[0]}"${g === cur ? ' selected' : ''}>${esc(g.text)} · ${g.sections} section${g.sections > 1 ? 's' : ''}</option>`;
  const heads = new Map(); G.forEach(g => { const k = headed ? g.heading || '' : ''; (heads.get(k) || heads.set(k, []).get(k)).push(g); });
  el.innerHTML = `<label class="partsel"><span class="lbl">Which part of ${esc(water.name)}? <span class="muted">(${G.length} choice${G.length > 1 ? 's' : ''}${G.length < n ? `, ${n} stretches` : ''}${water.outside_bc ? `; ${water.outside_bc} section${water.outside_bc > 1 ? 's' : ''} outside B.C., not governed here` : ''})</span></span><select id="part">${[...heads].map(([hd, gs]) => hd ? `<optgroup label="${esc(hd)}">${gs.map(opt).join('')}</optgroup>` : gs.map(opt).join('')).join('')}</select></label>`;
}
function renderAll(keep){
  PLACE = makePlace(state.wi, state.pi);
  MODEL = PLACE.k == null ? { R:{}, rows:[], spp:[], line:null, broad:[], status:null } : buildModel(PLACE, state.md);
  REOPEN = MODEL.status === 'closed' ? nextOpen(PLACE, state.md) : null;
  const water = WATERS[state.wi], dp = PLACE.disp;
  document.querySelectorAll('[data-f="name"]').forEach(e => e.textContent = water.name);
  document.querySelectorAll('[data-f="sub"]').forEach(e => e.textContent = `${water.kind} · ${(() => { const g = groupOf(water, state.pi), n = water.parts.filter(Boolean).length > 1 && g ? g.sections : (water.parts[state.pi]?.sections ?? water.sections); return `${n} section${n > 1 ? 's' : ''}`; })()}${water.parts.filter(Boolean).length > 1 ? (() => { const g = groupOf(water, state.pi); if (g && g.closed && g.parts.length > 1) return ' · closed all year'; if (!dp) return ''; return ' · ' + (dp.place ? dp.place + ' · ' : '') + cutWords(dp.runs || dp.label || '', 150); })() : ''}`);
  document.querySelectorAll('[data-f="date"]').forEach(e => e.textContent = fmtMd(state.md));
  document.getElementById('dchips').innerHTML = dateChips();
  if (!keep){ state.open = new Set(); state.dd = new Set(); }
  renderParts(); renderCard(); renderKit();
}
const wEl = document.getElementById('waters');
WATERS.forEach((w, i) => { const b = document.createElement('button'); b.type = 'button'; b.className = 'wbtn'; b.setAttribute('aria-pressed', i === state.wi);
  const np = w.parts.filter(Boolean).length, cls = w.parts.some(p => (p?.lic || []).some(([ix]) => LX(ix).kind === 'designation'));
  b.innerHTML = `<b>${esc(w.name)}</b><span>${esc(w.kind)} · ${np > 1 ? np + ' parts' : 'one set of rules'}${cls ? ' · classified' : ''}${w.part_of ? ' · lake part' : ''}${A.parts[w.id]?.some(k => k != null && A.keys[k][8]) ? ' · tidal' : ''}</span>`;
  b.onclick = () => { state.wi = i; state.pi = defaultPart(i); [...wEl.children].forEach((c, j) => c.setAttribute('aria-pressed', j === i)); renderAll(); }; wEl.appendChild(b); });
const dIn = document.getElementById('dateIn');
// the date box: every change, typed or picked, re-renders every view at once (banner, rows, gear, licence)
const isoOf = (y, md) => `${y}-${String(Math.floor(md/100)).padStart(2,'0')}-${String(md%100).padStart(2,'0')}`;
function setMd(md, y){ if (y) YEAR = y; state.md = md; const v = isoOf(YEAR, md); if (dIn.value !== v) dIn.value = v; renderAll(true); }
const fromBox = () => { const [y, m, d] = dIn.value.split('-').map(Number); if (y && m && d && (state.md !== (m === 2 && d === 29 ? 228 : m*100 + d) || y !== YEAR)) setMd(m === 2 && d === 29 ? 228 : m*100 + d, y); };
dIn.addEventListener('input', fromBox); dIn.addEventListener('change', fromBox);
dIn.value = isoOf(YEAR, TODAY);
function setTab(v){ state.tab = v; document.querySelectorAll('#tabs button').forEach(b => b.setAttribute('aria-selected', b.dataset.v === v)); document.querySelectorAll('.view').forEach(s => s.classList.toggle('on', s.id === 'v-' + v)); }
document.getElementById('tabs').addEventListener('click', e => { const b = e.target.closest('[data-v]'); if (b) setTab(b.dataset.v); });
document.addEventListener('change', e => {
  if (e.target.id === 'part'){ state.pi = +e.target.value; renderAll(); }
  if (e.target.dataset.axis){ state.who[e.target.dataset.axis] = e.target.value; renderKit(); }
});
document.addEventListener('click', e => {
  const md = e.target.closest('[data-md]'); if (md){ setMd(+md.dataset.md, md.dataset.today ? NOW.getFullYear() : null); return; }
  const gl = e.target.closest('[data-gl]'); if (gl){ e.stopPropagation(); openGloss(gl.dataset.gl); return; }
  const lic = e.target.closest('[data-lic]'); if (lic){ e.stopPropagation(); openLic(lic.dataset.lic); return; }
  if (e.target.closest('[data-licsrc]')){ openLicAll(); return; }
  if (e.target.closest('[data-gearsrc]')){ openGearAll(); return; }
  const rl = e.target.closest('[data-rule]'); if (rl && rl.dataset.rule){ e.stopPropagation(); openRule(rl.dataset.rule); return; }
  const src = e.target.closest('[data-src]'); if (src){ e.stopPropagation(); openRowSources(src.dataset.src); return; }
  const kt = e.target.closest('[data-kit]'); if (kt){ state.kit = kt.dataset.kit; renderKit(); if (kt.classList.contains('ktile')){ const pn = document.querySelector('.kpane'); if (pn && pn.getBoundingClientRect().top > innerHeight * .6) pn.scrollIntoView({ block:'start', behavior:'smooth' }); } return; }
  const fg = e.target.closest('[data-fishgroup]'); if (fg){ e.stopPropagation(); openFishGroup(fg.dataset.fishgroup.split(',')); return; }
  if (e.target.closest('[data-howtell]')){ e.stopPropagation(); openSheet('Hatchery or wild?', 'The adipose fin', adiposeHtml()); return; }
  const fish = e.target.closest('[data-fish]'); if (fish){ e.stopPropagation(); openFish(fish.dataset.fish); return; }
});
document.addEventListener('keydown', e => { if (e.key === 'Escape') scrim.hidden = true; });
scrim.addEventListener('click', e => { if (e.target === scrim) scrim.hidden = true; });
document.getElementById('shClose').onclick = () => scrim.hidden = true;
// sticky row headers sit just under the sticky tab bar (phone layout)
const setStick = () => { const t = document.getElementById('tabs'); const on = t && getComputedStyle(t).position === 'sticky' && t.offsetHeight; document.documentElement.style.setProperty('--stick', on ? (t.offsetHeight + 6) + 'px' : '0px'); };
addEventListener('resize', setStick); setStick();
document.addEventListener('toggle', e => { const k = e.target.dataset && e.target.dataset.dd; if (k){ if (e.target.open) state.dd.add(k); else state.dd.delete(k); } }, true);
if (PAIR_ERR){ document.querySelector('.views').innerHTML = `<div class="missing"><b>Refused:</b> ${esc(PAIR_ERR)}. The page reads nothing from a file pair that does not match (ANSWERS-SPEC §1).</div>`; }
else { state.pi = defaultPart(0); renderAll(); }
