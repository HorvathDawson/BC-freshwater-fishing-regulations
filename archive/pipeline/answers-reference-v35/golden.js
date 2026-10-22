#!/usr/bin/env node
'use strict';
/* Golden outputs: what the consumer page v35 shows, read off the page itself (harness.js).

   usage: node golden.js --build DIR --out OUT [--from I --to J] [--tag NAME]
     DIR  holds data.json + cases.json (convert.py); every display water in it is covered
     OUT  receives (JSON lines, one object per line):
       card.jsonl     per (water, part, date): status line, closed banner, fish asked, Stage 5 answer per
                      fish x origin (status, daily, winner, lines, roles), rows (+ real daily limit = effCap,
                      rowConds), Stage 6 per row (rowSources roles + the "How this was decided" ladder:
                      question, answer, place heading, sentence, tag), and the card as text
       ladder.jsonl   per (water, part, segment, fish, origin): Stage 4 state of every rule taking part
                      (state, partly, by); one line per segment of days the page caches the ladder for
       gear.jsonl     per (water, part, date): Stage 7.1 resolved slots (counts, specs, elements,
                      circumstantial clauses), 7.2-7.6 tile values and details as text
       licence.jsonl  per (water, part, date): Stage 7.7 answer id for each of the 60 angler profiles
       licence_answers.jsonl  answer id -> settleLic + licParts + licence tile (deduplicated)
     and manifest.json (counts, dates, the page's sha256, the build it read).

   Dates: the 1st and 15th of every month, plus every day a rule's, a lift's or a licensing record's
   window starts or ends the day after (the page's own segment breakpoints and more). Each date is
   picked through the page's date input (so Feb 29 would read as Feb 28, as on the page). */
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const zlib = require('zlib');
const { load, htmlText, PAGE } = require('./harness');

const args = (() => { const a = process.argv.slice(2), o = {}; for (let i = 0; i < a.length; i++) if (a[i].startsWith('--')) o[a[i].slice(2)] = a[i + 1] && !a[i + 1].startsWith('--') ? a[++i] : true; return o; })();

/* ---------- serialising the page's objects ---------- */
const isRec = x => x && typeof x === 'object' && typeof x.key === 'string' && ('rule_id' in x || 'record_id' in x);
function ser(x, depth = 0){
  if (x === Infinity) return 'Infinity';
  if (x === -Infinity) return '-Infinity';
  if (x == null || typeof x !== 'object') return typeof x === 'function' ? undefined : x;
  if (isRec(x)) return x.key;
  if (depth > 12) return '…';
  const tag = Object.prototype.toString.call(x);          // the page's Map / Set come from the vm's realm
  if (tag === '[object Set]') return [...x].map(v => ser(v, depth + 1)).sort();
  if (tag === '[object Map]') return [...x.entries()].map(([k, v]) => [ser(k, depth + 1), ser(v, depth + 1)]);
  if (Array.isArray(x)) return x.map(v => ser(v, depth + 1));
  const o = {}; for (const k of Object.keys(x)){ if (k.startsWith('_')) continue; const v = ser(x[k], depth + 1); if (v !== undefined) o[k] = v; } return o;
}
const hash = o => crypto.createHash('sha1').update(JSON.stringify(o)).digest('hex').slice(0, 16);
const mmdd = md => `${String(Math.floor(md / 100)).padStart(2, '0')}-${String(md % 100).padStart(2, '0')}`;

/* "How this was decided" as the page writes it -> [{q, answer, steps:[{level, lines:[{rule, text, for, tag, won}]}], lost:[…]}] */
function parseLadder(h){
  const out = { questions: [], lost: [] }; let q = null, step = null, inLost = false;
  const re = /<div class="lq"><div class="lbl">([^<]*)<\/div>|<div class="lres">✓ ([^<]*)<\/div>|<span class="lvl">([^<]*)<\/span>|<summary>Rules that don’t apply here \((\d+)\)<\/summary>|<li class="(won|lost)"><button class="pline pl2" type="button" data-rule="([^"]*)"><span>([^<]*)(?:<small>([^<]*)<\/small>)?<\/span><em>([^<]*)<\/em>/g;
  const un = s => htmlText(s || '');
  let m; while ((m = re.exec(h))){
    if (m[1] != null){ q = { q: un(m[1]), answer: null, steps: [] }; out.questions.push(q); step = null; }
    else if (m[2] != null){ if (q) q.answer = un(m[2]); else out.answer = un(m[2]); }
    else if (m[3] != null){ step = { level: un(m[3]), lines: [] }; if (!q){ q = { q: null, answer: out.answer || null, steps: [] }; out.questions.push(q); } q.steps.push(step); }
    else if (m[4] != null){ inLost = true; }
    else { const line = { rule: m[6], text: un(m[7]), for: m[8] ? un(m[8]) : undefined, tag: un(m[9]), won: m[5] === 'won' };
      if (inLost || !line.won) out.lost.push(line); else if (step) step.lines.push(line); }
  }
  return out;
}
const tiles = h => { const t = {}; const re = /data-kit="([^"]+)"><span class="kl">([^<]*)<\/span><b class="kv">([\s\S]*?)<\/b><span class="ks">([\s\S]*?)<\/span><\/button>/g; let m; while ((m = re.exec(h))) t[m[1]] = { title: m[2], value: htmlText(m[3]), sub: htmlText(m[4]) }; return t; };

/* ---------- the dates a part is read on ---------- */
function datesFor(P){
  const s = new Set(); for (let m = 1; m <= 12; m++){ s.add(m * 100 + 1); s.add(m * 100 + 15); }
  const next = md => md === 229 ? 301 : P.DAYS[(P.DAYS.indexOf(md) + 1) % 365];
  const add = W => (W || []).forEach(([a, z]) => { if (a !== 229) s.add(a); const n = next(z); if (n) s.add(n); });
  P.PLACE.cands.forEach(r => { add(r.wins); (r.f.exempts || []).forEach(e => add(P.whenDates(e.when))); });
  P.licPlace().forEach(({ l }) => { add(l.wins); if (l.f.steelhead_stamp_during) add(P.whenDates(l.f.steelhead_stamp_during.when)); });
  return [...s].filter(md => P.DAYS.includes(md)).sort((a, b) => a - b);
}

const PROFILES = P => { const out = []; const A = P.AXES;
  for (const [r] of A.residency) for (const [a] of A.age) for (const [g] of A.guidance) for (const [s] of A.status) out.push({ residency:r, age:a, guidance:g, status:s });
  return out; };
const pkey = p => `${p.residency}/${p.age}/${p.guidance}/${p.status}`;
/* a part, named by what its answers can depend on (set ids alone repeat: Skeena has three parts on one rule set) */
const partKey = p => [p.set, p.lset, p.anadromous_rainbow ? 'sw' : '', p.steelhead || '', p.steelhead_rules === false ? 'sr0' : '', (p.province_except || []).join('+'), p.home_region || ''].join('|');

/* ---------- one (water, part, date) ---------- */
function readCard(page, rec){
  const { P } = page, M = P.MODEL, ctx = P.settle(P.PLACE, P.state.md);
  const answers = {};
  for (const S of M.spp){ const R = M.R[S], m = P.mainRes(R.H, R.W); answers[S] = { hatchery: ser(R.H), wild: ser(R.W), main: m ? m.o : null }; }
  const rows = M.rows.map((row, i) => {
    const rc = row.pool ? P.rowConds(row) : null, ec = rc && P.effCap(row, rc);
    return { i, title: P.rowTitle(row), ...ser({ kind: row.kind, pool: row.pool, win: row.win, members: row.members, allMembers: row.allMembers, daily: row.daily,
      narrow: row.narrow, everyone: row.everyone, groups: row.groups, prot: row.prot, wins: row.wins, liftNotes: row.liftNotes }),
      real_daily: ser(ec), conds: rc ? ser(rc) : null,
      sources: P.rowSources(row).map(it => ({ rule: it.r.key, role: it.role, by: it.by || null, spp: [...it.spp].sort() })),
      decided: parseLadder(P.ladderHtml(row, 'r' + i)) };
  });
  const card = page.cardHtml(), wsline = (card.match(/<div class="wsline">([\s\S]*?)<\/div>/) || [])[1];
  return { ...rec, status: htmlText(wsline || ''), open: !ctx.active.some(P.isBroad), broad: ser(M.broad), reopen: P.REOPEN,
    active: ctx.active.map(r => r.key), timed: ctx.timed.map(r => r.key), beside: ctx.beside.map(r => r.key), inpart: ctx.inpart.map(r => r.key),
    lifted: ser(ctx.lifted), spp: M.spp, answers, rows, card_text: htmlText(card) };
}
function readGear(page, rec){
  const { P } = page;
  if (P.MODEL.broad.length) return { ...rec, closed: true, kit_text: htmlText(page.kitHtml()) };
  const G = P.settleGear(P.PLACE, P.state.md), gp = P.gearParts();
  const ent = e => e && { rule: e.r.key, clause: e.i, s: e.s, c: e.c };
  return { ...rec, closed: false,
    counts: Object.fromEntries(Object.entries(G.counts).map(([k, l]) => [k, l.map(ent)])),
    specs: Object.fromEntries(Object.entries(G.specs).map(([k, l]) => [k, l.map(ent)])),
    elems: Object.fromEntries(Object.entries(G.elems).map(([k, e]) => [k, ent(e)])),
    circ: G.circ.map(e => ({ ...ent(e), circ: e.circ, targeting: e.targeting, note: e.note })),
    conduct: G.conduct.map(r => r.key), whileRet: G.whileRet.map(r => r.key), timed: G.timed.map(r => r.key),
    tags: gp.tags, baitBan: gp.baitBan, baitList: gp.baitList, waysNames: gp.waysNames, waysNo: ser(gp.waysNo), boatFirst: gp.boatFirst, alwaysN: gp.alwaysN,
    text: Object.fromEntries(['pre', 'line', 'bait', 'ways', 'boats', 'always'].map(k => [k, htmlText(gp[k])])),
    tiles: tiles(page.kitHtml()) };
}
function readLicence(page, profiles, answers){
  const { P } = page, out = {};
  for (const prof of profiles){
    Object.assign(P.state.who, prof); P.state.kit = 'licence'; P.renderKit();     // as the profile picker does
    let a;
    if (P.MODEL.broad.length) a = { closed: true, kit_text: htmlText(page.kitHtml()) };
    else {
      const S = P.settleLic(P.state.md, P.state.who), L = P.licParts();
      a = { closed: false,
        settle: { des: S.des.map(d => d.key), contested: S.contested, classPeriod: S.classPeriod, stampPeriod: S.stampPeriod,
          exemptions: S.exemptions.map(l => l.key), released: [...S.released].sort(), nym: S.nym.map(x => x.l.key), unresolved: S.unresolved.map(x => x.l.key),
          reqs: S.reqs.map(x => ({ rule: x.l.key, via: x.via, mine: x.mine, displaced: x.displaced, presumesFreed: x.presumesFreed, presumesBy: x.presumesBy ? x.presumesBy.key : null,
            also: x.also.map(l => l.key), paths: ser(x.paths) })) },
        buy: L.buy, none: L.none, tile: tiles(page.kitHtml()).licence, text: htmlText(L.body) };
    }
    const id = hash(a); if (!answers.has(id)) answers.set(id, a); out[pkey(prof)] = id;
  }
  Object.assign(P.state.who, { residency:'resident', age:'16_plus', guidance:'non_guided', status:'none' });
  return out;
}

function main(){
  const dir = args.build, out = args.out;
  if (!dir || !out){ console.error('usage: node golden.js --build DIR --out OUT [--from I --to J]'); process.exit(2); }
  fs.mkdirSync(out, { recursive: true });
  const t0 = Date.now();
  const page = load({ data: fs.readFileSync(path.join(dir, 'data.json'), 'utf8'), cases: fs.readFileSync(path.join(dir, 'cases.json'), 'utf8') });
  const P = page.P, profiles = PROFILES(P);
  const from = +(args.from || 0), to = Math.min(+(args.to || P.WATERS.length), P.WATERS.length);
  const sfx = args.from != null ? `.${from}-${to}` : '';
  const done = [];
  const W = name => { const g = zlib.createGzip({ level: 6 }), f = fs.createWriteStream(path.join(out, name.replace('.jsonl', sfx + '.jsonl.gz')));
    g.pipe(f); done.push(new Promise(r => f.on('finish', r))); return g; };
  const fCard = W('card.jsonl'), fLad = W('ladder.jsonl'), fGear = W('gear.jsonl'), fLic = W('licence.jsonl');
  const answers = new Map(), n = { waters:0, parts:0, dates:0, card:0, ladder:0, gear:0, licence:0, fish_asked:0 };
  for (let wi = from; wi < to; wi++){
    const water = P.WATERS[wi]; page.pickWater(wi); n.waters++;
    const labels = P.partLabels(water), groups = P.partGroups(water);
    for (let pi = 0; pi < water.parts.length; pi++){
      page.pickPart(pi); n.parts++;
      const g = P.groupOf(water, pi), part = water.parts[pi];
      const base = { w: water.id, name: water.name, kind: water.kind, wi, p: pi, pk: partKey(part), set: part.set, lset: part.lset,
        label: labels[pi], picker: { closed_all_year: !!g.closed, reachable: g.idx[0] === pi, choice: groups.indexOf(g) } };
      const seen = new Set();
      for (const md of datesFor(P)){
        page.pickDate(Math.floor(md / 100), md % 100); n.dates++;
        const rec = { ...base, md, date: mmdd(md) };
        fCard.write(JSON.stringify(readCard(page, rec)) + '\n'); n.card++; n.fish_asked += P.MODEL.spp.length;
        // Stage 4: one line per segment the page caches the ladder for (PLACE._bp), per fish and origin
        const bp = P.PLACE._bp || [101]; let seg = 0; for (let i = 0; i < bp.length; i++) if (bp[i] <= md) seg = i;
        if (!seen.has(seg)){ seen.add(seg);
          const ctx = P.settle(P.PLACE, md);
          for (const S of P.MODEL.spp) for (const o of ['hatchery', 'wild']){
            const eff = P.ladderAt(ctx, S, o), states = {};
            for (const [k, x] of eff) if (x.state) states[k] = [x.state, x.partly ? 1 : 0, x.by || null];
            fLad.write(JSON.stringify({ w: water.id, p: pi, pk: partKey(part), set: part.set, seg, seg_from: mmdd(bp[seg]), md_read: md, fish: S, origin: o, states }) + '\n'); n.ladder++;
          } }
        fGear.write(JSON.stringify(readGear(page, rec)) + '\n'); n.gear++;
        fLic.write(JSON.stringify({ w: water.id, p: pi, pk: partKey(part), md, date: mmdd(md), profiles: readLicence(page, profiles, answers) }) + '\n'); n.licence++;
      }
    }
    if (args.verbose) console.error(`${wi} ${water.name}: ${water.parts.length} parts, ${((Date.now() - t0) / 1000).toFixed(0)} s`);
  }
  fs.writeFileSync(path.join(out, `licence_answers${sfx}.jsonl.gz`), zlib.gzipSync([...answers].map(([id, a]) => JSON.stringify({ id, ...a })).join('\n') + '\n'));
  const manifest = { what: 'golden outputs of the consumer page v35 (pipeline/deliver/answers/reference)', page_sha256: crypto.createHash('sha256').update(fs.readFileSync(PAGE)).digest('hex'),
    build: path.resolve(dir), bundle: JSON.parse(fs.readFileSync(path.join(dir, 'cases.json'), 'utf8'))._about || null, range: [from, to], counts: { ...n, licence_answers: answers.size, profiles: profiles.length },
    seconds: Math.round((Date.now() - t0) / 1000), node: process.version };
  fs.writeFileSync(path.join(out, `manifest${sfx}.json`), JSON.stringify(manifest, null, 1));
  [fCard, fLad, fGear, fLic].forEach(s => s.end());
  return Promise.all(done).then(() => console.error(JSON.stringify(manifest.counts), manifest.seconds + ' s'));
}
if (require.main === module) main();
module.exports = { parseLadder, ser, datesFor };
