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
//   u16-by-residency, week-parts-decided; follow-up: closed-card-once, nearer-row-tribs-front
//   (round 18) phone-real-height, header-compact, group-head-compact (place-in-header: the action now beside the label);
//   phase 2 (group.tree): tree-folders, own-tier-why-here; card-is-the-caps walks the tree; the bucket-display checks retired;
//   ONLY=18 runs just these
//   (round 19) header-label-two-lines (header-compact: the label two lines, the action on the name's row; ≤ 120 px); phase 2
//   (rows 17): size-tier-box, size-note-on-kind, origin-no-tag (tree-folders: chips "50 cm (20 in)", a size tier's box in
//   place of its guide line; group-head-compact: no tag when header.tag is null); ONLY=19
//   (user round 1, UR1; ONLY=ur1) rule-book-label: every book link and page label reads "Rule book p.N" (C);
//   ur1-tier-words: the tree's tier lines are the data's words whole, the total "per angler", a lifted water's share naming
//   the fish that don't use it, a size tier "Of these, only 1 may be over …" (UR6; was "of the fish in this box"), a count tier "… in all" (A, B, D); ur1-plain-words:
//   the species closure says not even catch and release (F), the record duty "Write each one … right away", the weight
//   "kg (… lb)", Kootenay Lake main body's lines "from shore … from a boat" (G); howtell-current-water: the How to tell
//   sheet sits over the CURRENT water's header and date (E: screen 59 had been shot over Iron River)
//   (user round 2, UR2; ONLY=ur2) ur2-headline-per-angler: the card's big number line says "per angler" (1);
//   ur2-worked-plain: the keep-here lines read "If you keep 3 hatchery rainbow trout: 1 more fish of any kind, …", no
//   arrow (2); ur2-kind-words: "Only 1 of the 2 may be …", "hatchery steelhead only — wild steelhead go back", the
//   presence line "…, so a rainbow trout over 50 cm (20 in) counts as a steelhead.", "After you keep 2, stop fishing for
//   the day" (3, 4, 5); ur2-closed-until: a closed day's gear note "No fishing here until Jul 1." with no licence words
//   (6); ur2-bait-allowed-note: an allowance ("roe is still OK") sits on the Allowed side, never under a banned item (7)
//   (user round 3, UR3; ONLY=ur3) ur3-howtell-steelhead: a hatchery-only steelhead kind row carries "How to tell ›",
//   which opens the How to tell sheet (1); ur3-buffer-feet: the no-fishing buffers "23 m (75 ft)", "100 m (330 ft)" (2);
//   ur3-count-part-of: a count tier in an own tier "… in all (part of the 2)" (3); ur3-lifted-share: "2 a day per angler
//   from this stream — your hatchery rainbow trout count toward the 4, not these 2" (4); ur3-licence-not-resident: the
//   licence dropdowns "If you’re not a B.C. resident · …" (5); ur3-combined-plain: the Kootenay LWA kokanee note
//   "Kokanee from the Lower and Upper West Arms together: no more than 5 a day." (6)
//   (round 20, owner review of v24; ONLY=20) r20-header-is-total: the group's header IS the tree's root tier (rows 21
//   `header.tier`): the root's words with its number big, the ⓘ and its Rule book page in the header block, no total tier
//   drawn, the root's children at the top of the tree with no indent (1); r20-share-inside-total: the share says "Only 2
//   of the 4 may be from this stream", lifted ", not counting hatchery rainbow trout", never "count toward the 4, not
//   these 2" (2); r20-big-numbers: a size box's and a count tier's number as big as the header's, the box's second number
//   ("— 2 if one is a hatchery steelhead") big too (3). tree-folders, size-tier-box, ur1-tier-words, group-head-compact,
//   card-headline and card-cap-once read the root from the header; ur3-lifted-share reads the new words
//   (round 20e, owner: Gear / Methods split, dated notes, sources per item, licence selectors, region vs stream; ONLY=20e)
//   r20e-four-tabs: What you can keep · Gear · Methods · Licence on one row, each its own section, each lands pressed, no dead
//   links; r20e-dated-on-its-line: a dated change is a tinted note joined to the line it changes ("May 1–31: fly fishing only"
//   on Hooks), no floating "Different on other days"; r20e-sources-per-item: every gear / methods item carries its own source
//   line (a gear line's = its parts' cites), none bunched; r20e-licence-selectors: two segmented selectors (age, residency;
//   the data's words), no dropdown or fold, all six pairs show their own answer; r20e-region-vs-stream: a row's streams tag
//   heads the share, not the region's total; the pin bar says the same; r20e-ways-columns: Ways to fish in Bait's two columns
//   (✓ Allowed | ✕ Not allowed). r20b-three-tabs retired (r20e-four-tabs)
//   (round 20i, owner: the UR4 user panel's page-side fixes; ONLY=20i) r20i-size-rule-chip (the chip carries `size_rule`),
//   r20i-other-size-hint ("· size limit" on an Other-fish row whose kinds carry a size rule), r20i-duty-line (a full-weight ink
//   line, the water's name for "this river", the stamp link on the steelhead row), r20i-licence-stamps (no steelhead stamp where
//   steelhead are closed, "(not kokanee)" kept, the Only-if items under their own head), r20i-closed-span ("Closed Jun 1–30"),
//   r20i-all-year-ban (the year-round ban's line), r20i-glare (--sub ≥ 6.5:1 on the card, "release" bold), r20i-scroll-margins
//   (round 20h, owner: the region total vs the stream share, one surface, pictures without a tile, the pin line's end; ONLY=20h)
//   + the round-20h data on the page: r20h-roe-names-ban (a bait allowance names the ban it is excepted from, the banned row
//   drops "is still OK", the dated line carries nothing of it); r20h-possession-own-example (the possession sheet shows the
//   card's own example(s) and doubles line, never the generic 5 → 10 beside them, the term line not repeating the rule)
//   r20h-one-surface: inside a keep card and the pin bar nothing band-sized (> 200 × 40 px) paints a background but the card's;
//   r20h-picture-surface: a fish picture and its tile paint no background of their own, light and dark; r20h-pin-drops: a pinned
//   tier line ends once the next section's head is in view (the own rainbow row never under "from streams"), and stands while
//   that head is below the fold; r20h-total-relation: a region total over a stream share shows both numbers in the group head
//   (form a: the share big + "of the …" note; form b: the total big + "↳ N of them from streams")
//   (round 20g, owner: colours with one meaning each, one future-change pattern, the ways list, possession; ONLY=20g)
//   r20g-palette: every text colour on a tab is a token (ink, sub, accent, open, closed, restricted, on-accent), status
//   colours only on status, every number in ink, one grouping tint, at most 6 / 5 / 5 / 4 distinct text colours on Keep /
//   Gear / Methods / Licence, light and dark; r20g-changes-line: a rule that differs on other dates is one quiet .chg line
//   under the value (Now / Next chip, dates, the data's words, "in force today" only when it is), before the item's source
//   line, never tinted / barred / linked, nothing else dated; r20g-possession-fold: the possession sheet is one plain line,
//   the example, the book's words in a fold closed by default, then the source line
//   (round 20f, owner: thumbnails in one place, tier headers without the left bar, "from streams"; ONLY=20f) r20f-pictures-first:
//   every kind row's picture strip right after the title + chip, before any note, no caption over two lines, no picture under
//   80 px; r20f-headers: no left bar on a tier header or kicker, headers and rows on the card's text edge, the size band tinted;
//   r20f-region-words (rows 23): the Vedder share "Only 2 of the 4 may be from streams…", the pin "↳ 2 of them from streams"
//   (round 20d, owner: stacked pinned headers instead of nesting, clearer grouping; in the 20c block, ONLY=20c)
//   r20d-pinned-stack: the pin bar under the header carries the chain of sections the reader is inside, one short line each
//   ("Trout and char · 4 a day across Region 2" / "↳ 2 of them from this stream" / "↳ of those, 1 over 50 cm (…)"), ≤ 90 px,
//   the row below; r20d-flat: no bordered section, no rail, one text edge for headers and rows, a size section a tinted band;
//   r20d-compact-head: the group head two lines, the total on one. size-tier-box takes the band; tree-folders wants no indent
//   (round 20c, owner: a format audit, context in every screenful; ONLY=20c) r20c-pinned-context (retired in 20d): scrolled deep into a group
//   at phone size, the group's name + total line sit under the header, a top-level tier line under it, a size box's head under
//   that, the row below (position:sticky, measured offsets); r20c-style-tokens: on the Keep tab at most 4 font sizes beside the
//   strip's axis, 4 weights, one link colour, no underlined link, ALL-CAPS only on the section kickers, every hit area ≥ 32 px.
//   group-head-compact reads the two header blocks (.ghd.gpin + .ghd.gsub); tree-folders takes a tinted chip as prominent
//   (round UR6, owner: the last prototype fixes before porting; ONLY=ur6) ur6-size-extent, ur6-size-words, ur6-pin-extent,
//   ur6-pin-last-row, ur6-status-key, ur6-paper-keepable, ur6-source-name, ur6-own-relation: see roundUr6
//   (round UR7, owner: the final prototype round before the port; ONLY=ur7) ur7-share-said-once, ur7-worked-here, ur7-kind-part-of-share,
//   ur7-size-head-colour, ur7-stamps-on-the-fish, ur7-stamp-link-lands: see roundUr7
//   (round 20b, owner: fewer words, real visuals, split tabs; ONLY=20b) r20b-three-tabs: What you can keep · Gear · Licence
//   on one row, each its own section (#gear: Gear & methods + Reference; #licence: the licence alone), each tab lands on
//   its section and reads pressed, sec=licence lands there, no words of the old combined tab, no dead [data-sec]/[data-jump];
//   r20b-number-once: a number never twice on a tier and its only kind (rows 22 `folded`: the own tier names the fish, the
//   kind row draws no sentence); r20b-book-once: a Rule book page once per branch; r20b-worked-ends: the keep-N lines are the
//   two ends (Vedder 4 and 2); r20b-howtell-inline: a kind's sentence with "How to tell ›" keeps one line spacing.
//   own-tier-why-here no longer wants "Replaces …" in the sub-line (C2)
//   (round 17) no-quota-table, kind-number-always, size-cap-line, within-shared-no-twice, bull-trout-row, card-is-the-caps
//   (internal test over group.table / caps / oracle.json; table-is-the-data, checker-is-the-caps, check-my-catch and size-group-box retired)
//   (round 16) gear-lines, worked-last; phase 2: (retired in round 17) rule-titles, inherited-caution, restates-one-line (table-mock retired; gear-sentence / gear-sentence-plain now read the labelled lines; no-bare-zero
//   no longer holds How it adds up before the not-kept lines)
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

// round 17 (owner): a size cap is a compact line ([data-sgroup], no pictures) naming the fish it binds and its number, over
// its size; szFirst = the first kind row ([data-sp]) of a bucket the cap covers, which the line must stand right before
const SZH = `var szShown = (el, z) => { const t = (el?.textContent || '').replace(/\\s+/g, ' ').trim().toLowerCase(), who = z.says.split(': ')[0];
  return !!el && !el.querySelector('img') && new RegExp('\\\\b' + z.n + '\\\\b').test(t) && t.includes('over ' + z.over_cm + ' cm') && who.split(/, | or /).every(nm => t.includes(nm.toLowerCase())); };
var szFirst = (sec, z) => [...(sec?.querySelectorAll('.krow[data-sp]') || [])].find(r => r.dataset.sp.split(' ').some(c => z.species.includes(c)));`;

// 1. zero hand-written facts
const src = fs.readFileSync(PAGE, 'utf8');
const hand = (src.match(/data-src="design"/g) || []).length, pend = (src.match(/pending/gi) || []).length;
check('no-hand-facts', !hand && !pend, `${hand} data-src="design", ${pend} "pending" in ${path.basename(PAGE)}`);
// ONLY=18: just the round 18 block (iterating on it); the full suite runs everything once
// round 20 (rows 21 `group.header.tier`): the tree's root tier is drawn in the group's header block, its children are the
// top of `.qtree`; before, the root was the first `.qtree` child. Every tree check walks from here.
const TOPS = `var topsOf = (g, art) => { const q = art.querySelector(':scope > .qtree'), els = q ? [...q.children] : [], root = [].concat(g.tree || [])[0];
  return root && g.header && g.header.tier === root.id ? {hdr: art.querySelector(':scope > .ghd [data-root-tier]'), root, nodes: root.children || [], els} : {hdr: null, root: null, nodes: [].concat(g.tree || []), els}; };
var hdrWords = el => { if (!el) return ''; const c = el.cloneNode(true); c.querySelectorAll('.tbk, .gl, .glk, .ghr, button').forEach(x => x.remove()); return (c.textContent || '').replace(/\\s+/g, ' ').replace(/ ⓘ$/, '').trim(); };`;
if (process.env.ONLY === '20') { await round20(); const f = results.filter(x => !x[1]).length; console.log(`${results.length - f} passed, ${f} failed`); process.exit(f ? 1 : 0); }
if (process.env.ONLY === '20b') { await round20b(); const f = results.filter(x => !x[1]).length; console.log(`${results.length - f} passed, ${f} failed`); process.exit(f ? 1 : 0); }
if (process.env.ONLY === '20e') { await round20e(); const f = results.filter(x => !x[1]).length; console.log(`${results.length - f} passed, ${f} failed`); process.exit(f ? 1 : 0); }
if (process.env.ONLY === '20c') { await round20c(); const f = results.filter(x => !x[1]).length; console.log(`${results.length - f} passed, ${f} failed`); process.exit(f ? 1 : 0); }
if (process.env.ONLY === 'int1') { await roundInt1(); const f = results.filter(x => !x[1]).length; console.log(`${results.length - f} passed, ${f} failed`); process.exit(f ? 1 : 0); }
if (process.env.ONLY === 'ur7') { await roundUr7(); const f = results.filter(x => !x[1]).length; console.log(`${results.length - f} passed, ${f} failed`); process.exit(f ? 1 : 0); }
if (process.env.ONLY === 'ur6') { await roundUr6(); const f = results.filter(x => !x[1]).length; console.log(`${results.length - f} passed, ${f} failed`); process.exit(f ? 1 : 0); }
if (process.env.ONLY === 'ur5') { await roundUr5(); const f = results.filter(x => !x[1]).length; console.log(`${results.length - f} passed, ${f} failed`); process.exit(f ? 1 : 0); }
if (process.env.ONLY === '20i') { await round20i(); const f = results.filter(x => !x[1]).length; console.log(`${results.length - f} passed, ${f} failed`); process.exit(f ? 1 : 0); }
if (process.env.ONLY === '20h') { await round20h(); const f = results.filter(x => !x[1]).length; console.log(`${results.length - f} passed, ${f} failed`); process.exit(f ? 1 : 0); }
if (process.env.ONLY === '20g') { await round20g(); const f = results.filter(x => !x[1]).length; console.log(`${results.length - f} passed, ${f} failed`); process.exit(f ? 1 : 0); }
if (process.env.ONLY === '20f') { await round20f(); const f = results.filter(x => !x[1]).length; console.log(`${results.length - f} passed, ${f} failed`); process.exit(f ? 1 : 0); }
if (process.env.ONLY === 'ur3') { await roundUr3(); const f = results.filter(x => !x[1]).length; console.log(`${results.length - f} passed, ${f} failed`); process.exit(f ? 1 : 0); }
if (process.env.ONLY === 'ur2') { await roundUr2(); const f = results.filter(x => !x[1]).length; console.log(`${results.length - f} passed, ${f} failed`); process.exit(f ? 1 : 0); }
if (process.env.ONLY === 'ur1') { await roundUr1(); const f = results.filter(x => !x[1]).length; console.log(`${results.length - f} passed, ${f} failed`); process.exit(f ? 1 : 0); }
if (process.env.ONLY === '19') { await round19(); const f = results.filter(x => !x[1]).length; console.log(`${results.length - f} passed, ${f} failed`); process.exit(f ? 1 : 0); }
if (process.env.ONLY === '18') { await round18(); const f = results.filter(x => !x[1]).length; console.log(`${results.length - f} passed, ${f} failed`); process.exit(f ? 1 : 0); }

// 2–4. on the Vedder, Oct 10, with the Line & hooks topic showing (v10 opens it with kit=line; v11 shows everything)
await page('w=vedder&d=1010&sec=gear&kit=line', async b => {
  const r = JSON.parse(await b.evaluate(`JSON.stringify((() => {
    const gear = document.getElementById('gear'), first = document.querySelector('.row.ledger');
    const fish = [...document.querySelectorAll('li, p, div, button')].filter(e => /fishway/i.test(e.textContent) && ![...e.children].some(c => /fishway/i.test(c.textContent)));
    const pos = e => first ? !!(e.compareDocumentPosition(first) & Node.DOCUMENT_POSITION_FOLLOWING) : false;
    // round 20e: Gear, Methods and Licence are three tabs; gear and methods items carry their own source line (r20e-sources-per-item),
    // so no section-end Sources line there; the licence keeps exactly ONE
    const secs = [...document.querySelectorAll('#gear .gpart, #methods .gpart, #licence .gpart')];
    return {
      fishway: fish.length, inGear: fish.filter(e => gear && gear.contains(e)).length, beforeFish: fish.filter(pos).length,
      chips: gear ? gear.querySelectorAll('.gsrc, .srcbtn:not([hidden]), [data-gearsrc]').length : -1,
      sections: secs.length, lines: secs.map(s => s.querySelectorAll('.gsources, [data-isrc-lic]').length - (s.id === 'g-licence' ? 1 : 0)),
      barbless: gear ? [...gear.querySelectorAll('.kitnote, p, div')].filter(e => /^Single barbless hook\\s*ⓘ?$/.test(e.textContent.trim())).length : -1,
    }; })())`));
  check('one-sources-line', r.chips === 0 && r.sections > 0 && r.lines.every(n => n === 0),
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
    // round 20b: the licence is its own section after #gear (the tab split), so the order is read across the whole scroller
    const g = document.getElementById('gear'), secs = [...document.querySelectorAll('#sc .gpart')], st = SNAP.states[sid()], sm = st.summary;
    const norm = t => (t || '').replace(/\\s+/g, ' ').trim();
    // round 16 (5): the sentence is drawn as labelled lines ([data-gline], gear-lines checks them); the box's lines are the shown text
    const box = g.querySelector('[data-summary]'), say = box?.querySelector('.glines');
    const pages = new Set((sm?.cites || []).concat((sm?.changes || []).flatMap(c => c.cites || [])).flatMap(k => (SNAP.ruleInfo[k]?.book || []).map(b => b[0])));   // round 20g: the change's rule joins its line's source line
    const chips = box ? [...box.querySelectorAll('[data-change]')].map(e => norm(e.textContent)) : [];
    const ref = document.querySelector('#methods details[data-reference]'), M = document.getElementById('methods');   // round 20e: it closes the Methods tab
    return {sm: !!sm, says: sm?.says || null, kind: st.kind, shown: say ? norm(say.textContent) : null, times: sm ? (norm(g.textContent).split(norm(sm.says)).length - 1) : 0,
      // round 20e: each line's own source line carries its pages; the box shows each of the sentence's pages
      pages: pages.size, books: box ? new Set([...box.querySelectorAll('[data-isrc] a.book')].map(a => (a.textContent.match(/p\\.(\\d+)/) || [])[1])).size : 0,
      changes: (sm?.changes || []).map(c => [c.when, c.says]), chips,
      lines: box ? box.querySelectorAll('[data-gline]').length : 0,
      terms: (sm?.terms || []), tips: box ? [...box.querySelectorAll('[data-gl]')].flatMap(e => e.dataset.gl.split(',')) : [],
      icons: g.querySelectorAll('svg.gi, .gi').length, bait: !!g.querySelector('[data-k="bait"]'), ways: !!document.querySelector('#methods [data-k="methods"]'),
      secs: secs.map(s => s.id), last: secs.length ? secs[secs.length - 1].id : null,
      always: secs.filter(s => /^always$/i.test(norm(s.querySelector('h3')?.textContent))).length,
      // round 20b: the Reference fold closes the GEAR tab (after that tab's last section); the licence is its own tab after it
      ref: ref ? {open: ref.open, after: (gs => !!gs.length && !!(gs[gs.length - 1].compareDocumentPosition(ref) & Node.DOCUMENT_POSITION_FOLLOWING))([...M.querySelectorAll('.gpart')]), n: ref.querySelectorAll('[data-rule], .factbtn').length} : null,
      hasAlways: !!(st.panes && st.panes.always)}; })())`));
});
const sbad = Object.entries(gear).filter(([, r]) => !(r.sm && r.shown && r.lines && r.books === r.pages
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
      // round 12: the label is a line of its own under the name; round 19 (owner 4): the action OFF the label's line, on the
      // name's row at its right end (header-label-two-lines) — measured before the map opens, which re-renders the header
      const own = !txt || (() => { const a = hdr.querySelector('.pgo') || act, w = hdr.querySelector('.ttl b').getBoundingClientRect(), ar = a?.getBoundingClientRect();
        return Math.abs(r.left - w.left) < 2 && (!ar || (!(ar.top < r.bottom && ar.bottom > r.top) && ar.top < w.bottom && ar.bottom > w.top && hr.right - ar.right < 20)); })();
      let title = ''; if (hasMap && act) { act.click(); await new Promise(r => setTimeout(r, 300)); title = norm(document.querySelector('#wmap .mh')?.textContent); }
      return JSON.stringify({hash: location.hash.slice(8), kind: st.kind, name: norm(hdr.querySelector('.ttl b')?.textContent) === st.name, txt, labels,
        // round 12 (owner 1 + 4): no kind ("stream ·"): the place is the part's label itself, nothing for a whole water
        label: txt ? labels.includes(txt) : !labels.length || labels.includes(st.name), lead: txt !== st.kind && !txt.startsWith(st.kind + ' · '),
        own,
        count: /\\b\\d+ sections?\\b/.test(norm(hdr.textContent)), visible: !!r && r.height > 0 && r.top >= hr.top && r.bottom <= hr.bottom,
        act: hasMap ? !!act && /view on map/i.test(act.getAttribute('aria-label') || act.textContent) && /where these rules apply/i.test(title) : many ? !!act && /other parts/i.test(act.getAttribute('aria-label') || act.textContent) : true,
        date: !!hdr.querySelector('[data-open="date"]'), mapbtn: document.querySelectorAll('.mapbtn').length,
        row: [...document.querySelectorAll('#sc > .where, [data-map], [data-open="parts"]')].filter(e => !document.getElementById('hdr').contains(e) && !document.getElementById('wmap').contains(e) && !document.getElementById('sheet').contains(e) && !e.closest('.poster')).length,
        strip: document.getElementById('sc').querySelectorAll('.parts, [data-part]').length}); })()`)));
  }
});
const hbad = HS.filter(h => !(h.name && h.lead && h.label && h.own && !h.count && (h.visible || !h.txt) && h.act && h.date && !h.mapbtn && !h.row));
check('place-in-header', HS.length > 40 && !hbad.length, `${hbad.length} of ${HS.length} fresh loads: ` + hbad.slice(0, 3).map(h => `${h.hash}: place ${JSON.stringify(h.txt.slice(0, 90))} (labels ${JSON.stringify(h.labels)})${h.name ? '' : ', no name'}${h.lead ? '' : ', still leads with the kind'}${h.label ? '' : ', not the bare label'}${h.own ? '' : ', not under the name with the action beside it'}${h.count ? ', a section count' : ''}${h.visible ? '' : ', not visible in the header'}${h.act ? '' : ', no map / parts action'}${h.date ? '' : ', no date control'}${h.mapbtn ? ', a MAP button' : ''}${h.row ? ', a place row outside the header' : ''}`).join('; '));
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
      // round 20 (owner 1): the header is the tree's root tier, so the big number is the root's (the total), read from .ghn
      const rootN = art.querySelector('.rhead .num[data-root-tier]') ? [].concat(g.tree)[0].n : null;
      const num = (art.querySelector('.rhead .num .ghn')?.textContent || art.querySelector('.rhead .num')?.firstChild?.textContent || '').trim();
      const lifts = (g.lifts || []).map(l => { const el = [...art.querySelectorAll('p, li, div')].reverse().find(e => norm(e.textContent).includes(norm(l.says).slice(0, 40)));
        // every lifting rule's page once (answers rows v9: each[] parts; older shape: the lift itself)
        const want = new Set((l.each && l.each.length ? l.each : [l]).flatMap(e => (SNAP.ruleInfo[(st.ruleKey || {})[e.by]]?.book || []).map(b => b[0])));
        return el ? [(norm(el.textContent).match(/Rule book p\\.\\d+/g) || []).length, Math.max(1, want.size)] : [-1, 1]; });
      const TK = g => [].concat(g?.tree || []).flatMap(function f(t) { return t.node === 'kind' ? [t] : (t.children || []).flatMap(f); });   /* round 18: the tree's kinds */
      const kinds = TK(g).map(k => { const row = [...art.querySelectorAll('.krow.kt')].find(e => e.dataset.kind === k.name);
        return [k.name, k.species.length, row ? row.querySelectorAll('img, svg').length : -1]; });
      const lines = [...art.querySelectorAll('p:not(.ktags):not(.kmeta), li')].map(e => norm(e.textContent)).filter(Boolean);   /* round 18: a size chip ("any size") repeats by design; round 17: a row's shares / within tags repeat by design */
      const dup = lines.filter((t, i) => lines.indexOf(t) !== i);
      out.push({head: rootN != null ? rootN : g.headline.n, hdrRoot: rootN != null, num, total: g.total, totalLine: !!g.total_line, lifts, kinds, dup});
    }); return out; })())`));
  for (const g of r) {
    if (g.missing) { card.headline.push([name, 'a state', 'not on this page']); continue; }
    if (g.head != null) card.headline.push([name, g.head, g.num]);
    g.lifts.forEach(n => card.book.push([name, n]));
    g.kinds.forEach(([k, want, got]) => card.pics.push([name, k, want, got]));
    card.once.push([name, g.dup, !g.hdrRoot && g.totalLine && g.total != null && g.head !== g.total && +g.num === g.total]);   // round 20: with the root as header, the big number IS the total
  }
});
const badH = card.headline.filter(([, n, num]) => String(n) !== num);
check('card-headline', card.headline.length > 0 && !badH.length, `${badH.length} of ${card.headline.length} groups: e.g. ${JSON.stringify(badH[0])} [water, headline.n, shown]`);
// card-one-book-per-lift retired in round 13 (D): the lift box is gone; no-lift-box / bucket-replaces below
const badP = card.pics.filter(([, , want, got]) => want !== got);
check('card-picture-per-species', card.pics.length > 0 && !badP.length, `${badP.length} of ${card.pics.length} kind rows: e.g. ${JSON.stringify(badP[0])} [water, kind, species, pictures]`);
const badO = card.once.filter(([, dup, total]) => dup.length || total);
check('card-cap-once', !badO.length, `${badO.length} groups: e.g. ${JSON.stringify(badO[0])} [water, repeated lines, big number = the total line's cap]`);

// round 11 bucket-number-big / bucket-note-once / kind-row-one-line retired in round 18 (owner 1, 7): the card is the tree
// (group.tree); its numbers, sentences and sizes are tree-folders' (round 18 phase 2)

// round 11 (A3): a lift another water's row carries here (display `inherited`) is a visible line on the Keep side, by the
// strip (before the quota tables), with the lifting rule's book page; none where the data has none
const inh = [];
for (const hash of ['w=iron&d=720', 'w=iron&d=1010', 'w=vedder&d=1010']) await page(hash, async b => {
  inh.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { if (!WATERS[S.w] || !WATERS[S.w].snap[S.d]) return {hash: location.hash, missing: true};
    const st = SNAP.states[sid()], norm = t => (t || '').replace(/\\s+/g, ' ').trim(), keep = document.getElementById('keep'), first = keep.querySelector('article.row.ledger');
    const els = [...keep.querySelectorAll('[data-inherited]')];
    return {hash: location.hash, want: (st.inherited || []).map(x => [x.says, (SNAP.ruleInfo[x.by]?.book || []).map(b => 'Rule book p.' + b[0])]),
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
for (const k of ['bait_ban', 'single_barbless_hook', 'bait_ban,single_barbless_hook', 'possession_quota']) await page(`w=vedder&d=1010&sheet=gl:${k}`, async b => {   // glossary 3: daily_quota's term is gone, possession_quota is the folded one
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
// ✕ not), Ways to fish inline (each allowed way one line, the ones that are not one line); round 20e (owner 4): Ways to fish in
// Bait's two columns too
const V13H = {'w=vedder&d=1010': 361, 'w=klwa&d=1010': 390, 'w=cowichan&d=1010&p=1': 352, 'w=vedder&d=515': 291};
// round 20e (owner 1, 4): Ways to fish is its own tab (Methods) in Bait's two columns, so the v13 bound holds Bait alone (its
// height on regs v28, + 6 px per dated note for the tinted note's padding) and the Methods tab's items fit one screenful
const V28BAIT = {'w=vedder&d=1010': 175, 'w=klwa&d=1010': 148, 'w=cowichan&d=1010&p=1': 211, 'w=vedder&d=515': 161};
const BW = [];
for (const hash of Object.keys(V13H)) await page(hash, async b => {
  BW.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const norm = t => (t || '').replace(/\\s+/g, ' ').trim(), st = ST(), out = {hash: ${JSON.stringify(hash)}, h: 0, cells: []};
    const P = h => { const d = document.createElement('div'); d.innerHTML = h || ''; return d; };
    const want = {bait: [...P(st.panes.bait).querySelectorAll('.bait')].map(e => [norm(e.querySelector('b')?.textContent), e.classList.contains('ok')]),
      methods: [...P(st.panes.ways).querySelectorAll('.wrow b')].map(e => [norm(e.textContent), true]).concat([...P(st.panes.ways).querySelectorAll('.nochip')].map(e => [norm(e.textContent.replace(/^✕/, '')), false]))};
    for (const k of ['bait', 'methods']) { const c = document.querySelector('[data-k="' + k + '"]'); if (!c) { out.cells.push({k, none: true}); continue; }
      // round 20e (owner 3): each item's own source line is new room the owner asked for; it is not counted against v13
      const hh = c.getBoundingClientRect().height - [...c.querySelectorAll(':scope > [data-isrc]')].reduce((a, s) => a + s.getBoundingClientRect().height + parseFloat(getComputedStyle(s).marginTop), 0);
      if (k === 'bait') { out.bait = Math.round(hh); out.notes = c.querySelectorAll('[data-dated]').length;
        /* round 20g (owner 3): ONE stacked list, so the v28 two-column height no longer bounds it; compact now means no slack — every row as tall as its
           text lines (name + its small condition) and no gap over 8 px between rows or under a group's header */
        const lis = [...c.querySelectorAll('.yn li')]; out.rows = lis.length; out.slack = 0; out.gaps = 0;
        lis.forEach((li, i) => { const r = li.getBoundingClientRect(), cs_ = getComputedStyle(li), lh = parseFloat(cs_.lineHeight) || 18, sm = li.querySelector('small');
          const lines = Math.max(1, Math.round((r.height - (sm ? sm.getBoundingClientRect().height : 0) - parseFloat(cs_.paddingTop) - parseFloat(cs_.paddingBottom)) / lh));
          const want = lines * lh + (sm ? sm.getBoundingClientRect().height : 0) + parseFloat(cs_.paddingTop) + parseFloat(cs_.paddingBottom); if (r.height - want > 2) out.slack += Math.round(r.height - want);
          const prev = li.previousElementSibling || li.parentElement.previousElementSibling; if (prev) { const g = r.top - prev.getBoundingClientRect().bottom; if (g > 8) out.gaps += Math.round(g); } }); }
      else { const M = document.getElementById('methods'), sc = document.getElementById('sc'); out.meth = Math.round(M.getBoundingClientRect().height); out.room = sc.clientHeight - document.getElementById('hdr').offsetHeight; }
      out.h += hh;
      const ok = c.querySelector('[data-col="ok"]'), no = c.querySelector('[data-col="no"]');
      const side = ok && no ? no.getBoundingClientRect().top >= ok.getBoundingClientRect().bottom - 1 : !!(ok || no);   /* round 20g: one stacked list */
      const miss = want[k].filter(([n, y]) => !norm((y ? ok : no)?.textContent).includes(n));
      out.cells.push({k, side, miss: miss.map(x => x[0])}); }
    out.h = Math.round(out.h); return out; })())`)));
});
const bwb = BW.filter(x => x.slack || x.gaps || !x.rows || x.meth > x.room || x.cells.some(c => c.none || !c.side || c.miss.length));
check('bait-ways-compact', !bwb.length, bwb.map(x => `${x.hash}: bait ${x.bait} px (v28 two-column ${V28BAIT[x.hash]}; ${x.rows} rows, ${x.slack} px slack, ${x.gaps} px gaps); Methods ${x.meth} px in ${x.room}; ${JSON.stringify(x.cells)}`).join('; '));

// (owner 3) every dated change in Gear & methods (the sentence's other days, Bait's bait ban) is ONE row component: the
// date label, then the change, the dates in one column and the changes in another, in the data's words
const DR = [];
for (const hash of ['w=vedder&d=1010', 'w=cowichan&d=1010&p=1', 'w=vedder&d=515']) await page(hash, async b => {
  DR.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const norm = t => (t || '').replace(/\\s+/g, ' ').trim(), g = document.getElementById('g-gear'), st = ST();
    // round 20e (owner 2): the component is a tinted note joined to its line — the date in bold, then the change ("May 1–31: fly
    // fishing only"); every note starts on the same left edge
    const rows = [...g.querySelectorAll('[data-dated]')].map(e => { const w = e.querySelector('b'), l = Math.round(e.getBoundingClientRect().left), cl_ = e.cloneNode(true); cl_.querySelectorAll('.chgi').forEach(x => x.remove());   /* round 20g: the Now / Next chip is not a word; the dates sit in the line's span */
      return {w: norm(w?.textContent).replace(/:$/, ''), c: norm(cl_.textContent).slice(norm(w?.textContent).length).trim(), wl: w ? l : -1, cl: l}; });
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
  W2.push(JSON.parse(await b.evaluate(`${SZH} JSON.stringify((() => { if (!WATERS[S.w] || !WATERS[S.w].snap[S.d]) return {name: ${JSON.stringify(name)}, missing: true};
    const st = SNAP.states[sid()], norm = t => (t || '').replace(/\\s+/g, ' ').trim(), out = {name: ${JSON.stringify(name)}, fe: /fished elsewhere today/i.test(document.getElementById('keep').textContent), g: []};
    document.querySelectorAll('#keep article.row.ledger').forEach(art => { const raw = st.rows[+(art.dataset.row || 'r0').slice(1)] || {}, g = raw.group; if (!g || !g.headline) return;
      const lists = [...art.querySelectorAll('.worked')], wl = lists[0], wt = norm(wl?.textContent), at = norm(art.textContent);
      const el = raw.elsewhere?.says || '', worked = g.worked || [];
      const secs = [...art.querySelectorAll('section.bkt')];
      out.g.push({lists: lists.length, want: !!(el || worked.length), ask: (raw.elsewhere?.ask || []).length,
        el: !el || (wt.includes(norm(el)) && at.split(norm(el)).length === 2),
        lines: worked.map(w => wt.includes(norm(w.q)) && wt.includes(norm(w.a)) && at.split(norm(w.a)).length === 2 && (w.at !== 'here') === !wt.includes(norm(w.q) + ':')),
        order: (g.buckets || []).map((bk, i) => !!secs[i] && bk.kinds.every(k => norm(secs[i].textContent).includes(norm(k.name)))),
        sizes: (g.buckets || []).flatMap((bk, i) => bk.kinds.map(k => !k.detail || [...(secs[i]?.querySelectorAll('.krow') || [])].some(r => norm(r.textContent).includes(norm(k.name)) && norm(r.querySelector('.kdet')?.textContent) === norm(k.detail)))),
        caps: (g.buckets || []).reduce((t, bk) => t + (bk.notes || []).length + (bk.sizes || []).length + (bk.groups || []).length + (bk.between ? 1 : 0), 0),
        notes: (g.buckets || []).flatMap((bk, i) => [...(bk.sizes || []).map((z, j) => szShown(secs[i]?.querySelectorAll('[data-sgroup]')[j], z)), ...(bk.notes || []).map(n => at.split(norm(n.says)).length === 2 && norm(secs[i]?.textContent).includes(norm(n.says))
          && (n.then || []).every(t => norm(n.says).includes(String(t.over_cm)) || [...secs[i].querySelectorAll('[data-then]')].some(e => norm(e.textContent).includes(String(t.over_cm)))))]),
        first: (g.buckets || [])[0]?.why || null, whys: (g.buckets || []).map(bk => bk.why)}); });
    return out; })())`)));
});
const wb = W2.filter(x => x.missing || x.fe || x.g.some(g => g.ask || (g.want ? g.lists !== 1 : g.lists) || !g.el || !g.lines.every(Boolean)));
check('one-worked-list', W2.some(x => x.g && x.g.some(g => g.lines.length > 3)) && !wb.length, wb.map(x => `${x.name}: ${x.missing ? 'not in the snapshot' : `${x.fe ? '"Fished elsewhere today?" still shown; ' : ''}${JSON.stringify(x.g.map(g => [g.lists, g.ask, g.el, g.lines]))}`}`).join('; '));
const ob = W2.filter(x => !x.missing && x.g.some(g => !g.order.every(Boolean) || (g.whys.includes('rule') && g.first !== 'rule') || !g.sizes.every(Boolean)));
// buckets-own-first-sizes-whole: retired in round 18 (the card is group.tree; the bucket display is gone) -> tree-folders
const nb = W2.filter(x => !x.missing && x.g.some(g => !g.notes.every(Boolean)));
// every-cap-once: retired in round 18 (the card is group.tree; the bucket display is gone) -> tree-folders
// gear 8: the sentence as the data says it (no em dash)
// round 16 (5): the sentence is labelled lines now (gear-lines); no em dash in them
const gd = Object.entries(gear).filter(([, r]) => !r.shown || /—/.test(r.shown));
check('gear-sentence-plain', !gd.length, gd.map(([w, r]) => `${w}: ${JSON.stringify(r.shown)}`).join('; '));

// ---------- round 13 ----------
// (H) the header keeps ONE shape: the same height, words and classes at scroll 0 and scrolled; "View on map ›" shown in
// both on a water with a map; the two section tabs slim (one row no taller than 40 px)
const H13 = [];
for (const hash of ['w=vedder&d=1010', 'w=chilliwack&d=1010&p=0', 'w=x14p&d=1010&p=1', 'w=cowichan&d=1010&p=1']) await page(hash, async b => {
  H13.push(JSON.parse(await b.evaluate(`(async () => { const sc = document.getElementById('sc'), raf = () => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));
    const look = () => { const h = document.getElementById('hdr'), go = [...h.querySelectorAll('.pgo')].find(e => /View on map/.test(e.getAttribute('aria-label') || e.textContent));
      const tl = h.querySelector('[role=tablist]');
      return {h: Math.round(h.getBoundingClientRect().height), cls: h.className, text: h.textContent.replace(/\\s+/g, ' ').trim(),
        map: !!(go && go.offsetHeight > 0 && getComputedStyle(go).display !== 'none'), tabs: tl ? Math.round(tl.getBoundingClientRect().height) : -1}; };
    await raf(); const a = look(); sc.scrollTop = 900; sc.dispatchEvent(new Event('scroll')); await raf(); const z = look();
    const w = WATERS[S.w]; return JSON.stringify({hash: location.hash.slice(8), hasMap: !!(w.map && WM.waters[w.map]), a, z, scrolled: sc.scrollTop}); })()`)));
});
const hb13 = H13.filter(x => x.scrolled < 300 || x.a.h !== x.z.h || x.a.cls !== x.z.cls || x.a.text !== x.z.text || (x.hasMap && !(x.a.map && x.z.map)) || x.a.tabs > 40 || x.a.tabs < 0);
check('header-one-shape', H13.some(x => x.hasMap) && !hb13.length, hb13.map(x => `${x.hash}: top ${JSON.stringify(x.a)} vs scrolled ${JSON.stringify(x.z)}`).join('; '));

// ---------- user round 3 (UR3, six-angler panel on v23) ----------
async function roundUr3(){
  const norm = 'var norm = t => (t || "").replace(/\\s+/g, " ").trim();';
  const K = {};
  for (const [k, hash] of [['vedder', 'w=vedder&d=1010'], ['bella', 'w=bellacoola&d=501&p=1'], ['lic', 'w=vedder&d=1010&at=%23g-licence'],
                           ['ko', `w=klwa&d=1010&sheet=${encodeURIComponent('other:klwa_1010|o0')}`]]) await page(hash, async b => {
    K[k] = JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm}
      const tl = t => [...document.querySelectorAll('#keep section.tnode[data-tnode="' + t + '"] > .tier .tl')].map(e => norm([...e.childNodes].filter(n => !(n.nodeType === 1 && (n.matches('.tbk') || n.matches('.gl')))).map(n => n.textContent).join(' ')).replace(/ ⓘ$/, ''));
      const st = document.querySelector('#keep .krow.kt[data-sp~="ST"] .ks');
      return {stks: norm(st?.textContent), stbtn: !!st?.querySelector('button[data-howtell]'),
        nofish: [...document.querySelectorAll('#keep .nofish li')].map(e => norm(e.textContent)),
        share: tl('share'), count: tl('count'),
        res: [...document.querySelectorAll('#g-licence details[data-res] > summary')].map(e => norm(e.textContent)),
        rest: [...document.querySelectorAll('#sheet [data-restates]')].map(e => norm(e.textContent))}; })())`));
    if (k === 'vedder' && K[k].stbtn) K[k].opens = await b.evaluate(`(async () => {
      document.querySelector('#keep .krow.kt[data-sp~="ST"] .ks button[data-howtell]').click(); await new Promise(r => setTimeout(r, 300));
      return /Hatchery or wild/.test(document.querySelector('.shead h2')?.textContent || ''); })()`);
  });
  check('ur3-howtell-steelhead', K.vedder.stbtn && K.vedder.opens === true && /^Up to 2 may be hatchery steelhead only — wild steelhead go back How to tell ›$/.test(K.vedder.stks), JSON.stringify([K.vedder.stks, K.vedder.opens]));
  check('ur3-buffer-feet', K.vedder.nofish.length === 2 && /within 23 m \(75 ft\) downstream/.test(K.vedder.nofish[0]) && /100 m \(330 ft\) radius/.test(K.vedder.nofish[1]), JSON.stringify(K.vedder.nofish));
  check('ur3-count-part-of', K.bella.count.includes('1 cutthroat or rainbow trout in all (part of the 2)'), JSON.stringify(K.bella.count));
  // round 20 (owner 2): the UR3 words read as 4 + 2; the share now sits inside the total, the lifted fish not counted in it
  // UR7 A (rows 29): the lifted fish are said once, by the own tier's relation; the share ("Only 2 of the 4 may be from streams") folds under the head
  check('ur3-lifted-share', K.vedder.share.length === 0, JSON.stringify(K.vedder.share));
  // round 20e (owner 5): ur3-licence-not-resident retired — no dropdowns; the residency choice is named by the data's own options (r20e-licence-selectors)
  check('ur3-combined-plain', K.ko.rest.some(t => /^Kokanee from the Lower and Upper West Arms together: no more than 5 a day\./.test(t)) && !K.ko.rest.some(t => /combined daily quota/i.test(t)), JSON.stringify(K.ko.rest));
}
// ---------- round 20e (owner: Gear / Methods split, dated notes on their line, sources per item, licence selectors, region vs stream) ----------
async function round20e(){
  const norm = 'var norm = t => (t || "").replace(/\\s+/g, " ").trim();';
  const txt = 'var txt = e => { if (!e) return ""; const c = e.cloneNode(true); c.querySelectorAll("button.gl,.glk,[data-nsrc],.chgi").forEach(x => x.remove()); return norm(c.textContent); };';   // round 20g: the changes line's glyph is not a word
  // (1) four tabs: What you can keep · Gear · Methods · Licence on ONE row at 390 px; each its own section (#gear: the gear
  // lines + bait; #methods: ways to fish, boats, the Reference fold; #licence: the licence alone); each tab lands pressed on
  // its section; sec=methods / sec=licence land; no dead [data-sec] / [data-jump]
  const TB = [];
  for (const hash of ['w=vedder&d=1010', 'w=klwa&d=1010', 'w=vedder&d=1010&sec=methods', 'w=klwa&d=1010&sec=licence', 'w=vedder&d=1010&sec=gear']) await page(hash, async b => {
    TB.push(JSON.parse(await b.evaluate(`(async () => { ${norm} const bad = [], R = e => e.getBoundingClientRect();
      const tabs = [...document.querySelectorAll('#hdr .segm[role=tablist] button')], names = tabs.map(t => norm(t.textContent));
      if (JSON.stringify(names) !== '["What you can keep","Gear","Methods","Licence"]') bad.push('tabs ' + JSON.stringify(names));
      if (new Set(tabs.map(t => Math.round(R(t).top))).size !== 1) bad.push('tabs not on one row');
      if (tabs.some(t => R(t).height > 48)) bad.push('a tab wraps');
      if (tabs.some(t => R(t).right > R(document.getElementById('hdr')).right + 0.5)) bad.push('a tab off the right edge');
      const S_ = id => document.getElementById(id), keep = S_('keep'), gear = S_('gear'), meth = S_('methods'), lic = S_('licence');
      if (!keep || !gear || !meth || !lic) bad.push('sections ' + [!!keep, !!gear, !!meth, !!lic].join(' '));
      if (gear && (!gear.querySelector('[data-gline], [data-k="bait"]') || gear.querySelector('[data-k="methods"], [data-k="boats"], #g-always, #g-licence'))) bad.push('#gear holds the wrong items');
      if (meth && (!meth.querySelector('[data-k="methods"]') || meth.querySelector('[data-gline], [data-k="bait"], #g-licence'))) bad.push('#methods holds the wrong items');
      if (meth && document.querySelector('#g-always') && !meth.contains(document.querySelector('#g-always'))) bad.push('the Reference fold is not in Methods');
      if (lic && (!lic.querySelector('#g-licence') || lic.querySelector('[data-gline], [data-k]'))) bad.push('#licence holds other sections');
      document.querySelectorAll('[data-sec]').forEach(e => { if (!S_(e.dataset.sec)) bad.push('dead data-sec ' + e.dataset.sec); });
      document.querySelectorAll('[data-jump]').forEach(e => { if (!S_(e.dataset.jump)) bad.push('dead data-jump ' + e.dataset.jump); });
      const pressed = () => tabs.find(t => t.getAttribute('aria-pressed') === 'true')?.dataset.sec;
      const sc = S_('sc'), hh = S_('hdr').offsetHeight, want = (location.hash.match(/sec=(\\w+)/) || [])[1] || 'keep';
      if (pressed() !== want) bad.push('hash sec=' + want + ' landed on ' + pressed());
      for (const sec of ['gear', 'methods', 'licence', 'keep']) { const tb = tabs.find(t => t.dataset.sec === sec); if (!tb) { bad.push('no tab ' + sec); continue; } tb.click(); await new Promise(r => setTimeout(r, 1000));
        const el = S_(sec), off = sec === 'keep' ? sc.scrollTop : Math.round(el.offsetTop - sc.scrollTop - hh);
        if (pressed() !== sec) bad.push('click ' + sec + ' pressed ' + pressed()); if (sec !== 'keep' && Math.abs(off + 2) > 3 && !(sc.scrollTop + sc.clientHeight >= sc.scrollHeight - 2)) bad.push('click ' + sec + ' lands ' + off + 'px off'); }
      return JSON.stringify({hash: location.hash.slice(8), names, bad}); })()`)));
  });
  const tbb = TB.filter(x => x.bad.length);
  check('r20e-four-tabs', TB.length === 5 && !tbb.length, tbb.map(x => `${x.hash}: ${x.bad.slice(0, 5).join(' | ')}`).join('; '));

  // (2) a dated change is a note JOINED to the line it changes ("May 1–31: fly fishing only" on the Hooks line; on a
  // fly-only day the change back sits on the fly-only line; the bait ban on Bait), in the data's words; no floating
  // "Different on other days" label; every note inside its row, the last thing in it, touching the content above it
  const DN = [], DNS = [['w=vedder&d=1010', [['hook', 'May 1–31: fly fishing only']], [['bait', 'May 1–31: bait ban']]],
    ['w=vedder&d=515', [['mode', 'Jul 1–Apr 30: one hook, lure or fly and weight on the line up to 1 kg (2.2 lb)']], []],
    ['w=cowichan&d=1010&p=1', [['mode', 'Nov 16–Jul 3: one hook, lure or fly and weight on the line up to 1 kg (2.2 lb)']], []]];   // gear 12: a change's span skips the closed days
  for (const [hash, lines, cells] of DNS) await page(hash + '&sec=gear', async b => {
    DN.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} ${txt} const bad = [], R = e => e.getBoundingClientRect(), st = ST();
      const g = document.getElementById('gear') || document.getElementById('sc');
      if (/Different on other days/.test(g.textContent)) bad.push('a floating "Different on other days"');
      const want = (st.summary?.changes || []).map(c => c.when + ': ' + c.says);
      const notes = [...g.querySelectorAll('[data-change]')].map(n => ({t: txt(n), row: n.closest('[data-gline]')?.dataset.gline || n.closest('[data-k]')?.dataset.k || '', n}));
      want.forEach(w => { if (!notes.some(x => x.t === w)) bad.push('change not said whole: ' + w); });
      ${JSON.stringify(lines)}.forEach(([k, w]) => { if (!notes.some(x => x.row === k && x.t === w)) bad.push('no note on ' + k + ': ' + w + ' — got ' + JSON.stringify(notes.map(x => [x.row, x.t]))); });
      ${JSON.stringify(cells)}.forEach(([k, w]) => { const ns = [...g.querySelectorAll('[data-k="' + k + '"] [data-dated]')].map(txt); if (!ns.includes(w)) bad.push('no note on ' + k + ': ' + w + ' — got ' + JSON.stringify(ns)); });
      // round 20g (owner 2): ONE pattern — a quiet changes line (no tint, no bar) right under the value, within 8 px of the content
      // above it, BEFORE the item's source line; no inline ⓘ or Rule book page (the change's rule is in that source line's keys)
      g.querySelectorAll('[data-change], [data-dated]').forEach(n => { const row = n.closest('[data-gline], [data-k]'); if (!row) { bad.push('a note outside any row'); return; }
        const prev = n.previousElementSibling, gap = prev ? R(n).top - R(prev).bottom : 0, cs = getComputedStyle(n);
        if (!prev || gap < -1 || gap > 8) bad.push('note ' + JSON.stringify(txt(n)) + ' sits ' + Math.round(gap) + ' px off its line');
        if (!/rgba\\(0, 0, 0, 0\\)|transparent/.test(cs.backgroundColor) || parseFloat(cs.borderLeftWidth) > 0) bad.push('note ' + JSON.stringify(txt(n)) + ' is a tinted box, not a quiet line');
        if (n.querySelector('a.book, .gl')) bad.push('note ' + JSON.stringify(txt(n)) + ' carries an inline page or ⓘ');
        const src = row.querySelector(':scope > .isrc, :scope > [data-isrc]'); if (src && !(n.compareDocumentPosition(src) & Node.DOCUMENT_POSITION_FOLLOWING)) bad.push('note ' + JSON.stringify(txt(n)) + ' after its source line'); });
      return {hash: location.hash.slice(8), notes: notes.map(x => [x.row, x.t]), bad}; })())`)));
  });
  const dnb = DN.filter(x => x.bad.length);
  check('r20e-dated-on-its-line', DN.length === 3 && !dnb.length, dnb.map(x => `${x.hash}: ${x.bad.slice(0, 4).join(' | ')}`).join('; '));

  // (3) every gear and methods item carries its OWN source line (who says so ›, Rule book p.N ↗) inside it — none bunched
  // at a section's end, none floating between items; a gear line's sources are exactly its parts' cites (data); every
  // source line one style (same size, same link colour)
  const SP = [];
  for (const hash of ['w=vedder&d=1010', 'w=klwa&d=1010', 'w=klmb&d=1010', 'w=vedder&d=515']) await page(hash + '&sec=gear', async b => {
    SP.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} const bad = [], st = ST(), S_ = id => document.getElementById(id);
      const scope = [S_('gear'), S_('methods')].filter(Boolean); if (scope.length !== 2) bad.push('no Gear / Methods sections');
      scope.forEach(s => { if (s.querySelector('.gsources, .gbooks')) bad.push(s.id + ': a bunched sources line'); });
      const items = scope.flatMap(s => [...s.querySelectorAll('[data-gline], .gxc[data-k]')]);
      if (!items.length) bad.push('no items');
      const pagesOf = k => (SNAP.ruleInfo[k]?.book || []).map(x => x[0]);
      items.forEach(it => { const id = it.dataset.gline || it.dataset.k, srcs = [...it.querySelectorAll('[data-isrc]')].filter(x => x.closest('[data-gline], .gxc[data-k]') === it);
        // a gear line the data cites no rule for (a fly-only day's "No float, sinker or attracting device": summary.parts[].cites
        // is empty) has nothing to source and draws no source line
        const uncited = it.dataset.gline && st.summary && !st.summary.parts.filter(p => (({lines: ['lines'], hook: ['attach', 'hook'], weight: ['weight'], mode: ['mode']})[it.dataset.gline] || []).includes(p.part)).some(p => p.cites.length);
        if (uncited && !srcs.length) return;
        if (srcs.length !== 1) { bad.push(id + ': ' + srcs.length + ' source lines'); return; }
        const s = srcs[0], keys = (s.dataset.keys || '').split(' ').filter(Boolean), shown = [...s.querySelectorAll('a.book')].map(a => +(a.textContent.match(/\\d+/) || [])[0]);
        if (!keys.length) bad.push(id + ': no rule behind its source line');
        const want = [...new Set(keys.flatMap(pagesOf))].sort((a, c) => a - c); if (JSON.stringify(want) !== JSON.stringify(shown)) bad.push(id + ': pages ' + JSON.stringify(shown) + ' want ' + JSON.stringify(want));
        if (!s.querySelector('[data-open^="srcs:"]')) bad.push(id + ': no Source button');
        if (it.dataset.gline && st.summary) { const ks = ({lines: ['lines'], hook: ['attach', 'hook'], weight: ['weight'], mode: ['mode']})[it.dataset.gline] || [];
          const cites = [...new Set(st.summary.parts.filter(p => ks.includes(p.part)).flatMap(p => p.cites))].sort();
          const chg = new Set((st.summary.changes || []).flatMap(c => c.cites || [])), extra = keys.filter(k => !cites.includes(k) && !chg.has(k));
          if (cites.some(c => !keys.includes(c)) || extra.length) bad.push(id + ': keys ' + JSON.stringify(keys) + ' want the parts\\' cites ' + JSON.stringify(cites) + ' plus the change rule'); }
        // the source line is the item's last thing but its dated note (it sits with what it sources)
        const after = [...it.children].slice([...it.children].indexOf(s.closest('[data-gline] > *, .gxc[data-k] > *') || s) + 1).filter(e => !e.matches('[data-change], [data-dated], .dlist'));
        if (after.length) bad.push(id + ': ' + after.length + ' blocks after its source line'); });
      const sty = [...new Set(scope.flatMap(s => [...s.querySelectorAll('[data-isrc]')]).map(s => { const a = s.querySelector('a.book'), c = getComputedStyle(s); return c.fontSize + '|' + (a ? getComputedStyle(a).color : ''); }))];
      if (sty.length > 1 && new Set(sty.map(x => x.split('|')[0])).size > 1) bad.push('source lines in ' + sty.length + ' styles ' + JSON.stringify(sty));
      return {hash: location.hash.slice(8), n: items.length, bad}; })())`)));
  });
  const spb = SP.filter(x => x.bad.length);
  check('r20e-sources-per-item', SP.length === 4 && !spb.length, spb.map(x => `${x.hash}: ${x.bad.slice(0, 5).join(' | ')}`).join('; '));


  // (4) Ways to fish in the same two columns as Bait (✓ Allowed | ✕ Not allowed, side by side), every way the data lists in its
  // column, its conditions under its name
  const WY = [];
  for (const hash of ['w=vedder&d=1010', 'w=klwa&d=1010', 'w=klmb&d=1010']) await page(hash + '&sec=methods', async b => {
    WY.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} const bad = [], st = ST(), R = e => e.getBoundingClientRect();
      const P = h => new DOMParser().parseFromString('<div id="x">' + h + '</div>', 'text/html').getElementById('x'), w = P(st.panes.ways || '');
      const okW = [...w.querySelectorAll('.wrow')].map(r => [norm(r.querySelector('b')?.textContent), norm(r.querySelector('small')?.textContent)]), noW = [...w.querySelectorAll('.nochip')].map(c => norm(c.textContent.replace(/^✕/, '')));
      const cell = document.querySelector('#methods [data-k="methods"]'), bait = document.querySelector('#gear [data-k="bait"]');
      if (!cell) return {hash: location.hash.slice(8), bad: ['no Ways to fish']};
      const ok = cell.querySelector('.yn [data-col="ok"]'), no = cell.querySelector('.yn [data-col="no"]');
      // round 20g (owner 3): ONE list — the ✓ group above the ✕ group on one left edge, a mark on every row
      if (!ok || !no) bad.push('a group missing'); else { if (R(no).top < R(ok).bottom - 1 || Math.abs(R(no).left - R(ok).left) > 1) bad.push('the two groups are not one stacked list');
        if ([...cell.querySelectorAll('.yn li')].some(li => !li.querySelector(':scope > i'))) bad.push('a row without its mark');
        const okT = [...ok.querySelectorAll('li')].map(li => [norm(li.querySelector('b')?.textContent), norm(li.querySelector('small')?.textContent)]), noT = [...no.querySelectorAll('li b')].map(e => norm(e.textContent));
        if (JSON.stringify(okT) !== JSON.stringify(okW)) bad.push('allowed ' + JSON.stringify(okT) + ' want ' + JSON.stringify(okW));
        if (JSON.stringify(noT) !== JSON.stringify(noW)) bad.push('not allowed ' + JSON.stringify(noT) + ' want ' + JSON.stringify(noW)); }
      if (bait && !bait.querySelector('.yn')) bad.push('bait is not the same component');
      return {hash: location.hash.slice(8), bad}; })())`)));
  });
  const wyb = WY.filter(x => x.bad.length);
  check('r20e-ways-columns', WY.length === 3 && !wyb.length, wyb.map(x => `${x.hash}: ${x.bad.slice(0, 4).join(' | ')}`).join('; '));

  // (5) the licence: TWO segmented selectors (age; where you live), named by the data's own option words, no dropdown and no
  // fold; every one of the six pairs, chosen by tapping, shows its own answer (the data's label for that pair above it, every
  // licence it lists named), on the Vedder and the Kootenay LWA; the guiding line is shown open, not folded
  const LS = [];
  for (const hash of ['w=vedder&d=1010&sec=licence', 'w=klwa&d=1010&sec=licence']) await page(hash, async b => {
    LS.push(JSON.parse(await b.evaluate(`(async () => { ${norm} const bad = [], st = ST(), seen = [];
      const P = h => new DOMParser().parseFromString('<div id="x">' + h + '</div>', 'text/html').getElementById('x');
      const ad = P(st.licence.adult), opts = ax => [...ad.querySelectorAll('#who-' + ax + ' option')].map(o => [o.value, norm(o.textContent)]);
      const AGE = opts('age'), RES = opts('residency');
      const L = () => document.getElementById('licence');
      if (!L()) return JSON.stringify({hash: location.hash.slice(8), bad: ['no #licence']});
      if (L().querySelector('select, details')) bad.push('a dropdown or fold in the licence: ' + L().querySelectorAll('select, details').length);
      const seg = ax => [...L().querySelectorAll('.segm[data-axis="' + ax + '"] button')];
      if (JSON.stringify(seg('age').map(x => norm(x.textContent))) !== JSON.stringify(AGE.map(x => x[1]))) bad.push('age choices ' + JSON.stringify(seg('age').map(x => norm(x.textContent))) + ' want ' + JSON.stringify(AGE.map(x => x[1])));
      if (JSON.stringify(seg('res').map(x => norm(x.textContent))) !== JSON.stringify(RES.map(x => x[1]))) bad.push('residency choices ' + JSON.stringify(seg('res').map(x => norm(x.textContent))) + ' want ' + JSON.stringify(RES.map(x => x[1])));
      const KEY = {'16_plus': {resident: 'adult', non_resident: 'adult_non', non_resident_alien: 'adult_alien'}, under_16: {resident: 'u16_res', non_resident: 'u16_non', non_resident_alien: 'u16_alien'}};
      for (const [av, an] of AGE) for (const [rv, rn] of RES) {
        const a = seg('age').find(x => norm(x.textContent) === an), r = seg('res').find(x => norm(x.textContent) === rn); if (!a || !r) { bad.push('no button for ' + an + ' / ' + rn); continue; }
        a.click(); await new Promise(z => setTimeout(z, 150)); seg('res').find(x => norm(x.textContent) === rn).click(); await new Promise(z => setTimeout(z, 150));
        const key = KEY[av]?.[rv], h = st.licence[key]; if (!h) { bad.push('no data for ' + key); continue; }
        const pa = seg('age').filter(x => x.getAttribute('aria-pressed') === 'true' || x.getAttribute('aria-checked') === 'true').map(x => norm(x.textContent)), pr = seg('res').filter(x => x.getAttribute('aria-pressed') === 'true' || x.getAttribute('aria-checked') === 'true').map(x => norm(x.textContent));
        if (JSON.stringify(pa) !== JSON.stringify([an]) || JSON.stringify(pr) !== JSON.stringify([rn])) bad.push(key + ': pressed ' + JSON.stringify([pa, pr]));
        const lab = norm(L().querySelector('[data-for-res]')?.textContent); if (lab !== st.licLabel[key]) bad.push(key + ': label ' + JSON.stringify(lab) + ' want ' + JSON.stringify(st.licLabel[key]));
        const body = norm(L().textContent).replace(/ⓘ/g, ''), names = [...P(h).querySelectorAll('.needs li > b, .classbox b')].map(e => norm(e.textContent.replace(/ⓘ/g, '')));
        names.forEach(nm => { if (!body.includes(nm)) bad.push(key + ': missing ' + JSON.stringify(nm)); });
        if (L().querySelector('select, details')) bad.push(key + ': a dropdown or fold');
        seen.push([key, lab, names.length]); }
      const guide = [...ad.querySelectorAll('details.lostlist summary')].map(s => norm(s.textContent)).find(t => /guiding/i.test(t));
      if (guide && !norm(L().textContent).includes(guide)) bad.push('the guiding line is gone: ' + guide);
      return JSON.stringify({hash: location.hash.slice(8), seen, bad}); })()`)));
  });
  const lsb = LS.filter(x => x.bad.length || x.seen.length !== 6);
  check('r20e-licence-selectors', LS.length === 2 && !lsb.length, lsb.map(x => `${x.hash}: ${x.seen.length} pairs; ${x.bad.slice(0, 5).join(' | ')}`).join('; '));

  // (6) the region quota and the stream quota read apart: where a group's total is a region's (or zone's) and the row's scope
  // tag names its streams ("All Region 2 streams") the tag is NOT on the head band with the total — it heads the share
  // ("Only 2 of the 4 may be from this stream") — and a "This water" tag never sits over a region's total; the pin bar
  // says the same (group line: the total across the region; the share line: from this stream)
  const RS = [];
  for (const hash of ['w=vedder&d=1010', 'w=glenn&d=615', 'w=anderson&d=1010', 'w=bellacoola&d=501&p=1', 'w=vedder&d=515']) await page(hash, async b => {
    RS.push(JSON.parse(await b.evaluate(`(async () => { ${norm} const bad = [], st = ST(), R = e => e.getBoundingClientRect(); let shares = 0;
      const tags = {}; for (const m of st.cardHTML.matchAll(/data-row="r(\\d+)"><div class="rhead"><span class="rtitle">[^<]*<span class="scope[^"]*">([^<]*)</g)) tags['r' + m[1]] = m[2];
      for (const art of document.querySelectorAll('#keep article.row.ledger')) { const g = (st.rows[+(art.dataset.row || 'r0').slice(1)] || {}).group; if (!g || !g.tree) continue;
        const root = [].concat(g.tree)[0], tag = tags[art.dataset.row] || ''; if (root.tier !== 'total' || root.whose || !/ across /.test(root.says) || !tag || /-wide$/.test(tag)) continue;
        const head = art.querySelector(':scope > .ghd.gpin'), headT = norm(head?.textContent);
        if (headT.toLowerCase().includes(tag.toLowerCase())) bad.push(art.dataset.row + ': the tag ' + JSON.stringify(tag) + ' sits on the region total');
        const share = (root.children || []).find(c => c.node === 'tier' && c.tier === 'share');
        if (share) { shares++; const sec = art.querySelector('section.tnode[data-tid="' + share.id + '"]'), k = sec?.querySelector(':scope > .tier [data-scope-tag]');
          const fold = !!sec?.querySelector(':scope > .tier[data-share-folded]');   /* int1: a folded share (the head's lead says "from streams") carries no kicker */
          if (!fold && (!k || norm(k.textContent).toLowerCase() !== tag.toLowerCase())) bad.push(art.dataset.row + ': the share has no ' + JSON.stringify(tag) + ' tag (' + JSON.stringify(norm(k?.textContent)) + ')');
          // the pin bar while inside the share: the group line names the region, the share line this stream
          const sc = document.getElementById('sc'), kr = [...(sec?.querySelectorAll('.krow') || [])].pop(), hh = document.getElementById('hdr').offsetHeight; if (kr) { sc.scrollTop += R(kr).top - R(sc).top - hh - 70; await new Promise(z => setTimeout(z, 120)); pinUpdate();
            const pl = [...document.querySelectorAll('#pins .pl')].map(p => norm(p.textContent)), across = (root.says.match(/ across (.+?)(?:,|$)/) || [])[1];
            if (!pl[0] || !pl[0].includes('across ' + across)) bad.push('pin group line ' + JSON.stringify(pl[0]) + ' does not say across ' + across);
            if (!(pl[1] && /from (this stream|this lake|streams|lakes)/.test(pl[1])) && !/from (this stream|this lake|streams|lakes)/.test(pl[0])) bad.push('pin share line ' + JSON.stringify(pl[1])); sc.scrollTop = 0; } } }   /* round 20h form a: the group line itself says the share */
      return JSON.stringify({hash: location.hash.slice(8), shares, bad}); })()`)));
  });
  const rsb = RS.filter(x => x.bad.length);
  check('r20e-region-vs-stream', RS.length === 5 && RS.filter(x => x.shares).length >= 3 && !rsb.length, rsb.map(x => `${x.hash}: ${x.bad.slice(0, 4).join(' | ')}`).join('; ') || 'shares ' + RS.map(x => x.shares).join(','));
}

// ---------- round 20c (owner: a format audit; context in every screenful) ----------
// ---------- round 20i (owner: the UR4 user panel's page-side fixes) ----------
async function round20i(){
  const norm = 'var norm = t => (t || "").replace(/\\s+/g, " ").trim();';
  // (1) the size chip says what happens outside the range where the data gives `size_rule`; (2) an Other-fish row whose kinds
  // carry a size rule says "· size limit" on the row; (4) the duty line is a full-weight ink line, "this river" is the water's
  // name, a steelhead row links the stamp when the licence names it; (O3) no steelhead-stamp line where steelhead are closed;
  // (kokanee) the salmon stamp's words keep "(not kokanee)"; (5) "Closed Jun 1–30"; (6) the all-year bait ban has its line;
  // (7) the "Only if…" stamps under their own head; (8) --sub one step stronger, "release" bold; (3) scroll margins under the stack
  const UI = [];
  const STATES_I = [['w=vedder&d=1010', 'vedder'], ['w=x35p&d=1010', 'bennett'], ['w=bellacoola&d=501&p=1&sec=licence', 'bella-lic'], ['w=klwa&d=1012&sec=licence', 'lwa-lic'], ['w=vedder&d=515', 'vedder-may15'], ['w=glenn&d=615&sec=gear', 'glenn-gear'], ['w=vedder&d=1010&sec=licence', 'vedder-lic'], ['w=vedder&d=1010&theme=dark', 'vedder-dark']];
  for (const [hash, tag] of STATES_I) await page(hash, async b => {
    UI.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} const st = ST(), bad = [], cs = e => getComputedStyle(e), R = e => e.getBoundingClientRect(), out = {tag: ${JSON.stringify(tag)}};
      const kinds = []; (st.rows || []).forEach(r => { const walk = n => { if (n.node === 'kind') kinds.push(n); (n.children || []).forEach(walk); }; if (r.group?.tree) [].concat(r.group.tree).forEach(walk); });
      // 1 the chip
      document.querySelectorAll('#keep .krow.kt').forEach(row => { const k = kinds.find(x => x.name === row.dataset.kind && x.species.join(' ') === row.dataset.sp); if (!k) return; const chip = row.querySelector('.szc');
        if (k.size_rule) { out.rules = (out.rules || 0) + 1; if (!chip || norm(chip.textContent) !== norm(k.size_rule)) bad.push('CHIP ' + k.name + ': ' + JSON.stringify(norm(chip?.textContent)) + ' vs ' + k.size_rule); }
        else if (chip && /release/.test(chip.textContent)) bad.push('CHIP a consequence from nowhere on ' + k.name); });
      // 2 the other-fish hint
      document.querySelectorAll('#keep .olist .orow').forEach(r => { const nm = norm(r.querySelector('.on')?.firstChild?.textContent).toLowerCase(), g = (st.rows || []).map(x => x.group).find(gg => gg && gg.tree && kinds.some(k => JSON.stringify(gg.tree).includes(JSON.stringify(k.name)) && k.name.toLowerCase().includes(nm.split(' ').pop()))); if (!g) return;   /* the collapsed list keeps no article: map by the fish's name */
        const has = /"size_rule":"[^"]|"detail":"[^"]*\\d+ cm/.test(JSON.stringify(g.tree || g.kinds || [])), hint = r.querySelector('[data-size-hint]'); if (has) out.hints = (out.hints || 0) + 1;
        if (has && !hint) bad.push('HINT no size hint on ' + norm(r.querySelector('.on')?.textContent)); if (!has && hint) bad.push('HINT a hint from nowhere on ' + norm(r.querySelector('.on')?.textContent)); if (hint && norm(hint.textContent) !== '· size limit') bad.push('HINT words ' + norm(hint.textContent)); });
      // 4 the duty line
      document.querySelectorAll('#keep .krow.kt').forEach(row => { const k = kinds.find(x => x.name === row.dataset.kind && x.species.join(' ') === row.dataset.sp), d = row.querySelector('.duty'); if (!k || !k.duties?.length) { if (d) bad.push('DUTY a duty line from nowhere'); return; } out.duties = (out.duties || 0) + 1;
        if (!d) { bad.push('DUTY no line on ' + k.name); return; } const c = cs(d), ink = cs(document.querySelector('#keep .ks') || document.body).color;
        if (parseFloat(c.fontSize) < parseFloat(cs(row.querySelector('.kmeta .kq') || d).fontSize) + 1 || parseInt(c.fontWeight) < 700 || c.color !== ink) bad.push('DUTY not a full-weight ink line: ' + c.fontSize + ' ' + c.fontWeight + ' ' + c.color);
        const txt = norm(d.textContent); if (/\\bthis (river|stream|lake|creek)\\b/i.test(txt)) bad.push('DUTY still says "this river": ' + txt); if (k.duties.some(x => /this (river|stream|lake|creek)/.test(x)) && !txt.includes('the ' + st.name)) bad.push('DUTY the water’s name missing: ' + txt);
        const stamp = d.querySelector('[data-stamp]'), named = /Steelhead Conservation Surcharge Stamp/.test(JSON.stringify(st.licence || '')); if (k.species.includes('ST') && named && !(stamp && stamp.dataset.sec === 'licence' && norm(stamp.textContent) === 'Steelhead stamp ›')) bad.push('DUTY no stamp link on the steelhead row'); if (stamp && !k.species.includes('ST')) bad.push('DUTY a stamp link off the steelhead row'); });
      // O3 + kokanee (licence tab)
      const lic = document.getElementById('licence'); if (lic) { const closedST = (st.rows || []).some(r => (r.group?.not_kept || []).some(x => x.status === 'closed' && (x.species || []).includes('ST')));
        const stampLi = [...lic.querySelectorAll('.needs li')].find(li => /Steelhead Conservation Surcharge Stamp/.test(li.textContent)); out.closedST = closedST; out.stampLi = !!stampLi;
        if (closedST && stampLi) bad.push('LIC the steelhead stamp listed where steelhead are closed'); if (!closedST && /Steelhead Conservation Surcharge Stamp/.test(JSON.stringify(st.licence || '')) && !stampLi) bad.push('LIC the steelhead stamp missing');
        const salmon = [...lic.querySelectorAll('.needs li')].find(li => /salmon/i.test(li.textContent)); if (salmon && /not kokanee/.test(JSON.stringify(st.licence || '')) && !/\\(not kokanee\\)/.test(salmon.textContent)) bad.push('LIC the salmon stamp lost "(not kokanee)": ' + norm(salmon.textContent));
        // 7 the Only-if head
        const only = [...lic.querySelectorAll('.needs li')].filter(li => /^Only if\\b/i.test(norm(li.querySelector(':scope > span')?.textContent))), head = lic.querySelector('[data-only-if-head]'); out.only = only.length;
        if (only.length && only.length < lic.querySelectorAll('.needs li').length) { if (!head) bad.push('LIC no "Only if" head'); only.forEach(li => { if (!li.closest('ul[data-only-if]')) bad.push('LIC an Only-if item in the everyone list: ' + norm(li.querySelector('b')?.textContent)); if (head && !(head.compareDocumentPosition(li) & Node.DOCUMENT_POSITION_FOLLOWING)) bad.push('LIC an Only-if item above its head'); });
          [...lic.querySelectorAll('.needs > ul:not([data-only-if]) li')].forEach(li => { if (/^Only if\\b/i.test(norm(li.querySelector(':scope > span')?.textContent))) bad.push('LIC Only-if left in the first list'); }); } }
      // 5 the closed span
      const soon = document.querySelector('#keep .wstrip .wsoon'); if (soon) { out.soon = norm(soon.textContent); if (/^Closed from\\b/.test(out.soon)) bad.push('CLOSED no span: ' + out.soon); if (/^Closed /.test(out.soon) && !/^Closed [A-Z][a-z]{2} \\d+–(?:[A-Z][a-z]{2} )?\\d+$/.test(out.soon)) bad.push('CLOSED odd span: ' + out.soon); }
      // 6 the all-year line
      const bait = document.querySelector('#gear [data-k="bait"]'); if (bait) { const allYear = (st.runs || []).length && st.runs.every(([f]) => f != null && (f & 16)); out.allYear = !!allYear; const line = bait.querySelector('[data-all-year]');
        if (allYear && !line) bad.push('BAIT no All-year line'); if (line) { if (!/^All year:/.test(norm(line.querySelector('span')?.textContent))) bad.push('BAIT the line reads ' + norm(line.textContent)); if (!line.classList.contains('now')) bad.push('BAIT the all-year ban not marked in force'); const banned = norm(bait.querySelector('.yn li.no small, .yn li.no b')?.textContent); if (!/bait ban/i.test(norm(line.textContent))) bad.push('BAIT the line does not say the rows’ ban: ' + norm(line.textContent)); } }   /* the note with its ⓘ stays above the line */
      // 8 contrast: --sub against the card, the release word bold
      const probe = (n, prop) => { const e = document.createElement('i'); e.style[prop] = 'var(--' + n + ')'; document.body.appendChild(e); const c = cs(e)[prop]; e.remove(); return c; };
      const rgb = c => c.match(/\\d+/g).map(Number), lum = ([r, g, b]) => { const f = x => { x /= 255; return x <= 0.03928 ? x / 12.92 : Math.pow((x + 0.055) / 1.055, 2.4); }; return 0.2126 * f(r) + 0.7152 * f(g) + 0.0722 * f(b); };
      const ratio = (a, b) => { const la = lum(rgb(a)), lb = lum(rgb(b)); return (Math.max(la, lb) + 0.05) / (Math.min(la, lb) + 0.05); };
      out.subRatio = +ratio(probe('sub', 'color'), probe('card', 'backgroundColor')).toFixed(2); if (out.subRatio < 6.5) bad.push('SUB contrast ' + out.subRatio);
      const rel = document.querySelector('#keep .k-release'); if (rel && parseInt(cs(rel).fontWeight) < 700) bad.push('RELEASE not bold: ' + cs(rel).fontWeight);
      // 3 scroll margins: the stack's height is a variable on the scroller and the heads carry it
      const sc = document.getElementById('sc'); out.scrollPad = cs(sc).scrollPaddingTop; if (!/^\\d/.test(out.scrollPad) || parseFloat(out.scrollPad) < (document.getElementById('hdr')?.offsetHeight || 0)) bad.push('SCROLL no scroll-padding under the header: ' + out.scrollPad);
      const tier = document.querySelector('#keep .tier'); if (tier && parseFloat(cs(tier).scrollMarginTop) < (document.getElementById('hdr')?.offsetHeight || 0)) bad.push('SCROLL a head without its scroll margin: ' + cs(tier).scrollMarginTop);
      out.bad = bad; return out; })())`)));
  });
  const pick = (re, pred) => { const bad = UI.flatMap(x => x.bad.filter(y => re.test(y)).map(y => x.tag + ': ' + y)); return [pred(UI) && !bad.length, bad.slice(0, 6).join('; ')]; };
  const say = (name, [ok, why], extra = '') => check(name, ok, (why || '') + (extra ? ' · ' + extra : ''));
  say('r20i-size-rule-chip', pick(/^CHIP /, u => u.reduce((a, x) => a + (x.rules || 0), 0) >= 3), 'rules ' + UI.map(x => x.rules || 0).join(','));
  say('r20i-other-size-hint', pick(/^HINT /, u => u.reduce((a, x) => a + (x.hints || 0), 0) >= 1), 'hints ' + UI.map(x => x.hints || 0).join(','));
  say('r20i-duty-line', pick(/^DUTY /, u => u.reduce((a, x) => a + (x.duties || 0), 0) >= 1));
  say('r20i-licence-stamps', pick(/^LIC /, u => u.some(x => x.closedST) && u.some(x => x.only)), JSON.stringify(UI.filter(x => /lic/.test(x.tag)).map(x => [x.tag, x.closedST, x.stampLi, x.only])));
  say('r20i-closed-span', pick(/^CLOSED /, u => u.some(x => /^Closed /.test(x.soon || ''))), JSON.stringify(UI.map(x => x.soon).filter(Boolean)));
  say('r20i-all-year-ban', pick(/^BAIT /, u => u.some(x => x.allYear)));
  say('r20i-glare', pick(/^SUB |^RELEASE /, u => u.length === STATES_I.length), 'sub ' + JSON.stringify(UI.map(x => x.subRatio)));
  say('r20i-scroll-margins', pick(/^SCROLL /, u => u.length === STATES_I.length), JSON.stringify(UI.map(x => x.scrollPad)));
}
// ---------- round 20h (owner: the region total vs the stream share; one surface; pictures without a tile; the pin line's end) ----------
async function round20h(){
  const norm = 'var norm = t => (t || "").replace(/\\s+/g, " ").trim();';
  // (2) ONE surface: inside a keep card (and the pin bar) no element wider than 200 px and taller than 40 px paints a background
  // other than the card's; (3) a fish picture and its tile paint no background of their own — light and dark
  const SF = [];
  for (const theme of ['light', 'dark']) for (const hash of ['w=vedder&d=1010', 'w=x35p&d=1010', 'w=klwa&d=1012']) await page(hash + (theme === 'dark' ? '&theme=dark' : ''), async b => {
    SF.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} const bad = [], cs = e => getComputedStyle(e), R = e => e.getBoundingClientRect();
      const probe = (n, prop) => { const e = document.createElement('i'); e.style[prop] = 'var(--' + n + ')'; document.body.appendChild(e); const c = cs(e)[prop]; e.remove(); return c; };
      const card = probe('card', 'backgroundColor'), clear = c => /rgba\\(0, 0, 0, 0\\)|transparent/.test(c); let bands = 0, pics = 0;
      document.querySelectorAll('#keep article.row.ledger *, #pins .pinbar').forEach(e => { const c = cs(e).backgroundColor, r = R(e); if (clear(c) || r.width < 200 || r.height < 40 || e.matches('.szc, .chgi')) return; bands++;   /* a chip may wrap to two lines (a long size rule) and is still a chip */
        if (c !== card) bad.push('SURFACE ' + (e.className || e.tagName) + ' ' + Math.round(r.width) + '×' + Math.round(r.height) + ' paints ' + c); });
      document.querySelectorAll('#keep .kp img, #keep .th img').forEach(im => { pics++; if (!clear(cs(im).backgroundColor)) bad.push('PIC a tile under ' + (im.closest('button')?.getAttribute('aria-label') || 'a picture') + ': ' + cs(im).backgroundColor);
        const p = im.parentElement; if (p && !p.matches('button, .kp, .th') && !clear(cs(p).backgroundColor)) bad.push('PIC a tile around a picture: ' + p.className); });
      return {hash: location.hash.slice(8), bands, pics, bad}; })())`)));
  });
  const sfb = SF.filter(x => x.bad.length);
  check('r20h-one-surface', SF.length === 6 && !sfb.some(x => x.bad.some(y => /^SURFACE /.test(y))),   /* bands counts band-sized painted elements: with one surface it may be 0 */ sfb.flatMap(x => x.bad.filter(y => /^SURFACE /.test(y)).map(y => x.hash + ': ' + y)).slice(0, 6).join('; '));
  check('r20h-picture-surface', SF.reduce((a, x) => a + x.pics, 0) >= 12 && !sfb.some(x => x.bad.some(y => /^PIC /.test(y))), sfb.flatMap(x => x.bad.filter(y => /^PIC /.test(y)).map(y => x.hash + ': ' + y)).slice(0, 6).join('; '));
  // (4 i) a pinned tier line ends when the next section's head is in view: with the Vedder's own rainbow row (Glenn's Duncan row)
  // at 60 % of the viewport the bar says nothing "from streams"; with the share's first rows there and the own head below the
  // fold, the share's line stands
  const PD = [];
  for (const [hash, sel] of [['w=vedder&d=1010', '#keep section.tnode[data-tnode="own"] .krow.kt'], ['w=glenn&d=615', '#keep section.tnode[data-tnode="own"] .krow.kt[data-sp~="RB"]']]) {
    const b = await open(`${pathToFileURL(PAGE).href}#shot=1&${hash}`, { width: 440, height: 900, offline: true });
    try { await new Promise(r => setTimeout(r, 500)); PD.push(JSON.parse(await b.evaluate(`(async () => { ${norm} await document.fonts.ready; const sc = document.getElementById('sc'), R = e => e.getBoundingClientRect(), pins = () => [...document.querySelectorAll('#pins .pl')].map(e => norm(e.textContent));
      const own = document.querySelector(${JSON.stringify(sel)}), share = document.querySelector('#keep section.tnode[data-tnode="share"] .krow.kt'), ownHead = document.querySelector('#keep section.tnode[data-tnode="own"] > .tier');
      sc.scrollTop = sc.scrollTop + R(own).top - R(sc).top - sc.clientHeight * 0.6; await new Promise(r => setTimeout(r, 400)); const atOwn = pins();
      sc.scrollTop = 0; await new Promise(r => setTimeout(r, 200)); const shareHead = document.querySelector('#keep section.tnode[data-tnode="share"] > .tier');
      sc.scrollTop = sc.scrollTop + R(shareHead).top - R(sc).top - document.getElementById('hdr').offsetHeight + 12; await new Promise(r => setTimeout(r, 400));   /* the share's head just slid under the bar */
      return JSON.stringify({hash: location.hash.slice(8), atOwn, atShare: pins(), ownHeadBelow: R(ownHead).top > R(sc).bottom}); })()`))); } finally { await b.close(); } }
  const pdb = PD.filter(x => x.atOwn.some(t => /↳.*from streams/.test(t)) || (x.ownHeadBelow && !x.atShare.some(t => /from streams/.test(t))));   /* form a: the group line itself says the share; form b: a ↳ line */
  check('r20h-pin-drops', PD.length === 2 && !pdb.length, JSON.stringify(PD));
  // (1) the region total and the stream share are both in the group head, each on its own line (form a: the share big, the total
  // a note "of the …"; form b: the total big, "↳ N of them from streams" under it); the pin bar's group line never puts the
  // region's number "from streams"
  const TR = [];
  for (const hash of ['w=vedder&d=1010', 'w=glenn&d=615', 'w=bellacoola&d=501&p=1', 'w=klwa&d=1012']) await page(hash, async b => {
    TR.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} const st = ST(), out = [];
      document.querySelectorAll('#keep article.row.ledger').forEach(art => { const g = (st.rows[+(art.dataset.row || 'r0').slice(1)] || {}).group; if (!g || !g.tree) return; const root = [].concat(g.tree)[0], num = art.querySelector('[data-root-tier]');
        const rel = root.tier === 'total' && g.headline && g.headline.n != null && g.headline.n !== root.n && (root.children || []).some(c => c.node === 'tier' && c.n === g.headline.n);
        out.push({says: root.says, rel, form: num?.dataset.tot || '', big: norm(num?.querySelector('.gln, .ghn')?.textContent), of: norm(num?.querySelector('[data-of-total]')?.textContent), under: norm(num?.querySelector('[data-share-of]')?.textContent), hl: g.headline?.says}); });
      return {hash: location.hash.slice(8), out}; })())`)));
  });
  const trb = TR.flatMap(x => x.out.filter(o => o.rel && !((o.form === 'a' && o.big === String((o.hl.match(/\d+/) || [])[0]) && /^of the \d+ a day/.test(o.of)) || (o.form === 'b' && /^↳ \d+ (of them|a day)/.test(o.under)))).map(o => x.hash + ': ' + JSON.stringify(o)));
  check('r20h-total-relation', TR.some(x => x.out.some(o => o.rel)) && !trb.length, trb.slice(0, 4).join('; '));
  // (data, gear 11) a bait allowance names the ban it is excepted from (the roe row: "the fish or fish parts ban excepts it"),
  // never a bare "the ban"; the banned row carries no "is still OK"; the NEXT bait-ban line carries nothing of it
  const RO = [];
  for (const hash of ['w=vedder&d=1010&sec=gear', 'w=vedder&d=515&sec=gear', 'w=klwa&d=1010&sec=gear']) await page(hash, async b => {
    RO.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} const bad = [], bait = document.querySelector('#gear [data-k="bait"]'); if (!bait) return {hash: location.hash.slice(8), bad: ['no bait item']};
      const rows = [...bait.querySelectorAll('.yn li')].map(li => ({ok: li.classList.contains('ok'), n: norm(li.querySelector('b')?.textContent), s: norm(li.querySelector('small')?.textContent)}));
      rows.forEach(r => { if (/\\bthe ban\\b/i.test(r.s)) bad.push(r.n + ': a bare "the ban": ' + r.s); if (!r.ok && /still OK/i.test(r.s)) bad.push(r.n + ': the banned row keeps the allowance: ' + r.s); if (r.ok && /still OK/i.test(r.s) && !/\\bban\\b/i.test(r.s)) bad.push(r.n + ': an allowance without its ban: ' + r.s); });
      const roe = rows.find(r => /^Roe/.test(r.n)); bait.querySelectorAll('.chg').forEach(c => { if (/roe|still OK|excepts/i.test(c.textContent)) bad.push('the dated line carries the allowance: ' + norm(c.textContent)); });
      return {hash: location.hash.slice(8), roe, bad}; })())`)));
  });
  const rob = RO.filter(x => x.bad.length), roeV = RO.find(x => x.hash.startsWith('w=vedder&d=1010'))?.roe;
  check('r20h-roe-names-ban', RO.length === 3 && !rob.length && !!roeV && roeV.ok && /\bban\b/i.test(roeV.s), rob.flatMap(x => x.bad.map(y => x.hash + ': ' + y)).slice(0, 5).join('; ') + ' · Vedder roe ' + JSON.stringify(roeV));
  // (data, rows 25) the possession sheet shows the card's own example(s) and the doubles line, not the glossary's generic 5 → 10
  const PX = await page('w=vedder&d=1010&sheet=' + encodeURIComponent('pos:vedder_1010|pos'), async b => JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} const sh = document.querySelector('#sheet.on .sbody'); if (!sh) return {bad: ['no sheet']}; const st = ST(), bad = [];
    const own = (st.rows || []).map(r => r.group && r.group.possession).filter(Boolean), lis = [...sh.querySelectorAll('[data-pos-own] li, ul[data-pos-own] li')].map(e => norm(e.textContent)), txt = norm(sh.textContent);
    own.forEach(x => { if (!lis.some(l => l.includes(x.example))) bad.push('example missing: ' + x.example); if (x.doubles && !lis.some(l => l.includes(x.doubles))) bad.push('doubles missing: ' + x.doubles); });
    if (own.length && sh.querySelector('[data-pos-generic]')) bad.push('the generic example drawn beside the card’s own');
    if (/5 trout/.test(txt) && own.length) bad.push('the generic 5 → 10 shown');
    const plain = norm(sh.querySelector('[data-possession]')?.textContent), ts = norm(sh.querySelector('[data-term-says]')?.textContent); if (ts && plain && (ts.includes(plain.replace(/^Possession: ?/, '').replace(/\\.$/, '')) || /2 days/.test(ts))) bad.push('the term line repeats the rule: ' + ts);
    return {bad, own: own.length, lis}; })())`)));
  check('r20h-possession-own-example', PX.own >= 1 && !PX.bad.length, PX.bad.join('; ') + ' · ' + JSON.stringify(PX.lis));
}
// ---------- round 20g (owner: colours with one meaning each; one way to show a future change; the ways list; possession) ----------
async function round20g(){
  const norm = 'var norm = t => (t || "").replace(/\\s+/g, " ").trim();';
  // (1) the palette, measured: a probe reads each token's computed colour; every text colour on a tab is one of them; the
  // status colours sit only on status (the year strip, Open / Closed, the ✓ / ✕ marks, a status word in the regulation
  // lines); every number in ink; one grouping tint; the count of distinct text colours per tab bounded (light AND dark)
  const BOUND = {keep: 7, gear: 5, methods: 5, licence: 4};   // UR7 (owner B): Keep +1, the size section's --rule head
  const PAL = [];
  for (const theme of ['light', 'dark']) for (const hash of ['w=vedder&d=1010', 'w=klwa&d=1012', 'w=bellacoola&d=501&p=1', 'w=glenn&d=615', 'w=vedder&d=515']) await page(hash + (theme === 'dark' ? '&theme=dark' : ''), async b => {
    PAL.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} const bad = [], counts = {}, cs = e => getComputedStyle(e);
      const vis = e => { const r = e.getBoundingClientRect(); const c = cs(e); return r.width > 0 && r.height > 0 && c.visibility !== 'hidden' && c.display !== 'none'; };
      const probe = (n, prop) => { const e = document.createElement('i'); e.style[prop] = 'var(--' + n + ')'; document.body.appendChild(e); const c = cs(e)[prop]; e.remove(); return c; };
      const tok = {}; ['ink', 'sub', 'accent', 'open', 'closed', 'restricted', 'on-accent', 'card', 'rule'].forEach(n => tok[probe(n, 'color')] = n);   /* UR7 (owner B): --rule, the size section's colour (its bar and its head line) */ const tint = probe('tint', 'backgroundColor');
      const status = e => !!e.closest('.wstrip, .olist') || e.matches('.yn li > i, .wyn li > i, .ynh i, .k-open, .k-closed, .k-release, .k-restricted, .wsstat, .krow .nm, .krow .kn');   // a kind row's title / number slot carries its closed / release status
      for (const id of ['keep', 'gear', 'methods', 'licence']) { const root = document.getElementById(id); if (!root) { bad.push('no #' + id); continue; }
        const els = [...root.querySelectorAll('*')].filter(e => vis(e) && !e.closest('script,style,svg') && [...e.childNodes].some(n => n.nodeType === 3 && n.textContent.trim()));
        const seen = new Map(); els.forEach(e => { const c = cs(e).color; if (!seen.has(c)) seen.set(c, e); });
        counts[id] = seen.size; if (seen.size > ${JSON.stringify(BOUND)}[id]) bad.push(id + ': ' + seen.size + ' text colours (' + [...seen.keys()].map(c => tok[c] || c).join(', ') + ')');
        seen.forEach((e, c) => { if (!tok[c]) bad.push(id + ': ' + c + ' is no token, on ' + e.className + ' “' + norm(e.textContent).slice(0, 30) + '”'); });
        els.forEach(e => { const t = tok[cs(e).color]; if ((t === 'open' || t === 'closed' || t === 'restricted') && !status(e)) bad.push(id + ': ' + t + ' on a non-status ' + e.className + ' “' + norm(e.textContent).slice(0, 30) + '”');
          if ((t === 'card' || t === 'on-accent') && !e.closest('.segm button, .pill, .obtn')) bad.push(id + ': ' + t + ' text off a filled control: ' + norm(e.textContent).slice(0, 30)); }); }
      document.querySelectorAll('#keep .tier .tn, #keep .ghd .ghn, #keep .ks .kn, #keep .szl .qgn, #pins .pl b').forEach(e => { if (vis(e) && tok[cs(e).color] !== (e.closest('section.tnode[data-tnode="size"] > .tier, #pins .pl[data-tier="size"]') ? 'rule' : 'ink'))   /* UR7 (owner B): the size head's numbers in its bar's colour */ bad.push('a number not in ink: ' + e.className + ' ' + norm(e.textContent)); });
      const bands = [...document.querySelectorAll('#keep .szc, .chg .chgi')]   /* round 20h: one surface; the tint is on chips only */.filter(vis).map(e => cs(e).backgroundColor).filter(c => !/rgba\\(0, 0, 0, 0\\)|transparent/.test(c));
      new Set(bands).forEach(c => { if (c !== tint) bad.push('a grouping tint that is not --tint: ' + c); });
      return {hash: location.hash.slice(8), counts, bad}; })())`)));
  });
  const palb = PAL.filter(x => x.bad.length);
  check('r20g-palette', PAL.length === 10 && !palb.length, palb.flatMap(x => x.bad.map(y => x.hash + ': ' + y)).slice(0, 8).join('; ') + ' · counts ' + JSON.stringify(PAL[0]?.counts));
  // (2) ONE pattern for a rule that differs on other dates: a quiet line (.chg) right under the item's value, a Now / Next
  // chip, the dates in ink then the data's words, "· in force today" only on the one in force, no tint, no bar, no page
  // link or ⓘ inside it, before the item's source line (whose keys carry the change's rule: r20e-sources-per-item); nothing
  // else dated anywhere (no .gnote); the bait ban's line stands between the bait sentence and the ways list
  const CH = [];
  for (const hash of ['w=vedder&d=1010&sec=gear', 'w=vedder&d=515&sec=gear', 'w=klwa&d=1010&sec=gear', 'w=bellacoola&d=501&p=1&sec=gear']) await page(hash, async b => {
    CH.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} const bad = [], cs = e => getComputedStyle(e), st = ST(); let n = 0, now = 0;
      if (document.querySelector('.gnote, [data-dated]:not(.chg)')) bad.push('a dated note outside the pattern');
      document.querySelectorAll('.chg').forEach(c => { n++; const it = c.closest('[data-gline], .gxc[data-k]'), chip = c.querySelector('.chgi'), b_ = c.querySelector('b'), isNow = c.classList.contains('now'); if (isNow) now++;
        if (!it) { bad.push('a changes line outside an item'); return; }
        if (!chip || norm(chip.textContent) !== (isNow ? 'Now' : 'Next')) bad.push('chip reads ' + norm(chip?.textContent) + ' on ' + norm(c.textContent).slice(0, 30));
        if (!b_ || !/:$/.test(norm(b_.textContent)) || !/\\d/.test(b_.textContent)) bad.push('no dates in ink first: ' + norm(c.textContent).slice(0, 40));
        if (/in force today/.test(c.textContent) !== isNow) bad.push('"in force today" and the Now chip disagree: ' + norm(c.textContent).slice(0, 40));
        if (c.querySelector('a.book, .gl, button')) bad.push('a link or ⓘ inside the changes line');
        if (!/rgba\\(0, 0, 0, 0\\)|transparent/.test(cs(c).backgroundColor) || parseFloat(cs(c).borderLeftWidth) > 0) bad.push('the changes line is tinted or barred');
        const src = it.querySelector('.isrc'); if (!src || !(c.compareDocumentPosition(src) & Node.DOCUMENT_POSITION_FOLLOWING)) bad.push('changes line not before the item’s source line');
        const val = it.querySelector('.gval, .gxs, .gxl'); if (!val || !(val.compareDocumentPosition(c) & Node.DOCUMENT_POSITION_FOLLOWING)) bad.push('changes line not under the value');
        const list = it.querySelector('.yn'); if (list && !(c.compareDocumentPosition(list) & Node.DOCUMENT_POSITION_FOLLOWING)) bad.push('the bait line is not before the ways list'); });
      const want = (st.summary?.changes || []).length; if (n < want) bad.push(n + ' changes lines for ' + want + ' changes in the data');
      return {hash: location.hash.slice(8), n, now, bad}; })())`)));
  });
  const chb = CH.filter(x => x.bad.length);
  check('r20g-changes-line', CH.reduce((a, x) => a + x.n, 0) >= 4 && CH.some(x => x.now) && !chb.length, chb.flatMap(x => x.bad.map(y => x.hash + ': ' + y)).slice(0, 8).join('; ') + ' · ' + JSON.stringify(CH.map(x => [x.hash, x.n, x.now])));
  // (3) the ways to fish / bait as ONE list is r20e-ways-columns (retargeted). (4) the possession sheet: one plain line
  // (the card row's data words), the example, the book's own words in a fold CLOSED by default, then the source line
  const PO = await page('w=vedder&d=1010&sheet=' + encodeURIComponent('pos:vedder_1010|pos'), async b => JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} const bad = [], sh = document.querySelector('#sheet.on .sbody'); if (!sh) return {bad: ['no sheet']};
    const plain = sh.querySelector('[data-possession]'), fold = sh.querySelector('details[data-book-fold]'), src = sh.querySelector('.isrc'), ex = sh.querySelector('.glex');
    if (!plain) bad.push('no plain line'); else { if (norm(plain.textContent).split(/(?<=\\.)\\s/).length > 2) bad.push('the plain line is more than two sentences');   /* display 16: the value, then the exception (Fish at home don’t count.) */ if (plain.querySelector('a, button')) bad.push('a link inside the plain line'); }
    if (!fold) bad.push('no fold'); else { if (fold.open) bad.push('the fold is open'); if (norm(fold.querySelector('summary')?.textContent) !== 'In the rule book’s words') bad.push('summary reads ' + norm(fold.querySelector('summary')?.textContent)); if (!fold.querySelector('blockquote, .glsays')) bad.push('no words in the fold'); }
    if (!src || !src.querySelector('a.book')) bad.push('no source line with a page');
    const order = [plain, ex, fold, src].filter(Boolean); for (let i = 1; i < order.length; i++) if (!(order[i - 1].compareDocumentPosition(order[i]) & Node.DOCUMENT_POSITION_FOLLOWING)) bad.push('out of order at ' + order[i].className);
    const vis = [...sh.querySelectorAll('*')].filter(e => e.getBoundingClientRect().height > 0 && !fold?.contains(e) || e === fold).length;
    return {bad, lines: norm(plain?.textContent), h: Math.round(sh.getBoundingClientRect().height)}; })())`)));
  check('r20g-possession-fold', !PO.bad.length && PO.h < 300,   /* rows 25: the card's own examples (three groups on the Vedder) + the doubles line; 273 px */ PO.bad.join('; ') + ' · ' + PO.lines + ' · ' + PO.h + 'px');
}
// ---------- round 20f (owner: thumbnails in one place; tier headers without the left bar; "from streams") ----------
async function round20f(){
  const norm = 'var norm = t => (t || "").replace(/\\s+/g, " ").trim();';
  const RF = [];
  for (const hash of ['w=vedder&d=1010', 'w=chilliwack&d=1010&p=1', 'w=bellacoola&d=501&p=1', 'w=x35p&d=1010', 'w=glenn&d=615', 'w=klwa&d=1012']) await page(hash, async b => {
    RF.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} const bad = [], R = e => e.getBoundingClientRect(), cs = e => getComputedStyle(e); let rows = 0, heads = 0, caps = 0;
      document.querySelectorAll('#keep .krow.kt').forEach(row => { rows++; const kids = [...row.querySelector('.kb').children], i = kids.findIndex(e => e.classList.contains('kpics'));
        if (i < 0) { bad.push('PIC no pictures: ' + norm(row.textContent).slice(0, 40)); return; }
        kids.slice(0, i).forEach(e => { if (!e.matches('.ks, .kmeta')) bad.push('PIC ' + e.className + ' before the pictures: ' + norm(row.textContent).slice(0, 40)); });
        kids.slice(i + 1).forEach(e => { if (e.matches('.ks, .kmeta')) bad.push('PIC ' + e.className + ' after the pictures'); });
        row.querySelectorAll('.kcap').forEach(c => { caps++; const lh = parseFloat(cs(c).lineHeight); if (R(c).height > lh * 2.6) bad.push('PIC a caption of ' + Math.round(R(c).height / lh) + ' lines: ' + norm(c.textContent)); });
        row.querySelectorAll('.kp img').forEach(im => { if (Math.round(R(im).width) < 80) bad.push('PIC a picture shrunk to ' + Math.round(R(im).width) + 'px'); }); });
      const szIn = e => 0;   /* UR6 put the size section's text one rule-and-gap in; UR7 (owner B): the rule is flush with the screen's edge and the section's text is back on the page's text edge */
      document.querySelectorAll('#keep article.row.ledger').forEach(art => { const pin = art.querySelector(':scope > .ghd.gpin'); if (!pin) return;
        const edge = Math.round(R(pin).left + parseFloat(cs(pin).paddingLeft));
        art.querySelectorAll('.tier .tl, .tier .tkick').forEach(h => { heads++; if (parseFloat(cs(h).borderLeftWidth) > 0 && cs(h).borderLeftStyle !== 'none') bad.push('HDR a left bar on ' + norm(h.textContent).slice(0, 40));
          if (Math.abs(Math.round(R(h).left + parseFloat(cs(h).paddingLeft) - szIn(h)) - edge) > 1) bad.push('HDR header off the text edge: ' + norm(h.textContent).slice(0, 40)); });
        art.querySelectorAll('.krow.kt').forEach(r => { if (Math.abs(Math.round(R(r).left + parseFloat(cs(r).paddingLeft) - szIn(r)) - edge) > 1) bad.push('HDR row off the text edge'); });
        // round 20h (owner 2): ONE surface — the size section is framed by two hairlines, not tinted
        art.querySelectorAll('section.tnode[data-tnode="size"]').forEach(t => { if (!/rgba\\(0, 0, 0, 0\\)|transparent/.test(cs(t).backgroundColor)) bad.push('HDR the size section paints a band'); if (parseFloat(cs(t).borderTopWidth) < 1 || parseFloat(cs(t).borderBottomWidth) < 1) bad.push('HDR the size section has no hairlines'); }); });
      return {hash: location.hash.slice(8), rows, heads, caps, bad}; })())`)));
  });
  const rfb = RF.filter(x => x.bad.length);
  check('r20f-pictures-first', RF.reduce((a, x) => a + x.rows, 0) >= 12 && !rfb.some(x => x.bad.some(y => /^PIC /.test(y))), rfb.flatMap(x => x.bad.filter(y => /^PIC /.test(y)).map(y => x.hash + ': ' + y)).slice(0, 6).join('; '));
  check('r20f-headers', RF.reduce((a, x) => a + x.heads, 0) >= 10 && !rfb.some(x => x.bad.some(y => /^HDR /.test(y))), rfb.flatMap(x => x.bad.filter(y => /^HDR /.test(y)).map(y => x.hash + ': ' + y)).slice(0, 6).join('; '));
  const RW = await (async () => { const b = await open(`${pathToFileURL(PAGE).href}#shot=1&w=vedder&d=1010`, { width: 440, height: 900, offline: true });
    try { await new Promise(r => setTimeout(r, 500)); return JSON.parse(await b.evaluate(`(async () => { ${norm} await document.fonts.ready; const sc = document.getElementById('sc'), R = e => e.getBoundingClientRect();
      const line = norm([...document.querySelectorAll('#keep section.tnode[data-tnode="share"] > .tier .tl')].map(e => { const c = e.cloneNode(true); c.querySelectorAll('.tbk, .gl').forEach(x => x.remove()); return c.textContent; }).join(' | '));
      const row = document.querySelector('#keep .krow.kt[data-sp~="DV"]'); sc.scrollTop = sc.scrollTop + R(row).top - R(sc).top - sc.clientHeight * 0.6; await new Promise(r => setTimeout(r, 400));
      return JSON.stringify({line, pin: [...document.querySelectorAll('#pins .pl')].map(e => norm(e.textContent))}); })()`)); } finally { await b.close(); } })();
  check('r20f-region-words', (RW.line === '' /* UR7 A: the Vedder's share folds under the head's "2 a day per angler from streams" */ || /^Only 2 of the 4 may be from streams/.test(RW.line)) && !/this stream/.test(RW.line) && RW.pin.some(t => t === '↳ 2 of them from streams' || /^Trout and char · 2 a day from streams ?\(of 4 across Region 2\)$/.test(t)), JSON.stringify(RW));   // round 20h form a (the bracket on its own line)
}
async function round20c(){
  const norm = 'var norm = t => (t || "").replace(/\\s+/g, " ").trim();';
  // round 20d (owner: stacked pinned headers instead of nesting): the pin bar under the header carries the chain of sections
  // the reader is inside — the group's line first ("Trout and char · 4 a day across Region 2"), then one "↳ …" line per tier
  // whose header has slid under the bar — each ONE line, the stack ≤ 90 px at 390, the row in view below it; at How it adds up
  // only the group line remains; inside Other fish the bar is hidden. Fails before: no bar (round 20c pinned the full headers).
  const PC = [];
  for (const [hash, sp] of [['w=vedder&d=1010', 'DV'], ['w=chilliwack&d=1010&p=1', 'ST'], ['w=bellacoola&d=501&p=1', 'CT'], ['w=x35p&d=1010', 'DV'], ['w=glenn&d=615', 'RB'], ['w=klwa&d=1012', 'DV']]) {
    const b = await open(`${pathToFileURL(PAGE).href}#shot=1&${hash}`, { width: 440, height: 900, offline: true });
    try { await new Promise(r => setTimeout(r, 500));
      PC.push(JSON.parse(await b.evaluate(`(async () => { ${norm} await document.fonts.ready; const sc = document.getElementById('sc'), hdr = document.getElementById('hdr'), R = e => e.getBoundingClientRect(), bad = [], st = ST();
        const row = document.querySelector('#keep .krow.kt[data-sp~="${sp}"]'); if (!row) return JSON.stringify({hash: location.hash.slice(8), bad: ['no row for ${sp}']});
        const settle = () => new Promise(r => setTimeout(r, 350)), bar = () => document.querySelector('#pins .pinbar'), lines = () => bar() && !bar().hidden ? [...bar().querySelectorAll('.pl')] : [];
        sc.scrollTop = sc.scrollTop + R(row).top - R(sc).top - sc.clientHeight * 0.6; await settle();
        const top = R(hdr).bottom, art = row.closest('article.row.ledger'), g = (st.rows[+(art.dataset.row || 'r0').slice(1)] || {}).group, root = [].concat(g.tree)[0], name = norm(art.querySelector('.rtitle')?.firstChild?.textContent);
        const L = lines(), y = top, chain = [...art.querySelectorAll('section.tnode')].filter(tn => { const r = R(tn); return r.top < y && r.bottom > y + 26 && R(tn.querySelector(':scope > .tier')).top < y + 26 * (1 + 0); });
        if (!L.length) bad.push('no pinned lines'); else {
          if (Math.abs(R(bar()).top - top) > 1.5) bad.push('bar at ' + Math.round(R(bar()).top - top) + 'px under the header');
          if (!norm(L[0].textContent).startsWith(name + ' · ') || !new RegExp('(^|[^0-9])' + root.n + '([^0-9]|$)').test(norm(L[0].textContent))) bad.push('group line ' + JSON.stringify(norm(L[0].textContent)));
          L.slice(1).forEach(l => { if (!/^↳ /.test(norm(l.textContent))) bad.push('a deeper line without ↳: ' + norm(l.textContent)); });
          L.forEach(l => { const lh = parseFloat(getComputedStyle(l).lineHeight); if (R(l).height > lh * 2.6) bad.push('a pinned line of ' + Math.round(R(l).height / lh) + ' lines: ' + norm(l.textContent).slice(0, 40)); if (l.scrollWidth > l.clientWidth + 1) bad.push('a pinned line cut: ' + norm(l.textContent).slice(0, 40)); });
          if (R(bar()).height > 90) bad.push('stack ' + Math.round(R(bar()).height) + 'px');
          // every section containing the pin line whose header has slid under the bar has a line, in order; no other line
          /* UR6 (owner 2): a line stays until its section's last row has gone under the stack (the round-20h next-head rule retired) */
          const endOf = tn => { let m = -Infinity; tn.querySelectorAll('.krow, .tier').forEach(e => { const q = R(e); if (q.height > 0 && q.bottom > m) m = q.bottom; }); return m; };
          const nextIn = tn => { const nx = (tn.nextElementSibling || tn.parentElement?.closest('section.tnode')?.nextElementSibling)?.querySelector(':scope > .tier, :scope > .ghd'); return nx ? R(nx).top < R(sc).bottom - 26 : false; };   /* round 20h: a line ends once the next head is in view */
          const leadA = !!art.querySelector('[data-tot="a"]');   /* form a: the group line says the share */
          const want = [...art.querySelectorAll('section.tnode')].filter(tn => { const r = R(tn), tl = tn.querySelector(':scope > .tier'); return tl && R(tl).top < R(bar()).bottom && endOf(tn) > R(bar()).bottom + 12 && !(leadA && tn.dataset.tnode === 'share'); })   /* UR6: head (going) under the stack, rows still below it */.map(tn => tn.dataset.tid);
          const must = want.filter(id => R(art.querySelector('section.tnode[data-tid="' + id + '"] > .tier')).bottom < R(bar()).bottom);   /* a head wholly under the stack must have its line; one half under may */
          if (L.length - 1 > want.length || L.length - 1 < must.length) bad.push('lines ' + (L.length - 1) + ' for sections ' + JSON.stringify(want) + ' (must ' + JSON.stringify(must) + ')');
          if (R(row).top < R(bar()).bottom - 1) bad.push('the row is under the bar'); }
        const res = {hash: location.hash.slice(8), stack: bar() && !bar().hidden ? Math.round(R(bar()).height) : 0, lines: L.map(l => norm(l.textContent))};
        // at How it adds up: the group line only; in Other fish: nothing
        const wk = art.querySelector('.worked'); if (wk) { sc.scrollTop = sc.scrollTop + R(wk).top - R(sc).top - top + R(sc).top + 40; await settle(); const L2 = lines(); res.atWorked = L2.map(l => norm(l.textContent)); if (L2.length !== 1) bad.push('at How it adds up: ' + L2.length + ' lines'); }
        const ol = document.querySelector('#keep .olist'); if (ol) { sc.scrollTop = sc.scrollTop + R(ol).top - R(sc).top - top + R(sc).top + 40; await settle(); if (lines().length) bad.push('a bar inside Other fish: ' + lines().map(l => norm(l.textContent)).join(' / ')); }
        return JSON.stringify({...res, bad}); })()`)));
    } finally { await b.close(); } }
  const pcb = PC.filter(x => x.bad.length);
  check('r20d-pinned-stack', PC.length === 6 && !pcb.length && PC.some(x => x.lines.length >= 2) && PC.every(x => x.stack <= 90),   /* round 20h form a: the share rides the group line, so two lines is a stack */ pcb.map(x => `${x.hash}: ${x.bad.slice(0, 5).join(' | ')}`).join('; ') || `stacks ${JSON.stringify(PC.map(x => [x.hash, x.stack, x.lines]))}`);

  // round 20d: FLAT — no bordered section, one indent level (every row and every section header's text on one edge, the
  // group head on the card's edge), no rail; a size section is a tinted band without a border. The group head is two lines:
  // the name line and ONE line for the total at phone width.
  const FL = [];
  for (const hash of ['w=vedder&d=1010', 'w=chilliwack&d=1010&p=1', 'w=bellacoola&d=501&p=1', 'w=x35p&d=1010', 'w=glenn&d=615', 'w=klwa&d=1012']) await page(hash, async b => {
    FL.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} const bad = [], R = e => e.getBoundingClientRect(), cs = e => getComputedStyle(e); let bands = 0, heads = 0;
      document.querySelectorAll('#keep article.row.ledger').forEach(art => { const pin = art.querySelector(':scope > .ghd.gpin'); if (!pin) return; heads++;
        const num = pin.querySelector('.num'), lh = num ? parseFloat(cs(num).lineHeight) : 0;
        const lead = num?.querySelector('.glead'), leadH = lead ? R(lead).height : 0;   /* round 20h form a: the share's lead line sits above the total line */
        if (R(pin).height > 66 + leadH) bad.push('group head ' + Math.round(R(pin).height) + 'px');
        if (num && R(num).height - leadH > lh * 1.6) bad.push('the total line wraps: ' + norm(num.textContent).slice(0, 50));
        /* round 20h (owner 2): the head paints the card (one surface), no band */
        const edge = Math.round(R(pin).left + parseFloat(cs(pin).paddingLeft));
        art.querySelectorAll('section.tnode').forEach(tn => { const c = cs(tn); if (['Right', 'Left'].filter(sd => !(sd === 'Left' && tn.dataset.tnode === 'size')).some(sd => c['border' + sd + 'Style'] !== 'none' && parseFloat(c['border' + sd + 'Width']) > 0) || (tn.dataset.tnode !== 'size' && ['Top', 'Bottom'].some(sd => c['border' + sd + 'Style'] !== 'none' && parseFloat(c['border' + sd + 'Width']) > 0))) bad.push('a bordered section: ' + tn.dataset.tnode);   /* round 20h: the size section's two hairlines are the one allowed; UR6: and its left rule (its extent) */
          const ch = tn.querySelector(':scope > .tch'); if (ch && (parseFloat(cs(ch).paddingLeft) > 0 || parseFloat(cs(ch).marginLeft) > 0 || cs(ch).borderLeftStyle !== 'none')) bad.push('a rail under ' + tn.dataset.tnode);
          if (tn.dataset.tnode === 'size') { bands++; if (!/rgba\\(0, 0, 0, 0\\)|transparent/.test(c.backgroundColor) || parseFloat(c.borderTopWidth) < 1 || parseFloat(c.borderBottomWidth) < 1) bad.push('the size section is a band, not a hairlined section'); } });   /* round 20h (owner 2): one surface */
        const szIn = e => 0;   /* UR6 put the size section's text one rule-and-gap in; UR7 (owner B): the rule is flush with the screen's edge and the section's text is back on the page's text edge */
        const tls = [...art.querySelectorAll('.tier .tl')].map(e => Math.round(R(e).left + parseFloat(cs(e).borderLeftWidth) + parseFloat(cs(e).paddingLeft) - szIn(e)));
        const rows = [...art.querySelectorAll('.krow.kt')].map(e => Math.round(R(e).left + parseFloat(cs(e).paddingLeft) - szIn(e)));
        const edges = new Set([...tls, ...rows]); if (edges.size > 1) bad.push('text edges ' + JSON.stringify([...edges]));
        if (rows.length && [...edges][0] - edge > 40) bad.push('rows ' + ([...edges][0] - edge) + 'px in'); });
      return {hash: location.hash.slice(8), heads, bands, bad}; })())`)));
  });
  const flb = FL.filter(x => x.bad.length);
  check('r20d-flat', FL.length === 6 && FL.reduce((a, x) => a + x.bands, 0) >= 4 && !flb.some(x => x.bad.some(y => /section|rail|edges|px in|band/.test(y))), flb.flatMap(x => x.bad.filter(y => /section|rail|edges|px in|band/.test(y)).map(y => x.hash + ': ' + y)).slice(0, 6).join('; '));
  check('r20d-compact-head', FL.reduce((a, x) => a + x.heads, 0) >= 6 && !flb.some(x => x.bad.some(y => /group head|total line/.test(y))), flb.flatMap(x => x.bad.filter(y => /group head|total line/.test(y)).map(y => x.hash + ': ' + y)).join('; '));

  // style tokens: on the Keep tab at most four font sizes beside the strip's 10 px month axis, at most four weights, one link
  // colour, no underlined link, ALL-CAPS only on .lab / .scope / .bkh, every button or link reaching 32 px (its hit area)
  const ST20 = [];
  for (const hash of ['w=vedder&d=1010', 'w=x35p&d=1010', 'w=klwa&d=1012', 'w=glenn&d=615']) await page(hash, async b => {
    ST20.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const keep = document.getElementById('keep'), R = e => e.getBoundingClientRect(), vis = e => { const r = R(e); return r.width > 0 && r.height > 0; };
      const els = [...keep.querySelectorAll('*')].filter(e => vis(e) && !e.closest('.sbar, .smonths, .slegend, svg') && [...e.childNodes].some(n => n.nodeType === 3 && n.textContent.trim()));
      const sizes = new Set(els.map(e => parseFloat(getComputedStyle(e).fontSize).toFixed(1))), weights = new Set(els.map(e => getComputedStyle(e).fontWeight));
      const links = [...keep.querySelectorAll('a, button.linkbtn, .golink, .linkrow, .book')].filter(vis), lc = new Set(links.map(e => getComputedStyle(e).color)), ul = links.filter(e => getComputedStyle(e).textDecorationLine.includes('underline')).length;
      const caps = els.filter(e => getComputedStyle(e).textTransform === 'uppercase' && !e.matches('.lab, .scope, .bkh, .nkt .lab, .nofish h3')).map(e => e.className || e.tagName);   // the section kickers
      const hit = e => { const r = R(e); const ps = getComputedStyle(e, '::before'); if (ps.position === 'absolute' && ps.content !== 'none') { const t = parseFloat(ps.top) || 0, bt = parseFloat(ps.bottom) || 0; return r.height - t - bt; } return r.height; };
      const small = [...keep.querySelectorAll('a, button, summary')].filter(vis).filter(e => hit(e) < 32).map(e => (e.className || e.tagName) + ' ' + Math.round(hit(e)));
      return {hash: location.hash.slice(8), sizes: [...sizes].sort(), weights: [...weights].sort(), linkColours: lc.size, underlined: ul, caps, small}; })())`)));
  });
  const stb = ST20.filter(x => x.sizes.length > 4 || x.weights.length > 4 || x.linkColours > 1 || x.underlined || x.caps.length || x.small.length);
  check('r20c-style-tokens', ST20.length === 4 && !stb.length, stb.map(x => `${x.hash}: ${x.sizes.length} sizes ${JSON.stringify(x.sizes)}, ${x.weights.length} weights ${JSON.stringify(x.weights)}, ${x.linkColours} link colours, ${x.underlined} underlined, caps ${JSON.stringify(x.caps.slice(0, 4))}, small ${JSON.stringify(x.small.slice(0, 4))}`).join('; '));
}
// ---------- round 20b (owner: fewer words, real visuals; split gear and licence tabs) ----------
async function round20b(){
  const norm = 'var norm = t => (t || "").replace(/\\s+/g, " ").trim();';
  // three tabs: What you can keep · Gear · Licence on ONE row at 390 px; each tab is its own section (#keep, #gear holding
  // Gear & methods + the Reference fold, #licence holding the licence answer alone); each tab button lands on its section
  // and reads pressed; sec=licence lands there; no words of the old combined tab; no dead link (every [data-sec] names a
  // section that exists, every [data-jump] target exists)
  const TB = [];
  for (const hash of ['w=vedder&d=1010', 'w=klwa&d=1010', 'w=vedder&d=1010&sec=licence', 'w=klwa&d=1010&sec=gear']) await page(hash, async b => {
    TB.push(JSON.parse(await b.evaluate(`(async () => { ${norm} const bad = [], R = e => e.getBoundingClientRect();
      const tabs = [...document.querySelectorAll('#hdr .segm[role=tablist] button')], names = tabs.map(t => norm(t.textContent));
      if (JSON.stringify(names) !== '["What you can keep","Gear","Licence"]') bad.push('tabs ' + JSON.stringify(names));
      if (new Set(tabs.map(t => Math.round(R(t).top))).size !== 1) bad.push('tabs not on one row');
      if (tabs.some(t => R(t).right > R(document.getElementById('hdr')).right + 0.5)) bad.push('a tab off the right edge');
      const keep = document.getElementById('keep'), gear = document.getElementById('gear'), lic = document.getElementById('licence');
      if (!keep || !gear || !lic) bad.push('sections ' + [!!keep, !!gear, !!lic].join(' '));
      if (lic && gear && gear.contains(lic)) bad.push('#licence inside #gear');
      if (gear && (gear.querySelector('#g-licence') || !gear.querySelector('#g-gear'))) bad.push('#gear holds ' + (gear.querySelector('#g-licence') ? 'the licence' : 'no gear'));
      if (lic && (!lic.querySelector('#g-licence') || lic.querySelector('#g-gear, #g-always'))) bad.push('#licence holds other sections');
      if (gear && !gear.querySelector('#g-always')) bad.push('the Reference fold left the gear tab');
      const body = norm(document.getElementById('sc').textContent); if (/Gear & licence|Gear &amp; licence/.test(body)) bad.push('old combined tab named');
      document.querySelectorAll('[data-sec]').forEach(e => { if (!document.getElementById(e.dataset.sec)) bad.push('dead data-sec ' + e.dataset.sec); });
      document.querySelectorAll('[data-jump]').forEach(e => { if (!document.getElementById(e.dataset.jump)) bad.push('dead data-jump ' + e.dataset.jump); });
      const pressed = () => tabs.find(t => t.getAttribute('aria-pressed') === 'true')?.dataset.sec;
      const landed = pressed(), sc = document.getElementById('sc'), hh = document.getElementById('hdr').offsetHeight;
      const want = /sec=licence/.test(location.hash) ? 'licence' : /sec=gear/.test(location.hash) ? 'gear' : 'keep';
      if (landed !== want) bad.push('hash sec=' + want + ' landed on ' + landed);
      const clicks = {};
      for (const sec of ['gear', 'licence', 'keep']) { const tb = tabs.find(t => t.dataset.sec === sec); if (!tb) { bad.push('no tab ' + sec); continue; } tb.click(); await new Promise(r => setTimeout(r, 1000));
        const el = document.getElementById(sec), off = sec === 'keep' ? sc.scrollTop : Math.round(el.offsetTop - sc.scrollTop - hh); clicks[sec] = [pressed(), off];
        if (pressed() !== sec) bad.push('click ' + sec + ' pressed ' + pressed()); if (sec !== 'keep' && Math.abs(off + 2) > 3 && !(sc.scrollTop + sc.clientHeight >= sc.scrollHeight - 2)) bad.push('click ' + sec + ' lands ' + off + 'px off'); }
      return JSON.stringify({hash: location.hash.slice(8), names, landed, clicks, bad}); })()`)));
  });
  const tbb = TB.filter(x => x.bad.length);
  // round 20e (owner 1): r20b-three-tabs retired — four tabs (r20e-four-tabs)

  // the card, fewer words: (C1) a number never appears twice in a row on a tier and its only kind — an own tier whose
  // only child is one kind at its number names the fish and the kind row draws no sentence (its size chip and pictures
  // stay); (book) a Rule book page is shown once per branch (never on a tier when a line above it already shows it);
  // (C4) the keep-N lines are the two ends; (b) a kind's sentence keeps one line spacing with "How to tell ›" inside it
  const WC = [], WCS = ['w=vedder&d=1010', 'w=chilliwack&d=1010&p=1', 'w=glenn&d=615', 'w=x35p&d=1010', 'w=bellacoola&d=1010&p=1', 'w=klwa&d=1010', 'w=ape&d=501'];
  for (const hash of WCS) await page(hash, async b => {
    WC.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} ${TOPS} const st = ST(), bad = [], R = e => e.getBoundingClientRect(); let folded = 0, twice = 0, books = 0, workedHere = [], tight = [];
      const pagesOf = key => (SNAP.ruleInfo[key]?.book || []).map(b => b[0]);
      document.querySelectorAll('#keep article.row.ledger').forEach(art => { const g = (st.rows[+(art.dataset.row || 'r0').slice(1)] || {}).group; if (!g || !g.tree) return;
        const T = topsOf(g, art); if (!T.root) return;
        // (C1) every tier with exactly one kind child
        art.querySelectorAll('section.tnode').forEach(sec => { const kids = [...(sec.querySelector(':scope > .tch')?.children || [])], tn = norm(sec.querySelector(':scope > .tier .tn')?.textContent);
          if (kids.length === 1 && kids[0].matches('.krow.kt')) { const kn = norm(kids[0].querySelector('.ks .kn')?.textContent);
            if (kn && kn === tn) { twice++; bad.push('C1 ' + hdrWords(sec.querySelector(':scope > .tier .tl')).slice(0, 50) + ': its only kind repeats ' + kn); } } });
        const walkK = (t, el) => { if (t.node === 'kind') { if (t.folded) { folded++; if (el?.querySelector('.ks')) bad.push('C1 ' + t.says + ': folded but a sentence drawn'); if (t.n && !el?.querySelector('[data-size]')) bad.push('C1 ' + t.says + ': folded, no size chip'); if (!el?.querySelector('.kpics')) bad.push('C1 ' + t.says + ': folded, no pictures'); }
            else if (!el?.querySelector('.ks')) bad.push('C1 ' + t.says + ': no sentence'); return; }
          const kids = [...(el?.querySelector(':scope > .tch')?.children || [])]; t.children.forEach((c, i) => walkK(c, kids[i])); };
        T.nodes.forEach((t, i) => walkK(t, T.els[i]));
        // (book) a page once per branch: walk tiers with the pages shown above (the header's root pages first)
        const seen0 = new Set(pagesOf(st.ruleKey?.[T.root.by]));
        if (T.hdr && !art.querySelector(':scope > .ghd a.book') && !art.querySelector('[data-tier-origin] a.book') && seen0.size) bad.push('BOOK header shows no Rule book page');   /* rows 24: the root's page may ride the share tier's origin line */
        const walkB = (t, el, seen) => { if (t.node !== 'tier') return; const key = st.ruleKey?.[t.by], mine = key ? pagesOf(key) : [], shown = [...(el?.querySelectorAll(':scope > .tier .tbk a.book') || [])].filter(a => !a.closest('[data-tier-origin]')).map(a => norm(a.textContent));   /* the origin line's page is the root's, not this tier's */
          const fresh = mine.filter(p => !seen.has(p));
          if (shown.length) { books++; if (!fresh.length) bad.push('BOOK ' + t.says.slice(0, 40) + ': repeats a page shown above (' + shown.join(', ') + ')'); }
          else if (fresh.length) bad.push('BOOK ' + t.says.slice(0, 40) + ': its page p.' + fresh.join('/') + ' shown nowhere above');
          const below = new Set([...seen, ...mine]), kids = [...(el?.querySelector(':scope > .tch')?.children || [])]; t.children.forEach((c, i) => walkB(c, kids[i], below)); };
        T.nodes.forEach((t, i) => walkB(t, T.els[i], seen0));
        // (C4) the keep-N lines drawn are the data's, and the data has at most two
        const here = (g.worked || []).filter(w => w.at === 'here'), li = [...art.querySelectorAll('.worked li.wh')].map(e => norm(e.textContent));
        if (here.length) { workedHere.push(here.map(w => w.kept)); if (here.length > 2) bad.push('C4 worked keep-N lines ' + here.length); if (li.length !== here.length) bad.push('C4 drawn keep-N lines ' + li.length + ' of ' + here.length); }
        // (b) a sentence with How to tell inside keeps one line spacing
        art.querySelectorAll('.krow.kt .ks').forEach(ks => { const btn = ks.querySelector('button'); if (!btn) return; const lh = parseFloat(getComputedStyle(ks).lineHeight), lines = Math.round(R(ks).height / lh);
          tight.push([Math.round(R(ks).height), Math.round(lh * lines)]); if (R(ks).height > lh * lines + 3 || R(btn).height > lh * 1.4) bad.push('B uneven lines: ' + norm(ks.textContent).slice(0, 40) + ' ' + Math.round(R(ks).height) + 'px for ' + lines + ' lines; button ' + Math.round(R(btn).height) + 'px'); }); });
      return {hash: location.hash.slice(8), folded, twice, books, workedHere, tight, bad}; })())`)));
  });
  const wcb = WC.filter(x => x.bad.length), sum = k => WC.reduce((a, x) => a + x[k], 0);
  check('r20b-number-once', WC.length === WCS.length && sum('folded') >= 3 && !WC.some(x => x.bad.some(y => /^C1 /.test(y))), WC.flatMap(x => x.bad.filter(y => /^C1 /.test(y)).map(y => x.hash + ': ' + y)).slice(0, 5).join('; ') || `folded ${sum('folded')}`);
  check('r20b-book-once', sum('books') >= 2 && !WC.some(x => x.bad.some(y => /^BOOK /.test(y))), WC.flatMap(x => x.bad.filter(y => /^BOOK /.test(y)).map(y => x.hash + ': ' + y)).slice(0, 5).join('; ') || `books ${sum('books')}`);
  const v = WC.find(x => x.hash === 'w=vedder&d=1010');
  check('r20b-worked-ends', JSON.stringify(v?.workedHere) === '[[4,2]]' && !WC.some(x => x.bad.some(y => /^C4 /.test(y))), `vedder ${JSON.stringify(v?.workedHere)}; ` + WC.flatMap(x => x.bad.filter(y => /^C4 /.test(y))).join('; '));
  check('r20b-howtell-inline', WC.some(x => x.tight.length) && !WC.some(x => x.bad.some(y => /^B /.test(y))), WC.flatMap(x => x.bad.filter(y => /^B /.test(y)).map(y => x.hash + ': ' + y)).join('; ') || `measured ${JSON.stringify(WC.flatMap(x => x.tight))}`);
}
// ---------- round 20 (owner review of v24) ----------
async function round20(){
  const norm = 'var norm = t => (t || "").replace(/\\s+/g, " ").trim();';
  // (1) the group's header and the tree's first tier are ONE block: the header block carries the root tier's words whole
  // with its number big, the ⓘ and its Rule book page; no total tier is drawn; the root's children are the top of the tree,
  // flush with it (no guide line, no indent). A total the tributary walk brought (Ape Creek) says why under it.
  // (2) the share sits inside the total: "Only 2 of the 4 may be from this stream", lifted ", not counting hatchery rainbow
  // trout"; nowhere "count toward the 4, not these 2" / "don’t use these 2".
  // (3) every tier's number is as big as the header's (a size box's, a count tier's), and the box's second number too.
  const R20 = [], R20S = ['w=vedder&d=1010', 'w=chilliwack&d=1010&p=1', 'w=bellacoola&d=501&p=1', 'w=bellacoola&d=1010&p=1', 'w=x35p&d=1010', 'w=klwa&d=1010', 'w=ape&d=501'];
  for (const hash of R20S) await page(hash, async b => {
    R20.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} ${TOPS} const st = ST(), bad = [], out = [], R = e => e.getBoundingClientRect();
      const tl = (art, t) => [...art.querySelectorAll('section.tnode[data-tnode="' + t + '"] > .tier .tl')].map(e => hdrWords(e));
      document.querySelectorAll('#keep article.row.ledger').forEach(art => { const g = (st.rows[+(art.dataset.row || 'r0').slice(1)] || {}).group; if (!g || !g.tree) return;
        const T = topsOf(g, art), o = {root: T.root ? T.root.says : null, hdr: hdrWords(T.hdr), why: norm(T.hdr?.querySelector('[data-why-here]')?.textContent), share: tl(art, 'share'), sizes: [], big: 0, nums: []};
        if (!T.root) { bad.push('header.tier does not name the root'); out.push(o); return; }
        if (!T.hdr) bad.push('no root line in the header block');
        else { if (o.hdr !== norm(T.root.says)) bad.push('header ' + JSON.stringify(o.hdr) + ' vs ' + JSON.stringify(T.root.says));
          const big = T.hdr.querySelector('.ghn'); if (T.root.n != null && norm(big?.textContent) !== String(T.root.n)) bad.push('header number ' + norm(big?.textContent));
          if (st.ruleKey?.[T.root.by] && !art.querySelector(':scope > .ghd a.book') && !art.querySelector('[data-tier-origin] a.book')) bad.push('no Rule book page in the group head');   // round 20d: in the block under the head; rows 24: or on the share tier's origin line, where the head has no origin
          if (T.root.why_here && !o.why) bad.push('why_here not drawn');
          o.big = big ? parseFloat(getComputedStyle(big).fontSize) : 0; }
        if (art.querySelector('section.tnode[data-tnode="total"]')) bad.push('a total tier drawn');
        if (T.els.length !== T.nodes.length) bad.push(T.els.length + ' top nodes of ' + T.nodes.length);
        const q = art.querySelector(':scope > .qtree'); if (q && T.els.length && Math.abs(R(T.els[0]).left - R(q).left) > 1) bad.push('top nodes indented');
        o.nums = [...art.querySelectorAll('.tier .tn')].map(e => [e.closest('.tier').dataset.tier, parseFloat(getComputedStyle(e).fontSize), norm(e.textContent)]);
        const walk = t => { if (t.node !== 'tier') return; if (t.tier === 'size' && t.extra) { const el = art.querySelector('section.tnode[data-tid="' + t.id + '"] > .tier .tl');
            o.sizes.push({says: t.says, n: t.n, extra: t.extra.n, second: norm(el?.querySelector('.tn2')?.textContent), secondPx: el?.querySelector('.tn2') ? parseFloat(getComputedStyle(el.querySelector('.tn2')).fontSize) : 0}); }
          (t.children || []).forEach(walk); };
        T.nodes.forEach(walk);
        out.push(o); });
      return {hash: location.hash.slice(8), out, bad}; })())`)));
  });
  const v = R20.find(x => x.hash === 'w=vedder&d=1010'), c = R20.find(x => x.hash.startsWith('w=chilliwack')), ape = R20.find(x => x.hash.startsWith('w=ape'));
  const hb = R20.filter(x => !x.out.length || x.bad.length);
  check('r20-header-is-total', R20.length === R20S.length && !hb.length && v?.out[0]?.hdr === '4 a day per angler across Region 2, lakes included' && !!ape?.out[0]?.why && /^This river’s rule: up to \d+ a day per angler$/.test(R20.find(x => x.hash === 'w=bellacoola&d=1010&p=1')?.out[0]?.hdr || ''),
    hb.map(x => `${x.hash}: ${x.bad.slice(0, 4).join(' | ')}`).join('; ') || `vedder header ${JSON.stringify(v?.out[0]?.hdr)}; ape why ${JSON.stringify(ape?.out[0]?.why)}; bella ${JSON.stringify(R20.find(x => x.hash === 'w=bellacoola&d=1010&p=1')?.out[0]?.hdr)}`);
  const stale = R20.flatMap(x => x.out.flatMap(o => o.share)).filter(t => /count toward the \d+, not these|don’t use these|\(part of the \d+\)$/.test(t));
  check('r20-share-inside-total', JSON.stringify(v?.out[0]?.share) === '[]' /* UR7 A: the Vedder's share folds too, its lifted fish said by the own tier's relation */ && JSON.stringify(c?.out[0]?.share) === '[]' && !stale.length,   /* int1 (coordinator after v34): Chilliwack's plain share ("Only 2 of the 4 may be from streams") folds under the head's lead; the Vedder's lifted share keeps its line */
    `vedder ${JSON.stringify(v?.out[0]?.share)}; chilliwack ${JSON.stringify(c?.out[0]?.share)}; stale ${JSON.stringify(stale)}`);
  const small = R20.flatMap(x => x.out.flatMap(o => o.nums.filter(([, px]) => o.big && px < o.big - 0.5).map(n => `${x.hash}: ${n[0]} ${n[2]} at ${n[1]}px (header ${o.big}px)`)));
  const seconds = R20.flatMap(x => x.out.flatMap(o => o.sizes.map(s => ({...s, big: o.big, hash: x.hash})))), sb = seconds.filter(s => s.second !== String(s.n + s.extra) || s.secondPx < s.big - 0.5);
  check('r20-big-numbers', R20.some(x => x.out.some(o => o.nums.some(n => n[0] === 'size'))) && R20.some(x => x.out.some(o => o.nums.some(n => n[0] === 'count'))) && seconds.length >= 2 && !small.length && !sb.length,
    small.slice(0, 4).concat(sb.map(s => `${s.hash}: second number ${JSON.stringify(s.second)} at ${s.secondPx}px for ${s.says}`)).join('; ') || `no size/count tier or second number seen`);
}
// (L) Licence: each tab shows only the default angler's (B.C. resident) answer; every other residency is a "Rules for
// other anglers" dropdown of its own, carrying that residency's 16-or-older AND under-16 answers (the snapshot's own words)
const L13 = [];
for (const age of ['adult', 'u16']) await page(`w=vedder&d=1010&age=${age}&at=%23g-licence`, async b => {
  L13.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const st = ST(), L = document.getElementById('g-licence'), norm = t => (t || '').replace(/\\s+/g, ' ').trim();
    const box = h => { if (!h) return ''; const d = document.createElement('div'); d.innerHTML = h; return norm((d.querySelector('.classbox') || d.querySelector('.needs li'))?.textContent); };
    const outside = L.cloneNode(true); outside.querySelectorAll('details').forEach(d => d.remove()); const out = norm(outside.textContent);
    const res = [['non_resident', 'u16_non', 'adult_non'], ['non_resident_alien', 'u16_alien', 'adult_alien']].map(([r, u, a]) => {
      const d = L.querySelector('details[data-res="' + r + '"]'), t = norm(d?.textContent);
      return {r, fold: !!d, closed: d ? !d.open : null, named: d ? /^If you’re not a B\.C\. resident · /.test(d.querySelector('summary')?.textContent || '') : false,
        u16: !!st.licence[u] && !!box(st.licence[u]) && t.includes(box(st.licence[u])), adult: !!st.licence[a] && !!box(st.licence[a]) && t.includes(box(st.licence[a])),
        leak: !!st.licence[u] && !!box(st.licence[u]) && box(st.licence[u]) !== box(st.licence[${JSON.stringify(age === 'u16' ? 'u16_res' : 'adult')}]) && out.includes(box(st.licence[u])) }; });
    const mine = box(st.licence[${JSON.stringify(age === 'u16' ? 'u16_res' : 'adult')}]);
    return {age: ${JSON.stringify(age)}, blocks: outside.querySelectorAll('.u16h').length, mine: !!mine && out.includes(mine), res}; })())`)));
});
const lb13 = L13.filter(x => x.blocks > 0 || !x.mine || x.res.some(r => !r.fold || !r.closed || !r.named || !r.u16 || !r.adult || r.leak));
// round 20e (owner 5): retired — no dropdowns; every residency × age is a choice of its own (r20e-licence-selectors)

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
// bucket-replaces: retired in round 18 (the card is group.tree; the bucket display is gone) -> own-tier-why-here
const nkb = C13.filter(x => !x.missing && x.nk.some(n => !n.row || n.cls !== n.status || n.mark !== (n.status === 'closed' ? '✕' : '') || !n.whose || !n.book));
check('not-kept-said', C13.some(x => x.nk && x.nk.some(n => n.status === 'closed')) && C13.some(x => x.nk && x.nk.some(n => n.status === 'release')) && !nkb.length,
  nkb.map(x => `${x.name}: ${JSON.stringify(x.nk)}`).join('; ') || 'no closed AND release line checked');
const zb = C13.filter(x => !x.missing && x.zero.some(z => /\bcm\b/.test(z.det) || z.det !== z.want));
// zero-kind-no-size: retired in round 18 (the card is group.tree; the bucket display is gone) -> tree-folders (no size chip on a fish kept none)
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
  );   // round 20e: the other residencies' dropdown order is gone (they are choices: r20e-licence-selectors)
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
          kinds: sg.kinds.every(k => kids.some(x => x.nm === norm(k.name) && x.det === norm(k.detail).replace(/(\\d) cm/g, '$1 cm').replace(/ /g, ' ') && x.n === (k.n ? String(k.n) : ''))),
          before: later.every(e => el.compareDocumentPosition(e) & Node.DOCUMENT_POSITION_FOLLOWING),
          once: sg.kinds.every(k => [...art.querySelectorAll('.krow .nm')].filter(e => norm(e.textContent) === norm(k.name)).length === 1)}); })); });
    return {at: location.hash, out}; })())`)));
});
const nb14 = N14.filter(x => !x.out.length || x.out.some(o => !o.el || !o.first || !o.shown || !o.kinds || !o.before || !o.once));
// nested-count-group: retired in round 18 (the card is group.tree; the bucket display is gone) -> tree-folders

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
    const TK = g => [].concat(g?.tree || []).flatMap(function f(t) { return t.node === 'kind' ? [t] : (t.children || []).flatMap(f); });
    const artOf = i => arts.find(a => a.dataset.row === 'r' + i);
    let placed = 0;
    st.rows.forEach((r, i) => { const g = r.group, art = artOf(i);
      // round 18: the kinds are the tree's (group.tree), each row .kt[data-kind]
      TK(g).forEach(k => { if (!k.presence) return; placed++;
        const row = [...(art?.querySelectorAll('.krow.kt') || [])].find(e => e.dataset.kind === k.name);
        out.push({where: 'kind ' + k.name, ok: !!row && norm(row.textContent).includes(norm(k.presence.says))}); });
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
    const words = [...new Set([...st.rows.flatMap(r => [r.presence, ...(r.group?.not_kept || []).map(x => x.presence), ...TK(r.group).map(k => k.presence)]).filter(Boolean).map(p => norm(p.says))])];
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
    const bare = [...document.querySelectorAll('#keep .kn, #keep .num, #keep .bkn, #keep .qgn, #keep .tn')].filter(e => own(e) === '0')
      .map(e => norm(e.closest('.krow, .rhead')?.textContent).slice(0, 70));
    const w = document.querySelector('#keep .worked'), prev = w?.previousElementSibling;
    // persona (round 15): a kind's number that only repeats its bucket's shared number (2+ kinds) is not drawn; the
    // not-kept lines come after "How it adds up"
    const st = ST(), rep = [], order = [];
    document.querySelectorAll('#keep article.row.ledger').forEach(art => { const g = (st.rows[+(art.dataset.row || 'r0').slice(1)] || {}).group; if (!g || !g.headline) return;
      const secs = [...art.querySelectorAll('section.bkt')], wk = art.querySelector('.worked'), nk = art.querySelector('.nkt');
      if (wk && nk) order.push(!!(wk.compareDocumentPosition(nk) & Node.DOCUMENT_POSITION_FOLLOWING));
      (g.buckets || []).forEach((bk, i) => { const n = bk.cap ?? +((bk.between || '').match(/\d+/) || [])[0]; if (!n || bk.kinds.length + (bk.groups || []).length < 2) return;
        /* round 17 (owner): a kind's own number is ALWAYS drawn now (kind-number-always), so no repeat is hidden */ }); });
    // round 16 (2): "How it adds up" now closes its group (worked-last), so the order is no longer held here
    return {at: location.hash, bare, rep, order: true, aboveWorked: prev && !prev.matches('section.bkt, .qtree') ? [...prev.querySelectorAll('.kn, .num')].map(own).filter(t => /^\\d+$/.test(t)) : []}; })())`)));
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
// round 17 (owner): size-group-box retired — the size cap is a compact line without pictures now (size-cap-line)
const bb15 = S15.filter(x => x.bw.some(z => !z.el || !z.big || !z.words || !z.once));
// bucket-between-big: retired in round 18 (the card is group.tree; the bucket display is gone) -> tree-folders

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
// round 20e (owner 5): retired — the per-residency under-16 list is the residency choice now (r20e-licence-selectors)

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

// ---------- round 15 follow-up ----------
// a closed card says each sentence once: the strip's status and reopening, the banner's headline (split at " · "), its
// sentences and its link's sentences, compared as words (case, punctuation and "again" aside) — Vedder Jun 15 said
// "opens Jul 1" three times; and the two nearer-row tributaries are in the snapshot on Jun 15 with quick pills
const C15 = [];
for (const hash of ['w=vedder&d=615', 'w=cowichan&d=1010&p=0']) await page(hash, async b => {
  C15.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const K = document.getElementById('keep'), key = t => (t || '').toLowerCase().replace(/\\bagain\\b/g, '').replace(/[^a-z0-9]+/g, ' ').trim();
    const ph = [...K.querySelectorAll('.wstrip .wsline > span')].map(e => e.textContent);
    K.querySelectorAll('.banner .big').forEach(e => ph.push(...e.textContent.split(' · ')));
    K.querySelectorAll('.banner > p').forEach(e => ph.push(e.textContent));
    K.querySelectorAll('.banner .golink').forEach(e => ph.push(...e.textContent.split(/(?<=\\.)\\s+/)));
    const ks = ph.map(key).filter(Boolean), dup = ks.filter((k, i) => ks.indexOf(k) !== i);
    return {at: location.hash, banner: !!K.querySelector('.banner'), dup}; })())`)));
});
const cb15 = C15.filter(x => !x.banner || x.dup.length);
check('closed-card-once', C15.length === 2 && !cb15.length, cb15.map(x => JSON.stringify(x)).join('; '));
const T15 = await page('w=glenn&d=615', async b => JSON.parse(await b.evaluate(`JSON.stringify((() => ['glenn', 'hamill'].map(w => ({w, ok: !!WATERS[w]?.snap[615] && !!SNAP.states[[].concat(WATERS[w].snap[615])[0]],
  pill: !!document.querySelector('#c-w [data-cw="' + w + '|615"]'), name: WATERS[w]?.name})))())`)));
check('nearer-row-tribs-front', T15.every(x => x.ok && x.pill && x.name), JSON.stringify(T15));

// ---------- round 16 ----------
// (5) the gear sentence as short labelled lines from the data's parts (gear.summary.parts): a fly-only mode first, then
// Lines · Hook, lure or fly · Weight; every part the sentence says drawn once, on a labelled line (a part the sentence
// leaves out, "any hook", not drawn); no run-on sentence; ONE Book line after the lines (a link per page); the dated
// changes after it; every glossary term still has its ⓘ
const G16 = [];
for (const hash of ['w=vedder&d=1010', 'w=vedder&d=515', 'w=bellacoola&d=501&p=0', 'w=klwa&d=1010', 'w=klmb&d=1010']) await page(hash + '&sec=gear', async b => {
  G16.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const sm = ST().summary, norm = t => (t || '').replace(/\\s+/g, ' ').trim().toLowerCase();
    const box = document.querySelector('#gear [data-summary]'); if (!sm || !box) return {at: location.hash, box: !!box, sm: !!sm};
    const lines = [...box.querySelectorAll('[data-gline]')], say = norm(sm.says), txt = norm(box.textContent);
    const shown = lines.map(l => ({k: l.dataset.gline, lab: norm(l.querySelector('.glab')?.textContent), val: norm(l.querySelector('.gval')?.textContent)}));
    const stem = t => norm(t).replace(/^(a|an)\\s+/, '').replace(/^(weight on the line|lines?|hooks?)\\s+/, '');
    const books = box.querySelectorAll('.gbooks'), ch = box.querySelector('[data-change]');
    const after = (a, b) => !!a && !!b && !!(a.compareDocumentPosition(b) & Node.DOCUMENT_POSITION_FOLLOWING);
    const pages = new Set((sm.cites || []).concat((sm.changes || []).flatMap(c => c.cites || [])).flatMap(k => (SNAP.ruleInfo[k]?.book || []).map(x => x[0])));   // round 20g: + the change's rule
    return {at: location.hash, box: true, sm: true, sentence: !!box.querySelector('.gsay'), shown,
      modeFirst: !sm.parts.some(p => p.part === 'mode') || shown[0]?.k === 'mode',
      each: sm.parts.filter(p => say.includes(norm(p.says))).map(p => ({p: p.part, n: txt.split(stem(p.says)).length - 1, onLine: shown.some(s => s.val.includes(stem(p.says)))})),
      labels: shown.filter(s => s.k !== 'mode').every(s => s.lab), dropped: sm.parts.filter(p => !say.includes(norm(p.says)) && txt.includes(norm(p.says))).map(p => p.part),
      // round 20e (owner 3): no bunched Book line — each line's own source line (inside it) carries its pages, every page of
      // the sentence shown; a dated change sits inside the line it changes (r20e-dated-on-its-line)
      books: books.length, bookN: new Set([...box.querySelectorAll('[data-gline] > [data-isrc] a.book')].map(a => (a.textContent.match(/p\\.(\\d+)/) || [])[1])).size, pages: pages.size,
      booksAfter: lines.every(l => !!l.querySelector(':scope > [data-isrc]') || !sm.parts.some(p => p.cites.length && (({lines: ['lines'], hook: ['attach', 'hook'], weight: ['weight'], mode: ['mode']})[l.dataset.gline] || []).includes(p.part))), changesAfter: !ch || !!ch.closest('[data-gline]'),
      terms: (sm.terms || []).every(t => [...box.querySelectorAll('[data-gl]')].some(e => e.dataset.gl.split(',').includes(t)))}; })())`)));
});
const gb16 = G16.filter(x => !x.box || !x.sm || x.sentence || !x.modeFirst || !x.each.length || x.each.some(e => e.n !== 1 || !e.onLine) || !x.labels || x.dropped.length
  || x.books !== 0 || x.bookN !== x.pages || !x.booksAfter || !x.changesAfter || !x.terms);
check('gear-lines', G16.length === 5 && G16.some(x => x.shown?.[0]?.k === 'mode') && !gb16.length, gb16.map(x => JSON.stringify(x)).join('; '));

// (2) "How it adds up" closes its group: no bucket, kind row, count box or not-kept line of the group after it (Ape Creek
// May 1: it sat between the trout and the steelhead / char lines)
const A16 = [];
for (const hash of ['w=ape&d=501', 'w=ape&d=1010', 'w=vedder&d=515', 'w=vedder&d=1010', 'w=bellacoola&d=501&p=0']) await page(hash, async b => {
  A16.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const out = [];
    document.querySelectorAll('#keep article.row.ledger').forEach(art => { const wk = art.querySelector(':scope > .worked'); if (!wk) return;
      const later = [...art.querySelectorAll('section.bkt, .krow, .nkt, .qgrp')].filter(e => !wk.contains(e) && !!(wk.compareDocumentPosition(e) & Node.DOCUMENT_POSITION_FOLLOWING));
      out.push({nk: !!art.querySelector('.nkt'), later: later.map(e => e.textContent.replace(/\\s+/g, ' ').trim().slice(0, 50))}); });
    return {at: location.hash, out}; })())`)));
});
const ab16 = A16.filter(x => x.out.some(o => o.later.length));
check('worked-last', A16.length === 5 && A16.some(x => x.out.some(o => o.nk)) && !ab16.length, ab16.map(x => JSON.stringify(x)).join('; ') || 'no group with How it adds up AND not-kept lines checked');

// ---------- round 16 phase 2 (rows 15) ----------
// round 17: the quota table and Check my catch are gone (owner); table-is-the-data, checker-is-the-caps and check-my-catch
// are retired; the data's table / caps / oracle.json now prove the card's numbers (card-is-the-caps, round 17 block)
// (3, display 12) a rule listed in a sources sheet is headed by its effect in words (`rules[].title`, "Release all
// steelhead"), its book words under it as the quote — never the raw list item ("ALL STEELHEAD") as the heading
const RT16 = await page('w=bellacoola&d=501&p=0&sheet=' + encodeURIComponent('src:bc0_501|r0'), async b => JSON.parse(await b.evaluate(`JSON.stringify((() => {
  const rows = [...document.querySelectorAll('#sheet [data-srow]')], norm = t => (t || '').replace(/\\s+/g, ' ').trim();
  const keyOf = r => r.dataset.key;
  return {n: rows.length, bad: rows.filter(r => { const k = keyOf(r), t = k && SNAP.ruleInfo[k]?.title; return t && norm(r.querySelector('[data-rtitle]')?.textContent) !== norm(t); }).map(r => norm(r.textContent).slice(0, 60)),
    titled: rows.filter(r => r.querySelector('[data-rtitle]')).length, raw: rows.filter(r => /^ALL STEELHEAD/.test(norm(r.querySelector('span')?.textContent))).length}; })())`)));
check('rule-titles', RT16.n > 3 && RT16.titled > 3 && !RT16.bad.length && !RT16.raw, JSON.stringify(RT16));
// (Q1, display 12) a lift the stream inherits from a farther row says the nearer row's silence under it (Glenn Creek May 1)
const IC16 = await page('w=glenn&d=501', async b => JSON.parse(await b.evaluate(`JSON.stringify((() => { const st = ST(), norm = t => (t || '').replace(/\\s+/g, ' ').trim();
  const want = (st?.inherited || []).filter(x => x.caution), shown = [...document.querySelectorAll('#keep [data-caution]')];
  return {want: want.length, ok: want.every(x => shown.some(e => norm(e.textContent) === norm(x.caution) && e.previousElementSibling?.matches('[data-inherited]')))}; })())`)));
check('inherited-caution', IC16.want > 0 && IC16.ok, JSON.stringify(IC16));
// (Q2, rows 15) another row's identical statement is folded into the winner's ladder line as the data's note, once
// (Schooler Creek's Arctic grayling: "Zone B also prints this: …"), and is not a ladder line of its own
const RS16 = await page('w=x41p&d=1010', async b => JSON.parse(await b.evaluate(`JSON.stringify((() => { const st = ST(), norm = t => (t || '').replace(/\\s+/g, ' ').trim(), out = [];
  st.rows.forEach((r, i) => { if (!r.restates?.length || r.week) return;
    const o = Object.entries(OTHERS_).find(([id, v]) => id.startsWith(sid() + '|') && new RegExp('data-row="r' + i + '"').test(v.html));
    const art = document.querySelector('#keep article.row[data-row="r' + i + '"]:not(details.others article)');
    const body = o ? new DOMParser().parseFromString(sheetHtml('other:' + o[0]), 'text/html').body : art;
    r.restates.forEach(x => { const notes = [...(body?.querySelectorAll('[data-restates]') || [])].filter(e => norm(e.textContent) === norm(x.says));
      const rk = st.ruleKey?.[x.r], own = rk ? [...(body?.querySelectorAll('.ladder2 li.won .pline') || [])].filter(e => e.dataset.rule === rk).length : 0;
      out.push({row: i, notes: notes.length, inWon: !!notes[0]?.closest('li.won'), own}); }); });
  return {out}; })())`)));
check('restates-one-line', RS16.out.length > 0 && RS16.out.every(x => x.notes === 1 && x.inWon && !x.own), JSON.stringify(RS16));

// ---------- round 17 (owner: the quota table and Check my catch go; the bucket card is THE display, refined) ----------
const R17 = ['w=bellacoola&d=501&p=0', 'w=bellacoola&d=501&p=1', 'w=vedder&d=1010', 'w=vedder&d=515', 'w=x35p&d=1010', 'w=ape&d=501', 'w=hamill&d=615'];
// (1) no table UI is left: none of its code in the page, nothing drawn, and an old #qt=1 link draws the buckets
const QSRC = ['data-qtable', 'data-qform', 'qtHtml', 'Check my catch', 'qAccepts', 'qMore', 'class="qtab'].filter(t => src.includes(t));
const NQ = [];
for (const hash of R17) await page(hash + '&qt=1&bk=CT:30:h:-', async b => NQ.push(JSON.parse(await b.evaluate(`JSON.stringify({at: location.hash,
  table: document.querySelectorAll('#keep [data-qtable], #keep table, #keep [data-qform], #keep select').length,
  buckets: document.querySelectorAll('#keep section.bkt, #keep .qtree').length, qt: 'qt' in S || 'bk' in S})`))));
const nq = NQ.filter(x => x.table || !x.buckets || x.qt);
check('no-quota-table', !QSRC.length && NQ.length === R17.length && !nq.length, `page source has ${JSON.stringify(QSRC)}; ${nq.map(x => JSON.stringify(x)).join('; ')}`);

// (3) every kind row keeping some ALWAYS shows its own number (the data's n), also when it equals a shared cap; a size
// cap is a compact line (no pictures) right before the rows it covers, naming them, and each covered row carries a small
// "shares the N over X cm" tag (no other row does); a row of a shared bucket says it is within that bucket's number;
// no species picture twice in a group
const K17 = [];
for (const hash of R17) await page(hash, async b => K17.push(JSON.parse(await b.evaluate(`${SZH} JSON.stringify((() => { const st = ST(), norm = t => (t || '').replace(/\\s+/g, ' ').trim(), out = {at: location.hash, num: [], sz: [], tag: [], within: [], twice: []};
  document.querySelectorAll('#keep article.row.ledger').forEach(art => { const g = (st.rows[+(art.dataset.row || 'r0').slice(1)] || {}).group; if (!g || !g.headline) return;
    const secs = [...art.querySelectorAll('section.bkt')];
    const pics = [...art.querySelectorAll('[data-open^="sp:"]')].map(e => e.dataset.open); out.twice.push(...pics.filter((c, i) => pics.indexOf(c) !== i));
    (g.buckets || []).forEach((bk, i) => { const sec = secs[i], rows = [...(sec?.querySelectorAll('.krow[data-sp]') || [])];
      const rowOf = k => rows.find(r => r.dataset.sp === k.species.join(' ') && norm(r.querySelector('.nm')?.textContent) === norm(k.name));
      const all = [...(bk.groups || []).flatMap(x => x.kinds), ...bk.kinds];
      all.forEach(k => { const r = rowOf(k), kn = norm(r?.querySelector('.kn')?.textContent);
        if (!r || kn !== (k.n ? String(k.n) : '')) out.num.push([k.name, k.n, r ? kn : 'no row']); });
      (bk.sizes || []).forEach((z, j) => { const el = sec?.querySelectorAll('[data-sgroup]')[j], first = szFirst(sec, z), h = el ? el.getBoundingClientRect().height : 0;
        const cap1 = el ? [...el.querySelectorAll('.qgn')].map(e => norm(e.textContent)) : [];
        const next = el?.nextElementSibling, adj = !!first && (next === first || (next && next.contains(first)));
        const want = 'shares the ' + z.n + ' over ' + z.over_cm + ' cm';
        if (!szShown(el, z) || cap1.join() !== String(z.n) || h > 60 || !adj) out.sz.push({says: z.says, shown: szShown(el, z), big: cap1, h, adj});
        all.forEach(k => { const r = rowOf(k), cov = k.n > 0 && k.species.some(c => z.species.includes(c)), tg = [...(r?.querySelectorAll('[data-shares]') || [])].map(e => norm(e.textContent));
          if (cov !== tg.includes(want)) out.tag.push([k.name, cov, tg]); }); });
      const n = bk.cap ?? +((bk.between || '').match(/\\d+/) || [])[0];
      if (n && bk.kinds.length + (bk.groups || []).length >= 2) bk.kinds.forEach(k => { const r = rowOf(k), w = norm(r?.querySelector('[data-within]')?.textContent);
        const said = k.notes.some(x => new RegExp('\\\\bthe ' + n + '\\\\b').test(x));
        if (k.n > 0 && !said && !new RegExp('^within the .*\\\\b' + n + '\\\\b').test(w)) out.within.push([k.name, w]); });
    }); });
  return out; })())`))));
const kb17 = K17.filter(x => x.num.length), sb17 = K17.filter(x => x.sz.length || x.tag.length), wb17 = K17.filter(x => x.within.length || x.twice.length);
// kind-number-always: retired in round 18 (the card is group.tree; the bucket display is gone) -> tree-folders
// size-cap-line: retired in round 18 (the card is group.tree; the bucket display is gone) -> tree-folders
// within-shared-no-twice: retired in round 18 (the card is group.tree; the bucket display is gone) -> tree-folders
// the owner's own question, read straight off one row: a 30 cm bull trout on the Bella Coola on May 1 (both parts).
// round 18: off the tree — the Dolly Varden/bull trout kind's sentence with its number, a size chip in cm and inches whose
// lower bound a 30 cm fish passes, its cross-tier line, inside its tier (the folder it shares)
const BT17 = [];
for (const p of [0, 1]) await page(`w=bellacoola&d=501&p=${p}`, async b => BT17.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const norm = t => (t || '').replace(/\\s+/g, ' ').trim();
  const TK = g => [].concat(g?.tree || []).flatMap(function f(t) { return t.node === 'kind' ? [t] : (t.children || []).flatMap(f); });
  const k = ST().rows.flatMap(r => TK(r.group)).find(k => k.species.join(' ') === 'DV'), r = document.querySelector('#keep .krow.kt[data-sp="DV"]');
  const tier = r?.closest('section.tnode'), chip = norm(r?.querySelector('[data-size]')?.textContent), lo = k?.sizes.cm[0]?.[0], loIn = k?.sizes.in[0]?.[0];
  return {p: ${p}, k: !!k, says: norm((e => { if (!e) return ''; const c = e.cloneNode(true); c.querySelectorAll('button').forEach(b => b.remove()); return c.textContent; })(r?.querySelector('.ks'))) === norm(k?.says), n: norm(r?.querySelector('.ks .kn')?.textContent), want: String(k?.n),
    chip, sized: lo != null && lo <= 30 && (k.sizes.cm[0][1] == null || k.sizes.cm[0][1] >= 30) && chip.includes(lo + ' cm') && chip.includes(loIn + ' in'),
    also: (k?.also_counts || []).length === r?.querySelectorAll('[data-also]').length, tier: norm(tier?.querySelector(':scope > .tier .tn')?.textContent)}; })())`))));
const bt17 = BT17.filter(x => !x.k || !x.says || x.n !== x.want || x.n !== '1' || !x.sized || !x.also || !x.tier);
check('bull-trout-row', BT17.length === 2 && !bt17.length, JSON.stringify(bt17));

// (2) INTERNAL TEST, never drawn: every number the card shows agrees with the data's own table and cap system (rows 15
// group.table / cap_kinds / caps) and with the pipeline's caps.most_here (oracle.json): each kind row's number is the most
// any of its fish's table cells keeps (each such cell = most_here); a kind keeping none has only none-cells; a size-covered
// row's cells over the size keep min(row, cap); a count group's number is a cap's take over exactly its fish; a size
// line's number is a cap's take over fish at / over its size including all it names; a bucket's number is a cap's take
// over all its kept fish
const ORACLE = JSON.parse(fs.readFileSync(path.join(HERE, '..', 'oracle.json'), 'utf8'));
const CC17 = [];
for (const hash of R17) await page(hash, async b => {
  const id = await b.evaluate('sid()'), orc = ORACLE.groups.filter(x => x.id === id);
  CC17.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const st = ST(), orc = ${JSON.stringify(orc)}, norm = t => (t || '').replace(/\\s+/g, ' ').trim(), bad = [];
    let nums = 0;
    document.querySelectorAll('#keep article.row.ledger').forEach(art => { const ri = +(art.dataset.row || 'r0').slice(1), g = (st.rows[ri] || {}).group; if (!g || !g.headline || !g.table) return;
      const T = g.table, ck = Object.fromEntries((g.cap_kinds || []).map(k => [k.id, k])), O = orc.find(x => x.ri === ri);
      if (!O) bad.push('row ' + ri + ': no oracle');
      const cellsOf = sp => (T.bands[sp] || []).map((bd, i) => ({...bd, ...T.cells[sp][i], sp, i})).filter(c => !(c.as && c.as !== sp && T.bands[c.as]));
      const most = c => O?.cells.find(x => x.sp === c.sp && x.i === c.i)?.most;
      const spOf = ids => new Set(ids.map(x => ck[x]?.species));
      const capOver = (n, test) => (g.caps || []).some(c => c.take === n && test(c));
      // round 18 phase 2: the card is the tree (group.tree); walk it with its drawing. A kind's number = the most of any of
      // its fish's table cells; a kind under a size tier keeps min(its number, the tier's) over the tier's size; a count /
      // share / own / total tier's number = a cap's take over all the kept fish it holds; a size tier's = a cap's take over
      // fish at / over its size naming every fish the tier names
      const keptUnder = t => t.node === 'kind' ? (t.n !== 0 ? t.species : []) : (t.children || []).flatMap(keptUnder);
      const walk = (t, el, sizes) => {
        if (t.node === 'kind') { const k = t, cs = k.species.flatMap(cellsOf), keep = cs.filter(c => c.n !== 0);
          // round 20b (C1): a folded kind draws no sentence — its number is its tier's line
          const shown = k.folded ? norm(el?.closest('section.tnode')?.querySelector(':scope > .tier .tn')?.textContent) : norm(el?.querySelector('.ks .kn')?.textContent);
          if (!el) { bad.push(k.says + ': not drawn'); return; }
          if (k.n === 0) { if (shown || keep.length) bad.push(k.says + ': keeps none, cells ' + JSON.stringify(keep.map(c => [c.sp, c.says, c.n]))); return; }
          if (k.n == null) return;
          nums++;
          const mx = keep.some(c => c.n == null) ? null : Math.max(...keep.map(c => c.n));
          if (shown !== String(mx)) bad.push(k.says + ': shows ' + shown + ', table most ' + mx);
          keep.forEach(c => { if (most(c) !== undefined && most(c) !== c.n) bad.push(k.says + ' ' + c.sp + ' ' + c.says + ': table ' + c.n + ', most_here ' + most(c)); });
          sizes.forEach(z => keep.filter(c => (c.from || 0) >= z.over_cm && (z.species || []).includes(c.sp)).forEach(c => {
            if (c.n !== Math.min(+shown, z.n)) bad.push(k.says + ' ' + c.sp + ' ' + c.says + ': ' + c.n + ' vs min(' + shown + ', ' + z.n + ')'); }));
          return; }
        if (!el) { bad.push(t.says + ': tier not drawn'); return; }
        // round 20: the root tier is the header block's line (its number .ghn), its children the top of .qtree
        const atHdr = el.matches('[data-root-tier]'), fold = !atHdr && el.querySelector(':scope > .tier[data-share-folded]'), n = +norm((fold ? art.querySelector('[data-share-lead] .gln') : el.querySelector(atHdr ? '.ghn' : ':scope > .tier .tn'))?.textContent);   /* int1: a folded share's number is the head's lead */
        const kids = [...((atHdr ? art.querySelector(':scope > .qtree') : el.querySelector(':scope > .tch'))?.children || [])];
        if (t.n != null) { nums++;
          if (n !== t.n) bad.push(t.says + ': shows ' + n);
          if (t.tier === 'size') { if (!capOver(n, c => c.kinds.here.length && c.kinds.here.every(x => (ck[x]?.from || 0) >= t.over_cm) && (t.species || []).every(x => spOf(c.kinds.here).has(x)))) bad.push(t.says + ': no cap of that take over its fish at its size'); }
          else { const want = new Set(keptUnder(t)); if (!capOver(n, c => [...want].every(x => spOf(c.kinds.here).has(x)))) bad.push(t.says + ': no cap of that take over all its kept fish'); } }
        (t.children || []).forEach((c, i) => walk(c, kids[i], t.tier === 'size' ? [...sizes, t] : sizes)); };
      const tops = [...(art.querySelector(':scope > .qtree')?.children || [])], root = [].concat(g.tree || [])[0];
      const hdr = root && g.header && g.header.tier === root.id ? art.querySelector(':scope > .ghd [data-root-tier]') : null;
      [].concat(g.tree || []).forEach((t, i) => walk(t, i === 0 && hdr ? hdr : tops[i], [])); });
    return {at: location.hash, id: sid(), nums, bad}; })())`)));
});
const cc17 = CC17.filter(x => x.bad.length || !x.nums);
check('card-is-the-caps', CC17.length === R17.length && !cc17.length, cc17.map(x => `${x.at}: ${x.nums} numbers; ${x.bad.slice(0, 6).join(' | ')}`).join('; '));

await round18();
await round19();
await roundUr1();
await roundUr2();
await roundUr3();
await round20();
await round20b();
await round20c();
await round20e();
await round20f();
await round20g();
await round20h();
await round20i();
await roundUr5();
await roundInt1();
await roundUr6();
await roundUr7();

const failed = results.filter(x => !x[1]).length;
console.log(`${results.length - failed} passed, ${failed} failed — ${path.relative(process.cwd(), PAGE)}`);
process.exit(failed ? 1 : 0);

// ---------- round 18 ----------
async function round18(){
  const norm = 'var norm = t => (t || "").replace(/\\s+/g, " ").trim();';
  // (5) the phone is a REAL phone: the frame's inside is 390 x 844 (an iPhone 14's points), the app's own four tabs and the
  // home indicator take their place at the bottom, and the regulations scroll INSIDE it in no more height than an iPhone
  // 14 leaves an app with a standard 49 pt tab bar (844 - 47 status - 49 tabs - 34 home = 714); framed on a desktop and as
  // the screens are shot. Before round 18 the frame was 846 tall with its border, no tab bar or home band: 800 px of rules
  const PH = [];
  for (const [mode, hash, width, height] of [['shot', 'shot=1&w=chilliwack&d=1010&p=1', 440, 900], ['desk', 'w=chilliwack&d=1010&p=1', 1280, 1000]]) {
    const b = await open(`${pathToFileURL(PAGE).href}#${hash}`, { width, height, offline: true });
    try { await new Promise(r => setTimeout(r, 500));
      PH.push({mode, ...JSON.parse(await b.evaluate(`JSON.stringify((() => { const app = document.querySelector('.app'), sc = document.getElementById('sc'), r = app.getBoundingClientRect();
        const tabs = app.querySelector(':scope > nav.tabs[role=tablist]'), home = app.querySelector(':scope > .home');
        return {w: app.clientWidth, h: app.clientHeight, inWin: r.bottom <= innerHeight + 0.5, tabs: tabs ? [...tabs.querySelectorAll('[role=tab]')].map(t => t.textContent.trim() + (t.getAttribute('aria-selected') === 'true' ? '*' : '')) : null,
          tabsBottom: tabs ? Math.round(tabs.getBoundingClientRect().bottom - r.top) : -1, home: home ? Math.round(home.getBoundingClientRect().height) : 0,
          seen: sc.clientHeight, scrolls: sc.scrollHeight > sc.clientHeight + 200, docScroll: document.scrollingElement.scrollHeight - innerHeight}; })())`))});
    } finally { await b.close(); } }
  const phb = PH.filter(x => x.w !== 390 || x.h !== 844 || !x.inWin || JSON.stringify(x.tabs?.map(t => t.replace('*', ''))) !== '["Map","Search","Conditions","Spots"]'
    || x.tabs.filter(t => t.endsWith('*')).length !== 1 || x.home < 30 || x.seen > 714 || !x.scrolls || (x.mode === 'shot' && x.docScroll > 0));
  check('phone-real-height', PH.length === 2 && !phb.length, phb.map(x => JSON.stringify(x)).join('; '));

  // (6) the header fits a phone: water name and date on ONE line; the part's label ONE line, the data's short label when it
  // is uncut (the full one in the map's title / the parts sheet), "View on map ›" (or "Other parts ›") inline beside it;
  // slim tabs; at most 120 px in all (Chilliwack part 2, Oct 10: 179 px before)
  const HC = [];
  for (const hash of ['w=chilliwack&d=1010&p=1', 'w=chilliwack&d=1010&p=0', 'w=vedder&d=1010', 'w=cowichan&d=1010&p=1', 'w=x14p&d=1010&p=1', 'w=klwa&d=1010', 'w=bellacoola&d=501&p=0', 'w=x19p&d=1010&p=4']) await page(hash, async b => {
    HC.push(JSON.parse(await b.evaluate(`(async () => { ${norm} const hdr = document.getElementById('hdr'), st = ST(), w = WATERS[S.w], hasMap = !!(w.map && WM.waters[w.map]);
      const o = st.parts.find(x => x.v === st.part), uncut = x => x && !/…/.test(x) ? x : null, mp = hasMap ? WM.waters[w.map].parts[curPart()] : null;
      const long = o ? o.t.replace(/ · \\d+ sections?$/, '') : mp ? mp.name : '', want = o ? (uncut(o.short) || long) : mp ? (uncut(mp.short) || mp.name) : '';
      const R = e => e?.getBoundingClientRect(), nm = R(hdr.querySelector('.ttl b')), dt = R(hdr.querySelector('[data-open="date"]'));
      const pl = hdr.querySelector('[data-place]'), pr = R(pl), act = hdr.querySelector('.pgo'), ar = R(act), lh = pl ? parseFloat(getComputedStyle(pl).lineHeight) : 0;
      const txt = norm(pl?.textContent), tl = R(hdr.querySelector('[role=tablist]'));
      const res = {hash: location.hash.slice(8), h: Math.round(R(hdr).height), dateInline: !!nm && !!dt && dt.top < nm.bottom && dt.bottom > nm.top,
        // round 19 (owner 4): the label TWO lines at a fixed height, the full label or the short one (which, header-label-two-
        // lines); the action on the name's row
        txt, want, oneLine: !txt || pr.height <= lh * 2.5, label: !want || want === st.name ? !txt : (txt === want || txt === long),
        actInline: !ar || (ar.top < nm.bottom && ar.bottom > nm.top), tabs: tl ? Math.round(tl.height) : -1};
      let full = '';
      if (txt && txt !== long) { const go = hdr.querySelector('[data-map]') || hdr.querySelector('[data-open="parts"]'); go?.click(); await new Promise(r => setTimeout(r, 350));
        full = norm((document.querySelector('#wmap.on .mh') || document.querySelector('#sheet'))?.textContent); }
      return JSON.stringify({...res, fullShown: !txt || txt === long || full.includes(long)}); })()`)));
  });
  const hcb = HC.filter(x => x.h > 120 || !x.dateInline || !x.oneLine || !x.label || !x.actInline || !x.fullShown || x.tabs < 0 || x.tabs > 36);
  check('header-compact', HC.length === 8 && !hcb.length, hcb.map(x => JSON.stringify(x)).join('; '));

  // (4) each group's top is ONE compact block (.ghd): the group's name and scope, its big number with its unit on the same
  // line (and the data's `also`), the origin as a tag (the data's words before " — ", "Hatchery only") with the rest and
  // How to tell beside it, the total line; every one of these the data's words, inside the block, nothing else in it; the
  // Vedder Oct 10 block no taller than 145 px (two thirds of the 218 px before)
  const GH = [];
  for (const hash of ['w=vedder&d=1010', 'w=vedder&d=515', 'w=bellacoola&d=501&p=0', 'w=ape&d=501', 'w=klwa&d=1010', 'w=x35p&d=1010']) await page(hash, async b => {
    GH.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} const st = ST(), out = [];
      document.querySelectorAll('#keep article.row.ledger').forEach(art => { const g = (st.rows[+(art.dataset.row || 'r0').slice(1)] || {}).group; if (!g || !g.headline) return;
        // round 20c (pinned context): the group's top is TWO blocks — .ghd.gpin (name + the total line, pinned while inside
        // the group) and .ghd.gsub (the origin line) — so the name and number are read from the first, the origin from either
        const blk = art.querySelector(':scope > .ghd'), h = g.headline, R = e => e?.getBoundingClientRect(), bad = [];
        if (!blk) { out.push({bad: ['no .ghd block']}); return; }
        const blks = [...art.querySelectorAll(':scope > .ghd')], og0 = blks.map(b => b.querySelector('[data-origin]')).find(Boolean);
        const num = blk.querySelector('.num'), big = num?.querySelector('.ghn'), unit = num?.querySelector('.ghu');
        if (!blk.querySelector('.rtitle')) bad.push('no name');
        // round 20 (owner 1): the header is the tree's root tier — its number big in its words, the words whole on the line
        const root = [].concat(g.tree || [])[0], hdrRoot = root && g.header && g.header.tier === root.id ? root : null;
        if (hdrRoot) { if (!num?.matches('[data-root-tier]')) bad.push('header not the root tier');
          if (hdrRoot.n != null) { if (norm(big?.textContent) !== String(hdrRoot.n)) bad.push('number ' + norm(big?.textContent));
            const c = num.cloneNode(true); c.querySelectorAll('.tbk, .gl, .glk, .ghr, button').forEach(x => x.remove());
            if (norm(c.textContent).replace(/ ⓘ$/, '') !== norm(hdrRoot.says)) bad.push('root words ' + norm(c.textContent));
            const bn = R(big), un = R([...num.querySelectorAll('.ghu')].pop()); if (!bn || !un || un.top >= bn.bottom || un.bottom <= bn.top) bad.push('unit not on the number line'); } }
        else if (h.n != null) { if (norm(big?.textContent) !== String(h.n)) bad.push('number ' + norm(big?.textContent));
          if (norm(unit?.textContent) !== h.unit) bad.push('unit ' + norm(unit?.textContent));
          const bn = R(big), un = R(unit); if (!bn || !un || un.top >= bn.bottom || un.bottom <= bn.top) bad.push('unit not on the number line'); }
        (g.header ? [] : h.also || []).forEach(x => { if (![...blk.querySelectorAll('.also')].some(e => norm(e.textContent) === 'or ' + x.says)) bad.push('also ' + x.says); });
        if (g.origin) { const og = og0, cut = g.origin.indexOf(' — ');
          const said = norm([...(og?.childNodes || [])].filter(n => !(n.nodeType === 1 && n.matches('button, .tbk, a.book'))).map(n => n.textContent).join(' '));
          if (said.toLowerCase() !== g.origin.replace(/ — /, ' ').toLowerCase() && said !== g.origin) bad.push('origin ' + said);
          // phase 2 (rows 16): the tag is the data's header.tag ("hatchery only", drawn in capitals), else the words before " — "
          // round 19 (owner 1): header.tag is null (the fish names carry the origin): no tag, the words whole (origin-no-tag)
          const tagW = g.header ? (g.header.tag || '') : (cut > 0 ? g.origin.slice(0, cut) : '');
          if (tagW && norm(og?.querySelector('.gtag')?.textContent).toLowerCase() !== tagW.toLowerCase()) bad.push('tag ' + norm(og?.querySelector('.gtag')?.textContent));
          if (art.querySelector('[data-howtell]') && !og?.querySelector('[data-howtell]')) bad.push('How to tell outside the block'); }
        if (g.total_line && !g.tree && norm(blk.querySelector('.gtotal')?.textContent).indexOf(g.total_line) !== 0) bad.push('total line');   // with a tree its root line says the total
        const numC = blk.querySelector('.num')?.cloneNode(true); numC?.querySelectorAll('.ghr').forEach(x => x.remove());   /* round 20h form a: the share's lead and 'of the' are notes (.ghr) around the root's words */
        if (g.header && h.n != null && !norm(numC?.textContent).startsWith(norm(g.header.says.split(' · ')[0]))) bad.push('header ' + norm(numC?.textContent));
        if (art.querySelector(':scope > .gorigin, :scope > .gtotal, :scope > .rhead') || blks.length > 2) bad.push('a head line outside the block');
        out.push({t: norm(blk.querySelector('.rtitle')?.textContent), h: Math.round(R(blk).height), bad}); });
      return {hash: location.hash.slice(8), out}; })())`)));
  });
  // round 20 (owner 1): the block now carries the root tier's line (two lines on the Vedder: 148 px; a whole tier level left
  // the tree below it), so the bound is 150 px — still two thirds of the 218 px before round 18
  const ghb = GH.filter(x => !x.out.length || x.out.some(g => g.bad.length) || (x.hash === 'w=vedder&d=1010' && x.out[0].h > 150));
  check('group-head-compact', GH.length === 6 && !ghb.length, ghb.map(x => `${x.hash}: ${JSON.stringify(x.out)}`).join('; '));

  // ---------- round 18 phase 2 (rows 16 `group.tree`) ----------
  // (1) the quota is FOLDERS: the page draws `group.tree` and nothing of the bucket fields — every tier a line of its own
  // (its words, its number big, a left bar and wash: not a kind row), what it holds indented beneath it on a guide line in
  // the data's order, every kind exactly once; no tags anywhere
  // (2, 7) each kind is the data's sentence (`says`, its number `n` bold), its size a chip of the detail's own words holding
  // every cm AND inch the data's `sizes` give, the detail's other words beside it, each `also_counts` a short line under it,
  // one picture strip; (8) an own tier says why it applies here and what it replaces (Glenn Creek: the tributary walk)
  const TR = [];
  const TRS = ['w=vedder&d=1010', 'w=vedder&d=515', 'w=bellacoola&d=501&p=0', 'w=bellacoola&d=501&p=1', 'w=ape&d=501', 'w=klwa&d=1010', 'w=x35p&d=1010', 'w=glenn&d=615', 'w=hamill&d=615', 'w=chilliwack&d=1010&p=1'];
  for (const hash of TRS) await page(hash, async b => {
    TR.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} ${TOPS} const st = ST(), bad = [], R = e => e.getBoundingClientRect(); let tiers = 0, kinds = 0, own = [];
      const cap1 = t => t ? t[0].toUpperCase() + t.slice(1) : t;
      const kind = (k, el, at) => { if (!el?.matches('.krow.kt[data-sp]')) return bad.push(at + ': kind ' + k.says + ' not drawn here'); kinds++;
        if (el.dataset.sp !== k.species.join(' ')) bad.push(k.says + ': species ' + el.dataset.sp);
        // round 20b (C1, rows 22 folded): the only kind of a tier at its number draws no sentence — the tier line names it
        // and carries the number once
        if (k.folded) { if (el.querySelector('.ks')) bad.push(k.says + ': folded but a sentence drawn');
          const tn = norm(el.closest('section.tnode')?.querySelector(':scope > .tier .tn')?.textContent); if (tn !== String(k.n)) bad.push(k.says + ': folded, tier number ' + tn); }
        else {
        if (norm((e => { if (!e) return ''; const c = e.cloneNode(true); c.querySelectorAll('button').forEach(b => b.remove()); return c.textContent; })(el.querySelector('.ks'))) !== norm(k.says)) bad.push(k.says + ': sentence ' + norm((e => { if (!e) return ''; const c = e.cloneNode(true); c.querySelectorAll('button').forEach(b => b.remove()); return c.textContent; })(el.querySelector('.ks'))));
        const kn = norm(el.querySelector('.ks .kn')?.textContent); if (k.n ? kn !== String(k.n) : kn) bad.push(k.says + ': number ' + kn); }
        const chip = el.querySelector('[data-size]'), ct = norm(chip?.textContent), nums = [...new Set([...k.sizes.cm.flat(), ...k.sizes.in.flat()].filter(x => x))];
        if (k.n !== 0) { if (!chip) bad.push(k.says + ': no size chip');
          else { if (!norm(k.detail).includes(ct) && norm(k.size_rule || '') !== ct) bad.push(k.says + ': chip not the detail\\'s words: ' + ct);   /* rows 26: the chip may be the kind's size_rule */
            const cmN = k.sizes.cm.flat().filter(x => x), inN = k.sizes.in.flat().filter(x => x);
            // round 19 (owner 2): "50 cm (20 in)" — every cm before its "cm", every inch inside the brackets
            const inT = (ct.match(/\\(([^)]*)\\)/g) || []).join(' ');
            if (cmN.some(x => !new RegExp('\\\\b' + x + '\\\\b[^(]*cm').test(ct)) || inN.some(x => !new RegExp('\\\\b' + x + '\\\\b[^)]*in\\\\b').test(inT)) || (cmN.length && !/ cm\\b/.test(ct)) || (inN.length && !/ in\\b/.test(ct))) bad.push(k.says + ': chip ' + ct + ' vs sizes ' + JSON.stringify(k.sizes));
            // round 20c (audit #2): a chip is a tinted block (no border inside the box's border) — prominent = tinted or bordered, bold
            const cs = getComputedStyle(chip); if ((cs.borderTopStyle === 'none' && /rgba\(0, 0, 0, 0\)|transparent/.test(cs.backgroundColor)) || +cs.fontWeight < 700) bad.push(k.says + ': chip not prominent'); } }
        else if (chip) bad.push(k.says + ': a size chip on a fish kept none');
        const ac = [...el.querySelectorAll('[data-also]')].map(e => norm(e.textContent)), want = (k.also_counts || []).map(a => norm(cap1(a.says)));
        if (JSON.stringify(ac) !== JSON.stringify(want)) bad.push(k.says + ': also ' + JSON.stringify(ac) + ' vs ' + JSON.stringify(want));
        if (el.querySelectorAll('.kpics').length !== 1) bad.push(k.says + ': ' + el.querySelectorAll('.kpics').length + ' picture strips'); };
      const tier = (t, el, at) => { if (!el?.matches('section.tnode')) return bad.push(at + ': tier ' + t.says + ' not drawn here'); tiers++;
        // int1 (coordinator after v34): a share the head's lead already says folds its line; its number is the lead's
        if (el.querySelector(':scope > .tier[data-share-folded]')) { const ln = norm(el.closest('article')?.querySelector('[data-share-lead] .gln')?.textContent); if (ln !== String(t.n)) bad.push('tier ' + t.says + ': folded, lead ' + ln); }
        else {
        const tl = el.querySelector(':scope > .tier > .tl'), cl = tl.cloneNode(true); cl.querySelectorAll('.tbk, .gl, .glk').forEach(x => x.remove());
        if (norm(cl.textContent) !== norm(t.says)) bad.push('tier ' + t.says + ': drawn ' + norm(cl.textContent));
        if (t.n != null && norm(tl.querySelector('.tn')?.textContent) !== String(t.n)) bad.push('tier ' + t.says + ': number ' + norm(tl.querySelector('.tn')?.textContent));
        const ts = getComputedStyle(tl); if (!(+ts.fontWeight >= 700 && (/Bricolage/.test(ts.fontFamily) || ts.borderTopStyle !== 'none' || parseFloat(ts.borderLeftWidth) >= 3))) bad.push('tier ' + t.says + ': not styled as a tier line');   /* round 20f: a header is bold in the display face (c), on a hairline (a, b) or barred (before); round 20f: the Rule book page sits on the origin line, read past it */ }
        const ch = el.querySelector(':scope > .tch'), kids = ch ? [...ch.children] : [];
        // round 19 (owner 3): a size tier is a box holding its kinds (size-tier-box); every other tier a guide line
        // round 20d: FLAT — the children sit on the section's own edge, no guide line (round 18's rail retired)
        if (t.children.length && (!ch || Math.abs(R(ch).left - (R(el).left + parseFloat(getComputedStyle(el).borderLeftWidth) + parseFloat(getComputedStyle(el).paddingLeft))) > 1 || getComputedStyle(ch).borderLeftStyle !== 'none')) bad.push('tier ' + t.says + ': children indented or railed');   // a size band bleeds and pads back: its content edge (UR6: the size section's left rule is its border — the edge is after it)
        if (kids.length !== t.children.length) bad.push('tier ' + t.says + ': ' + kids.length + ' drawn children of ' + t.children.length);
        if (t.tier === 'own') own.push({says: t.says, sub: norm([...el.querySelectorAll(':scope > .tier .tsub:not([data-relation])')].map(e => e.textContent).join(' '))   /* UR6: the relation line (rows 28) sits above why-here */, why: t.why_here || '', repl: t.replaces || '', book: !!el.querySelector(':scope > .tier .book')});
        t.children.forEach((c, i) => (c.node === 'tier' ? tier : kind)(c, kids[i], t.says)); };
      document.querySelectorAll('#keep article.row.ledger').forEach(art => { const g = (st.rows[+(art.dataset.row || 'r0').slice(1)] || {}).group; if (!g || !g.headline || !g.tree) return;
        // round 20 (owner 1): the root tier is the header block's line; its children are the top of the tree
        const T = topsOf(g, art);
        if (T.root) { tiers++; if (!T.hdr) bad.push('root ' + T.root.says + ' not in the header');
          else { if (hdrWords(T.hdr) !== norm(T.root.says)) bad.push('root drawn ' + hdrWords(T.hdr));
            if (T.root.n != null && norm(T.hdr.querySelector('.ghn')?.textContent) !== String(T.root.n)) bad.push('root number ' + norm(T.hdr.querySelector('.ghn')?.textContent)); }
          if (T.els.length !== T.nodes.length) bad.push('root: ' + T.els.length + ' drawn children of ' + T.nodes.length); }
        T.nodes.forEach((t, i) => (t.node === 'tier' ? tier : kind)(t, T.els[i], 'root'));
        if (art.querySelector('section.bkt, [data-qgroup], [data-sgroup], [data-between]')) bad.push('bucket fields drawn'); });
      const tags = document.querySelectorAll('#keep .ktag, #keep .ktags').length; if (tags) bad.push(tags + ' tags');
      return {hash: location.hash.slice(8), tiers, kinds, own, bad}; })())`)));
  });
  const trb = TR.filter(x => x.bad.length || !x.tiers || !x.kinds);
  check('tree-folders', TR.length === TRS.length && !trb.length, trb.map(x => `${x.hash}: ${x.tiers} tiers ${x.kinds} kinds; ${x.bad.slice(0, 5).join(' | ')}`).join('; '));
  // round 20b (C2): the sub-line is why the rule applies here; what it replaced is no longer drawn (`replaces` stays data)
  const ow = TR.flatMap(x => x.own.map(o => ({...o, hash: x.hash}))), owb = ow.filter(o => (o.why && !o.sub.toLowerCase().includes(o.why.toLowerCase())) || (o.repl && o.sub.includes(o.repl)));
  check('own-tier-why-here', ow.some(o => o.hash.includes('glenn') && o.why) && !owb.length, JSON.stringify(owb.slice(0, 3)));
}

// ---------- round 19 ----------
async function round19(){
  const norm = 'var norm = t => (t || "").replace(/\\s+/g, " ").trim();';
  // (4) the header keeps ONE fixed size but the part's label is never cut to "…" when it can be read: two lines at a fixed
  // height, the FULL label (the map's title words) when it fits in two lines, else the data's short label, clamped only if
  // longer still; the map / parts action is OFF the label's line (on the name's row), a 44 px target, so the label has the
  // header's full width. Measured independently: a clone of the label's box, unclamped, at the same width and font
  const HL = [];
  for (const hash of ['w=chilliwack&d=1010&p=1', 'w=chilliwack&d=1010&p=0', 'w=vedder&d=1010', 'w=cowichan&d=1010&p=1', 'w=x14p&d=1010&p=1', 'w=klwa&d=1010', 'w=bellacoola&d=501&p=0', 'w=x19p&d=1010&p=4', 'w=ape&d=501', 'w=x27p&d=1010']) await page(hash, async b => {
    HL.push(JSON.parse(await b.evaluate(`(async () => { ${norm} await document.fonts.ready; await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));
      const hdr = document.getElementById('hdr'), st = ST(), w = WATERS[S.w], hasMap = !!(w.map && WM.waters[w.map]), many = Array.isArray(w.snap[S.d]) && w.snap[S.d].length > 1;
      const o = st.parts.find(x => x.v === st.part), uncut = x => x && !/…/.test(x) ? x : '', mp = hasMap ? WM.waters[w.map].parts[curPart()] : null;
      const long = o ? (st.sub.includes(o.t.replace(/ · \\d+ sections?$/, '')) ? o.t.replace(/ · \\d+ sections?$/, '') : (uncut(o.short) || o.t.replace(/ · \\d+ sections?$/, ''))) : mp ? (uncut(mp.short) || mp.name) : '';
      const short = o ? (uncut(o.short) || o.t.replace(/ · \\d+ sections?$/, '')) : mp ? (uncut(mp.short) || mp.name) : '';
      const pl = hdr.querySelector('[data-place]'), txt = norm(pl?.textContent), R = e => e?.getBoundingClientRect();
      const lines = t => { if (!pl) return 0; const c = document.createElement('div'), cs = getComputedStyle(pl);
        c.style.cssText = 'position:absolute;visibility:hidden;left:0;top:0;white-space:normal;overflow:visible;display:block;width:' + pl.clientWidth + 'px;font:' + cs.font + ';letter-spacing:' + cs.letterSpacing + ';line-height:' + cs.lineHeight;
        c.textContent = t; document.body.append(c); const n = Math.round(c.getBoundingClientRect().height / parseFloat(cs.lineHeight)); c.remove(); return n; };
      const want = !short || short === st.name ? '' : lines(long) <= 2 ? long : short;
      const act = hasMap ? hdr.querySelector('[data-map]') : many ? hdr.querySelector('[data-open="parts"]') : null, ar = R(act), pr = R(pl), nm = R(hdr.querySelector('.ttl b'));
      const lh = pl ? parseFloat(getComputedStyle(pl).lineHeight) : 0;
      return JSON.stringify({hash: location.hash.slice(8), h: Math.round(R(hdr).height * 2) / 2, txt, want, longLines: lines(long), shortLines: lines(short),
        cut: !!pl && pl.scrollHeight > pl.clientHeight + 1 && lines(txt) <= 2, twoLineBox: !pl || Math.abs(pr.height - 2 * lh) < 2,
        act: !hasMap && !many ? 'none' : !act ? 'missing' : (ar.width < 44 || ar.height < 44) ? 'small ' + Math.round(ar.width) + 'x' + Math.round(ar.height)
          : pl && ar.top < pr.bottom && ar.bottom > pr.top ? 'on the label line' : !(ar.top < nm.bottom && ar.bottom > nm.top) ? 'not on the name row' : 'ok',
        width: !pl || pr.width >= R(hdr).width - 56 - 16 - 2}); })()`)));
  });
  const hs = new Set(HL.map(x => x.h)), hlb = HL.filter(x => x.txt !== x.want || x.cut || !x.twoLineBox || !/^(ok|none)$/.test(x.act) || !x.width);
  check('header-label-two-lines', HL.length === 10 && hs.size === 1 && !hlb.length && HL[0].hash.startsWith('w=chilliwack') && !!HL[0].txt,
    (hs.size > 1 ? `header heights ${JSON.stringify(HL.map(x => [x.hash, x.h]))}; ` : '') + hlb.map(x => JSON.stringify(x)).join('; '));

  // phase 2 (rows 17, display 13). (3) a size tier HOLDS every kind it caps, so it is drawn as a BOX that encloses its line
  // and everything it holds (kinds and whole tiers), inset inside it, nothing of it outside; the steelhead's `size_note`
  // ("it counts toward the 1 over 50 cm (20 in), but once you keep one, …") is drawn on the steelhead's own row, inside the
  // box of the tier it names. (1) the origin is in the fish names and header.tag is null: no tag drawn, the origin line is
  // the data's words whole
  const SB = [], SBS = ['w=vedder&d=1010', 'w=vedder&d=515', 'w=chilliwack&d=1010&p=1', 'w=bellacoola&d=501&p=0', 'w=bellacoola&d=501&p=1', 'w=x35p&d=1010', 'w=glenn&d=615', 'w=klwa&d=1010'];
  for (const hash of SBS) await page(hash, async b => {
    SB.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} ${TOPS} const st = ST(), bad = [], R = e => e.getBoundingClientRect(); let boxes = 0, notes = 0, origins = 0;
      const cap1 = t => t ? t[0].toUpperCase() + t.slice(1) : t;
      document.querySelectorAll('#keep article.row.ledger').forEach(art => { const g = (st.rows[+(art.dataset.row || 'r0').slice(1)] || {}).group; if (!g || !g.tree) return;
        const T = topsOf(g, art), ids = {};   // round 20: the root is the header's line, its children the top of the tree
        const walk = (t, el, inSize) => { if (t.node !== 'tier') { if (!t.size_note && el?.querySelector('[data-size-note]')) bad.push(t.says + ': size note drawn from no data');   /* rows 24: the steelhead has none */
            if (t.size_note) { notes++;
              const sn = el?.querySelector('[data-size-note]'), box = el?.closest('section.tnode[data-tnode="size"]');
              if (norm(sn?.textContent) !== norm(cap1(t.size_note.says))) bad.push(t.says + ': size note ' + JSON.stringify(norm(sn?.textContent)));
              if (!box || box.dataset.tid !== t.size_note.tier) bad.push(t.says + ': size note not inside the box of ' + t.size_note.tier); }
            return; }
          if (t.origin) { origins++; const og = el?.querySelector(':scope > .tier [data-tier-origin]'), said = norm([...(og?.childNodes || [])].filter(n => !(n.nodeType === 1 && n.matches('button, .tbk, a.book'))).map(n => n.textContent).join(' '));
            if (said !== norm(t.origin)) bad.push('tier origin drawn ' + JSON.stringify(said) + ' vs ' + JSON.stringify(t.origin)); }   /* rows 24: a share tier's origin under its line */
          if (t.tier === 'size') { boxes++; const cs = getComputedStyle(el), r = R(el);
            if (el.dataset.tid !== t.id) bad.push(t.says + ': box not marked ' + t.id);
            // round 20d: never a border inside a border; round 20h (owner 2): ONE surface — the section is set off by a hairline
            // above and below, full-bleed, no side borders, no band
            if ((cs.borderRightStyle !== 'none' && parseFloat(cs.borderRightWidth) > 0) || parseFloat(cs.borderLeftWidth) < 2 ||   /* UR6 (owner 1): ONE left rule, the section's extent (round 20h: no side borders) */ !/rgba\\(0, 0, 0, 0\\)|transparent/.test(cs.backgroundColor) || parseFloat(cs.borderTopWidth) < 1 || parseFloat(cs.borderBottomWidth) < 1) bad.push(t.says + ': not a hairlined section (borders ' + cs.borderStyle + ' ' + cs.borderTopWidth + '/' + cs.borderBottomWidth + ', background ' + cs.backgroundColor + ')');
            const kids = [...(el.querySelector(':scope > .tch')?.children || [])];
            if (kids.length !== t.children.length) bad.push(t.says + ': ' + kids.length + ' drawn of ' + t.children.length);
            kids.forEach(k => { const q = R(k); if (q.left < r.left - 0.5 || q.right > r.right + 0.5 || q.top < r.top || q.bottom > r.bottom) bad.push(t.says + ': ' + norm(k.textContent).slice(0, 40) + ' not inside the band'); });
            const tl = R(el.querySelector(':scope > .tier .tl')); if (tl.top < r.top || tl.left < r.left || tl.right > r.right + 0.5) bad.push(t.says + ': its line not inside the box'); }
          const kids = [...(el?.querySelector(':scope > .tch')?.children || [])]; t.children.forEach((c, i) => walk(c, kids[i])); };
        T.nodes.forEach((t, i) => walk(t, T.els[i]));
        if (g.origin) { origins++; const og = art.querySelector('[data-origin]'), said = norm([...(og?.childNodes || [])].filter(n => !(n.nodeType === 1 && n.matches('button, .tbk, a.book'))).map(n => n.textContent).join(' '));
          if (g.header && g.header.tag == null && said !== norm(g.origin)) bad.push('origin drawn ' + JSON.stringify(said) + ' vs ' + JSON.stringify(g.origin)); }
        if (g.header && g.header.tag == null && art.querySelector('.gtag')) bad.push('a tag drawn though header.tag is null'); });
      return {hash: location.hash.slice(8), boxes, notes, origins, bad}; })())`)));
  });
  const sbb = SB.filter(x => x.bad.length);
  check('size-tier-box', SB.length === SBS.length && SB.filter(x => x.boxes).length >= 6 && !sbb.length, sbb.map(x => `${x.hash}: ${x.boxes} boxes; ${x.bad.slice(0, 4).join(' | ')}`).join('; '));
  check('size-note-on-kind', !sbb.some(x => x.bad.some(y => /size note/.test(y))),   /* rows 24: no kind carries a size note; one in the data would be drawn inside its box, none drawn from nowhere */ sbb.map(x => x.bad.filter(y => /size note/.test(y)).join(' | ')).join('; '));
  check('origin-no-tag', SB.some(x => x.origins) && !sbb.some(x => x.bad.some(y => /origin|tag/.test(y))), sbb.map(x => x.bad.filter(y => /origin|tag/.test(y)).join(' | ')).join('; '));
}

// ---------- user round 1 (UR1, six-angler panel on v21) ----------
async function roundUr1(){
  const norm = 'var norm = t => (t || "").replace(/\\s+/g, " ").trim();';
  // C: "Book p.21" — what book? Every book link, and every page label before bare "p.N" links, says "Rule book"
  const RB = [];
  for (const hash of ['w=vedder&d=1010', 'w=vedder&d=1010&sec=gear', 'w=bellacoola&d=501&p=1', 'w=vedder&d=1010&sheet=hatch', 'w=glenn&d=615']) await page(hash, async b => {
    RB.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} const bad = [];
      const links = [...document.querySelectorAll('a.book')];
      links.forEach(a => { const t = norm(a.textContent);
        if (/^p\\.\\d+/.test(t)) { const lab = norm(a.parentElement.firstChild?.textContent || a.parentElement.textContent);
          if (!/^Rule book\\b/.test(lab)) bad.push('bare ' + t + ' under ' + JSON.stringify(lab.slice(0, 30))); }
        else if (!/^Rule book p\\.\\d+ ↗$/.test(t)) bad.push(JSON.stringify(t)); });
      const stray = [...document.querySelectorAll('body *')].filter(e => !e.children.length && !e.closest('script,style') && /(^|[^e] )Book p\\.\\d|^Book\\b/.test(norm(e.textContent))).map(e => norm(e.textContent).slice(0, 40));
      return {hash: location.hash.slice(8), n: links.length, bad: bad.concat(stray.map(x => 'stray ' + JSON.stringify(x)))}; })())`)));
  });
  const rbb = RB.filter(x => x.bad.length);
  check('rule-book-label', RB.filter(x => x.n).length >= 4 && !rbb.length, rbb.map(x => `${x.hash}: ${x.bad.slice(0, 4).join(' | ')}`).join('; '));

  // A, B, D: each tier line shows the data's words whole; the words a panel misread are the data's new ones
  const TW = [];
  for (const hash of ['w=vedder&d=1010', 'w=bellacoola&d=501&p=1', 'w=chilliwack&d=1010&p=1', 'w=klwa&d=1010']) await page(hash, async b => {
    TW.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} ${TOPS} const st = ST(), bad = [], seen = {total: 0, share: 0, lifted: 0, size: 0, count: 0};
      document.querySelectorAll('#keep article.row.ledger').forEach(art => { const g = (st.rows[+(art.dataset.row || 'r0').slice(1)] || {}).group; if (!g || !g.tree) return;
        const lifted = (g.lifts || []).length > 0, T = topsOf(g, art);
        // round 20: the root tier is the header block's line (header.tier names it)
        const walk = t => { if (t.node !== 'tier') return; const el = T.root === t ? T.hdr : art.querySelector('section.tnode[data-tid="' + t.id + '"] > .tier .tl');
          const elC = el ? el.cloneNode(true) : null; elC?.querySelectorAll('.tbk, button, .gl, .ghr').forEach(x => x.remove());   /* round 20h: notes (.ghr) may sit inside a span */
          const shown = norm([...(elC?.childNodes || [])].map(n => n.textContent).join(' '));
          const folded = !!art.querySelector('section.tnode[data-tid="' + t.id + '"] > .tier[data-share-folded]');   /* int1: a folded share says its words through the head's lead */
          if (!folded && shown.replace(/ ⓘ$/, '') !== norm(t.says)) bad.push(t.tier + ' drawn ' + JSON.stringify(shown) + ' vs ' + JSON.stringify(t.says));
          if (t.tier === 'total' && t.n != null && / across /.test(t.says)) { seen.total++; if (!/ a day per angler across /.test(t.says)) bad.push('total without per angler: ' + t.says); }
          // round 20 (owner 2): the lifted fish are named as not counted in the share, which sits inside the total
          if (t.tier === 'share') { seen.share++; if (lifted) { seen.lifted++; if (!/^Only \\d+ of the \\d+ may be from (this stream|this lake|streams|lakes)(, not counting .+)?$/.test(t.says))   /* UR7 A (rows 29): no "not counting" where the own tier's relation says it */ bad.push('lifted share: ' + t.says); } }
          if (t.tier === 'size') { seen.size++; if (!/^Of these, (only \\d+|none) may be over /.test(t.says)) bad.push('size: ' + t.says); }   /* ROUND UR6 (rows 28): no box — "Of these, only 1 may be over …" */
          if (t.tier === 'count') { seen.count++; if (!/^\\d+ .+ in all( \\(part of the \\d+\\))?$/.test(t.says)) bad.push('count: ' + t.says); }
          (t.children || []).forEach(walk); };
        [].concat(g.tree).forEach(walk);
        if (g.header && / across /.test(g.header.says) && !/ per angler across /.test(g.header.says)) bad.push('header: ' + g.header.says); });
      return {hash: location.hash.slice(8), seen, bad}; })())`)));
  });
  const twb = TW.filter(x => x.bad.length), sum = k => TW.reduce((a, x) => a + x.seen[k], 0);
  check('ur1-tier-words', !twb.length && sum('total') >= 3 && sum('lifted') >= 1 && sum('size') >= 3 && sum('count') >= 1,
    twb.map(x => `${x.hash}: ${x.bad.slice(0, 4).join(' | ')}`).join('; ') || `seen ${JSON.stringify(TW.map(x => x.seen))}`);

  // F, G: the plain words on the page
  const PW = {};
  for (const [k, hash] of [['bella', 'w=bellacoola&d=1010&p=1'], ['vedder', 'w=vedder&d=1010'], ['gear', 'w=vedder&d=1010&sec=gear'], ['klmb', 'w=klmb&d=1010&sec=gear']]) await page(hash, async b => {
    PW[k] = JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm}
      return {closed: [...document.querySelectorAll('[data-notkept="closed"] .nm')].map(e => norm(e.textContent)),
        duty: [...document.querySelectorAll('#keep .duty')].map(e => norm(e.textContent)),
        gear: norm(document.getElementById('gear')?.textContent)}; })())`));
  });
  const pwb = [];
  if (!PW.bella.closed.length || !PW.bella.closed.every(t => /no fishing for them, not even catch and release — release any you hook by accident$/.test(t))) pwb.push('Bella Coola closure ' + JSON.stringify(PW.bella.closed));
  if (!PW.vedder.duty.some(t => /Record each on your licence right away/.test(t)) || PW.vedder.duty.some(t => /Write each one/i.test(t))) pwb.push('Vedder duties ' + JSON.stringify(PW.vedder.duty));   // rows 24
  if (!/up to 1 kg \(2\.2 lb\)/i.test(PW.gear.gear)) pwb.push('Vedder gear: no "1 kg (2.2 lb)"');
  if (!/1 line from shore; any number from a boat/.test(PW.klmb.gear) || /no limit while in a boat/.test(PW.klmb.gear)) pwb.push('Kootenay main body lines: ' + JSON.stringify((PW.klmb.gear.match(/.{0,30}boat.{0,30}/g) || []).slice(0, 3)));
  check('ur1-plain-words', !pwb.length, pwb.join('; '));

  // E: the How to tell sheet opens over the water and day the page is on (the header behind it is the current one)
  const HT = [];
  for (const hash of ['w=vedder&d=1010&sheet=hatch', 'w=bellacoola&d=501&p=1&sheet=hatch', 'w=chilliwack&d=1010&p=1&sheet=hatch']) await page(hash, async b => {
    HT.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} const st = ST(), h = document.getElementById('hdr');
      return {hash: location.hash.slice(8), open: !!document.querySelector('.shead h2') && /Hatchery or wild/.test(document.querySelector('.shead h2').textContent),
        name: norm(h?.querySelector('.ttl b')?.textContent), want: st.name, date: norm(h?.textContent).includes(fmt(S.d))}; })())`)));
  });
  const htb = HT.filter(x => !x.open || x.name !== x.want || !x.date);
  check('howtell-current-water', HT.length === 3 && !htb.length, htb.map(x => JSON.stringify(x)).join('; '));
}

// ---------- user round 2 (UR2, six-angler panel on v22) ----------
async function roundUr2(){
  const norm = 'var norm = t => (t || "").replace(/\\s+/g, " ").trim();';
  const K = {};
  for (const [k, hash] of [['vedder', 'w=vedder&d=1010'], ['klwa', 'w=klwa&d=1010'], ['chw', 'w=chilliwack&d=1010&p=1'], ['shut', 'w=vedder&d=615'],
                           ['gear', 'w=vedder&d=1010&sec=gear']]) await page(hash, async b => {
    K[k] = JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm}
      const arts = [...document.querySelectorAll('#keep article.row.ledger')].filter(a => a.querySelector('.qtree'));
      const kind = sp => norm((e => { if (!e) return ''; const c = e.cloneNode(true); c.querySelectorAll('button').forEach(b => b.remove()); return c.textContent; })(document.querySelector('#keep .krow.kt[data-sp~="' + sp + '"] .ks')));  // UR3 1: the How to tell button aside
      const ys = document.querySelector('#gear [data-k="bait"] [data-col="ok"]'), ns = document.querySelector('#gear [data-k="bait"] [data-col="no"]');
      return {heads: arts.map(a => norm(a.querySelector('.rhead .num')?.textContent)),
        worked: [...document.querySelectorAll('#keep .worked li.wh')].map(e => norm(e.textContent)),
        dv: kind('DV'), st: kind('ST'),
        pres: [...document.querySelectorAll('#keep [data-presence]')].map(e => norm(e.textContent)),
        duty: [...document.querySelectorAll('#keep .duty')].map(e => norm(e.textContent)),
        foot: norm(document.querySelector('#gear .foot')?.textContent),
        gear: norm(document.getElementById('gear')?.textContent),
        ok: ys ? [...ys.querySelectorAll('li')].map(e => norm(e.textContent).replace(/^[✓✕]\s*/, '')) : [], no: ns ? [...ns.querySelectorAll('li')].map(e => norm(e.textContent).replace(/^[✓✕]\s*/, '')) : []}; })())`));   // round 20g: every row carries its own mark
  });
  const hb = ['vedder', 'klwa', 'chw'].flatMap(k => !K[k].heads.length ? [k + ': no card'] : K[k].heads.filter(t => !/\b\d+ a day per angler\b/.test(t)).map(t => k + ': ' + JSON.stringify(t)));
  check('ur2-headline-per-angler', !hb.length, hb.join('; '));
  // round 20b (C4): the keep-N lines are the two ends of the range
  const W = K.vedder.worked, want = [/^If you keep 4 hatchery rainbow trout here: that’s your trout and char for the day, everywhere\.$/,
    /^If you keep 2 hatchery rainbow trout here: they count as your 2 from streams anywhere else today — up to 2 more of any kind, from here or from lakes\.$/];   // rows 29 (UR7 1)
  check('ur2-worked-plain', W.length === 2 && W.every((t, i) => want[i].test(t)), JSON.stringify(W));
  const kb = [];
  if (!/^Only 1 of the 2 may be hatchery Dolly Varden\/bull trout or lake trout/.test(K.vedder.dv)) kb.push('Vedder DV ' + JSON.stringify(K.vedder.dv));
  if (!/^Only 1 of the \d+ may be Dolly Varden\/bull trout/.test(K.klwa.dv)) kb.push('LWA DV ' + JSON.stringify(K.klwa.dv));
  if (!/^Up to 2 may be hatchery steelhead only — wild steelhead go back$/.test(K.vedder.st)) kb.push('Vedder ST ' + JSON.stringify(K.vedder.st));
  if (!K.vedder.pres.some(t => /^A rainbow trout over 50 cm \(20 in\) here counts as a steelhead\./.test(t))) kb.push('presence ' + JSON.stringify(K.vedder.pres));   // rows 24: the shorter presence words
  if (!K.vedder.duty.some(t => /Stop fishing the Vedder River after 2 a day/.test(t)) || K.vedder.duty.some(t => /for the day after|this river/.test(t))) kb.push('duty ' + JSON.stringify(K.vedder.duty));   // rows 26 + round 20i: the water's name for "this river"
  check('ur2-kind-words', !kb.length, kb.join('; '));
  check('ur2-closed-until', K.shut.foot === 'No fishing here until Jul 1.' && !/licence/i.test(K.shut.gear.replace(/^Gear & licence · for this water (today|on \w+ \d+)/, '')), JSON.stringify(K.shut.foot) + ' / ' + JSON.stringify(K.shut.gear.slice(0, 120)));
  const roe = K.gear.ok.find(t => /^Roe/.test(t)) || '';
  check('ur2-bait-allowed-note', K.gear.no.length > 0 && !K.gear.no.some(t => /still OK/i.test(t)) && /ban excepts it|still OK/.test(roe),   /* gear 11: the roe row names the ban that excepts it */ `ok ${JSON.stringify(K.gear.ok)} / no ${JSON.stringify(K.gear.no)}`);
}

// ---------- round UR5 (user panel 5 + the coordinator's audit H1/H2) ----------
async function roundUr5(){
  const norm = 'var norm = t => (t || "").replace(/\\s+/g, " ").trim();';
  const TROUT = ['RB', 'CT', 'GB', 'ST'];
  // (1) the "Trout release only / Trout closed" badge only where no keep row holds trout today — Cowichan's said "Trout release
  // only until Jul 3" on Oct 10 (the CHAR row's strip) while its card keeps 2 hatchery trout (Shane S2 conf 2)
  const BADGE = [];
  for (const hash of ['w=cowichan&d=1010&p=1', 'w=cowichan&d=1010&p=2', 'w=iron&d=1010', 'w=iron&d=720', 'w=vedder&d=515']) await page(hash, async b => {
    BADGE.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} const st = ST(), soon = norm(document.querySelector('#keep .wstrip .wsoon')?.textContent);
      const keeps = (st.rows || []).some(r => r.kind === 'keep' && r.members.some(m => ${JSON.stringify(TROUT)}.includes(m)));
      return {hash: location.hash, soon, keeps, bad: keeps && /^Trout (release only|closed)/.test(soon)}; })())`)));
  });
  check('ur5-trout-badge', BADGE.length === 5 && BADGE.filter(x => x.keeps).length >= 3 && !BADGE.some(x => x.bad), BADGE.filter(x => x.bad).map(x => `${x.hash}: "${x.soon}" with a trout keep row`).join('; ') || 'states ' + BADGE.length);
  // (2) the data's "Through the year" window line (a group's `.strip .swin`) is on the page (Tom T4: Hamill keeps until Oct 31)
  const YEAR = [];
  for (const hash of ['w=hamill&d=615', 'w=hamill&d=1010', 'w=glenn&d=615']) await page(hash, async b => {
    YEAR.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} const st = ST(), d = new DOMParser().parseFromString('<div>' + st.cardHTML + '</div>', 'text/html');
      const want = [...d.querySelectorAll('article.row .strip .swin')].map(e => norm(e.textContent)).filter(Boolean), have = norm(document.getElementById('keep')?.textContent);
      return {hash: location.hash, want: want.length, miss: want.filter(w => !have.includes(w))}; })())`)));
  });
  check('ur5-year-window', YEAR.some(x => x.want) && YEAR.every(x => !x.miss.length), YEAR.filter(x => x.miss.length).map(x => x.hash + ': ' + x.miss.join(' / ')).join('; ') || 'no window in the data');
  // (3) a keep line's number never touches its words ("Keep 10any size", Linh, Doug)
  const GAP = [];
  for (const hash of ['w=klmb&d=1010', 'w=vedder&d=1010']) await page(hash, async b => {
    GAP.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const bad = []; let n = 0; document.querySelectorAll('#keep .sl .n').forEach(e => { const nx = e.nextElementSibling; if (!nx || !nx.matches('.segs')) return; n++;
      const g = nx.getBoundingClientRect().left - e.getBoundingClientRect().right; if (g < 3) bad.push(e.textContent + '|' + nx.textContent.slice(0, 20) + ' gap ' + g.toFixed(1)); }); return {n, bad}; })())`)));
  });
  check('ur5-keep-gap', GAP.some(x => x.n) && GAP.every(x => !x.bad.length), GAP.flatMap(x => x.bad).join('; ') || 'no keep line');
  // (4) "A rainbow trout over 50 cm here counts as a steelhead" is a rule: ink, not the quiet grey (Gail in glare, Fiona)
  const INK = [];
  for (const theme of ['', '&theme=dark']) await page('w=cowichan&d=1010&p=1' + theme, async b => {
    INK.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const e = document.createElement('i'); e.style.color = 'var(--ink)'; document.body.appendChild(e); const ink = getComputedStyle(e).color; e.remove();
      const q = [...document.querySelectorAll('#keep .kpres, #keep .gquiet')].filter(x => x.querySelector('[data-gl="steelhead"]') && /counts as a steelhead/.test(x.textContent)); return {n: q.length, bad: q.filter(x => getComputedStyle(x).color !== ink).map(x => getComputedStyle(x).color)}; })())`)));
  });
  check('ur5-steelhead-definition-ink', INK.every(x => x.n) && INK.every(x => !x.bad.length), INK.map(x => x.n + ' notes, ' + x.bad.join(',')).join('; '));
  // (D1, data rows 27) the kept-elsewhere over-the-size question names only the fish the clause holds here: the Kootenay main body's
  // trout and char card never asks about rainbow trout (its own row: 10 a day, 20 a year over 50 cm) — two readers took it as a daily cap
  let EL = {};
  await page('w=klmb&d=1010', async b => { EL = JSON.parse(await b.evaluate(`JSON.stringify((() => { const t = (document.querySelector('#keep article.row.ledger')?.textContent || '').replace(/\\s+/g, ' ');
    return {cut: /Kept cutthroat trout over 50 cm/.test(t), rb: /Kept rainbow trout or cutthroat trout over 50 cm/.test(t)}; })())`)); });
  check('ur5-elsewhere-names-here', EL.cut && !EL.rb, JSON.stringify(EL));
  // (H1) the steelhead look-for tip never says "50 cm or longer" (the book: longer than 50 cm; exactly 50 cm is a rainbow)
  // (H2) every fish name the snapshot carries is the export's species name (Lake whitefish, not the art table's "Whitefish")
  const ref = fs.readFileSync(path.join(APP, '../pipeline/deliver/answers/reference/page_v36.html'), 'utf8');
  const D = JSON.parse(ref.match(/<script type="application\/json" id="data">([\s\S]*?)<\/script>/)[1].replace(/<\\\//g, '</'));
  const ex = {...(D.species?.fish || {})};
  let fish = {};
  await page('w=vedder&d=1010', async b => { fish = JSON.parse(await b.evaluate('JSON.stringify(SNAP.fish)')); });
  const tips = (fish.ST?.tips || []).join(' | ');
  check('h1-steelhead-tip', !!fish.ST && !/50 cm[^|]*or longer|or longer[^|]*50 cm/.test(tips), tips);
  const wrong = Object.entries(fish).filter(([c, f]) => ex[c]?.name && f.name !== ex[c].name).map(([c, f]) => `${c}: "${f.name}" vs "${ex[c].name}"`);
  check('h2-fish-names', Object.keys(fish).length > 5 && Object.keys(ex).length > 5 && !wrong.length, wrong.join('; '));
}

// ---------- round UR6 (owner: the last prototype fixes before porting) ----------
async function roundUr6(){
  const norm = 'var norm = t => (t || "").replace(/\\s+/g, " ").trim();';
  // (1) "this box" with no box: the size section's extent is VISIBLE (a continuous left rule from its head to its last row, closed by
  // hairlines at both ends, light and dark, the rule at least 2:1 on the card) and its words never say "box"
  const EXT = [];
  for (const theme of ['', '&theme=dark']) for (const hash of ['w=vedder&d=1010', 'w=chilliwack&d=1010&p=1', 'w=bellacoola&d=501&p=1', 'w=cowichan&d=1010&p=1']) await page(hash + theme, async b => {
    EXT.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const R = e => e.getBoundingClientRect(), lum = c => { const m = c.match(/\\d+(\\.\\d+)?/g).map(Number).slice(0, 3).map(v => { v /= 255; return v <= .03928 ? v / 12.92 : ((v + .055) / 1.055) ** 2.4; }); return .2126 * m[0] + .7152 * m[1] + .0722 * m[2]; };
      const card = getComputedStyle(document.querySelector('#keep article.row.ledger') || document.body).backgroundColor, cardL = lum(card === 'rgba(0, 0, 0, 0)' ? getComputedStyle(document.body).backgroundColor : card);
      return [...document.querySelectorAll('#keep section.tnode[data-tnode="size"]')].map(tn => { const cs = getComputedStyle(tn), head = tn.querySelector(':scope > .tier'), rows = [...tn.querySelectorAll('.krow')];
        const last = Math.max(...rows.map(r => R(r).bottom)), L = lum(cs.borderLeftColor), ratio = (Math.max(L, cardL) + .05) / (Math.min(L, cardL) + .05);
        return {hash: location.hash.slice(8), rule: parseFloat(cs.borderLeftWidth), ratio: +ratio.toFixed(2), top: parseFloat(cs.borderTopWidth), bottom: parseFloat(cs.borderBottomWidth),
          headIn: R(head).top >= R(tn).top - 1, endGap: Math.round(R(tn).bottom - last), rows: rows.length}; }); })())`)));
  });
  const ex = EXT.flat(), exb = ex.filter(x => x.rule < 2 || x.ratio < 2 || x.top < 1 || x.bottom < 1 || !x.headIn || x.endGap < 0 || x.endGap > 28 || !x.rows);
  check('ur6-size-extent', ex.length >= 8 && !exb.length, exb.slice(0, 4).map(x => JSON.stringify(x)).join('; ') || 'size sections ' + ex.length);
  let BOX = {};
  await page('w=vedder&d=1010', async b => { BOX = JSON.parse(await b.evaluate(`JSON.stringify((() => { const bad = [], seen = []; let n = 0;
    Object.entries(SNAP.states).forEach(([id, st]) => (st.rows || []).forEach(r => { (function walk(ns){ (ns || []).forEach(t => { if (t.node === 'tier' && t.tier === 'size') { n++; seen.push(t.says); if (/\\bbox\\b/i.test(t.says)) bad.push(id + ': ' + t.says); } if (t.children) walk(t.children); }); })([].concat(r.group?.tree || [])); }));
    return {n, bad: bad.slice(0, 3), nbad: bad.length, page: /\\bin this box\\b/.test(document.getElementById('keep')?.textContent || ''), sample: [...new Set(seen)].slice(0, 4)}; })())`)); });
  check('ur6-size-words', BOX.n > 10 && !BOX.nbad && !BOX.page, `${BOX.nbad} of ${BOX.n} size tiers say "box": ` + (BOX.bad || []).join(' | '));
  // (2) a pinned line stays until its section's LAST row has scrolled up under the stack, then drops: on every snapshot state, at every
  // scroll step, each pinned tier line's section still shows a row below the stack, and each section whose head has gone under the
  // stack while rows of it still show below (more than its own line could cover: 72 px, a three-line line and a row's sliver) has its
  // line pinned (a share the group line already says, form a, is that line)
  let PE = {};
  await page('w=vedder&d=1010', async b => { PE = JSON.parse(await b.evaluate(`(async () => { const ids = Object.keys(SNAP.states), R = e => e.getBoundingClientRect(), bad = [], seen = new Set(); let steps = 0, pinnedN = 0;
    const end = tn => { let m = -Infinity; tn.querySelectorAll('.krow, .tier').forEach(e => { const r = R(e); if (r.height > 0 && r.bottom > m) m = r.bottom; }); return m; };
    for (const [w, o] of Object.entries(WATERS)) for (const [d, v] of Object.entries(o.snap || {})) [].concat(v).forEach((id, p) => { if (seen.has(id) || !ids.includes(id)) return; seen.add(id);
      go({w, d: +d, p, sec: 'keep', sheet: ''}); const sc = $('sc'), hh = $('hdr')?.offsetHeight || 0, st = ST();
      for (let yy = 0; yy <= sc.scrollHeight; yy += 60) { sc.scrollTop = yy; pinUpdate(); steps++;
        const bar = document.querySelector('#pins .pinbar'), top = R(sc).top + hh, B = bar && !bar.hidden ? R(bar).bottom : top;
        const art = [...document.querySelectorAll('#keep article.row.ledger')].find(a => { const r = R(a); return r.top < top && r.bottom > top + 26; });
        const pinned = [...document.querySelectorAll('#pins .pl.c')]; pinnedN += pinned.length;
        pinned.forEach(pl => { const tn = art?.querySelector('section.tnode[data-tid="' + pl.dataset.tid + '"]');
          if (!tn) bad.push(id + ' @' + yy + ': a pinned line with no section (' + pl.textContent.trim().slice(0, 40) + ')');
          else if (end(tn) <= B + 4) bad.push(id + ' @' + yy + ': "' + pl.textContent.trim().slice(0, 40) + '" outlived its rows (' + Math.round(end(tn) - B) + ' px)'); });
        if (!art) continue;
        const g = (st.rows[+(art.dataset.row || 'r0').slice(1)] || {}).group, hl = g?.headline, totA = art.querySelector('.ghd.gpin [data-tot="a"]');
        const byId = {}; (function walk(ns){ (ns || []).forEach(x => { if (x.node === 'tier') { byId[x.id] = x; walk(x.children); } }); })([].concat(g?.tree || []));
        art.querySelectorAll('section.tnode').forEach(tn => { const t = byId[tn.dataset.tid], head = tn.querySelector(':scope > .tier'); if (!t || !head) return;
          if (totA && t.tier === 'share' && t.n === hl?.n) return;
          if (R(head).bottom < B - 2 && end(tn) > B + 72 && !pinned.some(pl => pl.dataset.tid === t.id)) bad.push(id + ' @' + yy + ': ' + t.tier + ' "' + (t.says || '').slice(0, 40) + '" has rows below the stack but no pinned line'); }); } });
    return JSON.stringify({states: seen.size, of: ids.length, steps, pinnedN, n: bad.length, bad: bad.slice(0, 6)}); })()`)); });
  check('ur6-pin-extent', PE.states === PE.of && PE.steps > 2000 && PE.pinnedN > 50 && !PE.n, `${PE.states}/${PE.of} states, ${PE.steps} steps, ${PE.pinnedN} pinned; ${PE.n} bad: ` + (PE.bad || []).join('; '));
  // the four owner states: deep inside the size section at its LAST row (its bottom just under the stack + a row) the size line is pinned
  const LAST = [];
  for (const hash of ['w=vedder&d=1010', 'w=chilliwack&d=1010&p=1', 'w=bellacoola&d=501&p=1', 'w=cowichan&d=1010&p=1']) await page(hash, async b => {
    LAST.push(JSON.parse(await b.evaluate(`(async () => { const sc = $('sc'), R = e => e.getBoundingClientRect(), tn = document.querySelector('#keep section.tnode[data-tnode="size"]'); if (!tn) return JSON.stringify({hash: location.hash.slice(8), none: true});
      const rows = [...tn.querySelectorAll('.krow')], last = rows[rows.length - 1], out = [];
      for (const frac of [0.5, 0.15]) { sc.scrollTop = 0; pinUpdate(); const bar = () => document.querySelector('#pins .pinbar');
        sc.scrollTop += R(last).bottom - R(sc).top - sc.clientHeight * frac; pinUpdate(); await new Promise(r => setTimeout(r, 120)); pinUpdate();
        out.push({frac, size: [...document.querySelectorAll('#pins .pl')].some(p => p.dataset.tier === 'size'), lastBelow: Math.round(R(last).bottom - R(bar()).bottom)}); }
      return JSON.stringify({hash: location.hash.slice(8), out}); })()`)));
  });
  const lb = LAST.filter(x => x.none || x.out.some(o => o.lastBelow > 40 && !o.size));
  check('ur6-pin-last-row', LAST.length === 4 && !lb.length, JSON.stringify(lb.length ? lb : LAST).slice(0, 400));
  // (3) "release" vs "closed" in plain words on Other fish: each status a row shows has a key line in the glossary's own plain words
  const KEY = [];
  for (const hash of ['w=hamill&d=615', 'w=glenn&d=615', 'w=vedder&d=1010', 'w=ape&d=501']) await page(hash, async b => {
    KEY.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} const ol = document.querySelector('#keep .olist'), sec = ol?.closest('section');
      const shown = ['release', 'closed'].filter(w => [...(ol?.querySelectorAll('.orow .k-' + w) || [])].length), plain = id => norm(new DOMParser().parseFromString(SNAP.gloss[id]?.body || '', 'text/html').querySelector('.glsays')?.textContent);
      const keys = [...(sec?.querySelectorAll('[data-status-key]') || [])].map(e => ({w: e.dataset.statusKey, t: norm(e.textContent)}));
      const want = {release: plain('catch_and_release'), closed: plain('no_fishing_for')};
      return {hash: location.hash.slice(8), shown, keys: keys.map(k => k.w), miss: shown.filter(w => !keys.some(k => k.w === w && k.t.includes(want[w]) && k.t.startsWith(w))), extra: keys.filter(k => !shown.includes(k.w)).map(k => k.w)}; })())`)));
  });
  check('ur6-status-key', KEY.some(x => x.shown.includes('release')) && KEY.some(x => x.shown.includes('closed')) && KEY.every(x => !x.miss.length && !x.extra.length), JSON.stringify(KEY));
  // (4) the carry-your-paper-licence line names only fish the water lets you keep: no steelhead on the Ape or the Bella Coola (closed to
  // steelhead); the Vedder (keeps hatchery steelhead) still names them
  const PAP = [];
  for (const hash of ['w=ape&d=501', 'w=bellacoola&d=501&p=1', 'w=bellacoola&d=1010&p=0', 'w=vedder&d=1010']) await page(hash + '&sec=licence', async b => {
    PAP.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} return {hash: location.hash.slice(8), paper: norm(document.querySelector('#licence .paper')?.textContent)}; })())`)));
  });
  check('ur6-paper-keepable', PAP.slice(0, 3).every(x => !/steelhead/i.test(x.paper)) && /hatchery steelhead/.test(PAP[3]?.paper || ''), JSON.stringify(PAP));
  // (5) a source line never says "Downstream ›" (a rule reached from a water downstream is named by that water): Hamill's bait ban is
  // the Duncan River's
  let SRC = {};
  await page('w=vedder&d=1010', async b => { SRC = JSON.parse(await b.evaluate(`JSON.stringify((() => { const ids = Object.keys(SNAP.states), seen = new Set(), bad = []; let n = 0, hamill = '';
    for (const [w, o] of Object.entries(WATERS)) for (const [d, v] of Object.entries(o.snap || {})) [].concat(v).forEach((id, p) => { if (seen.has(id) || !ids.includes(id)) return; seen.add(id);
      go({w, d: +d, p, sec: 'keep', sheet: ''}); document.querySelectorAll('.isrc .srcs').forEach(e => { n++; const t = e.textContent.trim(); if (/^Downstream\\b|· Downstream\\b/.test(t)) bad.push(id + ': ' + t); });
      if (id === 'hamill_615') hamill = [...document.querySelectorAll('#gear .isrc .srcs')].map(e => e.textContent.trim()).join(' | '); });
    return {n, bad: bad.slice(0, 5), nbad: bad.length, hamill}; })())`)); });
  check('ur6-source-name', SRC.n > 500 && !SRC.nbad && /Duncan River ›/.test(SRC.hamill), `${SRC.nbad} of ${SRC.n}: ${(SRC.bad || []).join('; ')}; Hamill gear: ${SRC.hamill}`);
  // (6) a water's own rule under the region total says how it stands to the stream share and the total: Glenn and Hamill (the Duncan
  // River's 5 rainbow trout) and the Vedder (its own 4 hatchery rainbow trout) — "Not part of the 2 from streams; part of the 5 a day …"
  const OWN = [];
  for (const hash of ['w=glenn&d=615', 'w=hamill&d=615', 'w=vedder&d=1010']) await page(hash, async b => {
    OWN.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { ${norm} const t = norm(document.querySelector('#keep section.tnode[data-tnode="own"] > .tier')?.textContent); return {hash: location.hash.slice(8), t}; })())`)));
  });
  check('ur6-own-relation', OWN.length === 3 && OWN.every(x => /\bnot part of the 2 from streams\b/i.test(x.t) && /\bpart of the [45] a day\b/.test(x.t)), JSON.stringify(OWN));
}

async function roundUr7(){
  // every snapshot state, in turn, on the Keep tab (go() as ur6-pin-extent does); `fn` runs in the page per state
  const EACH = body => `(async () => { const ids = Object.keys(SNAP.states), seen = new Set(), out = [];
    for (const [w, o] of Object.entries(WATERS)) for (const [d, v] of Object.entries(o.snap || {})) [].concat(v).forEach((id, p) => { if (seen.has(id) || !ids.includes(id)) return; seen.add(id);
      go({w, d: +d, p, sec: 'keep', sheet: ''}); const st = ST(), keep = document.getElementById('keep'); ${body} });
    return JSON.stringify({states: seen.size, of: ids.length, out}); })()`;
  const walkT = 'var walkT = (ns, f, par) => (ns || []).forEach(x => { f(x, par); if (x.node === "tier") walkT(x.children, f, x); });';
  // (A) the 2 said once: no share says the lifted fish where an own tier says its relation; the Vedder's share folds under the head
  let A = {};
  await page('w=vedder&d=1010', async b => { A = JSON.parse(await b.evaluate(`(() => { ${walkT} const bad = []; let rel = 0;
    Object.entries(SNAP.states).forEach(([id, st]) => (st.rows || []).forEach(r => { const g = r.group; if (!g?.tree) return; const ts = []; walkT([].concat(g.tree), x => { if (x.node === 'tier') ts.push(x); });
      if (ts.some(t => t.relation)) { rel++; ts.filter(t => t.tier === 'share' && /not counting/.test(t.says)).forEach(t => bad.push(id + ': ' + t.says)); } }));
    const keep = document.getElementById('keep'), folded = !!keep.querySelector('[data-share-folded]'), txt = keep.textContent;
    return JSON.stringify({rel, bad, folded, notCounting: /not counting/.test(txt), relation: !!keep.querySelector('[data-relation]'), twos: (keep.querySelector('article.row.ledger')?.textContent.match(/\\b2 of the 4\\b/g) || []).length}); })()`)); });
  check('ur7-share-said-once', A.rel >= 3 && !A.bad.length && A.folded && !A.notCounting && A.relation && !A.twos, JSON.stringify(A).slice(0, 300));
  // (1) the worked keep-the-share line says the fish count as the 2 elsewhere ("here"), never "none from other streams"
  let W = {};
  await page('w=glenn&d=615', async b => { W = JSON.parse(await b.evaluate(`(() => { const bad = [], good = [];
    Object.entries(SNAP.states).forEach(([id, st]) => (st.rows || []).forEach(r => (r.group?.worked || []).forEach(w => { if (/none from other/.test(w.a)) bad.push(id + ': ' + w.a);
      if (w.at === 'here' && w.share != null && /^If you keep \\d+ .+ here$/.test(w.q)) good.push(id); })));
    const li = [...document.querySelectorAll('#keep .worked li')].map(e => e.textContent.replace(/\\s+/g, ' ').trim());
    return JSON.stringify({bad: bad.slice(0, 3), nbad: bad.length, good: good.length, glenn: li.find(t => /^If you keep 2 rainbow trout here:/.test(t)) || li.join(' | ')}); })()`)); });
  check('ur7-worked-here', !W.nbad && W.good >= 4 && /^If you keep 2 rainbow trout here: they count as your 2 from streams anywhere else today — up to 3 more of any kind, from here or from lakes\.$/.test(W.glenn), JSON.stringify(W).slice(0, 300));
  // (2) a kind directly under the share says it is part of it (data, every state); the Chilliwack part 2's rainbow row draws it
  let K = {};
  await page('w=chilliwack&d=1010&p=1', async b => { K = JSON.parse(await b.evaluate(`(() => { ${walkT} const bad = []; let n = 0;
    Object.entries(SNAP.states).forEach(([id, st]) => (st.rows || []).forEach(r => walkT([].concat(r.group?.tree || []), (x, par) => {
      if (x.node !== 'kind' || par?.tier !== 'share' || x.folded || !x.n) return; n++; if (!/\\(part of the \\d+ from |^Only \\d+ of the \\d+ from /.test(x.says)) bad.push(id + ': ' + x.says); })));
    const row = document.querySelector('#keep .tnode[data-tnode="share"] > .tch > .krow[data-sp="RB"] .ks');
    return JSON.stringify({n, bad: bad.slice(0, 3), nbad: bad.length, rb: (row?.textContent || '').replace(/\\s+/g, ' ').trim()}); })()`)); });
  check('ur7-kind-part-of-share', K.n >= 3 && !K.nbad && K.rb === 'Up to 2 hatchery rainbow trout (part of the 2 from streams)', JSON.stringify(K).slice(0, 300));
  // (B) the size section's head is the colour of its side bar (text contrast >= 4.5:1, the bar >= 3:1, light and dark), the bar flush
  // against the screen's left edge, the section's rows on the page's text edge; the pinned size line the same
  const B = [];
  for (const theme of ['', '&theme=dark']) for (const hash of ['w=vedder&d=1010', 'w=chilliwack&d=1010&p=1', 'w=bellacoola&d=501&p=1', 'w=cowichan&d=1010&p=1']) await page(hash + theme, async b => {
    B.push(JSON.parse(await b.evaluate(`(async () => { const R = e => e.getBoundingClientRect(), app = R(document.querySelector('.app')), lum = c => { const m = c.match(/\\d+(\\.\\d+)?/g).map(Number).slice(0, 3).map(v => { v /= 255; return v <= .03928 ? v / 12.92 : ((v + .055) / 1.055) ** 2.4; }); return .2126 * m[0] + .7152 * m[1] + .0722 * m[2]; };
      const cr = (a, b) => { const x = lum(a), y = lum(b); return +((Math.max(x, y) + .05) / (Math.min(x, y) + .05)).toFixed(2); };
      const card = getComputedStyle(document.querySelector('#keep article.row.ledger')).backgroundColor, bg = card === 'rgba(0, 0, 0, 0)' ? getComputedStyle(document.body).backgroundColor : card;
      const tn = document.querySelector('#keep section.tnode[data-tnode="size"]'); if (!tn) return JSON.stringify({hash: location.hash.slice(8), none: true});
      const cs = getComputedStyle(tn), tl = tn.querySelector(':scope > .tier .tl'), num = tl.querySelector('.tn'), ks = tn.querySelector('.krow .ks'), other = [...document.querySelectorAll('#keep .ghd .rtitle, #keep .tnode:not([data-tnode="size"]) > .tier .tl')].find(e => R(e).width);
      const sc = $('sc'); sc.scrollTop += R(ks).top - R(sc).top - 120; pinUpdate(); await new Promise(r => setTimeout(r, 150)); pinUpdate();
      const pl = document.querySelector('#pins .pl[data-tier="size"]'), pcs = pl && getComputedStyle(pl);
      return JSON.stringify({hash: location.hash.slice(8), bar: cs.borderLeftColor, head: getComputedStyle(tl).color, num: num ? getComputedStyle(num).color : null, text: cr(getComputedStyle(tl).color, bg), barCr: cr(cs.borderLeftColor, bg),
        barX: Math.round(R(tn).left - app.left), rowsX: Math.round(R(ks).left - app.left), textX: other ? Math.round(R(other).left - app.left) : null,
        pin: pl ? {x: Math.round(R(pl).left - app.left), bar: pcs.borderLeftColor, color: pcs.color, w: parseFloat(pcs.borderLeftWidth)} : null}); })()`)));
  });
  const bb = B.filter(x => x.none || x.head !== x.bar || (x.num && x.num !== x.bar) || x.text < 4.5 || x.barCr < 3 || x.barX !== 0 || x.rowsX !== x.textX || !x.pin || x.pin.x !== 0 || x.pin.bar !== x.bar || x.pin.color !== x.bar || x.pin.w < 2);
  check('ur7-size-head-colour', B.length === 8 && !bb.length, JSON.stringify(bb.length ? bb.slice(0, 3) : B.slice(0, 2)).slice(0, 500));
  // (4) a fish that needs a stamp or licence add-on names it on its own section (the licence's own words, a link to the Licence tab),
  // only where the water lets you fish for / keep it — every state: every such document whose fish has a section it may be fished
  // (kept) on carries a line there; no line on a closed fish; the Vedder's white sturgeon, Kootenay Lake's rainbow trout, the steelhead
  let S = {};
  await page('w=vedder&d=1010', async b => { S = JSON.parse(await b.evaluate(EACH(`
      const docs = [...parse(st.licence?.adult || '').querySelectorAll('li[data-doc]')].map(li => ({doc: li.dataset.doc, act: li.dataset.act, sp: li.dataset.sp.split(' '), name: li.querySelector('b').firstChild.textContent.trim()}));
      const lines = [...keep.querySelectorAll('[data-stamp-line]')], btn = [...keep.querySelectorAll('[data-stamp]')];
      const secs = [...keep.querySelectorAll('.krow[data-sp], .orow, article.row.ledger:not(.g28)')];
      lines.forEach(l => { if (!l.querySelector('[data-sec="licence"]')) out.push(id + ': a stamp line with no Licence link');
        const host = l.closest('.krow, article.row.ledger') || l.previousElementSibling; if (host?.querySelector?.('.k-closed') || host?.classList?.contains('closed')) out.push(id + ': a stamp line on a closed fish'); });
      docs.forEach(x => { const has = lines.some(l => l.dataset.stampLine === x.doc && l.textContent.includes(x.name)) || (x.doc === 'steelhead_stamp' && btn.length);
        const open = st.rows.some(r => (r.members || []).some(c => x.sp.includes(c)) && (x.act === 'targeting' ? ['keep', 'release', 'nolimit'] : ['keep', 'nolimit']).includes(r.kind));
        if (open && !has && x.doc !== 'steelhead_stamp') out.push(id + ': ' + x.doc + ' (' + x.act + ' ' + x.sp.join(' ') + ') not named on its fish'); });
      if (id === 'vedder_1010' || id === 'klmb_1010' || id === 'glenn_615') out.push('@' + id + ' ' + lines.map(l => l.dataset.stampLine).join(',') + (btn.length ? ' +btn' : ''));`))); });
  const named = S.out.filter(x => x.startsWith('@')), sbad = S.out.filter(x => !x.startsWith('@'));
  const ok4 = S.states === S.of && !sbad.length && named.some(x => /^@vedder_1010 .*white_sturgeon/.test(x) && /\+btn/.test(x)) && named.some(x => /^@klmb_1010 .*kootenay/i.test(x)) && named.some(x => /^@glenn_615 $/.test(x));
  check('ur7-stamps-on-the-fish', ok4, `${S.states}/${S.of} states; ${named.join(' | ')}; ${sbad.length} bad: ${sbad.slice(0, 4).join('; ')}`);
  // the stamp link lands on the Licence tab
  let L = {};
  await page('w=klmb&d=1010', async b => { L = JSON.parse(await b.evaluate(`(async () => { const l = document.querySelector('#keep [data-stamp-line] [data-sec="licence"]'); if (!l) return JSON.stringify({none: true});
    l.click(); await new Promise(r => setTimeout(r, 1500)); const sc = $('sc'), lic = document.getElementById('licence'), R = e => e.getBoundingClientRect();
    return JSON.stringify({pressed: [...document.querySelectorAll('.segm[role=tablist] button')].find(b => b.getAttribute('aria-pressed') === 'true')?.dataset.sec, licTop: Math.round(R(lic).top - R(sc).top)}); })()`)); });
  check('ur7-stamp-link-lands', L.pressed === 'licence' && Math.abs(L.licTop) < 200, JSON.stringify(L));
}

// ---------- integration round 1 (the app port's generators found three prototype bugs at v33; handoff/PLAN/app-integration D20, T14) ----------
async function roundInt1(){
  // (1) pinUpdate runs without throwing on every snapshot state, scrolled top to bottom (it threw on 25/134: a ledger row with no group)
  let PU = {};
  await page('w=vedder&d=1010', async b => { PU = JSON.parse(await b.evaluate(`(async () => { const bad = [], ids = Object.keys(SNAP.states), wait = ms => new Promise(r => setTimeout(r, ms)); let n = 0, chains = 0; const chainBad = [];
    const want = new Set(ids), seen = new Set();
    for (const [w, o] of Object.entries(WATERS)) for (const [d, v] of Object.entries(o.snap || {})) [].concat(v).forEach((id, p) => { if (!want.has(id) || seen.has(id)) return; seen.add(id);
      try { go({w, d: +d, p, sec: 'keep', sheet: ''}); } catch (e) { bad.push(id + ' render: ' + e.message); return; }
      document.querySelectorAll('#keep .closechain li .cd').forEach(e => { chains++; const t = (e.firstChild?.textContent || '').trim(); if (!/[A-Za-z]/.test(t)) chainBad.push(id + ': "' + t + '"'); });
      const sc = $('sc'); for (let y = 0; y <= sc.scrollHeight; y += 120) { sc.scrollTop = y; try { pinUpdate(); n++; } catch (e) { bad.push(id + ' @' + y + ': ' + e.message); break; } } });
    return JSON.stringify({states: seen.size, of: ids.length, n, bad, chains, chainBad}); })()`)); });
  check('int1-pin-never-throws', PU.states === PU.of && PU.n > 1000 && !PU.bad.length, `${PU.states}/${PU.of} states, ${PU.n} updates; ` + (PU.bad || []).slice(0, 5).join('; ') + ((PU.bad || []).length > 5 ? ` … ${PU.bad.length} in all` : ''));
  // (2) a closed-all-year chain line keeps its words (chw0 and the other closed-all-year states: "No fishing for any game fish." was cut to ".")
  check('int1-chain-words', PU.chains >= 1 && !PU.chainBad.length, `${PU.chains} chain lines; ` + PU.chainBad.slice(0, 5).join('; '));
  // (3) a tidal water's Gear/Methods do not say it is closed all year: they carry the data's tidal words (D20 b)
  let TD = {};
  await page('w=x31p&d=1010&sec=gear', async b => { TD = JSON.parse(await b.evaluate(`JSON.stringify({gear: (document.getElementById('gear')?.textContent || '').replace(/\\s+/g, ' ').trim(), methods: (document.getElementById('methods')?.textContent || '').replace(/\\s+/g, ' ').trim()})`)); });
  // (4, coordinator after v34) with form a the share is said once: a share tier whose words add nothing to the head's lead folds its
  // line and kicker (Cowichan, Chilliwack p2); a share with more to say keeps it (the Vedder's "not counting hatchery rainbow trout")
  const SO = [];
  for (const hash of ['w=cowichan&d=1010&p=1', 'w=chilliwack&d=1010&p=1', 'w=vedder&d=1010', 'w=glenn&d=615']) await page(hash, async b => {
    SO.push(JSON.parse(await b.evaluate(`JSON.stringify((() => { const n = t => (t || '').replace(/\\s+/g, ' ').trim(), art = document.querySelector('#keep article.row.ledger');
      const lead = !!art?.querySelector('[data-share-lead]'), lines = [...(art?.querySelectorAll('.tnode[data-tnode="share"] > .tier .tl') || [])].map(e => n(e.textContent));
      return {hash: location.hash.slice(8), lead, lines, kick: art?.querySelectorAll('.tnode[data-tnode="share"] > .tier [data-scope-tag]').length || 0}; })())`)));
  });
  const plain = SO.slice(0, 3), more = SO.slice(3);   // UR7 A: the Vedder joins the plain shares (its lifted fish said by the own tier's relation)
  check('int1-share-said-once', SO.every(x => x.lead) && plain.every(x => !x.lines.some(l => /^Only \d+ of the \d+ may be from streams\.?( |$)/.test(l)) && !x.kick) && more.every(x => x.lines.length >= 1),
    JSON.stringify(SO).slice(0, 400));
  check('int1-tidal-gear', !!TD.gear && !/any day of the year/.test(TD.gear + TD.methods) && /Tidal water/.test(TD.gear), JSON.stringify(TD).slice(0, 300));
}
