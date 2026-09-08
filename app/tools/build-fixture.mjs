/**
 * Packages the development bundle. `pnpm fixture`.
 *
 * FORMAT: SQLite, from `pipeline/deliver/bundle/schema.sql` — the SAME file the production bundler
 * (`python -m pipeline.deliver.bundle`) executes, so a development fixture cannot drift from the
 * artifact a real build produces.
 * One file serves both platforms — resident on the phone, range-read from R2 on the web —
 * and the tables are written in the order the queries read them, so one reach's rules land
 * in one or two 64 KB reads. An earlier pass of this shipped a single JSON blob, which
 * quietly dropped the whole storage design: no indexes, no query planner, and nothing to
 * range-read, so the web packaging had no way to exist.
 *
 * CONTENT: extracted from `design/riffle.html`, which embeds a real slice of build 54ea0bb4
 * for the Chilliwack / Harrison valley. Real data, so the UI is built against the cases
 * that exist — a gauge at its 4th percentile, a rule nobody could place, a creek the Fraser
 * must refuse to speak for.
 *
 * WHAT IS NOT HERE, and where it lives instead:
 *
 *   geometry, name, alias,     the TILES. `pipeline/deliver/tiles/tile-contract.json` puts
 *   magnitude, order, mus         section_id, item, name, alt, mag, ord, mus, areas on the
 *                                 stream layer. A second copy is a second source of truth.
 *   the current reading         a FEED. Written beside the bundle as feeds/gauge/*.json,
 *                                 because it changes every 30 minutes and the bundle
 *                                 changes per edition — different clocks, different files.
 *
 * `section_down` holds pointers only for sections INSIDE a gauge's watershed. The contract
 * cuts the rest: the only use is tracing a tap down to its gauge, and every reach on that
 * path is by definition in the same shed. 2.02 M pointers become ~300 K, losing nothing.
 */
import { DatabaseSync } from "node:sqlite";
import { readFileSync, writeFileSync, mkdirSync, rmSync, statSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { execFileSync } from "node:child_process";

const here = (p) => fileURLToPath(new URL(p, import.meta.url));
const html = readFileSync(here("../design/riffle.html"), "utf8");
const m = /<script id="bundle" type="application\/json">([\s\S]*?)<\/script>/.exec(html);
if (!m) throw new Error('design/riffle.html has no <script id="bundle">');
const src = JSON.parse(m[1]);

const OUT = here("../packages/data/dev");
mkdirSync(`${OUT}/feeds/gauge`, { recursive: true });
const dbPath = `${OUT}/bundle.sqlite`;
rmSync(dbPath, { force: true });
const db = new DatabaseSync(dbPath);

// Deterministic, so `git diff --exit-code` can gate the artifact: a fixed page size, no
// journal file, and no wall-clock anywhere in the content.
db.exec("PRAGMA page_size = 4096");
db.exec("PRAGMA journal_mode = OFF");

// THE SCHEMA IS NOT DEFINED HERE. `pipeline/deliver/bundle/schema.sql` is the one definition of
// the bundle format, and the production bundler executes the same file. A second copy of a
// DDL is a second contract, and they diverge the first time one is edited.
db.exec(readFileSync(here("../../pipeline/deliver/bundle/schema.sql"), "utf8"));

const insert = (sql, rows) => {
  const st = db.prepare(sql);
  for (const r of rows) st.run(...r);
  return rows.length;
};
const counts = {};

// ---- identity ---------------------------------------------------------------------
// Riffle's `waters` are SECTIONS. Group them by name to recover the item they belong to.
const byName = new Map();
for (const w of src.waters) {
  const item = w.rules?.length ? src.rule_entry[w.rules[0]] ?? null : null;
  const e = byName.get(w.name) ?? { name: w.name, kind: w.kind, item: null, sections: [] };
  e.sections.push(w.id);
  if (item && !e.item) e.item = item;
  byName.set(w.name, e);
}
// A water with no registry item still needs an id to be searchable — 97.6% of water has
// none, and "not regulated" is not "not findable".
const idOf = (name, e) => e.item ?? `name:${name}`;

counts.item = insert("INSERT OR REPLACE INTO item VALUES (?,?,?)",
  [...byName].sort().map(([n, e]) => [idOf(n, e), n, e.kind]));
counts.alias = insert("INSERT INTO alias VALUES (?,?)",
  [...byName].flatMap(([n, e]) => (src.alias[n] ?? []).map((a) => [idOf(n, e), a])));
counts.item_section = insert("INSERT INTO item_section VALUES (?,?)",
  [...byName].flatMap(([n, e]) => e.sections.map((s) => [idOf(n, e), s])));

// ---- regulations ------------------------------------------------------------------
const entries = Object.entries(src.entries).sort();
counts.entry = insert("INSERT INTO entry VALUES (?,?,?,?,?,?)", entries.map(([id, e]) => [
  id, id, e.identity?.name ?? id, e.regs_verbatim ?? "",
  JSON.stringify(e.source_symbols ?? []), JSON.stringify(e.identity?.mus ?? []),
]));
/**
 * The verbatim date strings, as the structured windows the client reads.
 *
 * THE FIXTURE SHIPPED THE RAW STRINGS ONCE, and so did the bundler, and the app threw
 * `Cannot read properties of undefined (reading 'month')` on every regulation screen that
 * evaluated a seasonal rule. The fixture having the SAME bug is why no test caught it: the
 * app suite runs against this file, so an agreeing pair of wrongs looked like a passing
 * suite. `packages/data/src/bundle/source.test.ts` now evaluates a seasonal rule out of the
 * bundle, which is the assertion that was missing.
 *
 * PARSED BY THE PIPELINE'S OWN PARSER, shelled out to. A JS reimplementation would be a
 * second answer to "when is this rule in force" — and `pipeline/regs/parsing/dates.py` is
 * not just a parser, it is the hallucination guard for these strings: a date that does not
 * resolve to a real calendar window is a curation error there, and inventing a lenient
 * second reading here would hide exactly the ones it exists to catch.
 */
// The repo root, two levels above app/tools — same shape as `here` above.
const REPO = here("../../");
const PY = `${REPO}.venv/bin/python`;
function windowsOf(dates) {
  if (!dates.length) return [];
  const script =
    "import json,sys\n" +
    `sys.path.insert(0, ${JSON.stringify(REPO)})\n` +
    "from pipeline.regs.parsing.dates import parse_date_windows\n" +
    "ws = parse_date_windows(json.loads(sys.argv[1]))\n" +
    "print(json.dumps([{'from': {'month': w.start_month, 'day': w.start_day},\n" +
    "                   'to': {'month': w.end_month, 'day': w.end_day}} for w in ws]))";
  return JSON.parse(execFileSync(PY, ["-c", script, JSON.stringify(dates)],
                                 { encoding: "utf8" }));
}

// The build's field names, not invented ones: `restriction_type` is the kind, `dates` are
// the windows, and `needs_review` + `unresolved_locators` are what make a rule UNCERTAIN —
// a rule nobody could place must never vote on an outcome (core/status.ts).
counts.rule = insert("INSERT OR REPLACE INTO rule VALUES (?,?,?,?,?,?,?,?,?,?)",
  entries.flatMap(([id, e]) => (e.rules ?? []).map((r) => [
    id, r.rule_id, r.restriction_type ?? "other",
    // Specificity, which drives precedence. Every rule in the real corpus is
    // `section`; `mu` arrives with zone regulations.
    (r.extents ?? []).some((x) => x.area_id) ? "area" : "section",
    JSON.stringify(windowsOf(r.dates ?? [])),
    r.species?.length ? JSON.stringify(r.species) : null,
    // `exempts_from` is a list; a subject is one thing, so join or drop it.
    Array.isArray(r.exempts_from) ? (r.exempts_from.join(",") || null)
                                  : r.exempts_from ?? null,
    r.needs_review || (r.unresolved_locators ?? []).length ? 1 : 0,
    r.rule_text ?? r.details ?? null,
    r.display_location ?? r.location_text ?? null,
  ])));
/*
 * THE INTERNED RULE SETS, built the way the pipeline builds them.
 *
 * The fixture must produce the SHAPE the bundler emits, not a hand-written approximation of
 * it: writing this file by hand is how 1,785 rows of a trust band the pipeline has never
 * emitted got in here and three test files asserted against them. So the sets are interned
 * here exactly as `pipeline/deliver/bundle/rules.py` interns them — group each section's
 * (entry, rule, via) triples, sort, intern, point at it.
 *
 * The design fixture records only direct bindings, so every `via` here is "reach". A
 * fixture with no tributary rows is honest about what the design file contains; it is not a
 * claim that the province has none (98.6% of real bindings are tributary).
 */
const bySection = new Map();
for (const [ruleId, scopes] of Object.entries(src.rule_sections)) {
  const entryId = src.rule_entry[ruleId];
  if (!entryId) continue;
  for (const section of scopes ?? []) {
    if (!bySection.has(section)) bySection.set(section, []);
    bySection.get(section).push([entryId, ruleId, "reach"]);
  }
}
const intern = new Map();
const sets = [];
const sectionSet = [];
for (const section of [...bySection.keys()].sort()) {
  const rows = bySection.get(section)
    .map((t) => t.join("\u0000")).sort();
  const key = rows.join("\u0001");
  let id = intern.get(key);
  if (id === undefined) {
    id = sets.length;
    intern.set(key, id);
    sets.push(rows.map((r) => r.split("\u0000")));
  }
  sectionSet.push([section, id]);
}
counts.section_ruleset = insert("INSERT INTO section_ruleset VALUES (?,?)", sectionSet);
counts.ruleset = insert("INSERT INTO ruleset VALUES (?,?,?,?)",
  sets.flatMap((rows, id) => rows.map(([e, r, via]) => [id, e, r, via])));

// ---- conditions -------------------------------------------------------------------
// Stream magnitudes, CONSTRUCTED. The design fixture records a trust band per section but
// never the magnitudes it was cut from, and the app ranks gauges on them — so a fixture
// with none would leave that ordering untested and every reach tied. Each section gets a
// magnitude consistent with its own band against a nominal gauge of 1,000, spread within
// the band by position so the ordering has something to order. Deterministic, and it never
// contradicts the band the design actually recorded.
const NOMINAL_GAUGE_MAG = 1000;
// The floors come from @app/core, which generates them from pipeline/gauges/consume/shed.py. There
// used to be a literal here — a THIRD copy of the trust vocabulary after the pipeline's and
// the app's — and it carried a `none: 0` band that the pipeline has never written, which is
// how 1,785 `trust = 'none'` rows got into the fixture bundle and into the assertions three
// test files made against it.
const BAND_FLOOR = JSON.parse(readFileSync(
  new URL("../packages/core/src/gauge-policy.generated.json", import.meta.url), "utf8")).floor;
const shedBands = {};
for (const [section, q] of Object.entries(src.shed_q)) (shedBands[q] ??= []).push(section);
const shedMag = {};
for (const [band, sections] of Object.entries(shedBands)) {
  sections.sort();
  const floor = BAND_FLOOR[band] ?? 0;
  for (const [i, section] of sections.entries())
    // Inside the band, never at or past the next one up: floor * (1 .. <10).
    shedMag[section] = Math.max(
      1, Math.round(NOMINAL_GAUGE_MAG * floor * (1 + (9 * i) / Math.max(1, sections.length))));
}

// The stations themselves. Every fixture gauge is treated as live and active: this is the
// happy path the UI is designed against, and the *unhappy* path — a station with a record
// but no current reading — is covered by `no-data-is-not-an-answer.test.ts` rather than by
// quietly making one fixture gauge silent, which would make every other test ambiguous.
// No liveness column of any kind. The bundle says a gauge exists and where; the feed
// index says which are talking, by containing them. Any boolean here would be right the
// week the fixture was cut and wrong afterwards.
counts.gauge = insert("INSERT INTO gauge VALUES (?,?,?,?,?,?,?,?,?,?)",
  src.gauges.map((g) => {
    const section = Object.keys(src.gauge_shed)
      .filter((s) => src.gauge_shed[s] === g.id).sort()[0] ?? null;
    // The design fixture's gauges were placed by hand, so their provenance is exactly
    // that — recorded rather than left null, which would read as "never attempted".
    return [g.id, g.name, section ? (src.section_item?.[section] ?? null) : null, section,
            g.at?.[0] ?? null, g.at?.[1] ?? null, g.area_km2 ?? null, NOMINAL_GAUGE_MAG,
            "fixture", null];
  }));

// One station per section, which is also what the production bundler emits: a second
// gauge on the same reach drains more or less country than the first, so it is a worse
// answer to the same question rather than a second opinion.
// A REACH NO GAUGE REPRESENTS GETS NO ROW, exactly as `pipeline/gauges/consume/shed.py` does it:
// `trust_for` returns None below the `weak` floor and the bundler writes nothing. Defaulting
// to a "none" band here invented a fourth value, and because three test files asserted
// against this file rather than against a real bundle, that invention looked like the
// contract for months. The refusal reaches the client as a null link, not as a labelled row.
counts.section_gauge = insert("INSERT OR REPLACE INTO section_gauge VALUES (?,?,?,?)",
  Object.entries(src.gauge_shed).sort()
    .filter(([section]) => src.shed_q[section] && src.shed_q[section] !== "none")
    .map(([section, station]) => [section, station, src.shed_q[section],
                                  shedMag[section] ?? null]));

/*
 * DONOR PANELS — the fixture's copy of what the app actually reads.
 *
 * These were missing entirely, so the fixture's Conditions map coloured NOTHING and every
 * sheet said "no gauge on this water is close enough in size to speak for it". The dev app
 * and the render tests were exercising the refusal path and only the refusal path, on a
 * fixture whose whole purpose is the happy one.
 *
 * BUILT THE WAY THE PIPELINE BUILDS THEM: the donor SET is interned and shared, weights are
 * derived at read time from the two catchments, and a section's own area lives on the
 * section. That is what makes the real dictionary collapse 191,026 sections into 1,898
 * panels, and a fixture with per-section weights would not exercise the same code.
 *
 * Every gauge in the fixture is a candidate for every shed section, which is a simplification
 * the province does not permit and the fixture does: five stations over one river system,
 * where the point is to have a panel with more than one member so the table, the map key and
 * the weight bars all have something to draw.
 */
const shedSections = Object.keys(src.gauge_shed).sort();
const panelDonors = src.gauges
  .filter((g) => g.area_km2)
  .map((g) => [g.id, g.area_km2]);
if (panelDonors.length) {
  counts.section_panel = insert("INSERT OR REPLACE INTO section_panel VALUES (?,?,?)",
    // One panel for the whole fixture shed: same donors, so the dictionary interns to a
    // single row — which is the shape the real bundle has and the client must handle.
    shedSections.map((section) => [section, 1, shedMag[section]
      // Area from the same drainage relation the pipeline fits, so a fixture ratio is a
      // plausible ratio rather than an invented one.
      ? 1.237 * Math.pow(shedMag[section], 0.851) : null]));
  counts.panel_member = insert("INSERT OR REPLACE INTO panel_member VALUES (?,?,?,?,?,?,?,?)",
    panelDonors.map(([station, area], i) =>
      // `role` alternates so the downstream penalty is exercised; `years` spans the record
      // gate so one donor counts at less than full weight; ONE donor is on regulated
      // water, so the dam caveat has something to fire on in dev rather than only in a
      // unit test; and one is on a TRIBUTARY, so the fourfold same-river weighting is
      // exercised rather than assumed.
      [1, i, station, i % 2 === 0 ? "up" : "down", area, 10 + i * 15,
       i === 1 ? 1 : 0, i === 2 ? 0 : 1]));
}

// Only inside a shed. The contract cuts the rest and nothing is lost.
const inShed = new Set(Object.keys(src.gauge_shed));
counts.section_down = insert("INSERT OR REPLACE INTO section_down VALUES (?,?)",
  Object.entries(src.down).filter(([s]) => inShed.has(s)).sort());

// KEYED BY PARAMETER, because a station measuring both stage and discharge has TWO
// envelopes in two different units. The design deck only carries the discharge one, so
// that is what this writes — labelled, rather than left ambiguous for a reader to assume.
counts.gauge_clim = insert("INSERT OR REPLACE INTO gauge_clim VALUES (?,?,?,?,?,?,?,?)",
  src.gauges.flatMap((g) => (g.clim?.band ?? []).map((b, i) =>
    // stored p10/p25/p50/p75/p90 — p0 and p100 do not interpolate (82% error, §5)
    [g.id, "discharge", i,
     b[1] ?? null, b[2] ?? null, b[3] ?? null, b[4] ?? null, b[5] ?? null])));

// ---- lakes ------------------------------------------------------------------------
counts.chart = insert("INSERT INTO chart VALUES (?,?,?,?,?,?,?,?,?)",
  src.bathy.map((b) => {
    const e = byName.get(b.name);
    return [e ? idOf(b.name, e) : null, b.name, b.wbid, b.name, "scan",
            b.draft ?? null, b.scale ?? null, b.area_km2 ?? null, b.pdf ?? null];
  }));
counts.release = insert("INSERT INTO release VALUES (?,?,?,?,?)",
  Object.entries(src.stocking).sort().flatMap(([name, s]) =>
    (s.recent ?? []).map((r) => [name, r.d.slice(0, 10), r.sp,
                                 r.q === null ? null : Number(r.q), r.st ?? null])));

// ---- precomputes ------------------------------------------------------------------
// An integer place_id, matching the production bundler: place_water carries it hundreds of
// thousands of times, and `osm` (name@lat,lon) is the stable natural key.
const places = src.places.map((p, i) => ({
  id: i + 1, osm: `${p.name}@${p.lat.toFixed(4)},${p.lon.toFixed(4)}`,
  name: p.name, kind: p.place, lat: p.lat, lon: p.lon,
}));
counts.place = insert("INSERT INTO place VALUES (?,?,?,?,?,?,?)",
  places.map((p) => [p.id, p.osm, p.name, p.kind, null, p.lat, p.lon]));

// Measure to every named WATERBODY, not every section — that is what made the full matrix
// cheap (§6). One representative point per name, the midpoint of its longest line.
const R = 6371, rad = (d) => (d * Math.PI) / 180;
const km = (aLat, aLon, bLat, bLon) => {
  const dLat = rad(bLat - aLat), dLon = rad(bLon - aLon);
  const h = Math.sin(dLat / 2) ** 2 +
            Math.cos(rad(aLat)) * Math.cos(rad(bLat)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(h));
};
const anchor = new Map();
for (const w of src.waters) {
  const line = (w.lines ?? []).reduce((a, b) => (b.length > (a?.length ?? 0) ? b : a), null);
  if (!line?.length) continue;
  const [lon, lat] = line[Math.floor(line.length / 2)];
  const cur = anchor.get(w.name);
  if (!cur || (w.mag ?? 0) > cur.mag) anchor.set(w.name, { lat, lon, mag: w.mag ?? 0 });
}
// Keyed on item_id, like the production bundler. A name that has no item is skipped
// rather than given a minted id — a forged id fails every later lookup silently.
const itemOfName = new Map([...byName].map(([n, e]) => [n, idOf(n, e)]));
const nearRows = [];
for (const p of places)
  for (const [name, a] of anchor) {
    const item = itemOfName.get(name);
    if (!item) continue;
    const d = km(p.lat, p.lon, a.lat, a.lon);
    if (d <= 25) nearRows.push([p.id, item, Number(d.toFixed(2))]);
  }
counts.place_water = insert("INSERT INTO place_water VALUES (?,?,?)", nearRows);

// ---- indexes, written AFTER the rows so they are built once ------------------------
db.exec(readFileSync(here("../../pipeline/deliver/bundle/indexes.sql"), "utf8"));
insert("INSERT INTO meta VALUES (?,?)", [
  ["version", `riffle-${src.report?.build ?? "full"}`],
  ["source", "design/riffle.html — build 54ea0bb4, Chilliwack/Harrison valley"],
  ["generated_by", "pnpm fixture"],
  // The staleness gate. Empty means "no expiry known", which the client renders as no
  // promise; a fabricated date would read as one. Present because the CLIENT reads it —
  // a fixture that omits a key the app asks for is a green suite testing the wrong file.
  ["valid_until", ""],
  ["shed_rule", JSON.stringify(src.shed_rule ?? {})],
  ["attribution", JSON.stringify(src.attr ?? {})],
]);
db.exec("VACUUM");
db.close();

// ---- feeds: a different clock, so a different file ---------------------------------
//
// A FEED CARRIES ONLY WHAT CHANGES. The index used to ship `name`, `at`, `areaKm2`,
// `river` and — worst — `section`, all of which the bundle already holds. That re-sent
// identity every thirty minutes, and the section id in particular broke AGENTS rule 5:
// section ids survive a rebuild only 94% of the time, so a feed keyed on one goes silently
// wrong on 6% of reaches at the next build. `tools/feed-contract.test.ts` now fails if any
// of it comes back.
//
// The index exists for ONE job: colouring 450 dots without 450 fetches. So it carries the
// percentile that decides the colour and the time that decides whether to trust it. A tap
// fetches the station's own file, which has everything.
const index = {};
for (const g of src.gauges) {
  const now = g.standing?.pctile ?? null;
  index[g.id] = {
    percentile: now,
    observedAt: g.last?.[0] ?? null,
    // Tomorrow, the day after, the day after that — as PERCENTILES, not discharges, for
    // the same reason `percentile` is: it is what picks a colour, the publisher computes
    // it once against the envelope, and every client then agrees by construction. CLEVER
    // only; three models drawn at once is noise, and CLEVER is the freshet model.
    forecast: null,
    /*
     * WHERE IT IS HEADING, at the horizons the publisher ranks — see HORIZONS in
     * `pipeline/gauges/feed/publish.py`. Without these the Conditions tab's +1 / +3 / +5
     * chips colour NOTHING in dev, and a control that does nothing looks like a bug in the
     * control rather than an absence in the fixture.
     *
     * A GENTLE RISE, and the same shape for every station, because the fixture's job here
     * is to exercise the path rather than to model a freshet: the numbers must be real
     * percentiles in [0,1] and must differ from today's, or the tests cannot tell that the
     * horizon was read at all.
     */
    ...(now === null ? {} : { ahead: Object.fromEntries([1, 3, 5].map((d) => [
      String(d),
      { discharge: Math.min(0.95, Math.max(0.05, now + d * 0.06)), model: "CLEVER" },
    ])) }),
  };
}
writeFileSync(`${OUT}/feeds/gauge/index.json`,
              JSON.stringify({ fetchedAt: src.fetched, stations: index }));
for (const g of src.gauges)
  writeFileSync(`${OUT}/feeds/gauge/${g.id}.json`, JSON.stringify({
    fetchedAt: src.fetched, station: g.id,
    now: { discharge: g.last?.[1] ?? null, level: g.level_last?.[1] ?? null,
           at: g.last?.[0] ?? null, percentile: g.standing?.pctile ?? null,
           parameter: g.last ? "discharge" : "level" },
    // `[timestamp, level, discharge]`, THE SAME SHAPE THE PUBLISHER WRITES. It used to be
    // two parallel arrays under their own keys, which no reader of the real feed knows how
    // to open — so the fixture pair charted nothing while the province pair charted fine,
    // and the difference looked like a bug in the app.
    recent: (g.discharge ?? []).map(([t, q], i) => [t, g.level?.[i]?.[1] ?? null, q]),
    // Where the BCRFC model runs will land — CLEVER / COFFEE / ELF, already keyed per
    // station upstream, so they merge in here rather than becoming a third artifact.
    forecast: null,
  }));

const kb = (n) => `${(n / 1024).toFixed(0)} KB`;
console.log(`bundle.sqlite  ${kb(statSync(dbPath).size)}`);
for (const [t, n] of Object.entries(counts))
  if (n) console.log(`  ${t.padEnd(14)} ${n.toLocaleString().padStart(7)} rows`);
console.log(`feeds          ${src.gauges.length + 1} files, ` +
            `${kb(statSync(`${OUT}/feeds/gauge/index.json`).size)} index`);
