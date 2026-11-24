// T225 port of pipeline/regs/dfo_salmon/live.py `run`: THE LIVE DFO SALMON FEED, one Worker invocation.
//
//   feed  (published, R2 feeds/dfo/)   index.json, unplaced.json — byte-identical to the Python's
//   priv  (never served)               state.json (T905: each region's own bookkeeping, STATE_KEYS — out of the
//                                      public index), notified.json (sent notices), manifest.json (fetch snapshots),
//                                      raw/region<slug>-eng.html (last good page), history/region<slug>_<stamp>.html
//                                      (every changed page: the corpus), entries/region-<slug>.json (the curated
//                                      entry files — see `EntrySource`), rows/region-<slug>.json (T366: the last
//                                      typed page's rows, the row digest's baseline — rows.ts)
//
// THE SAFETY RULES (each as the Python): a failed fetch keeps the region's last answers and counts `failures` (in a
// row); the region is `stale` only once its last GOOD read (`checked`) is older than STALE_AFTER_HOURS, and that is when
// the owner is told (T851: one failed daily fetch used to mark every region stale); a page that does not parse / verify HOLDS the region at the last feed; a
// STRUCTURAL reconcile holds the region; a `section_moved` location keeps its last answers; a fingerprint no curated
// location knows is UNPLACED (its rules held in unplaced.json, the owner told once). Nothing here writes a curated
// file.
//
// ROBUST MODE (the Worker's default since the T225 follow-up; `pythonParity: true` = the Python byte for byte, kept for
// the parity tests): (a) [T676, 2026-11-12: no longer a difference — the Python reads the date cell with the same
// reader, `dates.read_dates`, in both modes: an unreadable one is a `check` verdict ("dates unparsed — check DFO"),
// never a dropped limit]; (b) ONE BAD ROW HOLDS ONLY ITS LOCATION: a row the catalogue refuses
// (or a calendar that throws) keeps that location's last answers (a `check` verdict when it has none), listed in the
// region's `holds` with the reason, and the rest of the region publishes. A page that does not parse / verify, and a
// STRUCTURAL reconcile, still hold the region (those are page-level).
import { FEED_FORMATS, FEED_MIN_APP } from "../../../../packages/core/src/feedFormats.generated";
import { contentHash, loadManifest, rev, skipSameStore, type Fetch, type Meter, type Store } from "../io";
import { sendDueChecked } from "../notify";
import { derive, livePieces, pieceId, PIECES_SHAPE } from "./closures";
import { byFingerprint, loadEntryFile, reconcile, waterById, type EntryFile, type EntryLocation, type Report, type WaterBinding } from "./entries";
import { build, type TypedFeed } from "./feed";
import { ALL_SLUGS, fetchRegion, isoSeconds, normalizeSlug, PAGES, pageUrl, REAL_SLUGS, type FetchRegionOptions, type RegionSnapshot } from "./fetch";
import { sha256 } from "./hash";
import { digest, parseHeader, resolve, type Note } from "./header";
import { extract, type Location, type Structure } from "./locations";
import { key } from "./names";
import { parseRegion } from "./parse";
import { cmpStr, cmpTuple, formatFixed, pyRe, pyStrOf, pyTypeName, repr, search } from "./py";
import { feedText, loads, PyFloat } from "./pyjson";
import { headerAnswers, headerClosure, markShared, poolOf } from "./quota";
import { inForce, Interner, locationCalendar, NOT_LISTED, SPECIES } from "./salmon";
import { untangle, verify } from "./untangle";
import { readRegistry, REGISTRY_PATH, template, type Registry } from "./wording";
import { feedMap, readBaseline, rowChanges, rowsNotice, ROWS_KEY, type RowChange, type RowsBaseline } from "./rows";

// T332 (/2): a verdict's quota is `tree` (a game-fish TreeTier), a bucket `{tree, …}`; T356 (/3, live.py): a header
// closure's verdict carries `c`, a region its `shared` + `sizes`, a header note's length is `lengths`
// (T556) the string is generated from the Python table (pipeline/common/feed_formats.py), the app's reader with it
export const FORMAT = FEED_FORMATS.dfoFeed;
/** (T676 item 2) the index's `minAppVersion`: the oldest app that reads it (live.py's MIN_APP, one table) */
export const MIN_APP = FEED_MIN_APP.dfoFeed;
/** (T905, owner 2026-11-24 "Move worker state out") the private state's format (priv `state.json`) */
export const STATE_FORMAT = FEED_FORMATS.dfoState;
export const STATE_KEY = "state.json";
/**
 * (T905) A REGION'S OWN BOOKKEEPING: what only this run reads back — the page hash and when it last moved, the fetches
 * that failed in a row, the header's digest, the header names no water binds, the page's general text. In priv
 * `state.json`, never in the published index (they were ~11 kB of it); no app reads one (core salmonFeed.ts
 * `SalmonFeedRegion`). live.py's STATE_KEYS is the same list (the vectors' `live.state_keys` pin it).
 */
export const STATE_KEYS = ["sha256", "changed", "failures", "header", "unresolved", "texts"] as const;
export interface DfoState { format: typeof STATE_FORMAT; regions: Record<string, Record<string, unknown>> }
const SPECIES_N = [...SPECIES];
/**
 * (T851) A region is STALE when its last good read (`checked`) is older than this at a run whose fetch failed — not
 * because one fetch failed. 30 h is the app's own line for the DFO feed (packages/core staleness.ts
 * `STALE_AFTER.dfo` = `DAILY_FEED_STALE_MS`: the slowest cron, daily, plus 6 h; test/dfo.stale.test.ts pins them equal),
 * so a region's answers are doubted exactly when the app would doubt the whole feed. A failure inside the line is kept
 * honest as `failures` (fetches in a row that failed; 0 after a read), /status (`dfoHeld`) and the run's health record.
 * The Python's `live.STALE_AFTER_HOURS` is the same number (pinned by the same test).
 */
export const STALE_AFTER_HOURS = 30;
/** (T851) the last good read `checked` is past the line at `at` (no read at all, or an unreadable time: stale) */
export function staleAt(checked: string | null | undefined, at: string): boolean {
  const age = Date.parse(at) - Date.parse(checked ?? "");
  return !(age <= STALE_AFTER_HOURS * 3600_000);
}
/** (T549 item 9) `live queue` reads the MAC's unplaced.json, never the worker's on R2: the Mac reads DFO's page first
 *  (`live run`), then queues what it holds. `queue` alone seeded nothing when the Mac had not run since the page changed. */
export const QUEUE_ACTION = (slugs: string) =>
  `python -m pipeline.regs.dfo_salmon.live run --regions ${slugs} && python -m pipeline.regs.dfo_salmon.live queue --regions ${slugs}`;

/** How the curated entry files reach the run: the file's JSON text per region, null when there is none. */
export type EntrySource = (slug: string) => Promise<string | null>;

/** A page source: (html, fetched_at) or throws. Production: `liveFetcher`; tests: recorded pages. */
export type Fetcher = (slug: string) => Promise<[string, string | null]>;

export interface DfoDeps {
  /** network (DFO pages, the notify webhook) */
  fetch: Fetch;
  /** published: feeds/dfo/ — index.json, unplaced.json */
  feed: Store;
  /** private, never served: notified.json, manifest.json, raw/, history/, (default) entries/ */
  priv: Store;
  /** the curated entry files; default: `priv` key `entries/region-<slug>.json` */
  entries?: EntrySource;
  /** this run's stamp, "YYYY-MM-DDTHH:MM:SSZ" (default: now) */
  now?: string;
  regions?: string[] | null;
  notifyUrl?: string | null;
  /** default: the DFO site through `fetch` (validated, retried; history + manifest kept in `priv`) */
  fetcher?: Fetcher;
  /** the live fetcher's knobs (sleep between retries, attempts, timeout) */
  fetchOptions?: FetchRegionOptions;
  /** keep changed pages in priv history/ (the corpus). Default true with the live fetcher. */
  history?: boolean;
  /** true: reproduce the Python exactly (whole-region holds, Python's date reader) — the parity tests only */
  pythonParity?: boolean;
  /** skip the typing of a region whose page AND entry file are unchanged (its last answers carried), and skip feed
   *  writes whose content did not change (default: on unless `pythonParity`) */
  changeAware?: boolean;
  /** counts skipped writes / unchanged sources */
  meter?: Meter;
  log?: (s: string) => void;
}

/** Bump when a code change alters what a region's page types to: unchanged pages are re-typed once after a deploy. */
export const DFO_REV = "2026-11-20.1"; // T623 F13: the quota quote keeps a trailing "hatchery marked (fish) only"; T759: close matches apply, issues + dfo shape in index.json; T758: + the page-default places (cascade.ts statementLocators); T757: + one loc per closed-area piece today's row names; T521: an unplaced row carries its water's app items (`items`); T332 quota trees
// (T549 items 7, 10) index.json is written by EVERY run that gets to the end, so its `at` is when DFO was last CHECKED.
// It was skipped while unchanged and under a 20 h heartbeat: in season (a run every 6 h) `at` moved once a day and sat
// exactly on the app's stale line, so the card said "checked 24 h ago" 6 h after a check. One small put a run.

/** A location held at its last answers this run (robust mode): published in the region's `holds`. */
export interface RowHold {
  location_id: string;
  fingerprint: string;
  why: string;
  /** "carried": the last feed's answers stand; "check": there were none — the location reads `check` */
  kept: "carried" | "check";
}
export const ROW_HELD_VERDICT = { s: "check", t: "DFO's row for this place could not be read this run — check DFO" };
/** (T549 item 4, live.gone_places) A curated place that LEFT DFO's page: published in the region's `gone` (the wider rule
 *  answers — right when DFO removed the row, wrong when the page was misread), the owner told once. (T759: a place DFO
 *  REWORDED is not this: its close match applies the new row's rule, `closeMatches`, and the row is an issue.) */
export interface Gone {
  location_id: string;
  water: string;
  place: string;
  /** the first 12 of the page sha it was first missing from */
  page: string;
}

export interface DfoResult {
  index: Record<string, unknown>;
  /** (T905) what the run wrote to priv `state.json`: each region's bookkeeping, which `index` no longer carries */
  state: DfoState;
  unplaced: Record<string, unknown>[];
  notices: Notice[];
  sent: { sent: string[]; failed: string[]; off: boolean };
  /** change-aware: the regions whose typing was skipped (page + entries unchanged) */
  skipped?: string[];
}

type Json = any; // eslint-disable-line @typescript-eslint/no-explicit-any

/** The validated DFO fetch; the raw page and the snapshot manifest go to `priv` as `live_fetcher` writes the cache. */
export function liveFetcher(
  fetch: Fetch,
  priv: Store,
  log: (s: string) => void = () => {},
  opts: FetchRegionOptions = {},
): { get: Fetcher; flush: () => Promise<void> } {
  let manifest: Record<string, RegionSnapshot> | null = null;
  let dirty = false;
  const load = async () => {
    if (manifest === null) {
      try {
        manifest = JSON.parse((await priv.get("manifest.json")) ?? "{}");
      } catch {
        manifest = {};
      }
    }
    return manifest!;
  };
  return {
    get: async (slug) => {
      const { html, snap } = await fetchRegion(fetch, slug, opts);
      const m = await load();
      snap.changed = m[slug]?.sha256 !== snap.sha256;
      snap.path = `raw/region${slug}-eng.html`;
      if (snap.changed) await priv.put(snap.path, html, { contentType: "text/html; charset=utf-8" });
      m[slug] = snap;
      dirty = true;
      log(`region ${slug}: ${snap.bytes} B, mod=${snap.date_modified}, ${snap.changed ? "changed" : "same"}`);
      return [html, snap.fetched_at];
    },
    flush: async () => {
      if (dirty && manifest) await priv.put("manifest.json", JSON.stringify(manifest, null, 2) + "\n");
    },
  };
}

/** The page → (typed feed, locations, structure). Throws when the page does not parse or verify. */
export function typePage(html: string, slug: string, robust: boolean, reg: Registry): [TypedFeed, Location[], Structure] {
  const parsed = parseRegion(html, slug);
  const u = untangle(parsed);
  verify(parsed, u);
  const [locs, rules, structure] = extract(u);
  const typed = build({ region: slug, date_modified: parsed.date_modified, preamble: parsed.preamble, rules }, robust, reg);
  return [typed, locs, structure];
}

/** (T521) The app item ids of the binding the DFO water name (or one of its aliases) names, sorted; [] when none does. */
const itemsOf = (ef: EntryFile, water: string): string[] => {
  const k = key(water || "");
  if (!k) return [];
  const ids = ef.waters.filter((w) => key(w.name) === k || w.aliases.some((a) => key(a) === k)).flatMap((w) => w.item_ids);
  return [...new Set(ids)].sort(cmpStr);
};

const waterKnown = (ef: EntryFile, water: string): boolean => {
  const k = key(water || "");
  return !k || ef.waters.some((w) => key(w.name) === k || w.aliases.some((a) => key(a) === k));
};

/** (T759, live.close_matches; owner 2026-11-15: "close match needs confirming") fingerprint -> the curated place a
 *  `drift` row is read as: its TOP candidate (`entries.reconcile`, same water, score >= 0.5), when no row of today's page
 *  joins that place exactly and no earlier drift row (page order) took it. Its rule APPLIES there — and the row stays an
 *  issue (`drift`) until the owner confirms the wording in DFO review. A drift row whose top candidate is taken stays
 *  unplaced (its rules held) and is an `unmatched` issue (`near_place_taken`). */
export function closeMatches(ef: EntryFile, report: Report): Map<string, EntryLocation> {
  const byId = new Map(ef.locations.map((l) => [l.location_id, l] as const));
  const taken = new Set(report.outcomes.filter((o) => ["ok", "revived", "section_moved"].includes(o.status) && o.location_id).map((o) => o.location_id!));
  const out = new Map<string, EntryLocation>();
  for (const o of report.outcomes) {
    if (o.status !== "drift" || !o.candidates.length) continue;
    const lid = o.candidates[0]!.location_id;
    if (taken.has(lid) || !byId.has(lid)) continue;
    taken.add(lid);
    out.set(o.fingerprint, byId.get(lid)!);
  }
  return out;
}

function join(
  slug: string,
  ef: EntryFile,
  typed: TypedFeed,
  locations: Location[],
  report: Report,
  holds: Map<string, { fingerprint: string; why: string }> = new Map(),
  close: ReadonlyMap<string, EntryLocation> = new Map(),
): [Map<string, Json[]>, Json[]] {
  const index = byFingerprint(ef);
  const byFp = new Map(locations.map((l) => [l.fingerprint, l] as const));
  const outcome = new Map(report.outcomes.map((o) => [o.fingerprint, o] as const));
  const rules = new Map<string, Json[]>();
  const unplaced: Json[] = [];
  for (const loc of typed.locators || []) {
    const fp = loc.fingerprint;
    // (T759) a `drift` row in `close` (`closeMatches`) joins its close match too — and is ALSO listed unplaced (kind
    // drift), so `live queue` still seeds it for review
    const hit = index.get(fp) ?? close.get(fp);
    // (T787) a locator whose every row is from a table DFO commented out answers nothing, typed or not (the Python's
    // robust `feed.type_rows` marks its unread rule `hidden` too): it never holds its place
    const allHidden = (loc.hidden?.length ?? 0) > 0 && loc.hidden!.length === loc.rows.length;
    if (hit && loc.error !== undefined && !allHidden) {
      if (!holds.has(hit.location_id)) holds.set(hit.location_id, { fingerprint: fp, why: loc.error });
      if (!close.has(fp)) continue;
    } else if (hit) {
      // T349 / T358 (live.join): a quota pooled across areas — Region 5B's "in the aggregate" row (quota.markShared)
      // (T787, live.join) only the rows in force: a hidden row (a commented-out table) answers nothing
      const marked = markShared(inForce(loc.rules), poolOf(hit));
      const l = rules.get(hit.location_id);
      if (l) l.push(...marked);
      else rules.set(hit.location_id, [...marked]);
      if (!close.has(fp)) continue;
    }
    // T349 (live.join): a page default's sentence with no default location here is not a new place
    if (fp.startsWith("page:")) continue;
    const src = byFp.get(fp);
    const o = outcome.get(fp);
    const water = src ? src.water : "";
    const kind = !waterKnown(ef, water) ? "new_water" : o && o.status === "drift" ? "drift" : "new";
    unplaced.push({
      region: slug,
      fingerprint: fp,
      kind,
      water,
      scope: src ? src.specific_area : "",
      section: src ? src.section : null,
      candidates: o ? [...o.candidates] : [],
      rows: loc.rows,
      rules: loc.rules,
      page_date: typed.date_modified,
      // (T521, the Python's `_items_of` too) the app items of the water the row names — the water page that shows it as
      // "DFO rule, location not yet on the map"; [] for a water no binding knows (`new_water`)
      items: itemsOf(ef, water),
      ...(loc.error !== undefined ? { error: loc.error } : {}),
    });
  }
  return [rules, unplaced];
}

/** What the owner email renders (robust mode; stripped in pythonParity so the notices stay the Python's). */
export interface NoticeData {
  region: string;
  /** new | new_water | drift (a place to place) · held | fetch | row-held | moved | header | header-name ·
   *  (T366) rows: rule rows changed on known places (the digest) · rows-baseline: the row digest could not compare */
  kind: string;
  water?: string;
  /** the place as DFO prints it, in full */
  place?: string;
  /** what it affects / why, one short phrase */
  affects: string;
  /** a drift's likely match */
  likely?: string;
  /** (T320) kind "text": the page's general text that went, and what replaced it */
  was?: string[];
  now?: string[];
  /** (T321) kind "wording": the clause DFO never printed before, its template, the whole cell, the row's species /
   *  dates and its in-season notice */
  clause?: string;
  template?: string;
  cell?: string;
  species?: string;
  dates?: string;
  fn?: string;
  /** (T366) kind "rows": the page's date and every place whose rows changed (rows.ts `rowChanges`) */
  page?: string;
  rows?: RowChange[];
}
export interface Notice {
  key: string;
  text: string;
  /** (T366, live.py) false = the digest (a rule-row change on a known place); absent / true = urgent */
  urgent?: boolean;
  data?: NoticeData;
}

/** (T549 item 4) the owner notice for a place that left the page (`Gone`); keyed by place + the page it left on */
export function goneNotice(slug: string, g: Gone): Notice {
  const what = "is no longer on DFO's page: its rule left the feed and the wider rule of its water answers";
  return {
    key: `dfo-gone:${slug}:${g.location_id}:${g.page}`,
    text: `DFO Region ${slug}: ${g.location_id} (${g.water}: ${repr(g.place)}) ${what}. Read DFO's page: if the row is still printed, the page was misread`,
    data: { region: slug, kind: "gone", water: g.water || undefined, place: g.place || g.location_id,
            affects: `${g.location_id} ${what}; if DFO still prints the row, the page was misread` },
  };
}

/** (T759, live.BANNER) what every issue notice says the app does */
const banner = (water: string, until: string) =>
  `every stretch of ${water} shows a banner quoting it, and its line the issue look, until ${until}`;

/** (live.unplaced_notice) The one notice of an unplaced row — (T759) also its issue's (`issuesOf`): `applied`, the close
 *  match whose rule it now gives (`closeMatches`). */
export function unplacedNotice(u: Json, applied: string | null = null): Notice {
  const slug = u.region;
  const n = u.rules.length;
  const held = `${n} rule${n !== 1 ? "s" : ""} held unplaced`;
  const act = QUEUE_ACTION(slug);
  const ban = banner(u.water || "the water", "it is matched to a place in DFO review");
  const scoreOf = (c: Json) => formatFixed(c.score instanceof PyFloat ? c.score.v : Number(c.score), 2);
  const likely = u.kind === "drift" && u.candidates.length ? String(u.candidates[0].location_id) : undefined;
  const data = { region: slug, kind: u.kind, water: u.water || undefined, place: u.scope || undefined,
                 affects: applied ? `read as ${applied}, whose answer it now gives` : held.replace(" unplaced", ""),
                 ...(likely ? { likely } : {}) };
  if (applied) {
    const c = u.candidates[0];
    return {
      key: `dfo-drift:${slug}:${u.fingerprint}`,
      urgent: true,
      text: `DFO Region ${slug}: new wording on ${u.water}: ${repr(u.scope)} — read as ${applied} (${scoreOf(c)}), whose answer it now gives; `
        + banner(u.water, "you confirm it in DFO review") + `. Confirm (or refute) it in DFO review after: ${act}`,
      data,
    };
  }
  let text: string;
  if (u.kind === "drift" && u.candidates.length) {
    const c = u.candidates[0];
    text = `DFO Region ${slug}: new wording on ${u.water}, likely ${c.location_id} (${scoreOf(c)}) — ${held} (${c.location_id} is held by another row today); ${ban}. Confirm in DFO review after: ${act}`;
  } else if (u.kind === "new_water")
    text = `DFO Region ${slug}: a water no entry knows, ${repr(u.water)}: ${repr(u.scope)} — ${held}; no section can show it until the water is bound. Bind it in DFO review after: ${act}`;
  else text = `DFO Region ${slug}: new location on ${u.water}: ${repr(u.scope)} — ${held}; ${ban}. Place it in DFO review after: ${act}`;
  // T366: a row whose place matches no curated DFO place is URGENT (and a place question: `live queue`)
  return { key: `dfo-${u.kind}:${slug}:${u.fingerprint}`, urgent: true, text, data };
}

// ── (T549 item 4) a curated place the page no longer shows ───────────────────────────────────────────────────────────

/** (live.gone_places) every ACTIVE curated place `reconcile` found absent from today's page (`dormant`) and not answering
 *  today: it leaves `locs` and the wider place of its chain answers — no status change, nothing shown. Listed in the
 *  region's `gone` and told once, only when the last feed published it (or already listed it). Returns [gone, notices]. */
export function gonePlaces(slug: string, rep: Report, prev: Json, locs: Readonly<Record<string, Json>>, page: string): [Gone[], Notice[]] {
  const was = (prev.regions || {})[slug] || {};
  const wasGone = new Map(((was.gone || []) as Gone[]).map((g) => [g.location_id, g] as const));
  const prevLocs = (prev.locs || {}) as Record<string, Json>;
  const gone: Gone[] = [];
  const notices: Notice[] = [];
  for (const o of rep.outcomes) {
    if (o.status !== "dormant" || !o.location_id || o.location_id in locs) continue;
    const lid = o.location_id;
    const g = wasGone.get(lid) ?? (lid in prevLocs && prevLocs[lid].r === slug
      ? { location_id: lid, water: o.water, place: o.specific_area, page } : null);
    if (g === null) continue;
    gone.push(g);
    notices.push(goneNotice(slug, g));
  }
  return [gone.sort((x, y) => cmpStr(x.location_id, y.location_id)), notices];
}

// ── (T759) the issues: rules on today's page we cannot stand behind yet ─────────────────────────────────────────────

/** (live.SHAPE) the feed's shape, served at top level and on /status.json `dfo`: `pieces` >= 1 — it emits the closed
 *  areas of a place as their own locations (`<water_id>#closed:<key>`, T573). A release binding a piece needs it. */
export const SHAPE = { pieces: PIECES_SHAPE } as const;

/** live._water_of: the curated water a DFO water name (or an alias) names, or undefined */
const waterOf = (ef: EntryFile, water: string): WaterBinding | undefined => {
  const k = key(water || "");
  return ef.waters.find((w) => !!k && (key(w.name) === k || w.aliases.some((a) => key(a) === k)));
};

/** (live.confirmed) a curated place whose wording the owner stands behind: not a variant, or a variant he CONFIRMED in
 *  DFO review (`duplicate_confirmed` true). A variant only PROPOSED (`duplicate_confirmed` null) is not. */
export const confirmed = (loc: EntryLocation): boolean => !loc.duplicate_of || loc.duplicate_confirmed === true;

/** (live.drawn) a curated place that says where it is: an extent, tidal water, or a cascade default (not a water's
 *  reach) — or a confirmed variant (it rides the place it rewords) */
export const drawn = (loc: EntryLocation): boolean =>
  !!loc.binding?.extents?.length || !!loc.tidal_water || loc.kind !== "water" || !!loc.duplicate_of;

/** (live.curated_pieces) every closed-area piece id the curated places' OWN stored wording names (`closures.derive` over
 *  every place, active or dormant) — the pieces the release can bind */
export function curatedPieces(ef: EntryFile): Set<string> {
  const out = new Set<string>();
  for (const e of derive(ef.locations).values()) if (e.water_id) for (const a of e.areas) out.add(pieceId(e.water_id, a.key));
  return out;
}

export interface Issue {
  kind: "unmatched" | "drift" | "closed_area_unbound"; key: string; fingerprint: string; water: string; area: string;
  rows: string[]; why?: string; place?: string; location_id?: string; piece?: string;
}
export interface IssueWater { r: string; name: string; items: string[]; list: Issue[] }

/** the `dfo-issue` notice of an issue the unplaced notice does not tell (live.issues_of `tell`): a proposed-variant
 *  drift, a closed area no curated wording names */
function issueNotice(slug: string, waterName: string, issue: Issue): Notice {
  const [what, until] = issue.kind === "closed_area_unbound"
    ? ["a closed area no curated wording names (DFO reworded it inside its place)", "you place the closed area in DFO review"]
    : [`a reworded wording read as ${issue.place}, not yet confirmed`, "you confirm it in DFO review"];
  return {
    key: `dfo-issue:${slug}:${issue.key}`, urgent: true,
    text: `DFO Region ${slug}: ${what} on ${issue.water}: ${repr(issue.area)} — ${issue.rows.join("; ") || "no rows"}. The answers stand; `
      + banner(waterName, until),
    data: { region: slug, kind: issue.kind, water: issue.water || undefined, place: issue.area || undefined, affects: what },
  };
}

/**
 * (live.issues_of, T759, owner 2026-11-15) THE ISSUES OF ONE REGION READ TODAY, by curated water. An issue changes no
 * answer; the app gives the water's line the issue look and a banner quoting the row on every section of its `items`:
 *   unmatched  no curated wording (`new`), its close match held by another row (`near_place_taken`), or its wording
 *              joins a place that is not drawn (`undrawn`)
 *   drift      a row read as a close match (`place`), or one joining a PROPOSED variant (`place` = the place it rewords)
 *   closed_area_unbound  a closed area today's row names that no curated wording names (`curatedPieces`)
 * A `new_water` row goes to `noWater`. Returns [waters, noWater, notices] — notices only for the issues the unplaced
 * notice does not tell.
 */
export function issuesOf(slug: string, ef: EntryFile, typed: TypedFeed, found: Location[], unplaced: Json[],
                         close: ReadonlyMap<string, EntryLocation>,
                         pieces: ReadonlyMap<string, [Json, Location | undefined, Json]>): [Map<string, IssueWater>, Issue[], Notice[]] {
  const index = byFingerprint(ef);
  const rowsOf = new Map((typed.locators || []).map((l) => [l.fingerprint, l.rows] as const));
  const held = new Map(unplaced.map((u) => [u.fingerprint as string, u] as const));
  const waters = new Map<string, IssueWater>();
  const noWater: Issue[] = [];
  const notices: Notice[] = [];
  const add = (w: WaterBinding | undefined, issue: Issue) => {
    if (w === undefined) return void noWater.push(issue);
    let got = waters.get(w.water_id);
    if (!got) waters.set(w.water_id, (got = { r: slug, name: w.name, items: [...w.item_ids].sort(cmpStr), list: [] }));
    if (got.list.every((i) => i.key !== issue.key)) got.list.push(issue);
  };
  const tell = (w: WaterBinding | undefined, issue: Issue) => notices.push(issueNotice(slug, w ? w.name : issue.water, issue));
  for (const f of found) {
    const fp = f.fingerprint;
    const base = { fingerprint: fp, water: f.water, area: f.specific_area, rows: [...(rowsOf.get(fp) || [])] };
    const hit = index.get(fp);
    if (close.has(fp)) {
      add(waterById(ef, close.get(fp)!.water_id) ?? waterOf(ef, f.water), { kind: "drift", key: `drift:${fp}`, ...base, place: close.get(fp)!.location_id });
    } else if (held.has(fp)) {
      const u = held.get(fp)!;
      const why = u.kind === "new_water" ? "new_water" : u.kind === "drift" ? "near_place_taken" : "new";
      add(why === "new_water" ? undefined : waterOf(ef, f.water),
          { kind: "unmatched", key: `unmatched:${fp}`, ...base, why, ...(why === "near_place_taken" ? { place: u.candidates[0].location_id } : {}) });
    } else if (hit !== undefined && !confirmed(hit)) {
      const w = waterById(ef, hit.water_id);
      const issue: Issue = { kind: "drift", key: `drift:${fp}`, ...base, place: hit.duplicate_of!, location_id: hit.location_id };
      add(w, issue);
      tell(w, issue);
    } else if (hit !== undefined && !drawn(hit)) {
      // not told again: the row was told when it was unplaced, the place is in review
      add(waterById(ef, hit.water_id), { kind: "unmatched", key: `unmatched:${fp}`, ...base, why: "undrawn", location_id: hit.location_id });
    }
  }
  const known = curatedPieces(ef);
  for (const lid of [...pieces.keys()].filter((l) => !known.has(l)).sort(cmpStr)) {
    const [rule, row, area] = pieces.get(lid)!;
    const w = waterById(ef, lid.split("#", 1)[0]);
    const issue: Issue = { kind: "closed_area_unbound", key: `closed_area_unbound:${lid}`, fingerprint: row!.fingerprint,
                           water: row!.water, area: area.item || area.words || "", rows: [rule.verbatim], piece: lid };
    add(w, issue);
    tell(w, issue);
  }
  for (const got of waters.values()) got.list.sort((a, b) => cmpStr(a.key, b.key));
  return [waters, [...noWater].sort((a, b) => cmpStr(a.key, b.key)), notices];
}

/** (T759, change-aware) the last feed's issues of a region whose page and entries did not change: its waters' (`r`), and
 *  its `no_water` rows (by the region's unplaced fingerprints). An index from before issues carries none. */
function carryIssues(prev: Json, slug: string, prevUnplaced: Json[]): [Map<string, IssueWater>, Issue[]] {
  const iss = prev.issues || {};
  const waters = new Map(Object.entries((iss.waters || {}) as Record<string, IssueWater>).filter(([, w]) => w.r === slug));
  const fps = new Set(prevUnplaced.map((u) => u.fingerprint));
  return [waters, ((iss.no_water || []) as Issue[]).filter((i) => fps.has(i.fingerprint))];
}

/** live._carry: the last feed's answers for a region (or some of its places), re-indexed into this run's tables — with
 *  their rows in DFO's words (`x`) and (T870) as printed (`pr`). Exported for the vectors' `carry` case. */
export function carry(prev: Json, slug: string, verdicts: Interner<unknown>, gears: Interner<unknown>, only: Set<string> | null = null): Record<string, Json> {
  const pv = prev.verdicts || [];
  const pg = prev.gears || [];
  const out: Record<string, Json> = {};
  for (const [lid, c] of Object.entries((prev.locs || {}) as Record<string, Json>)) {
    if (c.r !== slug || (only !== null && !only.has(lid))) continue;
    const sp = c.sp.map((runs: number[]) => runs.map((x, k) => (k % 2 === 0 || x < 0 ? x : verdicts.add(pv[x]))));
    const g = c.g.map((x: number, k: number) => (k % 2 === 0 || x < 0 ? x : gears.add(pg[x])));
    // (T770) another fish's closure lines `o` index the same gear table: carried with the place, never dropped
    const o = c.o ? { o: c.o.map((x: number, k: number) => (k % 2 === 0 || x < 0 ? x : gears.add(pg[x]))) } : {};
    out[lid] = { r: slug, sp, g, ...o, ...(c.x && c.x.length ? { x: c.x } : {}), ...(c.pr && c.pr.length ? { pr: c.pr } : {}) };
  }
  return out;
}

/** (T558) absent → `dflt`; a failed read or a body that does not parse THROWS: the last feed read as `{}` would publish a
 *  region whose fetch also failed with no places at all. The run fails, the last good files stand. */
async function readJson(store: Store, k: string, dflt: Json): Promise<Json> {
  const t = await store.get(k);
  if (t === null) return dflt;
  try {
    return loads(t);
  } catch (e) {
    throw new Error(`${k} is stored but does not parse (${e instanceof Error ? e.message : String(e)}); fix or delete it`);
  }
}

const nowStamp = () => isoSeconds(new Date(), true);

/**
 * (T905, live._read_state) Each region's bookkeeping (STATE_KEYS) as the last run left it: priv `state.json` (`text`, as
 * read; a failed read throws before this) when it is there (a body that does not parse, or another format, THROWS: the
 * run fails, the last good files stand); with none — the first run after T905's deploy, or a first run at all — the last index's regions, ONCE: an
 * index a worker before T905 wrote still carries the keys, so the cutover run compares the page hash and the page's
 * text with the last read's (no page counted as changed, no text re-told, the failures counted on). Logged when it seeds.
 */
export function readState(text: string | null, prev: Json, log: (s: string) => void): Record<string, Record<string, unknown>> {
  if (text === null) {
    const seeded: Record<string, Record<string, unknown>> = {};
    for (const [slug, r] of Object.entries((prev.regions || {}) as Record<string, Json>))
      seeded[slug] = Object.fromEntries(STATE_KEYS.filter((k) => r && k in r).map((k) => [k, r[k]]));
    const from = Object.entries(seeded).filter(([, r]) => Object.keys(r).length).map(([s]) => s);
    if (from.length) log(`dfo: no private ${STATE_KEY}: seeded once from the last index's regions (${from.join(", ")})`);
    return seeded;
  }
  let got: Json;
  try {
    got = loads(text);
  } catch (e) {
    throw new Error(`${STATE_KEY} is stored but does not parse (${e instanceof Error ? e.message : String(e)}); fix or delete it`);
  }
  if (!got || typeof got !== "object" || Array.isArray(got) || got.format !== STATE_FORMAT || !got.regions
      || typeof got.regions !== "object" || Array.isArray(got.regions))
    throw new Error(`${STATE_KEY} is not a ${STATE_FORMAT} state; fix or delete it (the next run then seeds it from the last index)`);
  return got.regions;
}

/** (T905, live._split) A region's record as the run builds it -> [what the index publishes, the private bookkeeping]. */
export function splitRegion(reg: Json): [Json, Record<string, unknown>] {
  const keys = new Set<string>(STATE_KEYS);
  return [Object.fromEntries(Object.entries(reg).filter(([k]) => !keys.has(k))),
          Object.fromEntries(STATE_KEYS.filter((k) => k in reg).map((k) => [k, reg[k]]))];
}

/** (T905) Each published region with its bookkeeping back on it: the worker's own readers (/status `dfoHeld`, the
 *  run's health record) read `failures` there, as they read it on the index before. */
export function regionsWithState(index: Json, state: DfoState): Record<string, Json> {
  return Object.fromEntries(Object.entries((index.regions || {}) as Record<string, Json>)
    .map(([slug, r]) => [slug, { ...r, ...(state.regions[slug] || {}) }]));
}

/** (T668 option 2) The pair table as the feed carries it: `{format, release, pairs: [[a, b, answer]]}`. (T669 item 3,
 *  LF) `named`, when present: `[a, b, region of a, region of b, answer]` for two openings whose answer names other
 *  regions in another region pair (`pipeline.deliver.salmon_pairs.pairs`); additive, so an app before 0.8.4 reads
 *  `pairs` alone (there the numbers with no names). */
export interface PairTable { format: typeof PAIRS_FORMAT; release: string; pairs: [Json, Json, Json][];
                             named?: [Json, Json, string, string, Json][] }
export const PAIRS_FORMAT = FEED_FORMATS.dfoPairs;

/** (T668 option 2) priv `pairs.json` read whole: null (logged) when there is none; anything but a pairs table (`PAIRS_FORMAT`) of
 *  [opening, opening, answer] verdicts THROWS (the job fails and is reported, the last good feed stands). */
export function readPairs(text: string | null, log: (s: string) => void): PairTable | null {
  if (text === null) {
    log("dfo: no pairs.json (python -m pipeline.deliver.sync_inputs stages it): the feed carries no straddler pairs; the app shows both regions' words for two openings");
    return null;
  }
  const t = JSON.parse(text) as Partial<PairTable> | null;
  const verdict = (v: Json) => !!v && typeof v === "object" && typeof v.s === "string" && typeof v.t === "string";
  if (!t || t.format !== PAIRS_FORMAT || typeof t.release !== "string" || !Array.isArray(t.pairs)
      || !t.pairs.every((p) => Array.isArray(p) && p.length === 3 && p[0]?.s === "open" && p[1]?.s === "open" && p.every(verdict)))
    throw new Error(`dfo: pairs.json is not a ${PAIRS_FORMAT} table of [opening, opening, answer] verdicts`);
  if (t.named !== undefined && (!Array.isArray(t.named) || !t.named.every((p) => Array.isArray(p) && p.length === 5
      && p[0]?.s === "open" && p[1]?.s === "open" && verdict(p[0]) && verdict(p[1]) && typeof p[2] === "string"
      && typeof p[3] === "string" && verdict(p[4]))))
    throw new Error(`dfo: pairs.json \`named\` is not a list of [opening, opening, region, region, answer]`);
  return { format: t.format, release: t.release, pairs: t.pairs, ...(t.named ? { named: t.named } : {}) };
}

/** One feed run (live.run). Writes feed index.json + unplaced.json, sends the due notices. */
export async function runDfo(deps: DfoDeps): Promise<DfoResult> {
  const log = deps.log ?? (() => {});
  const robust = !deps.pythonParity;
  const aware = deps.changeAware ?? robust;
  const at = deps.now ?? nowStamp();
  const writes = aware ? await loadManifest(deps.priv, "writes.json") : {};
  // unplaced.json is written only when it changed; index.json every run (its `at` is the check time: T549 items 7, 10)
  const feedStore: Store = aware ? skipSameStore(deps.feed, writes, { now: Date.parse(at), meter: deps.meter }) : deps.feed;
  // T321: the wording registry is a runtime input (priv `wording.json`), read once per run. T324: its ONLY copy —
  // missing / invalid THROWS before anything is written, so the job fails and is reported and the last good feed stands
  const wordingText = await deps.priv.get("wording.json");
  const wordingReg = readRegistry(wordingText);
  const wordingHash = contentHash(wordingText ?? "");
  // (T668 option 2, owner 2026-11-12) THE STRADDLER PAIR TABLE (priv `pairs.json`, `pipeline.deliver.salmon_pairs` via
  // sync_inputs): the Python's answer for two regions' differing openings on a two-region chain, carried into the feed
  // as is (`pairs`) for the app to look up. Robust mode only (the Python's live run publishes none). Missing: logged, the
  // feed carries none and the app keeps both regions' words (a pair not held); invalid THROWS, as the wording registry.
  const pairs = robust ? readPairs(await deps.priv.get("pairs.json"), log) : null;
  let inputs: { rev?: string; wording?: string; regions?: Record<string, { page: string; entries: string }> } = {};
  if (aware)
    try {
      inputs = JSON.parse((await deps.priv.get("inputs.json")) ?? "{}");
    } catch {
      inputs = {};
    }
  if (inputs.wording !== wordingHash) inputs = {}; // a changed registry re-types every region (cached answers carry its verdicts)
  const inputsOut: Record<string, { page: string; entries: string }> = {};
  const skipped: string[] = [];
  const entrySource: EntrySource = deps.entries ?? ((slug) => deps.priv.get(`entries/region-${slug}.json`));
  const live = deps.fetcher ? null : liveFetcher(deps.fetch, deps.priv, log, deps.fetchOptions);
  const fetcher: Fetcher = deps.fetcher ?? live!.get;
  const keepHistory = deps.history ?? !deps.fetcher;

  const prev: Json = upgrade(await readJson(deps.feed, "index.json", {}));
  const prevUnplacedList: Json[] = await readJson(deps.feed, "unplaced.json", []);
  const prevUnplaced = new Map<string, Json>();
  for (const u of prevUnplacedList) prevUnplaced.set(JSON.stringify([u.region, u.fingerprint]), u);
  const prevUnplacedOf = (slug: string) => [...prevUnplaced.values()].filter((u) => u.region === slug);

  const todo = deps.regions && deps.regions.length ? deps.regions.map(normalizeSlug) : REAL_SLUGS;
  const verdicts = new Interner<unknown>();
  const gears = new Interner<unknown>();
  verdicts.add(NOT_LISTED);
  const locs: Record<string, Json> = {};
  const regionOut: Record<string, Json> = {};
  const unplaced: Json[] = [];
  const notices: Notice[] = [];
  // (T905) each region as the last run left it: its public record with its private bookkeeping back on it — the
  // bookkeeping from the state only (an index before T905 still carrying it is read through readState's seed)
  const stateRead = await deps.priv.get(STATE_KEY);
  const lastState = readState(stateRead, prev, log);
  const prevRegions: Record<string, Json> = Object.fromEntries(Object.entries((prev.regions || {}) as Record<string, Json>)
    .map(([slug, r]) => [slug, { ...splitRegion(r)[0], ...(lastState[slug] || {}) }]));
  // (T759, live.run) the issues of the regions READ this run, by water; `unread` names the others and why
  const issues = new Map<string, IssueWater>();
  const noWater: Issue[] = [];
  const unread: Record<string, string> = {};

  const entryCache = new Map<string, EntryFile>();
  const entryText = new Map<string, string | null>();
  const rawEntries = async (slug: string) => {
    if (!entryText.has(slug)) entryText.set(slug, await entrySource(slug));
    return entryText.get(slug)!;
  };
  const loadEf = async (slug: string) => {
    let ef = entryCache.get(slug);
    if (!ef) {
      ef = loadEntryFile(slug, await rawEntries(slug));
      entryCache.set(slug, ef);
    }
    return ef;
  };

  for (const slug of ALL_SLUGS)
    if (!todo.includes(slug) && slug in prevRegions) {
      Object.assign(locs, carry(prev, slug, verdicts, gears));
      regionOut[slug] = prevRegions[slug];
      unread[slug] = "not asked";
      unplaced.push(...prevUnplacedOf(slug));
    }

  for (const slug of todo) {
    const page = PAGES[slug]!;
    const was: Json = prevRegions[slug] || {};
    const reg: Json = {
      name: page.name,
      url: pageUrl(slug),
      asof: was.asof ?? null,
      sha256: was.sha256 ?? null,
      changed: was.changed ?? null,
      checked: was.checked ?? null,
      stale: false,
      held: null,
      failures: 0,
      notes: was.notes || [],
      header: was.header ?? null,
      unresolved: was.unresolved || [],
      // (T320) the page's general text, kept to tell the owner what changed (readTexts)
      texts: was.texts || [],
      // (T549 item 4) the places DFO dropped: kept with the answers when the page is not read today; a page read today
      // replaces them (`gonePlaces`)
      gone: [...(was.gone || [])],
    };
    setBucket(reg);
    if (robust) reg.holds = was.holds || [];
    regionOut[slug] = reg;
    const tellGone = () => { for (const g of (reg.gone || []) as Gone[]) notices.push(goneNotice(slug, g)); };
    const hold = (why: string, k: string) => {
      reg.held = why;
      unread[slug] = "held";
      Object.assign(locs, carry(prev, slug, verdicts, gears));
      unplaced.push(...prevUnplacedOf(slug));
      notices.push({ key: `dfo-held:${slug}:${k}`, text: `DFO Region ${slug} held at the last feed: ${why}`, data: { region: slug, kind: "held", affects: `the whole region is held at the last feed: ${why}` } });
    };

    let html: string;
    let fetched: string | null;
    try {
      [html, fetched] = await fetcher(slug);
    } catch (e) {
      // (T851) the last answers stand; `stale` only once the last good read is older than the line (STALE_AFTER_HOURS)
      reg.stale = staleAt(was.checked, at);
      reg.failures = Number(was.failures || 0) + 1;
      unread[slug] = "stale";
      Object.assign(locs, carry(prev, slug, verdicts, gears));
      unplaced.push(...prevUnplacedOf(slug));
      log(`region ${slug}: fetch failed (${pyStrOf(e)}); the last feed stands${reg.stale ? " (stale)" : ""}`);
      if (reg.stale)
        notices.push({
          key: `dfo-fetch:${slug}:${was.checked || "never"}`,
          text: `DFO Region ${slug}: no good read since ${was.checked || "never"} (over ${STALE_AFTER_HOURS} h), ${reg.failures} fetches in a row failed (${pyStrOf(e)}); the feed shows the page of ${reg.asof || "no date"}, marked stale`,
          data: { region: slug, kind: "fetch", affects: `no good read since ${was.checked || "never"} (over ${STALE_AFTER_HOURS} h), ${reg.failures} fetches in a row failed (${pyStrOf(e)}); the app shows the page of ${reg.asof || "no date"}, marked stale` },
        });
      continue;
    }
    // Python reads the page back from its cache with Path.read_text: universal newlines ("\r\n" / "\r" → "\n")
    html = html.replace(/\r\n?/g, "\n");
    const sha = sha256(html);
    if (sha !== reg.sha256) {
      reg.changed = at;
      if (keepHistory) {
        const stamp = at.replace(/-/g, "").replace(/:/g, "").replace(/T/g, "").slice(0, 14);
        await deps.priv.put(`history/region${slug}_${stamp}.html`, html, { contentType: "text/html; charset=utf-8" });
      }
    }
    reg.sha256 = sha;
    reg.checked = fetched || at;
    if (aware) {
      // CHANGE-AWARE: the same page and the same entry file type to the same answers — carry them, skip the typing
      const key = { page: sha, entries: contentHash((await rawEntries(slug)) ?? "") };
      const last = inputs.rev === rev(DFO_REV) ? inputs.regions?.[slug] : undefined;
      const usable = last && last.page === key.page && last.entries === key.entries && !was.held && slug in prevRegions;
      if (usable) {
        inputsOut[slug] = key;
        Object.assign(locs, carry(prev, slug, verdicts, gears));
        // (T759) the same page and entries give the same issues: the last feed's for this region, carried (the Python
        // re-reads them; this is what it would read) — and their notices re-offered with the rest
        const [cw, cn] = carryIssues(prev, slug, prevUnplacedOf(slug));
        for (const [wid, w] of cw) issues.set(wid, w);
        noWater.push(...cn);
        const applied = new Map([...cw.values()].flatMap((w) => w.list.filter((i) => i.kind === "drift" && !i.location_id).map((i) => [i.fingerprint, i.place!] as const)));
        for (const u of prevUnplacedOf(slug)) {
          unplaced.push(u);
          notices.push(unplacedNotice(u, applied.get(u.fingerprint) ?? null)); // re-offered: a failed send is retried (notified.json dedupes)
        }
        for (const w of cw.values())
          for (const i of w.list) if (i.kind === "closed_area_unbound" || (i.kind === "drift" && i.location_id)) notices.push(issueNotice(slug, w.name, i));
        tellGone(); // re-offered likewise (the outbox tells a key once)
        skipped.push(slug);
        if (deps.meter) deps.meter.sourcesUnchanged += 1;
        continue;
      }
    }
    let typed: TypedFeed;
    let found: Location[];
    let structure: Structure;
    try {
      [typed, found, structure] = typePage(html, slug, robust, wordingReg);
    } catch (e) {
      hold(`the page did not parse (${pyTypeName(e)}: ${[...pyStrOf(e)].slice(0, 120).join("")})`, sha.slice(0, 12));
      continue;
    }
    reg.asof = typed.date_modified;
    const ef = await loadEf(slug);
    const rep = reconcile(ef, found, structure);
    if (rep.structural.length) {
      hold(rep.structural.join("; "), sha.slice(0, 12));
      continue;
    }
    readHeader(slug, html, reg, was, ef, notices);
    readTexts(slug, html, reg, was, notices);
    const closure = headerClosure(reg.notes || [], slug); // T320: the header's closure wins over the table's rows
    const close = closeMatches(ef, rep); // (T759) a close match applies its rule
    const words = closure ? new Map<string, Json[]>() : wordsByLocation(ef, typed, close); // T321: a closed page's rows are not in force
    notices.push(...wordingNotices(slug, ef, typed, robust));
    const rowHolds = new Map<string, { fingerprint: string; why: string }>();
    const printed = closure ? new Map<string, string[]>() : rowsByLocation(ef, typed, close); // T870: the rows behind its answers
    const xOf = (lid: string) => ({ ...(words.get(lid)?.length ? { x: words.get(lid)! } : {}),
                                    ...(printed.get(lid)?.length ? { pr: printed.get(lid)! } : {}) });
    const [rules, held] = join(slug, ef, typed, found, rep, rowHolds, close);
    // T573 / T757: every closed area TODAY'S rows still name, its own rule (`closures.livePieces`, the Python's
    // `live_pieces`); one DFO dropped is NOT in the feed, so its place answers there (T759: a removal, nothing shown; a
    // closed area DFO reworded is a `closed_area_unbound` issue). A row the port cannot read THROWS, as the Python's run
    // raises: the job fails, the last good feed stands.
    const pieces = livePieces(ef, found, close);
    for (const [lid, [rule]] of pieces) rules.set(lid, [rule]);
    if (aware) inputsOut[slug] = { page: sha, entries: contentHash((await rawEntries(slug)) ?? "") };
    const moved = new Set(rep.outcomes.filter((o) => o.status === "section_moved" && o.location_id).map((o) => o.location_id!));
    for (const lid of [...rules.keys()].sort(cmpStr)) {
      if (moved.has(lid) || rowHolds.has(lid)) continue;
      if (!robust) {
        locs[lid] = { r: slug, ...locationCalendar(closure && rules.get(lid)!.length ? closure : rules.get(lid)!, verdicts, gears), ...xOf(lid) };
        continue;
      }
      try {
        locs[lid] = { r: slug, ...locationCalendar(closure && rules.get(lid)!.length ? closure : rules.get(lid)!, verdicts, gears), ...xOf(lid) };
      } catch (e) {
        const fp = typed.locators.find((l) => byFingerprint(ef).get(l.fingerprint)?.location_id === lid)?.fingerprint ?? "";
        rowHolds.set(lid, { fingerprint: fp, why: `${pyTypeName(e)}: ${[...pyStrOf(e)].slice(0, 200).join("")}` });
      }
    }
    if (robust) {
      const kept: RowHold[] = [];
      for (const lid of [...rowHolds.keys()].sort(cmpStr)) {
        const h = rowHolds.get(lid)!;
        const carried = carry(prev, slug, verdicts, gears, new Set([lid]));
        let how: RowHold["kept"] = "carried";
        if (carried[lid]) locs[lid] = carried[lid];
        else {
          how = "check";
          const v = verdicts.add(ROW_HELD_VERDICT);
          locs[lid] = { r: slug, sp: SPECIES_N.map(() => [0, v]), g: [0, -1] };
        }
        kept.push({ location_id: lid, fingerprint: h.fingerprint, why: h.why, kept: how });
        notices.push({
          key: `dfo-row-held:${slug}:${h.fingerprint}:${sha.slice(0, 12)}`,
          text: `DFO Region ${slug}: the row for ${lid} did not read (${[...h.why].slice(0, 160).join("")}); ${how === "carried" ? "its last answers stand" : "it reads 'check DFO'"}, the rest of the region is published`,
          data: { region: slug, kind: "row-held", place: lid, affects: `its row did not read (${h.why.split("\n")[0]}); ${how === "carried" ? "its last answers stand" : "it reads 'check DFO'"}` },
        });
      }
      reg.holds = kept.sort((x, y) => cmpStr(x.location_id, y.location_id));
    }
    if (moved.size) {
      Object.assign(locs, carry(prev, slug, verdicts, gears, moved));
      for (const o of rep.outcomes)
        if (o.status === "section_moved")
          notices.push({
            key: `dfo-section:${slug}:${o.fingerprint}`,
            text: `DFO Region ${slug}: ${o.location_id} moved section (${o.detail}); its last answers stand until it is reviewed in DFO mode`,
            data: { region: slug, kind: "moved", place: o.location_id ?? undefined, affects: `moved section (${o.detail}); its last answers stand` },
          });
    }
    for (const u of held) {
      u.first_seen = prevUnplaced.get(JSON.stringify([slug, u.fingerprint]))?.first_seen || at;
      unplaced.push(u);
      notices.push(unplacedNotice(u, close.get(u.fingerprint)?.location_id ?? null));
    }
    // (T549 item 4, live.gone_places) the places DFO dropped: listed and told once; the wider rule answers
    const [gone, told] = gonePlaces(slug, rep, prev, locs, sha.slice(0, 12));
    reg.gone = gone;
    notices.push(...told);
    // (T759) the rules of this page we cannot stand behind yet, by water — they change no answer
    const [got, nw, toldIssues] = issuesOf(slug, ef, typed, found, held, close, pieces);
    for (const [wid, w] of got) issues.set(wid, w);
    noWater.push(...nw);
    notices.push(...toldIssues);
    if (robust) await rowDigest(slug, sha, typed, ef, deps.priv, notices, log);
  }

  const sortedLocs: Record<string, Json> = {};
  for (const k of Object.keys(locs).sort(cmpStr)) sortedLocs[k] = locs[k];
  // (T905) each region split: the index publishes what an app reads, priv state.json keeps the run's own bookkeeping
  const regions: Record<string, Json> = {};
  const state: DfoState = { format: STATE_FORMAT, regions: {} };
  for (const s of ALL_SLUGS) if (s in regionOut) [regions[s], state.regions[s]] = splitRegion(regionOut[s]);
  // robust: number verdicts / gears canonically (first use over the sorted locations; NOT_LISTED stays 0) so a carried
  // region and a re-typed one publish the same bytes — what lets an unchanged feed skip its write
  const [vItems, gItems] = robust ? canonical(sortedLocs, verdicts.items, gears.items) : [verdicts.items, gears.items];
  const index = { format: FORMAT, minAppVersion: MIN_APP, at, regions, verdicts: vItems, gears: gItems, locs: sortedLocs, unplaced: unplaced.length,
                  dfo: { ...SHAPE },
                  issues: { waters: Object.fromEntries([...issues].sort(([x], [y]) => cmpStr(x, y))), no_water: noWater,
                            unread: Object.fromEntries(ALL_SLUGS.filter((s) => s in unread).map((s) => [s, unread[s]!])) },
                  ...(pairs ? { pairs } : {}) };
  const unplacedSorted = unplaced
    .map((u, i) => ({ u, i }))
    .sort((a, b) => cmpTuple([a.u.region, a.u.fingerprint], [b.u.region, b.u.fingerprint]) || a.i - b.i)
    .map((x) => x.u);
  // (T905) the state first: an index written over a state that was not would leave the next run with neither (no
  // state.json and an index without the keys), and it would read every page for the first time. Put only when it moved.
  const stateText = JSON.stringify(state);
  if (stateText !== stateRead) await deps.priv.put(STATE_KEY, stateText, { contentType: "application/json" });
  await deps.feed.put("index.json", feedText(index), { contentType: "application/json", cacheControl: "public, max-age=300, stale-while-revalidate=3600" });
  await feedStore.put("unplaced.json", feedText(unplacedSorted), { contentType: "application/json", cacheControl: "public, max-age=300, stale-while-revalidate=3600" });
  if (aware) {
    // a region not run this time keeps its last inputs; one held / failed this run has none (re-typed next run)
    const keep: Record<string, { page: string; entries: string }> = {};
    if (inputs.rev === rev(DFO_REV)) for (const [s2, v] of Object.entries(inputs.regions ?? {})) if (!todo.includes(s2)) keep[s2] = v;
    const next = JSON.stringify({ rev: rev(DFO_REV), wording: wordingHash, regions: { ...keep, ...inputsOut } });
    if (next !== JSON.stringify(inputs)) await deps.priv.put("inputs.json", next);
    if ((feedStore as { changed?: boolean }).changed) await deps.priv.put("writes.json", JSON.stringify(writes));
    if (skipped.length) log(`unchanged (page + entries): region ${skipped.join(", ")} — typing skipped, last answers carried`);
  }
  if (live) await live.flush();
  const sent = await sendDueChecked(notices as unknown as Record<string, unknown>[], {
    url: deps.notifyUrl,
    fetch: deps.fetch,
    state: deps.priv,
    stateKey: "notified.json",
    keyOf: (n) => n.key,
    messageOf: (n) => n.text,
    log,
  });
  return { index, state, unplaced, notices: robust ? notices : notices.map(({ key, text }) => ({ key, text })), sent, skipped };
}

/** (T366) the page's rule-row changes on known places against the last typed page of the region (priv `rows/`): ONE
 *  digest notice (`urgent: false`). No baseline yet → this page is the baseline (logged). An unreadable baseline is
 *  told (urgent: the row changes of this page were not compared) and replaced by this page — never read as "no change". */
async function rowDigest(slug: string, sha: string, typed: TypedFeed, ef: EntryFile, priv: Store, notices: Notice[], log: (s: string) => void) {
  const now = feedMap(typed);
  let base: RowsBaseline | null = null;
  let broken = false;
  try {
    base = readBaseline(await priv.get(ROWS_KEY(slug)));
  } catch (e) {
    broken = true;
    const why = `${pyTypeName(e)}: ${[...pyStrOf(e)].slice(0, 160).join("")}`;
    log(`region ${slug}: the rows baseline ${ROWS_KEY(slug)} is unreadable (${why}) — this page's row changes were not compared`);
    notices.push({
      key: `dfo-rows-baseline:${slug}:${sha.slice(0, 12)}`,
      text: `DFO Region ${slug}: the row digest's baseline (${ROWS_KEY(slug)}) did not read (${why}); this page's rule-row changes were NOT compared — read DFO's page for Region ${slug}; the baseline is rewritten from it`,
      data: { region: slug, kind: "rows-baseline", affects: `the row digest could not compare this page (${why}); read DFO's page by hand` },
    });
  }
  if (base && base.sha256 === sha) return;
  if (base) {
    const changes = rowChanges(ef, base.rows, now);
    if (changes.length) notices.push(rowsNotice(slug, sha, typed.date_modified, changes));
  } else if (!broken) log(`region ${slug}: no rows baseline yet — this page (${typed.date_modified ?? "no date"}) is it; row changes are told from the next page on`);
  await priv.put(ROWS_KEY(slug), JSON.stringify({ sha256: sha, date_modified: typed.date_modified, rows: now } satisfies RowsBaseline));
}

/** live.rows_by_location (T870): location id → its DFO table rows as printed, the rows in force (a commented-out table's
 *  `hidden` rows left out), its locators in page order, each once (`locs[lid].pr`); a close-matched row's on its match */
export function rowsByLocation(ef: EntryFile, typed: TypedFeed, close: ReadonlyMap<string, EntryLocation> = new Map()): Map<string, string[]> {
  const index = new Map<string, EntryLocation>([...close, ...byFingerprint(ef)]);
  const out = new Map<string, string[]>();
  for (const loc of typed.locators || []) {
    const hit = index.get(loc.fingerprint);
    if (!hit) continue;
    const hidden = new Set(loc.hidden ?? []);
    const rows = [...new Set(loc.rows.filter((r, i) => !hidden.has(i) && r))];
    if (!rows.length) continue;
    const l = out.get(hit.location_id) ?? [];
    for (const r of rows) if (!l.includes(r)) l.push(r);
    out.set(hit.location_id, l);
  }
  return out;
}

/** live.words_by_location (T321): location id → the rows its card shows in DFO's words, `{k, sp, d, t}` (`locs[lid].x`) */
export function wordsByLocation(ef: EntryFile, typed: TypedFeed, close: ReadonlyMap<string, EntryLocation> = new Map()): Map<string, Json[]> {
  // (T759) a close-matched row's words on its close match (an exact fingerprint wins)
  const index = new Map<string, EntryLocation>([...close, ...byFingerprint(ef)]);
  const out = new Map<string, Json[]>();
  for (const loc of typed.locators || []) {
    const hit = index.get(loc.fingerprint);
    if (!hit || !loc.words?.length) continue;
    const l = out.get(hit.location_id) ?? [];
    l.push(...loc.words.map((w) => ({ k: w.k, sp: w.sp, d: w.d, t: w.t })));
    out.set(hit.location_id, l);
  }
  return out;
}

const RE_CELL_FN = pyRe("\\bFN\\s*\\d{3,4}\\b", "i");

/** live.wording_notices (T321, owner 2026-10-31: "It should be urgent"): a clause no saved page ever printed — one
 *  URGENT notice per new TEMPLATE (keyed by it: never told twice, whatever the run or region). */
export function wordingNotices(slug: string, ef: EntryFile, typed: TypedFeed, withData = true): Notice[] {
  const index = byFingerprint(ef);
  const out: Notice[] = [];
  for (const loc of typed.locators || []) {
    const hit = index.get(loc.fingerprint);
    for (const w of loc.words ?? [])
      for (const c of w.new ?? []) {
        const t = template(c);
        if (out.some((n) => n.key === `dfo-wording:${t}`)) continue; // one notice a template, wherever it is printed
        const fn = search(RE_CELL_FN, w.t);
        const where = hit ? hit.location_id : loc.fingerprint;
        const text =
          `DFO Region ${slug}: new wording in a limits cell, ${repr(c)}, on ${where} (${w.sp || "no species"}, ${w.d || "all year"}` +
          `${fn ? ", " + fn[0] : ""}): ${repr(w.t)} — the row shows DFO's words and no number until ${REGISTRY_PATH} has an entry for ` +
          `${repr(t)} (and pipeline/tests/test_dfo_wording.py a line for it)`;
        out.push({
          key: `dfo-wording:${t}`,
          text,
          ...(withData
            ? { data: { region: slug, kind: "wording", place: where, clause: c, template: t, cell: w.t, species: w.sp, dates: w.d,
                        ...(fn ? { fn: fn[0] } : {}),
                        affects: "the row shows DFO's words and no number in the app until the wording is in the registry" } }
            : {}),
        });
      }
  }
  return out;
}

function canonical(locs: Record<string, Json>, vs: unknown[], gs: unknown[]): [unknown[], unknown[]] {
  const vMap = new Map<number, number>([[0, 0]]);
  const gMap = new Map<number, number>();
  const vOut: unknown[] = [vs[0]];
  const gOut: unknown[] = [];
  const re = (x: number, m: Map<number, number>, src: unknown[], out: unknown[]) => {
    if (x < 0) return x;
    let n = m.get(x);
    if (n === undefined) {
      n = out.length;
      m.set(x, n);
      out.push(src[x]);
    }
    return n;
  };
  for (const l of Object.values(locs)) {
    l.sp = l.sp.map((runs: number[]) => runs.map((x, k) => (k % 2 === 0 ? x : re(x, vMap, vs, vOut))));
    l.g = l.g.map((x: number, k: number) => (k % 2 === 0 ? x : re(x, gMap, gs, gOut)));
    if (l.o) l.o = l.o.map((x: number, k: number) => (k % 2 === 0 ? x : re(x, gMap, gs, gOut)));
  }
  return [vOut, gOut];
}

function readHeader(slug: string, html: string, reg: Json, was: Json, ef: EntryFile, notices: Notice[]): void {
  let notes: Note[];
  try {
    notes = parseHeader(html, slug);
  } catch (e) {
    notices.push({
      key: `dfo-header-parse:${slug}:${reg.sha256.slice(0, 12)}`,
      text: `DFO Region ${slug}: the region header did not parse (${pyTypeName(e)}); the last one stands`,
      data: { region: slug, kind: "header", affects: `the region header did not parse (${pyTypeName(e)}); the last one stands` },
    });
    return;
  }
  if (!notes.length && was.notes && was.notes.length) {
    notices.push({
      key: `dfo-header-empty:${slug}:${reg.sha256.slice(0, 12)}`,
      text: `DFO Region ${slug}: the page has no region header any more; the last one stands`,
      data: { region: slug, kind: "header", affects: "the page has no region header any more; the last one stands" },
    });
    return;
  }
  const d = digest(notes).slice(0, 16);
  const [, unresolved] = resolve(notes, ef);
  for (const u of unresolved)
    notices.push({
      key: `dfo-header-name:${slug}:${u.name}`,
      text: `DFO Region ${slug}: the header names ${repr(u.name)}, which no curated DFO water binds — its note shows on no water until it is bound (DFO review)`,
      data: { region: slug, kind: "header-name", water: u.name, affects: "named in the region header, bound to no water: its note shows nowhere" },
    });
  reg.notes = notes;
  reg.header = d;
  reg.unresolved = unresolved;
  setBucket(reg);
}

/** (T332) live._upgrade: the last feed in an older format — a carried verdict's T320 `q` is dropped (DFO's words stand
 *  until the region is read again), each region's bucket re-read from its notes; never the old shape in a new feed */
function upgrade(prev: Json): Json {
  if (!prev || !Object.keys(prev).length || prev.format === FORMAT) return prev;
  const regions: Record<string, Json> = {};
  for (const [slug, r] of Object.entries((prev.regions || {}) as Record<string, Json>)) {
    const copy = { ...r, ...(r.notes ? { notes: (r.notes as Json[]).map(noteLengths) } : {}) };
    setBucket(copy);
    regions[slug] = copy;
  }
  const verdicts = ((prev.verdicts || []) as Json[]).map((v) => Object.fromEntries(Object.entries(v).filter(([k]) => k !== "q")));
  return { ...prev, regions, verdicts };
}

/** (T320) the salmon bucket — the header's combined daily / possession — beside the notes it is read from; (T356) with
 *  the region's `shared` and `sizes` (deliver.salmon.header_answers) */
function setBucket(reg: Json): void {
  delete reg.bucket;
  Object.assign(reg, headerAnswers(reg.notes || []));
}

/** (T358) a dfo-header/1 note's `cm` as `lengths` (the LengthBand header.py writes since dfo-header/2) */
function noteLengths(n: Json): Json {
  if (!("cm" in n)) return n;
  const { cm, ...rest } = n;
  return { ...rest, lengths: Array.isArray(cm) ? [{ min_cm: cm[0], max_cm: cm[1] }] : [{ min_cm: cm }] };
}

/** live.page_texts: every sentence above the table, the section banners and the notes under it, in page order */
export function pageTexts(html: string, slug: string): string[] {
  const p = parseRegion(html, slug);
  const out = [...p.preamble, ...p.sections.map((s) => s.title), ...p.notes.map((n) => n.text || "")];
  return [...new Set(out.filter((t) => t))];
}

/** live._read_texts (T320, owner 2026-10-31): a change in the page's general text is for a human — the feed reads no
 *  rule out of it. One URGENT notice per new text, old vs new, with what to do. The first read only stores it. */
function readTexts(slug: string, html: string, reg: Json, was: Json, notices: Notice[]): void {
  let texts: string[];
  try {
    texts = pageTexts(html, slug);
  } catch {
    return;
  }
  const old: string[] = was.texts || [];
  if (old.length && JSON.stringify(texts) !== JSON.stringify(old)) {
    const gone = old.filter((t) => !texts.includes(t));
    const fresh = texts.filter((t) => !old.includes(t));
    const d = sha256(texts.join("\n")).slice(0, 16);
    const lines = [...gone.map((t) => `- was: ${t}`), ...fresh.map((t) => `+ now: ${t}`)];
    notices.push({
      key: `dfo-text:${slug}:${d}`,
      text: `DFO Region ${slug}: the page's general text changed — review the header; the feed reads no rule from it (${gone.length} gone, ${fresh.length} new)\n` + lines.join("\n"),
      data: { region: slug, kind: "text", was: gone, now: fresh,
              affects: "the region header / general text: review it (the feed does not read it into rules); the card shows the new words" },
    });
  }
  reg.texts = texts;
}
