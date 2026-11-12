// T225 port of pipeline/regs/dfo_salmon/live.py `run`: THE LIVE DFO SALMON FEED, one Worker invocation.
//
//   feed  (published, R2 feeds/dfo/)   index.json, unplaced.json — byte-identical to the Python's
//   priv  (never served)               notified.json (sent notices), manifest.json (fetch snapshots),
//                                      raw/region<slug>-eng.html (last good page), history/region<slug>_<stamp>.html
//                                      (every changed page: the corpus), entries/region-<slug>.json (the curated
//                                      entry files — see `EntrySource`), rows/region-<slug>.json (T366: the last
//                                      typed page's rows, the row digest's baseline — rows.ts)
//
// THE SAFETY RULES (each as the Python): a failed fetch keeps the region's last answers, marked `stale`, and the
// second failure in a row notifies; a page that does not parse / verify HOLDS the region at the last feed; a
// STRUCTURAL reconcile holds the region; a `section_moved` location keeps its last answers; a fingerprint no curated
// location knows is UNPLACED (its rules held in unplaced.json, the owner told once). Nothing here writes a curated
// file.
//
// ROBUST MODE (the Worker's default since the T225 follow-up; `pythonParity: true` = the Python byte for byte, kept for
// the parity tests): (a) the date cell is read by dates.ts and an unreadable one is a `check` verdict ("dates
// unparsed — check DFO"), never a dropped limit; (b) ONE BAD ROW HOLDS ONLY ITS LOCATION: a row the catalogue refuses
// (or a calendar that throws) keeps that location's last answers (a `check` verdict when it has none), listed in the
// region's `holds` with the reason, and the rest of the region publishes. A page that does not parse / verify, and a
// STRUCTURAL reconcile, still hold the region (those are page-level).
import { FEED_FORMATS } from "../../../../packages/core/src/feedFormats.generated";
import { contentHash, loadManifest, rev, skipSameStore, type Fetch, type Meter, type Store } from "../io";
import { sendDue } from "../notify";
import { byFingerprint, loadEntryFile, reconcile, type EntryFile, type Report } from "./entries";
import { build, type TypedFeed } from "./feed";
import { ALL_SLUGS, fetchRegion, isoSeconds, normalizeSlug, PAGES, pageUrl, REAL_SLUGS, type FetchRegionOptions, type RegionSnapshot } from "./fetch";
import { sha256 } from "./hash";
import { digest, parseHeader, resolve, type Note } from "./header";
import { extract, type Location, type Structure } from "./locations";
import { key } from "./names";
import { parseRegion } from "./parse";
import { cmpStr, cmpTuple, formatFixed, pyRe, pyStrOf, pyTypeName, repr, search } from "./py";
import { feedText, loads, PyFloat } from "./pyjson";
import { headerAnswers, headerClosure } from "./quota";
import { Interner, locationCalendar, NOT_LISTED, SPECIES } from "./salmon";
import { untangle, verify } from "./untangle";
import { readRegistry, REGISTRY_PATH, template, type Registry } from "./wording";
import { feedMap, readBaseline, rowChanges, rowsNotice, ROWS_KEY, type RowChange, type RowsBaseline } from "./rows";

// T332 (/2): a verdict's quota is `tree` (a game-fish TreeTier), a bucket `{tree, …}`; T356 (/3, live.py): a header
// closure's verdict carries `c`, a region its `shared` + `sizes`, a header note's length is `lengths`
// (T556) the string is generated from the Python table (pipeline/common/feed_formats.py), the app's reader with it
export const FORMAT = FEED_FORMATS.dfoFeed;
const SPECIES_N = [...SPECIES];
export const FAIL_NOTIFY = 2;
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
export const DFO_REV = "2026-11-09.1"; // T521: an unplaced row carries its water's app items (`items`); T332 quota trees
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
/** (T549 item 4) A curated place DFO REWORDED: its row no longer matches (held unplaced, the owner told), and the place
 *  it most likely was would leave the feed, so the WIDER rule of its chain would answer. It reads `check` instead,
 *  listed in the region's `holds`, until the owner confirms the rebind or the row goes. */
export const REWORDED_VERDICT = { s: "check", t: "DFO reworded this place and the new wording is not confirmed yet — check DFO" };
/** (T549 item 4) A curated place that LEFT DFO's page with no reworded row to explain it: published in the region's
 *  `gone` (the wider rule answers — right when DFO removed the row, wrong when the page was misread), the owner told once. */
export interface Gone {
  location_id: string;
  water: string;
  place: string;
  /** the first 12 of the page sha it was first missing from */
  page: string;
}

export interface DfoResult {
  index: Record<string, unknown>;
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

function join(
  slug: string,
  ef: EntryFile,
  typed: TypedFeed,
  locations: Location[],
  report: Report,
  holds: Map<string, { fingerprint: string; why: string }> = new Map(),
): [Map<string, Json[]>, Json[]] {
  const index = byFingerprint(ef);
  const byFp = new Map(locations.map((l) => [l.fingerprint, l] as const));
  const outcome = new Map(report.outcomes.map((o) => [o.fingerprint, o] as const));
  const rules = new Map<string, Json[]>();
  const unplaced: Json[] = [];
  for (const loc of typed.locators || []) {
    const fp = loc.fingerprint;
    const hit = index.get(fp);
    if (hit && loc.error !== undefined) {
      if (!holds.has(hit.location_id)) holds.set(hit.location_id, { fingerprint: fp, why: loc.error });
      continue;
    }
    if (hit) {
      const l = rules.get(hit.location_id);
      if (l) l.push(...loc.rules);
      else rules.set(hit.location_id, [...loc.rules]);
      continue;
    }
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

export function unplacedNotice(u: Json): Notice {
  const slug = u.region;
  const n = u.rules.length;
  const held = `${n} rule${n !== 1 ? "s" : ""} held unplaced`;
  const act = QUEUE_ACTION(slug);
  let text: string;
  if (u.kind === "drift" && u.candidates.length) {
    const c = u.candidates[0];
    const score = c.score instanceof PyFloat ? c.score.v : Number(c.score);
    text = `DFO Region ${slug}: new wording on ${u.water}, likely ${c.location_id} (${formatFixed(score, 2)}) — ${held}. Confirm in DFO review after: ${act}`;
  } else if (u.kind === "new_water")
    text = `DFO Region ${slug}: a water no entry knows, ${repr(u.water)}: ${repr(u.scope)} — ${held}. Bind it in DFO review after: ${act}`;
  else text = `DFO Region ${slug}: new location on ${u.water}: ${repr(u.scope)} — ${held}. Place it in DFO review after: ${act}`;
  const likely = u.kind === "drift" && u.candidates.length ? String(u.candidates[0].location_id) : undefined;
  return {
    key: `dfo-${u.kind}:${slug}:${u.fingerprint}`,
    text,
    data: { region: slug, kind: u.kind, water: u.water || undefined, place: u.scope || undefined, affects: held.replace(" unplaced", ""), ...(likely ? { likely } : {}) },
  };
}

function carry(prev: Json, slug: string, verdicts: Interner<unknown>, gears: Interner<unknown>, only: Set<string> | null = null): Record<string, Json> {
  const pv = prev.verdicts || [];
  const pg = prev.gears || [];
  const out: Record<string, Json> = {};
  for (const [lid, c] of Object.entries((prev.locs || {}) as Record<string, Json>)) {
    if (c.r !== slug || (only !== null && !only.has(lid))) continue;
    const sp = c.sp.map((runs: number[]) => runs.map((x, k) => (k % 2 === 0 || x < 0 ? x : verdicts.add(pv[x]))));
    const g = c.g.map((x: number, k: number) => (k % 2 === 0 || x < 0 ? x : gears.add(pg[x])));
    out[lid] = { r: slug, sp, g, ...(c.x && c.x.length ? { x: c.x } : {}) };
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
  const prevRegions = prev.regions || {};

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
    };
    setBucket(reg);
    if (robust) {
      reg.holds = was.holds || [];
      reg.gone = was.gone || [];
    }
    regionOut[slug] = reg;
    const tellGone = () => { for (const g of (reg.gone || []) as Gone[]) notices.push(goneNotice(slug, g)); };
    const hold = (why: string, k: string) => {
      reg.held = why;
      Object.assign(locs, carry(prev, slug, verdicts, gears));
      unplaced.push(...prevUnplacedOf(slug));
      notices.push({ key: `dfo-held:${slug}:${k}`, text: `DFO Region ${slug} held at the last feed: ${why}`, data: { region: slug, kind: "held", affects: `the whole region is held at the last feed: ${why}` } });
    };

    let html: string;
    let fetched: string | null;
    try {
      [html, fetched] = await fetcher(slug);
    } catch (e) {
      reg.stale = true;
      reg.failures = Number(was.failures || 0) + 1;
      Object.assign(locs, carry(prev, slug, verdicts, gears));
      unplaced.push(...prevUnplacedOf(slug));
      log(`region ${slug}: fetch failed (${pyStrOf(e)}); the last feed stands`);
      if (reg.failures >= FAIL_NOTIFY)
        notices.push({
          key: `dfo-fetch:${slug}:${was.checked || "never"}`,
          text: `DFO Region ${slug}: ${reg.failures} fetches in a row failed (${pyStrOf(e)}); the feed shows the page of ${reg.asof || "no date"}`,
          data: { region: slug, kind: "fetch", affects: `${reg.failures} fetches in a row failed (${pyStrOf(e)}); the app shows the page of ${reg.asof || "no date"}` },
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
        for (const u of prevUnplacedOf(slug)) {
          unplaced.push(u);
          notices.push(unplacedNotice(u)); // re-offered: a send that failed is retried (notified.json dedupes)
        }
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
    const words = closure ? new Map<string, Json[]>() : wordsByLocation(ef, typed); // T321: a closed page's rows are not in force
    notices.push(...wordingNotices(slug, ef, typed, robust));
    const rowHolds = new Map<string, { fingerprint: string; why: string }>();
    const xOf = (lid: string) => (words.get(lid)?.length ? { x: words.get(lid)! } : {});
    const [rules, held] = join(slug, ef, typed, found, rep, rowHolds);
    if (aware) inputsOut[slug] = { page: sha, entries: contentHash((await rawEntries(slug)) ?? "") };
    const moved = new Set(rep.outcomes.filter((o) => o.status === "section_moved" && o.location_id).map((o) => o.location_id!));
    for (const lid of [...rules.keys()].sort(cmpStr)) {
      if (moved.has(lid) || rowHolds.has(lid)) continue;
      if (!robust) {
        locs[lid] = { r: slug, ...locationCalendar(closure && rules.get(lid)!.length ? closure : rules.get(lid)!, verdicts, gears), ...xOf(lid) };
        continue;
      }
      try {
        locs[lid] = { r: slug, ...locationCalendar(closure && rules.get(lid)!.length ? closure : rules.get(lid)!, verdicts, gears, true), ...xOf(lid) };
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
      // (T549 item 4) A PLACE THAT LEFT THE PAGE. `reconcile` calls an active curated place no row matched `dormant`
      // and nothing read it: the place left `locs` and the wider rule of its chain answered, with nobody told.
      //   reworded  the top candidate of a `drift` row (held unplaced, its own urgent notice): reads `check`, in `holds`
      //   gone      no row explains it: listed in the region's `gone` and told once — only a place the last feed
      //             published (or already listed), so a place that was never live says nothing
      const reworded = new Map<string, string>();
      for (const o of rep.outcomes)
        if (o.status === "drift" && o.candidates.length && !reworded.has(o.candidates[0]!.location_id)) reworded.set(o.candidates[0]!.location_id, o.fingerprint);
      const wasGone = new Map(((was.gone || []) as Gone[]).map((g) => [g.location_id, g] as const));
      const prevLocs = (prev.locs || {}) as Record<string, Json>;
      const gone: Gone[] = [];
      for (const o of rep.outcomes) {
        if (o.status !== "dormant" || !o.location_id) continue;
        const lid = o.location_id;
        if (reworded.has(lid)) {
          const v = verdicts.add(REWORDED_VERDICT);
          locs[lid] = { r: slug, sp: SPECIES_N.map(() => [0, v]), g: [0, -1] };
          kept.push({ location_id: lid, fingerprint: reworded.get(lid)!, why: "DFO reworded this place; the new wording is held unplaced until it is confirmed", kept: "check" });
        } else if (wasGone.has(lid)) gone.push(wasGone.get(lid)!);
        else if (prevLocs[lid]?.r === slug) gone.push({ location_id: lid, water: o.water, place: o.specific_area, page: sha.slice(0, 12) });
      }
      reg.holds = kept.sort((x, y) => cmpStr(x.location_id, y.location_id));
      reg.gone = gone.sort((x, y) => cmpStr(x.location_id, y.location_id));
      tellGone();
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
      notices.push(unplacedNotice(u));
    }
    if (robust) await rowDigest(slug, sha, typed, ef, deps.priv, notices, log);
  }

  const sortedLocs: Record<string, Json> = {};
  for (const k of Object.keys(locs).sort(cmpStr)) sortedLocs[k] = locs[k];
  const regions: Record<string, Json> = {};
  for (const s of ALL_SLUGS) if (s in regionOut) regions[s] = regionOut[s];
  // robust: number verdicts / gears canonically (first use over the sorted locations; NOT_LISTED stays 0) so a carried
  // region and a re-typed one publish the same bytes — what lets an unchanged feed skip its write
  const [vItems, gItems] = robust ? canonical(sortedLocs, verdicts.items, gears.items) : [verdicts.items, gears.items];
  const index = { format: FORMAT, at, regions, verdicts: vItems, gears: gItems, locs: sortedLocs, unplaced: unplaced.length };
  const unplacedSorted = unplaced
    .map((u, i) => ({ u, i }))
    .sort((a, b) => cmpTuple([a.u.region, a.u.fingerprint], [b.u.region, b.u.fingerprint]) || a.i - b.i)
    .map((x) => x.u);
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
  const sent = await sendDue(notices as unknown as Record<string, unknown>[], {
    url: deps.notifyUrl,
    fetch: deps.fetch,
    state: deps.priv,
    stateKey: "notified.json",
    keyOf: (n) => n.key,
    messageOf: (n) => n.text,
    log,
  });
  return { index, unplaced, notices: robust ? notices : notices.map(({ key, text }) => ({ key, text })), sent, skipped };
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

/** live.words_by_location (T321): location id → the rows its card shows in DFO's words, `{k, sp, d, t}` (`locs[lid].x`) */
export function wordsByLocation(ef: EntryFile, typed: TypedFeed): Map<string, Json[]> {
  const index = byFingerprint(ef);
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
