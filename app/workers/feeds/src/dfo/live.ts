// T225 port of pipeline/regs/dfo_salmon/live.py `run`: THE LIVE DFO SALMON FEED, one Worker invocation.
//
//   feed  (published, R2 feeds/dfo/)   index.json, unplaced.json — byte-identical to the Python's
//   priv  (never served)               notified.json (sent notices), manifest.json (fetch snapshots),
//                                      raw/region<slug>-eng.html (last good page), history/region<slug>_<stamp>.html
//                                      (every changed page: the corpus), entries/region-<slug>.json (the curated
//                                      entry files — see `EntrySource`)
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
import { contentHash, loadManifest, skipSameStore, type Fetch, type Meter, type Store } from "../io";
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
import { bucket, headerClosure, markShared, sharedScope } from "./quota";
import { Interner, locationCalendar, NOT_LISTED, SPECIES } from "./salmon";
import { untangle, verify } from "./untangle";
import { readRegistry, REGISTRY_PATH, template, type Registry } from "./wording";

export const FORMAT = "dfo-feed/2"; // T332: a verdict's quota is `tree` (a game-fish TreeTier), a bucket `{tree, …}`
const SPECIES_N = [...SPECIES];
export const FAIL_NOTIFY = 2;
const QUEUE_ACTION = (slug: string) => `python -m pipeline.regs.dfo_salmon.live queue --regions ${slug}`;

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
export const DFO_REV = "2026-10-31.2"; // T332: quotas as game-fish quota trees (`tree`), the wording split tidied
/** published index.json is rewritten at least this often even when nothing changed (the app's "checked" age) */
export const INDEX_HEARTBEAT_MS = 24 * 3600_000;

/** Index.json minus what changes on every run (`at`, each region's `checked`): what "changed" means for a write. */
export function stableIndex(body: string): string {
  try {
    const j = JSON.parse(body);
    delete j.at;
    for (const r of Object.values<Json>(j.regions ?? {})) delete r.checked;
    return JSON.stringify(j);
  } catch {
    return body;
  }
}

/** A location held at its last answers this run (robust mode): published in the region's `holds`. */
export interface RowHold {
  location_id: string;
  fingerprint: string;
  why: string;
  /** "carried": the last feed's answers stand; "check": there were none — the location reads `check` */
  kept: "carried" | "check";
}
export const ROW_HELD_VERDICT = { s: "check", t: "DFO's row for this place could not be read this run — check DFO" };

export interface DfoResult {
  index: Record<string, unknown>;
  unplaced: Record<string, unknown>[];
  notices: Notice[];
  sent: { sent: string[]; failed: string[]; off: boolean };
  /** change-aware: the regions whose typing was skipped (page + entries unchanged) */
  skipped?: string[];
  /** non-fatal problems this run (e.g. the R2 wording registry missing / invalid → compiled-in copy used) */
  warnings?: string[];
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
export function typePage(html: string, slug: string, robust = false, reg?: Registry): [TypedFeed, Location[], Structure] {
  const parsed = parseRegion(html, slug);
  const u = untangle(parsed);
  verify(parsed, u);
  const [locs, rules, structure] = extract(u);
  const typed = build({ region: slug, date_modified: parsed.date_modified, preamble: parsed.preamble, rules }, robust, reg);
  return [typed, locs, structure];
}

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
      // T349: a location whose words say "in the aggregate" shares its counts across its scope (quota.markShared)
      const rs = markShared(loc.rules as unknown as Record<string, unknown>[], sharedScope(hit.source_text)) as unknown as typeof loc.rules;
      const l = rules.get(hit.location_id);
      if (l) l.push(...rs);
      else rules.set(hit.location_id, [...rs]);
      continue;
    }
    if (fp.startsWith("page:")) continue;   // T349: a page default's sentence with no default location here is not a new place
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
      ...(loc.error !== undefined ? { error: loc.error } : {}),
    });
  }
  return [rules, unplaced];
}

/** What the owner email renders (robust mode; stripped in pythonParity so the notices stay the Python's). */
export interface NoticeData {
  region: string;
  /** new | new_water | drift (a place to place) · held | fetch | row-held | moved | header | header-name */
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
}
export interface Notice {
  key: string;
  text: string;
  data?: NoticeData;
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

async function readJson(store: Store, k: string, dflt: Json): Promise<Json> {
  try {
    const t = await store.get(k);
    return t === null ? dflt : loads(t);
  } catch {
    return dflt;
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
  const feedStore: Store = aware
    ? skipSameStore(deps.feed, writes, {
        now: Date.parse(at),
        stable: (k, b) => (k === "index.json" ? stableIndex(b) : b),
        maxAgeMs: (k) => (k === "index.json" ? INDEX_HEARTBEAT_MS : undefined),
        meter: deps.meter,
      })
    : deps.feed;
  // T321: the wording registry is a runtime input (priv `wording.json`), read once per run; missing / invalid → the compiled-in copy
  const wordingText = await deps.priv.get("wording.json");
  const { reg: wordingReg, warning: wordingWarning } = readRegistry(wordingText);
  const warnings: string[] = [];
  if (wordingWarning) {
    warnings.push(wordingWarning);
    log(`warning: ${wordingWarning}`);
  }
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
    if (robust) reg.holds = was.holds || [];
    regionOut[slug] = reg;
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
      const last = inputs.rev === DFO_REV ? inputs.regions?.[slug] : undefined;
      const usable = last && last.page === key.page && last.entries === key.entries && !was.held && slug in prevRegions;
      if (usable) {
        inputsOut[slug] = key;
        Object.assign(locs, carry(prev, slug, verdicts, gears));
        for (const u of prevUnplacedOf(slug)) {
          unplaced.push(u);
          notices.push(unplacedNotice(u)); // re-offered: a send that failed is retried (notified.json dedupes)
        }
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
      reg.holds = kept;
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
  await feedStore.put("index.json", feedText(index), { contentType: "application/json", cacheControl: "public, max-age=300, stale-while-revalidate=3600" });
  await feedStore.put("unplaced.json", feedText(unplacedSorted), { contentType: "application/json", cacheControl: "public, max-age=300, stale-while-revalidate=3600" });
  if (aware) {
    // a region not run this time keeps its last inputs; one held / failed this run has none (re-typed next run)
    const keep: Record<string, { page: string; entries: string }> = {};
    if (inputs.rev === DFO_REV) for (const [s2, v] of Object.entries(inputs.regions ?? {})) if (!todo.includes(s2)) keep[s2] = v;
    const next = JSON.stringify({ rev: DFO_REV, wording: wordingHash, regions: { ...keep, ...inputsOut } });
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
  return { index, unplaced, notices: robust ? notices : notices.map(({ key, text }) => ({ key, text })), sent, skipped, ...(warnings.length ? { warnings } : {}) };
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
    const copy = { ...r };
    setBucket(copy);
    regions[slug] = copy;
  }
  const verdicts = ((prev.verdicts || []) as Json[]).map((v) => Object.fromEntries(Object.entries(v).filter(([k]) => k !== "q")));
  return { ...prev, regions, verdicts };
}

/** (T320) the salmon bucket — the header's combined daily / possession — beside the notes it is read from */
function setBucket(reg: Json): void {
  const b = bucket(reg.notes || []);
  if (b) reg.bucket = b;
  else delete reg.bucket;
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
