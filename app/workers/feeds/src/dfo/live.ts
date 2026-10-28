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
import type { Fetch, Store } from "../io";
import { sendDue } from "../notify";
import { byFingerprint, loadEntryFile, reconcile, type EntryFile, type Report } from "./entries";
import { build, type TypedFeed } from "./feed";
import { ALL_SLUGS, fetchRegion, isoSeconds, normalizeSlug, PAGES, pageUrl, REAL_SLUGS, type FetchRegionOptions, type RegionSnapshot } from "./fetch";
import { sha256 } from "./hash";
import { digest, parseHeader, resolve, type Note } from "./header";
import { extract, type Location, type Structure } from "./locations";
import { key } from "./names";
import { parseRegion } from "./parse";
import { cmpStr, cmpTuple, formatFixed, pyStrOf, pyTypeName, repr } from "./py";
import { feedText, loads, PyFloat } from "./pyjson";
import { Interner, locationCalendar, NOT_LISTED } from "./salmon";
import { untangle, verify } from "./untangle";

export const FORMAT = "dfo-feed/1";
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
  log?: (s: string) => void;
}

export interface DfoResult {
  index: Record<string, unknown>;
  unplaced: Record<string, unknown>[];
  notices: { key: string; text: string }[];
  sent: { sent: string[]; failed: string[]; off: boolean };
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
export function typePage(html: string, slug: string): [TypedFeed, Location[], Structure] {
  const parsed = parseRegion(html, slug);
  const u = untangle(parsed);
  verify(parsed, u);
  const [locs, rules, structure] = extract(u);
  const typed = build({ region: slug, date_modified: parsed.date_modified, preamble: parsed.preamble, rules });
  return [typed, locs, structure];
}

const waterKnown = (ef: EntryFile, water: string): boolean => {
  const k = key(water || "");
  return !k || ef.waters.some((w) => key(w.name) === k || w.aliases.some((a) => key(a) === k));
};

function join(slug: string, ef: EntryFile, typed: TypedFeed, locations: Location[], report: Report): [Map<string, Json[]>, Json[]] {
  const index = byFingerprint(ef);
  const byFp = new Map(locations.map((l) => [l.fingerprint, l] as const));
  const outcome = new Map(report.outcomes.map((o) => [o.fingerprint, o] as const));
  const rules = new Map<string, Json[]>();
  const unplaced: Json[] = [];
  for (const loc of typed.locators || []) {
    const fp = loc.fingerprint;
    const hit = index.get(fp);
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
    });
  }
  return [rules, unplaced];
}

export function unplacedNotice(u: Json): { key: string; text: string } {
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
  return { key: `dfo-${u.kind}:${slug}:${u.fingerprint}`, text };
}

function carry(prev: Json, slug: string, verdicts: Interner<unknown>, gears: Interner<unknown>, only: Set<string> | null = null): Record<string, Json> {
  const pv = prev.verdicts || [];
  const pg = prev.gears || [];
  const out: Record<string, Json> = {};
  for (const [lid, c] of Object.entries((prev.locs || {}) as Record<string, Json>)) {
    if (c.r !== slug || (only !== null && !only.has(lid))) continue;
    const sp = c.sp.map((runs: number[]) => runs.map((x, k) => (k % 2 === 0 || x < 0 ? x : verdicts.add(pv[x]))));
    const g = c.g.map((x: number, k: number) => (k % 2 === 0 || x < 0 ? x : gears.add(pg[x])));
    out[lid] = { r: slug, sp, g };
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
  const at = deps.now ?? nowStamp();
  const entrySource: EntrySource = deps.entries ?? ((slug) => deps.priv.get(`entries/region-${slug}.json`));
  const live = deps.fetcher ? null : liveFetcher(deps.fetch, deps.priv, log, deps.fetchOptions);
  const fetcher: Fetcher = deps.fetcher ?? live!.get;
  const keepHistory = deps.history ?? !deps.fetcher;

  const prev: Json = await readJson(deps.feed, "index.json", {});
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
  const notices: { key: string; text: string }[] = [];
  const prevRegions = prev.regions || {};

  const entryCache = new Map<string, EntryFile>();
  const loadEf = async (slug: string) => {
    let ef = entryCache.get(slug);
    if (!ef) {
      ef = loadEntryFile(slug, await entrySource(slug));
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
    };
    regionOut[slug] = reg;
    const hold = (why: string, k: string) => {
      reg.held = why;
      Object.assign(locs, carry(prev, slug, verdicts, gears));
      unplaced.push(...prevUnplacedOf(slug));
      notices.push({ key: `dfo-held:${slug}:${k}`, text: `DFO Region ${slug} held at the last feed: ${why}` });
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
    let typed: TypedFeed;
    let found: Location[];
    let structure: Structure;
    try {
      [typed, found, structure] = typePage(html, slug);
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
    const [rules, held] = join(slug, ef, typed, found, rep);
    const moved = new Set(rep.outcomes.filter((o) => o.status === "section_moved" && o.location_id).map((o) => o.location_id!));
    for (const lid of [...rules.keys()].sort(cmpStr))
      if (!moved.has(lid)) locs[lid] = { r: slug, ...locationCalendar(rules.get(lid)!, verdicts, gears) };
    if (moved.size) {
      Object.assign(locs, carry(prev, slug, verdicts, gears, moved));
      for (const o of rep.outcomes)
        if (o.status === "section_moved")
          notices.push({
            key: `dfo-section:${slug}:${o.fingerprint}`,
            text: `DFO Region ${slug}: ${o.location_id} moved section (${o.detail}); its last answers stand until it is reviewed in DFO mode`,
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
  const index = { format: FORMAT, at, regions, verdicts: verdicts.items, gears: gears.items, locs: sortedLocs, unplaced: unplaced.length };
  const unplacedSorted = unplaced
    .map((u, i) => ({ u, i }))
    .sort((a, b) => cmpTuple([a.u.region, a.u.fingerprint], [b.u.region, b.u.fingerprint]) || a.i - b.i)
    .map((x) => x.u);
  await deps.feed.put("index.json", feedText(index), { contentType: "application/json", cacheControl: "public, max-age=300, stale-while-revalidate=3600" });
  await deps.feed.put("unplaced.json", feedText(unplacedSorted), { contentType: "application/json", cacheControl: "public, max-age=300, stale-while-revalidate=3600" });
  if (live) await live.flush();
  const sent = await sendDue(notices, {
    url: deps.notifyUrl,
    fetch: deps.fetch,
    state: deps.priv,
    stateKey: "notified.json",
    keyOf: (n) => n.key,
    messageOf: (n) => n.text,
    log,
  });
  return { index, unplaced, notices, sent };
}

function readHeader(slug: string, html: string, reg: Json, was: Json, ef: EntryFile, notices: { key: string; text: string }[]): void {
  let notes: Note[];
  try {
    notes = parseHeader(html, slug);
  } catch (e) {
    notices.push({
      key: `dfo-header-parse:${slug}:${reg.sha256.slice(0, 12)}`,
      text: `DFO Region ${slug}: the region header did not parse (${pyTypeName(e)}); the last one stands`,
    });
    return;
  }
  if (!notes.length && was.notes && was.notes.length) {
    notices.push({
      key: `dfo-header-empty:${slug}:${reg.sha256.slice(0, 12)}`,
      text: `DFO Region ${slug}: the page has no region header any more; the last one stands`,
    });
    return;
  }
  const d = digest(notes).slice(0, 16);
  if (was.header && was.header !== d) {
    const oldIds = new Set<string>((was.notes || []).map((n: Note) => n.id));
    const newIds = new Set<string>(notes.map((n) => n.id!));
    const plus = [...newIds].filter((i) => !oldIds.has(i)).length;
    const minus = [...oldIds].filter((i) => !newIds.has(i)).length;
    notices.push({
      key: `dfo-header:${slug}:${d}`,
      text: `DFO Region ${slug}: the region header changed (+${plus} / -${minus} notes); the card's Region rules now read the new words`,
    });
  }
  const [, unresolved] = resolve(notes, ef);
  for (const u of unresolved)
    notices.push({
      key: `dfo-header-name:${slug}:${u.name}`,
      text: `DFO Region ${slug}: the header names ${repr(u.name)}, which no curated DFO water binds — its note shows on no water until it is bound (DFO review)`,
    });
  reg.notes = notes;
  reg.header = d;
  reg.unresolved = unresolved;
}
