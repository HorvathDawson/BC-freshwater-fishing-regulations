/**
 * T193-T195 — THE WHOLE SITE FROM ONE RELEASE: water pages (T269: the app's own regs pane, prerendered — `waterPage`), About, region hubs, "open this weekend", "opening and closing
 * dates", "rule changes", sitemap, robots, slug map, share cards and the snapshot the next release is diffed against.
 * Everything is returned as in-memory files (`write.ts` puts them on disk); the pages are built from the release bundle only.
 *
 * Gates that throw (the release fails): a water the app would answer differently (verify.ts), JSON-LD that says what the page
 * does not, a duplicate title or description, a water no hub links to, a page count that differs from the sitemap's.
 */
import { createHash } from "node:crypto";
import type { RegsStore } from "@app/data/regs";
import { slugShard, SLUG_SHARDS } from "@app/ui";
import { aboutBody, privacyBody, CSS, SHARE_FONT, SHARE_GROUND, SHARE_H, SHARE_INK, SHARE_W, clip, descriptionOf, esc, headOf, NOT_LEGAL, notFoundBody, regionLabel, shell, titleOf, waterRegion, type Release, type Site } from "./html";
import { aboutLd, checkJsonLd, pageLd, waterLd } from "./jsonld";
import { appPage, listWaterOrds, managementUnits, nearestTowns, rulesKey, waterModel, type BuildDay, type WaterModel } from "./model";
import { TOPICS, topicLines, type TopicKey } from "./topics";
import { assignSlugs, type SlugMap, type Steps } from "./slug";
import { verifyAgainstApp } from "./verify";
import { regionYear, yearCardHtml } from "./regionYear";
import { pageSize, type PageSize } from "./budget";
import { clientScript, gaugeBody, gaugeDescription, gaugeModels, gaugesIndexBody, gaugeTitle, siteGaugeCardSvg, stationsFile, type GaugeModel, type GaugeRows } from "./gauges";

export interface PageRec {
  readonly path: string; readonly kind: "water" | "region" | "letter" | "topic" | "changes" | "about" | "privacy" | "gauge" | "gauges";
  /** the page; "" for a water page the build wrote out itself (T279: compressed, by a render thread; 19,000 panes are not held) */
  readonly html: string; readonly hash: string;
  readonly title: string; readonly description: string; readonly size: PageSize;
}
/**
 * (T279) A water page as made: its HTML (kept in `files`, checked here) — or, written out by a render thread (compressed),
 * only its numbers and what the thread's gates found (the same `checkWaterPage`).
 */
export type WaterMade = { readonly html: string } | { readonly html: null; readonly size: PageSize; readonly problems: readonly string[] };
/** The gate a water page's HTML must pass (JSON-LD held to the page; a water page, the app's pane, carries no breadcrumb). */
export const checkWaterPage = (html: string, origin: string): string[] => checkJsonLd(html, origin, ["WebPage", "Place"]);

export interface Snapshot {
  readonly release: string; readonly asOf: string;
  readonly pages: Readonly<Record<string, { readonly hash: string; readonly lastmod: string }>>;
  /** `hash`: the water's page words (lastmod); `rules`: what an angler must do there (`rulesKey`, T490: /changes) */
  readonly waters: Readonly<Record<string, { readonly slug: string; readonly name: string; readonly hash: string; readonly rules: string }>>;
}
/**
 * (T529) The /changes split, one function for the page and for the cheap pre-check (`build-seo.mjs --check-changes`): a water
 * whose rules key (`rulesKey`: what applies where) moved, a water the previous release did not list, one it listed and this
 * one does not.
 */
export function changesOf(was: Snapshot["waters"], ids: readonly string[], rules: ReadonlyMap<string, string>):
    { added: Set<string>; changed: Set<string>; gone: string[] } {
  const now = new Set(ids);
  return { added: new Set(ids.filter((id) => !(id in was))), changed: new Set(ids.filter((id) => id in was && was[id]!.rules !== rules.get(id))),
           gone: Object.keys(was).filter((id) => !now.has(id)) };
}
export interface BuildOpts {
  readonly store: RegsStore; readonly site: Site; readonly release: Release; readonly on: BuildDay;
  readonly slugMap: SlugMap; readonly previous: Snapshot | null;
  /** a region with more waters than this lists them on A-Z pages, not on its hub (default 400) */
  readonly hubSplitAt?: number;
  /**
   * (T269) THE WATER PAGE: the app's own document with the water's regs pane prerendered in it (build.ts over pane.tsx and
   * prerender.ts), given the page's head tags. There is no other water page.
   */
  readonly waterPage: (m: WaterModel, head: string, file: string) => Promise<WaterMade>;
  /** build only these waters (item ids): a sample to look at, never a release's site */
  readonly only?: ReadonlySet<string>;
  /** (T487) the release's gauges and the waters each serves (gauges.ts `readGaugeRows`); null: the release has none */
  readonly gauges?: GaugeRows | null;
}
export interface SiteBuild {
  readonly files: ReadonlyMap<string, string>; readonly pages: readonly PageRec[];
  readonly slugSteps: Steps; readonly slugMap: SlugMap; readonly removed: readonly { id: string; slug: string; region: number }[];
  readonly snapshot: Snapshot; readonly models: readonly WaterModel[]; readonly cssHref: string; readonly regions: readonly number[];
  /** (T487) the gauge pages built */
  readonly gauges: readonly GaugeModel[];
}

const sha = (s: string, n = 16) => createHash("sha256").update(s).digest("hex").slice(0, n);
/** (T529) a water's rules key as the snapshot keeps it */
export const rulesSha = (key: string): string => sha(key);
const outFile = (path: string) => `${path.slice(1)}/index.html`;
/**
 * (T605, owner 2026-11-10) THE LIST PAGES CARRY NAMES AND LINKS, never an open / closed word: the site is built once a
 * release, so a day-relative word is right only on the day it was built. The app (the map, a water's page) works out open
 * and closed for the day itself. The pages that were all about "today" (what is open this weekend, waters
 * opening and closing soon) are gone: hosting.ts `REMOVED_PAGES` 301s their addresses to the region page, and they are in no sitemap.
 */
/** the live app's map: it opens on the current day (a link carries no `date`) and works out open / closed itself */
export const mapLink = (site: Site): string => `${site.origin}/`;

/** A-Z buckets of a region's waters ("0" = a name that does not start with a letter), in letter order */
function groupByLetter(ws: readonly WaterModel[]): Map<string, WaterModel[]> {
  const g = new Map<string, WaterModel[]>();
  for (const m of ws) { const c = m.name.normalize("NFKD").trim()[0]?.toUpperCase() ?? "0", k = /[A-Z]/.test(c) ? c : "0"; g.set(k, [...(g.get(k) ?? []), m]); }
  return new Map([...g].sort((a, b) => (a[0] === "0" ? 1 : b[0] === "0" ? -1 : a[0] < b[0] ? -1 : 1)));
}

/** stable under a changed date: the "as of" times are what a rebuild on another day moves */
const stable = (html: string) => html.replace(/<time [^>]*>[^<]*<\/time>/g, "<time/>");

/** (T622 G2) what a water's share picture says under its name: nothing that depends on the day it was drawn */
export const SHARE_LINE = "Season, what you can keep, gear and limits";
function shareSvg(m: WaterModel, site: Site): string {
  const words = m.name.split(" "), lines: string[] = [""];
  for (const w of words) { if ((lines.at(-1) + " " + w).trim().length > 26 && lines.at(-1)) lines.push(w); else lines[lines.length - 1] = (lines.at(-1) + " " + w).trim(); }
  const t = lines.slice(0, 3).map((l, i) => `<text x="64" y="${190 + i * 84}" font-size="72" font-weight="700">${esc(l)}</text>`).join("");
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${SHARE_W}" height="${SHARE_H}" viewBox="0 0 ${SHARE_W} ${SHARE_H}" font-family="${SHARE_FONT}">` +
    `<rect width="${SHARE_W}" height="${SHARE_H}" fill="${SHARE_GROUND}"/><g fill="${SHARE_INK}">${t}` +
    `<text x="64" y="470" font-size="40">${esc(SHARE_LINE)}</text>` +
    `<text x="64" y="560" font-size="34" opacity=".85">${esc(site.name)} · ${esc(waterRegion(m))} · ${esc(site.origin.replace("https://", ""))}</text></g></svg>`;
}

export async function buildSite(o: BuildOpts): Promise<SiteBuild> {
  const { store, site, release, on } = o;
  const asOf = on.iso;
  // ── the waters ─────────────────────────────────────────────────────────────────────────────────────────────────────
  const models: WaterModel[] = [];
  for (const ord of await listWaterOrds(store)) {
    const m = await waterModel(store, ord, on);
    if (m && m.region >= 1 && (!o.only || o.only.has(m.id))) models.push(m);
  }
  const bad: string[] = [];
  for (const m of models) {
    const pages = await Promise.all(m.parts.map((p) => appPage(store, m.id, p.partIx, on)));
    bad.push(...verifyAgainstApp(m, pages));
  }
  if (bad.length) throw new Error(`seo: ${bad.length} page(s) differ from the app:\n${bad.slice(0, 20).join("\n")}`);

  const towns = await nearestTowns(store), units = await managementUnits(store);
  const { map: slugMap, removed, steps: slugSteps } = assignSlugs(models.map((m) => ({ id: m.id, name: m.name, region: m.region, town: towns.get(m.id) ?? null, mus: units.get(m.id) ?? [] })), o.slugMap);
  const slugOf = (id: string) => slugMap.waters[id]!.slug;
  const byName = (a: WaterModel, b: WaterModel) => a.name.localeCompare(b.name) || a.id.localeCompare(b.id);
  const regions = [...new Set(models.flatMap((m) => m.regions))].sort((a, b) => a - b);
  const regionMemo = new Map<number, WaterModel[]>();
  const inRegion = (r: number) => { let l = regionMemo.get(r); if (!l) regionMemo.set(r, l = models.filter((m) => m.regions.includes(r)).sort(byName)); return l; };
  const cssHref = `/assets/site.${sha(CSS, 10)}.css`;
  const regionPath = (r: number) => `/region/${r}`;
  const waterPath = (m: WaterModel) => `/water/${slugOf(m.id)}`;

  const pages: PageRec[] = [];
  const files = new Map<string, string>();
  const add = (kind: PageRec["kind"], path: string, html: string, hash = sha(stable(html))) => {
    // the gates that read a page's HTML run here, as it is made: JSON-LD held to the page, its title and description
    bad.push(...(kind === "water" ? checkWaterPage(html, site.origin) : checkJsonLd(html, site.origin)).map((x) => `${path}: ${x}`));
    const title = /<title>([^<]*)<\/title>/.exec(html)?.[1] ?? "", description = /<meta name="description" content="([^"]*)">/.exec(html)?.[1] ?? "";
    pages.push({ path, kind, html, hash, title, description, size: pageSize(path, html) });
    files.set(outFile(path), html);
  };
  const shellOf = (path: string, title: string, description: string, body: string, ld: string, image?: string) =>
    shell({ title, description, path, body: body + regionFooter(), jsonld: ld, cssHref, ...(image ? { image } : {}) }, site);
  /** the build date is shown (the JSON-LD `dateModified` must be on the page); it is a date, never "today" */
  const built = () => `Rules from release ${esc(release.id)}, built <time datetime="${asOf}">${asOf}</time>.`;
  const regionFooter = () => regions.length
    ? `<nav class="small" aria-label="Regions"><p>Browse by region: ${regions.map((r) => `<a href="${regionPath(r)}">${esc(regionLabel(r))}</a>`).join(" · ")}</p></nav>` : "";

  // ── water pages ────────────────────────────────────────────────────────────────────────────────────────────────────
  const titles = new Map<string, string>(), descriptions = new Map<string, string>();
  const unique = (what: Map<string, string>, v: string, path: string) => {
    const was = what.get(v);
    if (was) bad.push(`${path} and ${was} share a ${what === titles ? "title" : "description"}: ${v}`);
    what.set(v, path);
  };
  const waterHash = new Map<string, string>(), waterRules = new Map<string, string>();
  const made: { path: string; hash: string; title: string; description: string; page: Promise<WaterMade> }[] = [];
  const waterTopics = new Map<string, Map<TopicKey, string[]>>();
  const nameCount = new Map<string, number>(), sameQual = new Map<string, WaterModel[]>();
  for (const m of models) {
    nameCount.set(m.name, (nameCount.get(m.name) ?? 0) + 1);
  }
  const qualOf = (x: WaterModel) => ((nameCount.get(x.name) ?? 0) > 1 ? (towns.get(x.id) ? `near ${towns.get(x.id)}` : waterRegion(x)) : "");
  for (const m of models) { const k = `${m.name}|${qualOf(m)}`; const l = sameQual.get(k); if (l) l.push(m); else sameQual.set(k, [m]); }
  for (const m of models) {
    const path = waterPath(m);
    // two waters with one name keep distinct titles: where they are (nearest town, else the region), then "no. k" if that repeats
    const town = towns.get(m.id) ?? null;
    const qual0 = (x: WaterModel) => ((nameCount.get(x.name) ?? 0) > 1 ? (towns.get(x.id) ? `near ${towns.get(x.id)}` : waterRegion(x)) : "");
    const twins = [...(sameQual.get(`${m.name}|${qual0(m)}`) ?? [])].sort((x, y) => x.id.localeCompare(y.id));
    const qual = qual0(m) && twins.length > 1 ? `${qual0(m)}, no. ${twins.indexOf(m) + 1}` : qual0(m);
    const title = titleOf(m.name, site, qual), description = descriptionOf(m, `${m.name}${/no\. \d+$/.exec(qual)?.[0] ? ` ${/no\. \d+$/.exec(qual)![0]}` : ""}`, town);
    unique(titles, title, path); unique(descriptions, description, path);
    const hash = sha(JSON.stringify(m.parts.map((p) => [p.label, p.sections.filter((s) => s.title !== "Why it is closed").map((s) => [s.title, s.items.filter((i) => !i.dated)])])));
    waterHash.set(m.id, hash);
    waterRules.set(m.id, rulesSha(await rulesKey(store, m.ord)));
    // the topic lines feed the hubs (each a sentence the water's own pane shows)
    waterTopics.set(m.id, topicLines(m));
    const head = headOf({ title, description, path, jsonld: waterLd(m, path, title, site), image: `/share/${slugOf(m.id)}.png` }, site);
    // (T279) asked at once, all of them: the build's render threads take them in parallel; kept in order below
    made.push({ path, hash, title, description, page: o.waterPage(m, head, outFile(path)) });
    // (T406) the card's source: build.ts draws it as `share/<slug>.png` (share.ts) and ships the PNG only
    files.set(`share/${slugOf(m.id)}.svg`, shareSvg(m, site));
  }
  for (const w of made) {
    const p = await w.page;
    if (p.html !== null) { add("water", w.path, p.html, w.hash); continue; }
    bad.push(...p.problems.map((x) => `${w.path}: ${x}`));
    pages.push({ path: w.path, kind: "water", html: "", hash: w.hash, title: w.title, description: w.description, size: p.size });
  }

  // ── topic hubs: waters by what their rules say (steelhead, salmon, trout, bait ban, fly only, closed, boats) ──────────────────
  const topicWaters = (r: number, k: TopicKey) => inRegion(r).filter((m) => waterTopics.get(m.id)?.has(k));
  const TOPIC_CAP = 600;

  // ── region hubs (T195) ─────────────────────────────────────────────────────────────────────────────────────────────
  for (const r of regions) {
    const ws = inRegion(r), path = regionPath(r), name = regionLabel(r);
    const list = (xs: readonly WaterModel[]) => `<ul class="cols">${xs.map((m) => `<li><a href="${waterPath(m)}">${esc(m.name)}</a></li>`).join("")}</ul>`;
    const letters = ws.length > (o.hubSplitAt ?? 400) ? groupByLetter(ws) : null;
    const title = clip(`${name} fishing regulations (BC) — ${site.name}`, 60);
    const body = `<nav class="crumbs"><a href="/">Home</a> › ${esc(name)}</nav><h1>${esc(name)} fishing regulations</h1>` +
      `<p>${ws.length} waters in ${esc(name)} with the full rules in plain words, from release ${esc(release.id)}.</p>` +
      // (owner 2026-11-10, T605) the live map, opening on the current day: the app says open or closed (it takes no region or bbox in a link yet)
      `<div class="card"><p>To see which waters are open, open the map. It opens on the current day and colours every water for it.</p>` +
      `<a class="app" href="${esc(mapLink(site))}">Open the map for ${esc(name)}</a></div>` +
      `<ul>` +
      TOPICS.filter((t) => topicWaters(r, t.key).length).map((t) => `<li><a href="${path}/${t.key}">${esc(t.hub)} in ${esc(name)}</a> (${topicWaters(r, t.key).length})</li>`).join("") + `</ul>` +
      yearCardHtml(name, regionYear(ws)) +
      (letters ? `<h2>Waters in ${esc(name)} by first letter</h2><ul class="cols">${[...letters].map(([l, xs]) => `<li><a href="${path}/waters-${l.toLowerCase()}">${l === "0" ? "Other" : l} (${xs.length})</a></li>`).join("")}</ul>`
        : `<h2>All waters in ${esc(name)}</h2>${list(ws)}`) +
      `<div class="legal"><p>${built()} This is not a legal document. Check the official synopsis before you fish.</p></div>`;
    const description = clip(`Full BC freshwater fishing regulations for ${ws.length} waters in ${name}: what you can keep and when each water is open, in plain words.`);
    unique(titles, title, path); unique(descriptions, description, path);
    add("region", path, shellOf(path, title, description, body, pageLd(site, path, title, asOf, [["Home", "/"], [name, path]])));
    for (const [l, xs] of letters ?? []) {
      const lp = `${path}/waters-${l.toLowerCase()}`, lname = l === "0" ? "other names" : `names starting with ${l}`;
      const lt = clip(`${name} waters, ${l === "0" ? "other" : l} — ${site.name}`, 60);
      const ld = clip(`${xs.length} waters in ${name} with ${lname}: the full rules for each, in plain words.`);
      const lb = `<nav class="crumbs"><a href="/">Home</a> › <a href="${path}">${esc(name)}</a> › ${l === "0" ? "Other" : l}</nav><h1>${esc(name)} waters: ${esc(lname)}</h1>` +
        `<p>${xs.length} waters, from release ${esc(release.id)}.</p>${list(xs)}` +
        `<div class="legal"><p>${built()} This is not a legal document. Check the official synopsis before you fish.</p></div>`;
      unique(titles, lt, lp); unique(descriptions, ld, lp);
      add("letter", lp, shellOf(lp, lt, ld, lb, pageLd(site, lp, lt, asOf, [["Home", "/"], [name, path], [l === "0" ? "Other" : l, lp]])));
    }
  }

  for (const t of TOPICS) {
    const rows = regions.map((r) => [r, topicWaters(r, t.key)] as const).filter(([, w]) => w.length);
    if (!rows.length) continue;
    const ipath = `/topic/${t.key}`, ititle = clip(`${t.hub} in B.C. — ${site.name}`, 60);
    const total = rows.reduce((n, [, w]) => n + w.length, 0);
    const idesc = clip(`${t.hub} for ${total} B.C. freshwater waters, by region, each linking to the rule in plain words from the provincial synopsis.`);
    const ibody = `<nav class="crumbs"><a href="/">Home</a> › ${esc(t.hub)}</nav><h1>${esc(t.hub)} in B.C.</h1>` +
      `<p>${total} waters have a rule on this, listed by region. Each water page has the full rule.</p>` +
      `<ul>${rows.map(([r, w]) => `<li><a href="${regionPath(r)}/${t.key}">${esc(regionLabel(r))}</a> (${w.length})</li>`).join("")}</ul>` +
      `<div class="legal"><p>${built()} ${esc(NOT_LEGAL)}</p></div>`;
    unique(titles, ititle, ipath); unique(descriptions, idesc, ipath);
    add("topic", ipath, shellOf(ipath, ititle, idesc, ibody, pageLd(site, ipath, ititle, asOf, [["Home", "/"], [t.hub, ipath]])));
    for (const [r, ws] of rows) {
      const p = `${regionPath(r)}/${t.key}`, nm = regionLabel(r);
      const title = clip(`${t.hub} in ${nm}, B.C. — ${site.name}`, 60);
      const desc = clip(`${t.hub} in ${nm}: ${ws.length} waters with a rule on this, each linking to the rule in plain words.`);
      const ls = ws.slice(0, TOPIC_CAP);
      const body = `<nav class="crumbs"><a href="/">Home</a> › <a href="${regionPath(r)}">${esc(nm)}</a> › ${esc(t.hub)}</nav><h1>${esc(t.hub)} in ${esc(nm)}, B.C.</h1>` +
        `<p>${ws.length} waters in ${esc(nm)}${ws.length > ls.length ? `; the first ${ls.length} by name are listed, every water is on <a href="${regionPath(r)}">the ${esc(nm)} page</a>` : ""}. Each water page has the rule in full.</p>` +
        `<ul class="cols">${ls.map((m) => `<li><a href="${waterPath(m)}">${esc(m.name)}</a> </li>`).join("")}</ul>` +
        `<p><a href="${ipath}">${esc(t.hub)} in all regions</a></p>` +
        `<div class="legal"><p>${built()} ${esc(NOT_LEGAL)}</p></div>`;
      unique(titles, title, p); unique(descriptions, desc, p);
      add("topic", p, shellOf(p, title, desc, body, pageLd(site, p, title, asOf, [["Home", "/"], [nm, regionPath(r)], [t.hub, p]])));
    }
  }

  // ── what changed since the last release (only with a previous snapshot to compare) ────────────────────────────────────
  const prev = o.previous;
  if (prev && prev.release !== release.id) {
    const was = prev.waters;
    // (T490) a snapshot from before the rules hash cannot say which waters' RULES moved: refused, never read as "all changed"
    const old = Object.values(was).find((w) => typeof w.rules !== "string");
    if (old) throw new Error(`seo: the previous snapshot (${prev.release}) has no rules hash (made before T490); rewrite it, keeping its pages: ` +
      `node tools/build-seo.mjs --release-dir <${prev.release} folder> --snapshot-only --keep-pages`);
    const split = changesOf(was, models.map((m) => m.id), waterRules);
    const added = models.filter((m) => split.added.has(m.id)), changed = models.filter((m) => split.changed.has(m.id));
    // the words moved, the rules did not (a wording release): one line, never a list of every water as "changed"
    const reworded = models.filter((m) => m.id in was && was[m.id]!.rules === waterRules.get(m.id) && was[m.id]!.hash !== waterHash.get(m.id)).length;
    const gone = split.gone.map((id) => was[id]!.name).sort();
    // nothing differs (two cuts of the same rules): no page — an empty "changes" page is thin content, not news
    if (changed.length + added.length + gone.length + reworded > 0) {
    const ul = (xs: readonly WaterModel[]) => `<ul class="cols">${xs.map((m) => `<li><a href="${waterPath(m)}">${esc(m.name)}</a></li>`).join("")}</ul>`;
    const path = "/changes", title = clip(`BC fishing regulation changes this release — ${site.name}`, 60);
    const body = `<nav class="crumbs"><a href="/">Home</a> › Changes</nav><h1>Fishing regulation changes in this release</h1>` +
      `<p class="asof">As of <time datetime="${asOf}">${asOf}</time>: release ${esc(release.id)} compared with release ${esc(prev.release)} (<time datetime="${prev.asOf}">${prev.asOf}</time>).</p>` +
      `<h2>Rules changed (${changed.length})</h2>${ul(changed)}<h2>New waters (${added.length})</h2>${ul(added)}` +
      `<h2>Waters no longer listed (${gone.length})</h2><ul class="cols">${gone.map((n) => `<li>${esc(n)}</li>`).join("")}</ul>` +
      (reworded ? `<p>Wording updated on ${reworded} ${reworded === 1 ? "water" : "waters"}: the rules there are the same.</p>` : "") +
      `<div class="legal"><p>This is not a legal document. Check the official synopsis before you fish.</p></div>`;
    const description = clip(`${changed.length} waters with changed rules, ${added.length} new and ${gone.length} removed since release ${prev.release}.`);
    unique(titles, title, path); unique(descriptions, description, path);
    add("changes", path, shellOf(path, title, description, body, pageLd(site, path, title, asOf, [["Home", "/"], ["Changes", path]])));
    }
  }

  // ── gauge pages (T487): one per station serving a water with a page; their live half reads the feed in the browser ──
  const byId = new Map(models.map((m) => [m.id, m]));
  const gauges = o.gauges ? gaugeModels(o.gauges, (item) => { const m = byId.get(item); return m ? { id: m.id, name: m.name, path: waterPath(m) } : null; }) : [];
  if (gauges.length) {
    const client = clientScript(), clientHref = `/assets/gauge.${sha(client, 10)}.js`;
    files.set(clientHref.slice(1), client);
    const gaugeCrumbs = (g: GaugeModel): [string, string][] => [["Home", "/"], ["River gauges", "/gauges"], [g.name, g.path]];
    for (const g of gauges) {
      const title = gaugeTitle(g), description = gaugeDescription(g);
      unique(titles, title, g.path); unique(descriptions, description, g.path);
      // lastmod moves with what the build wrote (name, waters, place), not with the release id the live half names
      add("gauge", g.path, shellOf(g.path, title, description, gaugeBody(g, release.id, clientHref, asOf), pageLd(site, g.path, title, asOf, gaugeCrumbs(g)),
          `/gauge/${g.station}/card.png`), sha(JSON.stringify(g)));
    }
    const title = clip(`River gauges in BC: live flow and weather — ${site.name}`, 60);
    const description = clip(`Live flow, the last 7 days, flow forecasts and the weather at ${gauges.length} BC river gauges on waters with fishing regulations.`);
    unique(titles, title, "/gauges"); unique(descriptions, description, "/gauges");
    add("gauges", "/gauges", shellOf("/gauges", title, description, gaugesIndexBody(gauges, asOf), pageLd(site, "/gauges", title, asOf, [["Home", "/"], ["River gauges", "/gauges"]]), "/share/gauge.png"),
        sha(JSON.stringify(gauges.map((g) => [g.station, g.name, g.path]))));
    files.set("gauge/stations.json", stationsFile(gauges, release.id));
    // (T487) the fixed card a gauge without its own is answered with; build.ts draws it as share/gauge.png
    files.set("share/gauge.svg", siteGaugeCardSvg(site));
  }

  // ── About ──────────────────────────────────────────────────────────────────────────────────────────────────────────
  {
    const title = `About ${site.name}`, description = `${site.name} is a free, non-profit app that says whether a BC freshwater water is open and what you can keep, from the provincial synopsis.`;
    unique(titles, title, "/about"); unique(descriptions, description, "/about");
    add("about", "/about", shellOf("/about", title, description, aboutBody(site, asOf), aboutLd(site, title, asOf)));
  }

  // ── Privacy policy (owner 2026-10-29; Google Play's required URL) ──────────────────────────────────────────────────
  {
    const title = "Privacy policy", description = `What ${site.name} does and does not do with your information: no accounts, no ads, no tracking; location, photos and spots stay on your device.`;
    unique(titles, `${title} · ${site.name}`, "/privacy"); unique(descriptions, description, "/privacy");
    add("privacy", "/privacy", shellOf("/privacy", `${title} · ${site.name}`, description, privacyBody(site, asOf), pageLd(site, "/privacy", `${title} · ${site.name}`, asOf, [["Home", "/"], ["Privacy policy", "/privacy"]])));
  }

  // ── gates over the HTML ────────────────────────────────────────────────────────────────────────────────────────────
  // a water page (the app's pane) carries no breadcrumb: the pane draws none, and the JSON-LD says only what the page shows
  const linked = new Set(pages.filter((p) => p.kind === "region" || p.kind === "letter").flatMap((p) => [...p.html.matchAll(/href="(\/water\/[^"]+)"/g)].map((x) => x[1]!)));
  for (const m of models) if (!linked.has(waterPath(m))) bad.push(`${waterPath(m)} is linked from no region hub`);
  if (bad.length) throw new Error(`seo: ${bad.length} problem(s):\n${bad.slice(0, 20).join("\n")}`);

  // ── lastmod: moves only when a page's content hash moved ────────────────────────────────────────────────────────────
  const lastmod = (path: string, hash: string) => { const w = o.previous?.pages[path]; return w && w.hash === hash ? w.lastmod : asOf; };
  const snapPages: Record<string, { hash: string; lastmod: string }> = { "/": { hash: release.id, lastmod: lastmod("/", release.id) } };
  for (const p of pages) snapPages[p.path] = { hash: p.hash, lastmod: lastmod(p.path, p.hash) };
  const urls = Object.keys(snapPages).sort();
  const chunks: string[][] = [];
  for (let i = 0; i < urls.length; i += 50000) chunks.push(urls.slice(i, i + 50000));
  const xml = (body: string) => `<?xml version="1.0" encoding="UTF-8"?>\n${body}\n`;
  chunks.forEach((c, i) => files.set(`sitemap-${i + 1}.xml`, xml(`<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">${c.map((u) =>
    `<url><loc>${esc(site.origin)}${esc(u)}</loc><lastmod>${snapPages[u]!.lastmod}</lastmod></url>`).join("")}</urlset>`)));
  files.set("sitemap.xml", xml(`<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">${chunks.map((_, i) =>
    `<sitemap><loc>${esc(site.origin)}/sitemap-${i + 1}.xml</loc><lastmod>${asOf}</lastmod></sitemap>`).join("")}</sitemapindex>`));
  files.set("robots.txt", `User-agent: *\nAllow: /\nDisallow: /api/\nDisallow: /data/\n\nSitemap: ${site.origin}/sitemap.xml\n`);
  files.set("404.html", shell({ title: `Page not found — ${site.name}`, description: "That page is not here.", path: "/404", body: notFoundBody(site, regions), cssHref, robots: "noindex" }, site));
  files.set(cssHref.slice(1), CSS);
  files.set("slug-map.json", `${JSON.stringify(slugMap, null, 1)}\n`);
  // (T269) the app's address bar: item id -> slug, in the shards the app reads one at a time (@app/ui `slugShardPath`)
  const shards = Array.from({ length: SLUG_SHARDS }, () => ({} as Record<string, string>));
  for (const m of models) shards[slugShard(m.id)]![m.id] = slugOf(m.id);
  shards.forEach((sh, i) => files.set(`water-slugs/${i}.json`, JSON.stringify(sh)));
  const snapshot: Snapshot = {
    release: release.id, asOf, pages: snapPages,
    waters: Object.fromEntries(models.map((m) => [m.id, { slug: slugOf(m.id), name: m.name, hash: waterHash.get(m.id)!, rules: waterRules.get(m.id)! }])),
  };
  files.set("snapshot.json", `${JSON.stringify(snapshot, null, 1)}\n`);
  return { files, pages, slugSteps, slugMap, removed, snapshot, models, cssHref, regions, gauges };
}
