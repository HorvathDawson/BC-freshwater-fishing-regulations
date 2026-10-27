/**
 * T193-T195 — THE WHOLE SITE FROM ONE RELEASE: water pages, About, region hubs, "open this weekend", "opening and closing
 * dates", "rule changes", sitemap, robots, slug map, share cards and the snapshot the next release is diffed against.
 * Everything is returned as in-memory files (`write.ts` puts them on disk); the pages are built from the release bundle only.
 *
 * Gates that throw (the release fails): a water the app would answer differently (verify.ts), JSON-LD that says what the page
 * does not, a duplicate title or description, a water no hub links to, a page count that differs from the sitemap's.
 */
import { createHash } from "node:crypto";
import type { RegsStore } from "@app/data/regs";
import { aboutBody, CSS, SHARE_GROUND, SHARE_INK, clip, descriptionOf, esc, notFoundBody, regionLabel, shell, titleOf, waterBody, type Release, type Site } from "./html";
import { aboutLd, checkJsonLd, pageLd, waterLd } from "./jsonld";
import { appPage, buildDayOf, listWaterOrds, statusOn, waterModel, type BuildDay, type WaterModel, type WaterStatus } from "./model";
import { assignSlugs, type SlugMap } from "./slug";
import { verifyAgainstApp } from "./verify";

export interface PageRec { readonly path: string; readonly kind: "water" | "region" | "weekend" | "calendar" | "changes" | "about"; readonly html: string; readonly hash: string }
export interface Snapshot {
  readonly release: string; readonly asOf: string;
  readonly pages: Readonly<Record<string, { readonly hash: string; readonly lastmod: string }>>;
  readonly waters: Readonly<Record<string, { readonly slug: string; readonly name: string; readonly hash: string }>>;
}
export interface BuildOpts {
  readonly store: RegsStore; readonly site: Site; readonly release: Release; readonly on: BuildDay;
  readonly slugMap: SlugMap; readonly previous: Snapshot | null;
}
export interface SiteBuild {
  readonly files: ReadonlyMap<string, string>; readonly pages: readonly PageRec[];
  readonly slugMap: SlugMap; readonly removed: readonly { id: string; slug: string; region: number }[];
  readonly snapshot: Snapshot; readonly models: readonly WaterModel[]; readonly cssHref: string; readonly regions: readonly number[];
}

const sha = (s: string, n = 16) => createHash("sha256").update(s).digest("hex").slice(0, n);
const outFile = (path: string) => `${path.slice(1)}/index.html`;
const WORD: Record<WaterStatus, string> = { open: "open today", closed: "closed today", mixed: "parts differ", tidal: "tidal water", outside: "outside BC", unknown: "check the page" };
const dayOfDate = (d: Date): BuildDay => buildDayOf(d);

/** the days of "this weekend" from `on`: Mon-Fri the coming Sat+Sun, Sat today+Sun, Sun today */
export function weekendDays(on: BuildDay): BuildDay[] {
  const d = new Date(`${on.iso}T12:00:00`), dow = d.getDay();            // 0 Sun .. 6 Sat
  const first = new Date(d);
  if (dow >= 1 && dow <= 5) first.setDate(d.getDate() + (6 - dow));
  const days = [first];
  if (first.getDay() === 6) { const sun = new Date(first); sun.setDate(first.getDate() + 1); days.push(sun); }
  return days.map(dayOfDate);
}

/** stable under a changed date: the "as of" times are what a rebuild on another day moves */
const stable = (html: string) => html.replace(/<time [^>]*>[^<]*<\/time>/g, "<time/>");

function shareSvg(m: WaterModel, site: Site): string {
  const words = m.name.split(" "), lines: string[] = [""];
  for (const w of words) { if ((lines.at(-1) + " " + w).trim().length > 26 && lines.at(-1)) lines.push(w); else lines[lines.length - 1] = (lines.at(-1) + " " + w).trim(); }
  const t = lines.slice(0, 3).map((l, i) => `<text x="64" y="${190 + i * 84}" font-size="72" font-weight="700">${esc(l)}</text>`).join("");
  return `<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="630" viewBox="0 0 1200 630" font-family="system-ui,sans-serif">` +
    `<rect width="1200" height="630" fill="${SHARE_GROUND}"/><g fill="${SHARE_INK}">${t}` +
    `<text x="64" y="470" font-size="40">${esc(clip(m.answer, 70))}</text>` +
    `<text x="64" y="560" font-size="34" opacity=".85">${esc(site.name)} · ${esc(regionLabel(m.region))} · ${esc(site.origin.replace("https://", ""))}</text></g></svg>`;
}

export async function buildSite(o: BuildOpts): Promise<SiteBuild> {
  const { store, site, release, on } = o;
  const asOf = on.iso;
  // ── the waters ─────────────────────────────────────────────────────────────────────────────────────────────────────
  const models: WaterModel[] = [];
  for (const ord of await listWaterOrds(store)) {
    const m = await waterModel(store, ord, on);
    if (m && m.region >= 1) models.push(m);
  }
  const bad: string[] = [];
  for (const m of models) {
    const pages = await Promise.all(m.parts.map((p) => appPage(store, m.id, p.partIx, on)));
    bad.push(...verifyAgainstApp(m, pages));
  }
  if (bad.length) throw new Error(`seo: ${bad.length} page(s) differ from the app:\n${bad.slice(0, 20).join("\n")}`);

  const { map: slugMap, removed } = assignSlugs(models.map((m) => ({ id: m.id, name: m.name, region: m.region })), o.slugMap);
  const slugOf = (id: string) => slugMap.waters[id]!.slug;
  const byName = (a: WaterModel, b: WaterModel) => a.name.localeCompare(b.name) || a.id.localeCompare(b.id);
  const regions = [...new Set(models.flatMap((m) => m.regions))].sort((a, b) => a - b);
  const inRegion = (r: number) => models.filter((m) => m.regions.includes(r)).sort(byName);
  const cssHref = `/assets/site.${sha(CSS, 10)}.css`;
  const known = new Map(models.map((m) => [m.id, m]));
  const regionPath = (r: number) => `/region/${r}`;
  const waterPath = (m: WaterModel) => `/water/${slugOf(m.id)}`;

  const pages: PageRec[] = [];
  const files = new Map<string, string>();
  const add = (kind: PageRec["kind"], path: string, html: string, hash = sha(stable(html))) => {
    pages.push({ path, kind, html, hash });
    files.set(outFile(path), html);
  };
  const shellOf = (path: string, title: string, description: string, body: string, ld: string, image?: string) =>
    shell({ title, description, path, body: body + regionFooter(), jsonld: ld, cssHref, ...(image ? { image } : {}) }, site);
  const regionFooter = () => regions.length
    ? `<nav class="small" aria-label="Regions"><p>Browse by region: ${regions.map((r) => `<a href="${regionPath(r)}">${esc(regionLabel(r))}</a>`).join(" · ")}</p></nav>` : "";

  // ── water pages ────────────────────────────────────────────────────────────────────────────────────────────────────
  const titles = new Map<string, string>(), descriptions = new Map<string, string>();
  const unique = (what: Map<string, string>, v: string, path: string) => {
    const was = what.get(v);
    if (was) bad.push(`${path} and ${was} share a ${what === titles ? "title" : "description"}: ${v}`);
    what.set(v, path);
  };
  const waterHash = new Map<string, string>();
  const nameCount = new Map<string, number>(), sameRegion = new Map<string, number>();
  for (const m of models) {
    nameCount.set(m.name, (nameCount.get(m.name) ?? 0) + 1);
    sameRegion.set(`${m.name}|${m.region}`, (sameRegion.get(`${m.name}|${m.region}`) ?? 0) + 1);
  }
  for (const m of models) {
    const path = waterPath(m), siblings = inRegion(m.region), at = siblings.indexOf(m);
    const near = [...siblings.slice(Math.max(0, at - 3), at), ...siblings.slice(at + 1, at + 4)];
    // two waters with one name keep distinct titles: the region, then the slug
    const label = (nameCount.get(m.name) ?? 0) === 1 ? m.name
      : (sameRegion.get(`${m.name}|${m.region}`) ?? 0) === 1 ? `${m.name} (${regionLabel(m.region)})` : `${m.name} (${regionLabel(m.region)}, ${slugOf(m.id)})`;
    const title = titleOf(label, site), description = descriptionOf(m, label);
    unique(titles, title, path); unique(descriptions, description, path);
    const hash = sha(JSON.stringify(m.parts.map((p) => [p.label, p.sections.filter((s) => s.title !== "Why it is closed").map((s) => [s.title, s.items.filter((i) => !i.dated)])])));
    waterHash.set(m.id, hash);
    const how = { "flows-into": "Flows into", "continues-as": "Continues as", "continues-from": "Continues from" } as const;
    const body = waterBody(m, site, release, asOf, {
      region: regionPath(m.region),
      neighbours: m.neighbours.flatMap((n) => { const t = known.get(n.id); return t ? [{ href: waterPath(t), text: t.name, how: how[n.how] }] : []; }),
      near: near.map((n) => ({ href: waterPath(n), text: n.name })),
    });
    add("water", path, shellOf(path, title, description, body, waterLd(m, site, path, title, asOf, regionPath(m.region)), `/share/${slugOf(m.id)}.svg`), hash);
    files.set(`share/${slugOf(m.id)}.svg`, shareSvg(m, site));
  }

  // ── region hubs (T195) ─────────────────────────────────────────────────────────────────────────────────────────────
  const days = weekendDays(on);
  for (const r of regions) {
    const ws = inRegion(r), path = regionPath(r), name = regionLabel(r);
    const list = (xs: readonly WaterModel[], word?: (m: WaterModel) => string) =>
      `<ul class="cols">${xs.map((m) => `<li><a href="${waterPath(m)}">${esc(m.name)}</a>${word ? ` <span class="tag">${esc(word(m))}</span>` : ""}</li>`).join("")}</ul>`;
    const title = clip(`${name} fishing regulations (BC) — ${site.name}`, 60);
    const body = `<nav class="crumbs"><a href="/">Home</a> › ${esc(name)}</nav><h1>${esc(name)} fishing regulations</h1>` +
      `<p>${ws.length} waters in ${esc(name)} with the full rules in plain words, from release ${esc(release.id)}.</p>` +
      `<ul><li><a href="${path}/open-this-weekend">What is open this weekend in ${esc(name)}</a></li>` +
      `<li><a href="${path}/season-changes">Waters opening and closing soon in ${esc(name)}</a></li></ul>` +
      `<h2>All waters in ${esc(name)}</h2>${list(ws, (m) => WORD[m.status])}` +
      `<div class="legal"><p>Open or closed is as of <time datetime="${asOf}">${asOf}</time>. This is not a legal document. Check the official synopsis before you fish.</p></div>`;
    const description = clip(`Full BC freshwater fishing regulations for ${ws.length} waters in ${name}: what is open today and what you can keep, in plain words.`);
    unique(titles, title, path); unique(descriptions, description, path);
    add("region", path, shellOf(path, title, description, body, pageLd(site, path, title, asOf, [["Home", "/"], [name, path]])));

    // open this weekend: today's status is the model's; the other days come from the app's presenter for that day
    const status = new Map<string, WaterStatus[]>();
    for (const m of ws) {
      const row: WaterStatus[] = [];
      for (const d of days) row.push(d.iso === on.iso ? m.status : (await statusOn(store, m.ord, d, on.day)) ?? "unknown");
      status.set(m.id, row);
    }
    const all = (m: WaterModel, s: WaterStatus) => status.get(m.id)!.every((x) => x === s);
    const openAll = ws.filter((m) => all(m, "open")), closedAll = ws.filter((m) => all(m, "closed"));
    const rest = ws.filter((m) => !openAll.includes(m) && !closedAll.includes(m));
    const wk = days.map((d) => d.iso).join(" and ");
    const wpath = `${path}/open-this-weekend`, wtitle = clip(`${name}: what is open this weekend — ${site.name}`, 60);
    const wbody = `<nav class="crumbs"><a href="/">Home</a> › <a href="${path}">${esc(name)}</a> › This weekend</nav>` +
      `<h1>${esc(name)}: what is open this weekend</h1><p class="asof">Weekend of ${days.map((d) => `<time datetime="${d.iso}">${d.iso}</time>`).join(" and ")}, as of <time datetime="${asOf}">${asOf}</time>, from release ${esc(release.id)}.</p>` +
      `<h2>Open all weekend (${openAll.length})</h2>${list(openAll)}<h2>Closed all weekend (${closedAll.length})</h2>${list(closedAll)}` +
      `<h2>Different parts or days (${rest.length})</h2>${list(rest, (m) => status.get(m.id)!.map((s) => WORD[s]).join(", then "))}` +
      `<div class="legal"><p>This is not a legal document. Check the official synopsis before you fish.</p></div>`;
    const wdesc = clip(`${name} on ${wk}: ${openAll.length} waters open all weekend, ${closedAll.length} closed, ${rest.length} that differ by part or day.`);
    unique(titles, wtitle, wpath); unique(descriptions, wdesc, wpath);
    add("weekend", wpath, shellOf(wpath, wtitle, wdesc, wbody, pageLd(site, wpath, wtitle, asOf, [["Home", "/"], [name, path], ["This weekend", wpath]])));

    // opening and closing dates in the next 60 days (the strip's year runs)
    const turns = ws.flatMap((m) => m.parts.flatMap((p) => p.turns.map((t) => ({ m, p, t })))).sort((a, b) => a.t.inDays - b.t.inDays || a.m.name.localeCompare(b.m.name));
    const cpath = `${path}/season-changes`, ctitle = clip(`${name}: waters opening and closing — ${site.name}`, 60);
    const cbody = `<nav class="crumbs"><a href="/">Home</a> › <a href="${path}">${esc(name)}</a> › Opening and closing</nav>` +
      `<h1>${esc(name)}: waters opening and closing</h1><p class="asof">The next 60 days from <time datetime="${asOf}">${asOf}</time>, from release ${esc(release.id)}.</p>` +
      (turns.length ? `<ul>${turns.map(({ m, p, t }) => `<li><strong>${esc(t.on)}</strong> ${t.to === "open" ? "opens" : "closes"}: <a href="${waterPath(m)}">${esc(m.name)}</a>${m.parts.length > 1 ? ` (${esc(p.label)})` : ""}</li>`).join("")}</ul>`
        : `<p>No water in ${esc(name)} opens or closes in the next 60 days.</p>`) +
      `<div class="legal"><p>This is not a legal document. Check the official synopsis before you fish.</p></div>`;
    const cdesc = clip(`${turns.length} openings and closings in ${name} over the next 60 days, by date, each linking to the water's full rules.`);
    unique(titles, ctitle, cpath); unique(descriptions, cdesc, cpath);
    add("calendar", cpath, shellOf(cpath, ctitle, cdesc, cbody, pageLd(site, cpath, ctitle, asOf, [["Home", "/"], [name, path], ["Opening and closing", cpath]])));
  }

  // ── what changed since the last release (only with a previous snapshot to compare) ────────────────────────────────────
  const prev = o.previous;
  if (prev && prev.release !== release.id) {
    const was = prev.waters, now = new Map(models.map((m) => [m.id, m]));
    const added = models.filter((m) => !(m.id in was)), changed = models.filter((m) => m.id in was && was[m.id]!.hash !== waterHash.get(m.id));
    const gone = Object.entries(was).filter(([id]) => !now.has(id)).map(([, w]) => w.name).sort();
    const ul = (xs: readonly WaterModel[]) => `<ul class="cols">${xs.map((m) => `<li><a href="${waterPath(m)}">${esc(m.name)}</a></li>`).join("")}</ul>`;
    const path = "/changes", title = clip(`BC fishing regulation changes this release — ${site.name}`, 60);
    const body = `<nav class="crumbs"><a href="/">Home</a> › Changes</nav><h1>Fishing regulation changes in this release</h1>` +
      `<p class="asof">As of <time datetime="${asOf}">${asOf}</time>: release ${esc(release.id)} compared with release ${esc(prev.release)} (<time datetime="${prev.asOf}">${prev.asOf}</time>).</p>` +
      `<h2>Rules changed (${changed.length})</h2>${ul(changed)}<h2>New waters (${added.length})</h2>${ul(added)}` +
      `<h2>Waters no longer listed (${gone.length})</h2><ul class="cols">${gone.map((n) => `<li>${esc(n)}</li>`).join("")}</ul>` +
      `<div class="legal"><p>This is not a legal document. Check the official synopsis before you fish.</p></div>`;
    const description = clip(`${changed.length} waters with changed rules, ${added.length} new and ${gone.length} removed since release ${prev.release}.`);
    unique(titles, title, path); unique(descriptions, description, path);
    add("changes", path, shellOf(path, title, description, body, pageLd(site, path, title, asOf, [["Home", "/"], ["Changes", path]])));
  }

  // ── About ──────────────────────────────────────────────────────────────────────────────────────────────────────────
  {
    const title = `About ${site.name}`, description = `${site.name} is a free, non-profit app that says whether a BC freshwater water is open and what you can keep, from the provincial synopsis.`;
    unique(titles, title, "/about"); unique(descriptions, description, "/about");
    add("about", "/about", shellOf("/about", title, description, aboutBody(site, asOf), aboutLd(site, title, asOf)));
  }

  // ── gates over the HTML ────────────────────────────────────────────────────────────────────────────────────────────
  for (const p of pages) bad.push(...checkJsonLd(p.html, site.origin).map((x) => `${p.path}: ${x}`));
  const linked = new Set(pages.filter((p) => p.kind === "region").flatMap((p) => [...p.html.matchAll(/href="(\/water\/[^"]+)"/g)].map((x) => x[1]!)));
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
  const snapshot: Snapshot = {
    release: release.id, asOf, pages: snapPages,
    waters: Object.fromEntries(models.map((m) => [m.id, { slug: slugOf(m.id), name: m.name, hash: waterHash.get(m.id)! }])),
  };
  files.set("snapshot.json", `${JSON.stringify(snapshot, null, 1)}\n`);
  return { files, pages, slugMap, removed, snapshot, models, cssHref, regions };
}
