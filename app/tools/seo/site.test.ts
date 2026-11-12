/**
 * T193-T198 on the committed regs fixture release (43 waters on 2026-10-27), written to a gitignored dist folder: the water
 * pages (T269: the app's own regs pane, prerendered) and their verdicts, the sitemap / canonical / JSON-LD gates, the hubs,
 * the performance budget and the hosting config. Mutation-pinned where a gate could be vacuous: each planted fault must be
 * reported by the gate that owns it.
 */
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { beforeAll, describe, expect, it } from "vitest";
import { buildRegsPage, type RegsPage } from "@app/core";
import { loadRegsInputs, openRegsStore } from "@app/data/regs";
import { openRegsFixture } from "@app/data/release/node";
import { BUDGET, blockingScripts, checkBudget, measure, pageSize } from "./budget";
import { buildAll, readConfig, writeSite, type Built } from "./build";
import { pngSize } from "./share";
import { SHARE_H, SHARE_W } from "./html";
import { TODAY } from "./fixture";
import { headersFile, hostWorker, HSTS, redirectFor, redirectsFile } from "./hosting";
import { checkJsonLd } from "./jsonld";
import { buildDayOf } from "./model";
import { SHARE_LINE } from "./site";

const APP = join(dirname(fileURLToPath(import.meta.url)), "../..");
const OUT = join(APP, "dist/seo-test");
const site = readConfig();
const text = (html: string) => html.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ");
let built: Built;
beforeAll(async () => { built = await buildAll({ loader: openRegsFixture(), on: TODAY }); }, 180_000);
const cssPath = () => built.build.cssHref.slice(1);
const waters = () => built.build.pages.filter((p) => p.kind === "water");
const hubs = () => built.build.pages.filter((p) => p.kind !== "water");
/** what a water page draws: its prerendered pane, without the head */
const pane = (html: string) => html.slice(html.indexOf('<div id="prerender">'), html.indexOf('<div id="root">'));

describe("T193 + T269 water pages (the app's own pane) and About", () => {
  it("one page per water with rules; a hub page ships no script, a water page only the app's (deferred) and the hand-over (a module)", () => {
    expect(waters().length).toBe(built.build.models.length);
    expect(waters().length).toBeGreaterThan(20);
    for (const p of built.build.pages) expect(blockingScripts(p.html), p.path).toEqual([]);
    for (const p of hubs()) {
      expect(p.html, p.path).not.toMatch(/<script(?![^>]*ld\+json)/);
      expect(text(p.html), p.path).toMatch(/not a legal document/i);
    }
    for (const p of waters()) {
      expect([...p.html.matchAll(/<script(?![^>]*ld\+json)([^>]*)>/g)].map((m) => m[1]), p.path).toEqual([' type="module"']);
      expect(text(pane(p.html)), p.path).toMatch(/Not legal advice/);   // the pane's own line, as the app draws it
    }
  });

  it("(T269) a water page IS the app's pane: the app's document, the Shell's regs pane in it, no text-only body", () => {
    for (const p of waters()) {
      const m = built.build.models.find((x) => p.path === `/water/${built.build.slugMap.waters[x.id]!.slug}`)!;
      expect(p.html, p.path).toContain(`<meta name="cift:item" content="${m.id}">`);
      // (T304) and its bundle region, the one the app reads the page's frames from: one of the water's own regions
      expect(m.focus, p.path).toMatch(/^(?:[1-8]|7a|7b)$/);
      expect(m.regions, p.path).toContain(parseInt(m.focus!, 10));
      expect(p.html, p.path).toContain(`<meta name="cift:region" content="${m.focus}">`);
      for (const id of ["water-frame", "regs-header", "regs-scroll", "keep-tab"]) expect(pane(p.html), `${p.path} ${id}`).toContain(`data-testid="${id}"`);
      expect(pane(p.html), p.path).toContain(m.name);
      // the retired text page (T193's html.ts body) is nowhere: no <main>, no answer paragraph, no "Open in the app"
      expect(p.html, p.path).not.toMatch(/<main>|class="answer|class="app"/);
      expect(p.html.indexOf('<div id="prerender">'), p.path).toBeLessThan(p.html.indexOf('<div id="root"></div>'));
    }
  });

  it("(owner 2026-10-29) the pane's ‹ and × are links to the map at that water before the app runs", () => {
    for (const p of waters()) {
      const id = /<meta name="cift:item" content="([^"]+)">/.exec(p.html)![1]!;
      const links = [...pane(p.html).matchAll(/<a href="([^"]+)"[^>]*data-testid="(water-back|water-close)"/g)].map((m) => [m[2], m[1]]);
      expect(new Set(links.map(([t]) => t)), p.path).toEqual(new Set(["water-back", "water-close"]));
      for (const [, href] of links) expect(href, p.path).toBe(`/?show=${id}`);
      expect(pane(p.html), p.path).not.toMatch(/role="button"[^>]*data-testid="water-(back|close)"/);
    }
  });

  it("each page's verdict is the app's answer for that water (and a planted wrong verdict is caught)", async () => {
    const store = await openRegsStore(openRegsFixture());
    // the pane's own status word: the strip's `st:<status>` (one part: the page opens on it)
    const shown = (html: string) => [...new Set([...pane(html).matchAll(/data-testid="st:(open|closed)"/g)].map((m) => m[1]))][0] ?? "";
    const app = async (id: string, partIx: number): Promise<RegsPage> => buildRegsPage(
      await loadRegsInputs(store, { kind: "part", item: id, partIx }, TODAY.day, TODAY.weekday, TODAY.year),
      { today: TODAY.day, who: { age: "adult", residency: "resident" } });
    const mismatches = (html: string, status: string) => (shown(html) === status ? [] : [`${shown(html)} vs ${status}`]);
    let checked = 0;
    // the build renders today, the fixture's verdicts are for its own day: compare on today
    const now = buildDayOf(new Date());
    for (const m of built.build.models.filter((x) => x.parts.length === 1)) {
      const page = waters().find((p) => p.path === `/water/${built.build.slugMap.waters[m.id]!.slug}`)!;
      const s = (buildRegsPage(await loadRegsInputs(store, { kind: "part", item: m.id, partIx: 0 }, now.day, now.weekday, now.year),
        { today: now.day, who: { age: "adult", residency: "resident" } })).status;
      if (s !== "open" && s !== "closed") continue;
      expect(mismatches(page.html, s), m.id).toEqual([]);
      checked++;
    }
    expect(checked).toBeGreaterThan(10);
    const vedder = waters().find((p) => p.path === "/water/vedder-river")!;
    const flipped = shown(vedder.html) === "open" ? "closed" : "open";
    expect(mismatches(vedder.html.replaceAll(`data-testid="st:${shown(vedder.html)}"`, `data-testid="st:${flipped}"`), shown(vedder.html))).not.toEqual([]);
    expect(await app("gnis:8634", 0)).toBeTruthy();
  }, 120_000);

  it("About names the sources and the contact only when there is one", () => {
    const about = built.build.pages.find((p) => p.path === "/about")!;
    expect(text(about.html)).toMatch(/free, non-profit/);
    expect(text(about.html)).toMatch(/Environment and Climate Change Canada/);
    expect(about.html).not.toMatch(/mailto:/);                 // site.json contact is null until the owner sets it
  });

  it("the privacy policy is a page, linked from every footer, and says what leaves the device", () => {
    const pv = built.build.pages.find((p) => p.path === "/privacy")!;
    expect(text(pv.html)).toMatch(/Open-Meteo/);
    expect(text(pv.html)).toMatch(/salted one-way hash/);
    expect(pv.html).toMatch(/href="\/privacy"/);
    // every hub page's footer (a water page is the app's pane: the app's More menu links it)
    expect(hubs().every((p) => p.html.includes('<a href="/privacy">Privacy policy</a>'))).toBe(true);
  });

  it("pages refresh with each release: a new release id and date are in the page", async () => {
    const next = await buildAll({ loader: openRegsFixture(), on: buildDayOf(new Date(2026, 10, 3, 12)) });
    const a = built.build.pages.find((p) => p.kind === "region")!, b = next.build.pages.find((p) => p.path === a.path)!;
    expect(text(b.html)).toContain("2026-11-03");
    expect(text(a.html)).toContain("2026-10-27");
  }, 120_000);
});

describe("T194 sitemap, robots, canonicals, structured data", () => {
  it("the sitemap lists exactly the built pages (and the app's own root), robots points to it", () => {
    const sm = built.files.get("sitemap-1.xml")!;
    const locs = [...sm.matchAll(/<loc>([^<]+)<\/loc>/g)].map((m) => m[1]!);
    expect(locs.length).toBe(built.build.pages.length + 1);
    expect(new Set(locs)).toEqual(new Set([`${site.origin}/`, ...built.build.pages.map((p) => `${site.origin}${p.path}`)]));
    expect(built.files.get("sitemap.xml")).toContain(`${site.origin}/sitemap-1.xml`);
    expect(built.files.get("robots.txt")).toContain(`Sitemap: ${site.origin}/sitemap.xml`);
    expect(built.files.get("robots.txt")).toMatch(/Disallow: \/api\//);
  });

  it("every canonical is the page's own https canifishthis.ca URL; titles <= 60, descriptions <= 155, all unique", () => {
    const titles = new Set<string>(), descs = new Set<string>();
    for (const p of built.build.pages) {
      expect(/<link rel="canonical" href="([^"]+)">/.exec(p.html)![1], p.path).toBe(`https://canifishthis.ca${p.path}`);
      const title = /<title>([^<]*)<\/title>/.exec(p.html)![1]!, desc = /<meta name="description" content="([^"]*)">/.exec(p.html)![1]!;
      expect(title.length, title).toBeLessThanOrEqual(60);
      expect(desc.length, desc).toBeLessThanOrEqual(155);
      expect(titles.has(title), `dup title ${title}`).toBe(false); titles.add(title);
      expect(descs.has(desc), `dup description ${desc}`).toBe(false); descs.add(desc);
    }
    expect(waters().find((p) => p.path === "/water/vedder-river")!.html).toContain("<title>Vedder River fishing regulations, B.C. — Can I Fish This?</title>");
    // the target query is in the title and the description, with where the water is; the pane names the water
    const v = waters().find((p) => p.path === "/water/vedder-river")!.html;
    expect(/<meta name="description" content="([^"]*)">/.exec(v)![1]).toMatch(/^Vedder River fishing regulations, B\.C\. \((near [^,]+, )?Region \d\): /);
    expect(v).toContain('<html lang="en-CA"');
  });

  it("the JSON-LD parses, has its required fields, and every fact in it is on the page; a planted fact is caught", () => {
    for (const p of built.build.pages) expect(checkJsonLd(p.html, site.origin, p.kind === "water" ? ["WebPage", "Place"] : undefined), p.path).toEqual([]);
    const v = waters().find((p) => p.path === "/water/vedder-river")!.html;
    // (T269) the pane draws no breadcrumb, date or FAQ: the water page's JSON-LD is the page and the water, nothing more
    expect(v).toMatch(/"@type":\["Place","RiverBodyOfWater"\]/);
    expect(v).not.toMatch(/BreadcrumbList|FAQPage|GeoCoordinates/);
    const planted = v.replace('"name":"Vedder River"', '"name":"Vedder River","slogan":"The best fishing in BC"');
    expect(checkJsonLd(planted, site.origin, ["WebPage", "Place"]).join("|")).toContain("The best fishing in BC");
    expect(checkJsonLd(v.replace(/<script type="application\/ld\+json">/, '<script type="application/ld+json">{'), site.origin)[0]).toMatch(/does not parse/);
    const hub = built.build.pages.find((p) => p.kind === "region")!.html;
    expect(checkJsonLd(hub.replace('"dateModified"', '"dateModifiedX"'), site.origin).join("|")).toMatch(/lacks dateModified/);
    expect(checkJsonLd(v, site.origin).join("|")).toMatch(/missing WebSite/);   // a hub's needs are not a water page's
  });

  it("lastmod moves only when a page's content hash moved", async () => {
    const later = await buildAll({ loader: openRegsFixture(), on: buildDayOf(new Date(2026, 10, 3, 12)), previous: built.build.snapshot });
    const mod = (b: Built, path: string) => b.build.snapshot.pages[path]!.lastmod;
    const hashOf = (b: Built, path: string) => b.build.snapshot.pages[path]!.hash;
    const same = waters().filter((p) => hashOf(later, p.path) === hashOf(built, p.path)), moved = waters().filter((p) => hashOf(later, p.path) !== hashOf(built, p.path));
    expect(same.length).toBeGreaterThan(5);
    for (const p of same) expect(mod(later, p.path), p.path).toBe("2026-10-27");   // same rules a week on: lastmod stays
    for (const p of moved) expect(mod(later, p.path), p.path).toBe("2026-11-03");  // a season turned: lastmod is the build day
    const a = same[0]!.path;
    const edited = { ...built.build.snapshot, pages: { ...built.build.snapshot.pages, [a]: { hash: "old", lastmod: "2026-01-01" } } };
    const changed = await buildAll({ loader: openRegsFixture(), on: buildDayOf(new Date(2026, 10, 3, 12)), previous: edited });
    expect(mod(changed, a)).toBe("2026-11-03");                 // the hash moved: lastmod is the build day
  }, 180_000);

  it("a slug, once committed, is the URL: a committed map is honoured and a removed water is reported for its 301", async () => {
    const slugMap = { version: 1 as const, waters: { ...built.build.slugMap.waters, "gnis:99999999": { slug: "long-gone-creek", region: 3 } } };
    const again = await buildAll({ loader: openRegsFixture(), on: TODAY, slugMap });
    expect(again.build.removed).toEqual([{ id: "gnis:99999999", slug: "long-gone-creek", region: 3 }]);
    expect(again.files.get("_redirects")!.split("\n")[0]).toBe("/water/long-gone-creek /region/3 301");   // then the removed list pages
    expect(again.build.pages.map((p) => p.path)).toEqual(built.build.pages.map((p) => p.path));
  }, 120_000);
});

describe("specific-regulation pages (topics)", () => {
  it("a topic hub lists each water with one of its own sentences and links its water page", () => {
    const topics = built.build.pages.filter((p) => p.kind === "topic");
    expect(topics.length).toBeGreaterThan(3);
    const paths = new Set(waters().map((p) => p.path));
    for (const t of topics) {
      expect(t.html).toMatch(/<h1>/);
      for (const href of [...t.html.matchAll(/href="(\/water\/[^"#]+)/g)].map((x) => x[1]!)) expect(paths.has(href), `${t.path} -> ${href}`).toBe(true);
      expect(t.html, t.path).not.toMatch(/href="\/water\/[^"]*#/);   // the text page's anchors are gone with it (T269)
    }
  });
});

describe("T195 hubs and dated pages", () => {
  it("a big region's hub lists letters, not every water; the A-Z pages hold them all, each water is listed on a hub (small region) or a letter page", async () => {
    const split = await buildAll({ loader: openRegsFixture(), on: TODAY, hubSplitAt: 5 });
    const hubs = split.build.pages.filter((p) => p.kind === "region"), letters = split.build.pages.filter((p) => p.kind === "letter");
    expect(letters.length).toBeGreaterThan(hubs.length);
    for (const h of hubs.filter((p) => (p.html.match(/waters-/g) ?? []).length > 0)) expect(h.html).not.toMatch(/href="\/water\//);
    for (const m of split.build.models) {
      const path = `/water/${split.build.slugMap.waters[m.id]!.slug}`;
      expect([...letters, ...hubs].some((l) => l.html.includes(`href="${path}"`)), path).toBe(true);
    }
    expect(split.problems).toEqual([]);
    const listed = [...split.files.get("sitemap-1.xml")!.matchAll(/<loc>/g)].length - 1;
    expect(listed).toBe(split.build.pages.length);
  }, 120_000);

  it("every water is one click from its region hub and every hub is one click from the footer of any page", () => {
    const regionPages = built.build.pages.filter((p) => p.kind === "region");
    expect(regionPages.length).toBeGreaterThan(2);
    for (const m of built.build.models) {
      const path = `/water/${built.build.slugMap.waters[m.id]!.slug}`;
      expect(regionPages.some((r) => r.html.includes(`href="${path}"`)), path).toBe(true);
    }
    for (const r of regionPages) expect(built.build.pages.find((p) => p.kind === "about")!.html).toContain(`href="${r.path}"`);
  });

  it("(T605, owner 2026-11-10) the list pages carry names and links, never an open / closed word; the weekend and opening/closing pages are gone", () => {
    const lists = built.build.pages.filter((p) => p.kind === "region" || p.kind === "letter" || p.kind === "topic");
    expect(lists.length).toBeGreaterThan(10);
    // a status word on a list: the old per-water tags, or the page's own "today" / "this weekend" / "as of"
    const DAY_WORDS = /\b(open today|closed today|today|tomorrow|yesterday|this weekend|parts differ|tidal water|check the page|outside BC)\b|Open or closed is as of/i;
    for (const p of lists) {
      expect(text(p.html), p.path).not.toMatch(DAY_WORDS);
      expect(p.html, p.path).not.toContain('class="tag"');
      expect(p.description, p.path).not.toMatch(DAY_WORDS);
    }
    // the removed page types are in no page, no link, no sitemap
    for (const p of built.build.pages) {
      expect(["weekend", "calendar"], p.path).not.toContain(p.kind);
      expect(p.path, p.path).not.toMatch(/\/(open-this-weekend|season-changes)$/);
      expect(p.html, p.path).not.toMatch(/open-this-weekend|season-changes/);
    }
    expect(built.files.get("sitemap-1.xml")!).not.toMatch(/open-this-weekend|season-changes/);
    // their addresses 301 to the region page: the static `_redirects`, and the host worker answers as redirectFor does
    const regions = built.build.regions;
    expect(regions.length).toBeGreaterThan(2);
    const rd = built.files.get("_redirects")!;
    for (const r of regions) for (const pg of ["open-this-weekend", "season-changes"]) expect(rd, `${r} ${pg}`).toContain(`/region/${r}/${pg} /region/${r} 301\n`);
    expect(redirectFor(`https://canifishthis.ca/region/${regions[0]}/season-changes`, site, [], regions)).toEqual({ status: 301, to: `https://canifishthis.ca/region/${regions[0]}` });
    expect(redirectFor(`https://canifishthis.ca/region/${regions[0]}`, site, [], regions)).toBeNull();
    // every region page links to the live app's map, which opens on the current day: no `date` in the link
    for (const r of built.build.pages.filter((p) => p.kind === "region")) {
      const href = /<a class="app" href="([^"]+)">Open the map for/.exec(r.html)?.[1];
      expect(href, r.path).toBe(`${site.origin}/`);
      expect(href, r.path).not.toMatch(/date=/);
      expect(text(r.html), r.path).toMatch(/built 2026-10-27/);
    }
  });

  it("(T622 G2) a water's description and share picture carry no day-relative word: the build day's answer is not published", async () => {
    const DAY_WORDS = /\b(open|closed)\b.*\btoday\b|\btoday\b|\bnow\b|\bthis weekend\b/i;
    for (const p of waters()) {
      expect(p.description, p.path).not.toMatch(DAY_WORDS);
      expect(p.description, p.path).not.toMatch(/\b(Open|Closed) to fishing/);
      expect(/<meta property="og:description" content="([^"]*)">/.exec(p.html)![1], p.path).toBe(p.description);
    }
    // the share picture's text is built from the same model, built on any day
    for (const [name, svg] of [...built.build.files].filter(([n]) => /^share\/.+\.svg$/.test(n) && n !== "share/gauge.svg")) {
      expect(text(svg), name).not.toMatch(DAY_WORDS);
      expect(text(svg), name).toContain(SHARE_LINE);
    }
    // and a build on a closed day and on an open day publish the same description
    const later = await buildAll({ loader: openRegsFixture(), on: buildDayOf(new Date(2026, 6, 15, 12)) });
    const was = new Map(waters().map((p) => [p.path, p.description]));
    expect(later.build.pages.filter((p) => p.kind === "water").every((p) => was.get(p.path) === p.description)).toBe(true);
    // (T622 G23) the region in it is the water's own code, as the app names it ("Region 7A"); (T672 W4, owner
    // 2026-11-12) a water in several regions names every hub number instead ("Regions 5, 6, 7": the West Road)
    let several = 0;
    for (const m of built.build.models.filter((x) => x.focus && /[ab]$/.test(x.focus))) {
      const p = waters().find((w) => w.path === `/water/${built.build.slugMap.waters[m.id]!.slug}`)!;
      const hubs = [...new Set(m.regions)].sort((a, b) => a - b);
      if (hubs.length > 1) several += 1;
      expect(p.description, m.id).toContain(hubs.length > 1 ? `B.C. (Regions ${hubs.join(", ")}):` : `B.C. (Region ${m.focus!.toUpperCase()}):`);
    }
    expect(several, "the fixture holds a lettered-region water in several regions (the case T672 W4 decided)").toBeGreaterThan(0);
  }, 120_000);

  it("the changes page appears only against a previous release, and lists changed (rules), new and removed waters", async () => {
    expect(built.build.pages.some((p) => p.kind === "changes")).toBe(false);
    const ids = Object.keys(built.build.snapshot.waters);
    const prev = {
      ...built.build.snapshot, release: "r-000000000000", asOf: "2026-01-01",
      waters: { ...built.build.snapshot.waters, [ids[0]!]: { ...built.build.snapshot.waters[ids[0]!]!, hash: "old", rules: "old" },
                [ids[2]!]: { ...built.build.snapshot.waters[ids[2]!]!, hash: "old" },
                "gnis:1": { slug: "gone", name: "Gone Creek", hash: "x", rules: "x" } },
    };
    delete (prev.waters as Record<string, unknown>)[ids[1]!];
    const next = await buildAll({ loader: openRegsFixture(), on: TODAY, previous: prev });
    const page = next.build.pages.find((p) => p.kind === "changes")!;
    const t = text(page.html);
    expect(t).toMatch(/Rules changed \(1\)/); expect(t).toMatch(/New waters \(1\)/); expect(t).toMatch(/no longer listed \(1\)/);
    expect(t).toContain("Gone Creek"); expect(t).toContain("r-000000000000");
    // (T490) ids[2]'s words moved, its rules did not: not a rule change, one wording line (a wording release listed 19k waters)
    expect(t).toContain("Wording updated on 1 water: the rules there are the same.");
    expect(page.html).not.toContain(`/water/${built.build.snapshot.waters[ids[2]!]!.slug}"`);
    expect(next.files.get("sitemap-1.xml")).toContain("/changes");
    // a wording-only release: the page says so once and lists no water
    const words = { ...built.build.snapshot, release: "r-000000000000", waters: Object.fromEntries(Object.entries(built.build.snapshot.waters).map(([k, w]) => [k, { ...w, hash: "old" }])) };
    const wt = text((await buildAll({ loader: openRegsFixture(), on: TODAY, previous: words })).build.pages.find((p) => p.kind === "changes")!.html);
    expect(wt).toMatch(/Rules changed \(0\)/); expect(wt).toContain(`Wording updated on ${ids.length} waters: the rules there are the same.`);
    // a snapshot from before the rules hash is refused, naming the upgrade, never read as "every water changed"
    const before = { ...built.build.snapshot, release: "r-000000000000", waters: Object.fromEntries(Object.entries(built.build.snapshot.waters).map(([k, w]) => [k, { slug: w.slug, name: w.name, hash: w.hash }])) };
    await expect(buildAll({ loader: openRegsFixture(), on: TODAY, previous: before as never })).rejects.toThrow(/no rules hash .*--snapshot-only --keep-pages/);
    // another release with the same rules (a re-cut): nothing to say, so no page and no sitemap entry
    const same = await buildAll({ loader: openRegsFixture(), on: TODAY, previous: { ...built.build.snapshot, release: "r-000000000000" } });
    expect(same.build.pages.some((p) => p.kind === "changes")).toBe(false);
    expect(same.files.get("sitemap-1.xml")).not.toContain("/changes");
  }, 180_000);
});

describe("T196 performance budget", () => {
  it("the fixture site is inside the budget, and the numbers are recorded", () => {
    expect(built.problems).toEqual([]);
    const rec = JSON.parse(built.files.get("budget.json")!) as { measured: { p95GzipBytes: number; cssGzipBytes: number }; budget: typeof BUDGET };
    expect(rec.measured.cssGzipBytes).toBeLessThanOrEqual(BUDGET.cssGzipBytes);
    expect(rec.measured.p95GzipBytes).toBeLessThanOrEqual(BUDGET.p95HtmlGzipBytes);
    const lh = JSON.parse(built.files.get("lighthouserc.json")!) as { ci: { collect: { url: string[]; settings: { formFactor: string } }; assert: { assertions: Record<string, unknown> } } };
    expect(lh.ci.collect.settings.formFactor).toBe("mobile");
    expect(lh.ci.collect.url.every((u) => built.files.has(u))).toBe(true);
    // fixed by the T196 performance budget (LCP 1.8 s), not by the release
    expect(lh.ci.assert.assertions["largest-contentful-paint"]).toEqual(["error", { maxNumericValue: 1800 }]);
  });

  it("(T269) the water pages are measured as built (the prerendered pane, its critical CSS inlined, the app's script deferred)", () => {
    const water = waters().map((p) => p.size);
    const m = measure(water, [built.files.get(cssPath())!]);
    expect(m.p95GzipBytes).toBeLessThanOrEqual(BUDGET.p95HtmlGzipBytes);
    expect(m.criticalCssBytes).toBeLessThanOrEqual(BUDGET.criticalCssBytes);
    const paneCss = [...built.files.keys()].find((k) => /^assets\/pane\.[0-9a-f]+\.css$/.test(k))!;
    expect(waters().every((p) => p.html.includes(`href="/${paneCss}"`))).toBe(true);
    expect(measure(water, [built.files.get(paneCss)!]).cssGzipBytes).toBeLessThanOrEqual(BUDGET.cssGzipBytes);
  });

  it("MUTATION: an oversized page, a fat stylesheet and a blocking script each fail the gate", () => {
    const sizes = built.build.pages.map((p) => p.size);
    const noise = Array.from({ length: 30000 }, (_, i) => `w${i}x${(i * 7919) % 10007}`).join(" ");
    const css = built.files.get(cssPath())!;
    const fat = [...sizes.slice(1), pageSize(sizes[0]!.path, built.build.pages[0]!.html + noise)];
    // fixed by the T196 budget (30 KiB page limit = 30720 B); `fat` is built to exceed it, whatever the release
    expect(checkBudget(measure(fat, [css]), fat).join("|")).toMatch(/over 30720/);
    expect(checkBudget(measure(sizes, [css, noise + noise]), sizes).join("|")).toMatch(/stylesheet/);
    const js = [...sizes.slice(1), pageSize(sizes[0]!.path, built.build.pages[0]!.html.replace("</head>", '<script src="/app.js"></script></head>'))];
    expect(checkBudget(measure(js, [css]), js).join("|")).toMatch(/blocking JavaScript/);
    const crit = [...sizes.slice(1), pageSize(sizes[0]!.path, built.build.pages[0]!.html.replace("<style>", `<style>${noise.slice(0, 3000)}`))];
    expect(checkBudget(measure(crit, [css]), crit).join("|")).toMatch(/critical CSS/);
    expect(blockingScripts('<script src="/a.js" defer></script><script type="module" src="/m.js"></script>')).toEqual([]);
  });
});

describe("T198 hosting config (code half)", () => {
  it("HSTS and the cache split are in _headers; the 404 page is a real page", () => {
    const h = built.files.get("_headers")!;
    expect(h).toContain(`Strict-Transport-Security: ${HSTS}`);
    // fixed by T198's hosting contract: one year (31536000 s), immutable, for the hashed assets
    expect(h).toMatch(/\/assets\/\*\n {2}Cache-Control: public, max-age=31536000, immutable/);
    expect(h).toBe(headersFile());
    expect(built.files.get("404.html")).toContain('content="noindex"');
    expect(built.files.get("404.html")).toContain('href="/region/');
  });

  it("canifishthis.com, www and http go to the canonical https host with the path kept; slashes and removed slugs 301 too", () => {
    const rm = [{ slug: "old-creek", region: 4 }];
    const to = (u: string) => redirectFor(u, site, rm);
    expect(to("https://canifishthis.com/water/vedder-river")).toEqual({ status: 301, to: "https://canifishthis.ca/water/vedder-river" });
    expect(to("https://www.canifishthis.com/region/2?x=1")).toEqual({ status: 301, to: "https://canifishthis.ca/region/2?x=1" });
    expect(to("https://www.canifishthis.ca/about")?.to).toBe("https://canifishthis.ca/about");
    expect(to("http://canifishthis.ca/")?.to).toBe("https://canifishthis.ca/");
    expect(to("https://canifishthis.ca/water/vedder-river/")?.to).toBe("https://canifishthis.ca/water/vedder-river");
    expect(to("https://canifishthis.ca/water/old-creek")).toEqual({ status: 301, to: "https://canifishthis.ca/region/4" });
    expect(to("https://canifishthis.ca/water/vedder-river")).toBeNull();
    expect(to("https://canifishthis.ca/")).toBeNull();
    const rules = JSON.parse(built.files.get("redirects.json")!) as { redirects: { from_host: string }[] };
    expect(rules.redirects.map((r) => r.from_host)).toEqual(["canifishthis.com", "www.canifishthis.com", "www.canifishthis.ca"]);
    expect(redirectsFile([{ slug: "a", region: 1 }])).toBe("/water/a /region/1 301\n");
  });

  it("the generated host worker (canifishthis.com, www) answers every URL exactly as redirectFor does: one 301, HSTS", async () => {
    const rm = [{ slug: "old-creek", region: 4 }];
    const mod = await import(/* @vite-ignore */ `data:text/javascript;base64,${Buffer.from(hostWorker(site, rm)).toString("base64")}`);
    for (const u of ["https://canifishthis.com/", "https://canifishthis.com/water/vedder-river", "http://www.canifishthis.com/region/2/?x=1",
                     "https://www.canifishthis.ca/about", "https://canifishthis.com/water/old-creek/", "https://www.canifishthis.com/waterbody/930-055749/"]) {
      const r: Response = await mod.default.fetch(new Request(u));
      expect([r.status, r.headers.get("location")], u).toEqual([301, redirectFor(u, site, rm)!.to]);
      expect(r.headers.get("strict-transport-security")).toBe(HSTS);
    }
    expect(built.files.get("host-worker.js")).toContain(`const ORIGIN = "${site.origin}"`);
  });

  it("verification tokens render only when the owner has set them", () => {
    expect(built.build.pages[0]!.html).not.toMatch(/site-verification|msvalidate/);
  });
});

describe("the static output", () => {
  it("is written to a gitignored dist folder, and refuses any other", () => {
    writeSite(OUT, built.files, built.binary);
    expect(existsSync(join(OUT, "water/vedder-river/index.html"))).toBe(true);
    // (T406) the card a water's head names is on disk, a 1200 x 630 PNG; its SVG source is not shipped
    const card = /<meta property="og:image" content="https:\/\/[^/]+(\/share\/vedder-river\.png)">/.exec(readFileSync(join(OUT, "water/vedder-river/index.html"), "utf8"))?.[1];
    expect([card, pngSize(readFileSync(join(OUT, card ?? "-"))), existsSync(join(OUT, "share/vedder-river.svg"))])
      .toEqual(["/share/vedder-river.png", { width: SHARE_W, height: SHARE_H }, false]);
    expect(readFileSync(join(OUT, "robots.txt"), "utf8")).toContain("Sitemap:");
    expect(existsSync(join(OUT, cssPath()))).toBe(true);
    expect(() => writeSite(join(APP, "seo-out"), built.files)).toThrow(/inside a dist folder/);
    expect(readFileSync(join(APP, "../.gitignore"), "utf8")).toMatch(/^app\/\*\*\/dist\/$/m);
  });
});
