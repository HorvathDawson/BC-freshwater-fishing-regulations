/**
 * T193-T198 on the committed regs fixture release (43 waters on 2026-10-27), written to a gitignored dist folder: the water
 * pages and their verdicts, the sitemap / canonical / JSON-LD gates, the hubs, the performance budget and the hosting config.
 * Mutation-pinned where a gate could be vacuous: each planted fault must be reported by the gate that owns it.
 */
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { beforeAll, describe, expect, it } from "vitest";
import { buildRegsPage, type RegsPage } from "@app/core";
import { loadRegsInputs, openRegsStore } from "@app/data/regs";
import { openRegsFixture } from "@app/data/release/node";
import { BUDGET, blockingScripts, checkBudget, measure } from "./budget";
import { buildAll, readConfig, writeSite, type Built } from "./build";
import { TODAY } from "./fixture";
import { headersFile, HSTS, redirectFor, redirectsFile } from "./hosting";
import { checkJsonLd } from "./jsonld";
import { buildDayOf } from "./model";
import { weekendDays } from "./site";

const APP = join(dirname(fileURLToPath(import.meta.url)), "../..");
const OUT = join(APP, "dist/seo-test");
const site = readConfig();
const text = (html: string) => html.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ");
let built: Built;
beforeAll(async () => { built = await buildAll({ loader: openRegsFixture(), on: TODAY }); }, 180_000);
const cssPath = () => built.build.cssHref.slice(1);
const waters = () => built.build.pages.filter((p) => p.kind === "water");

describe("T193 water pages and About", () => {
  it("one page per water with rules, readable without JavaScript, every page stating the not-a-legal-document line", () => {
    expect(waters().length).toBe(built.build.models.length);
    expect(waters().length).toBeGreaterThan(20);
    for (const p of built.build.pages) {
      expect(blockingScripts(p.html), p.path).toEqual([]);
      expect(p.html, p.path).not.toMatch(/<script(?![^>]*ld\+json)/);
      expect(text(p.html), p.path).toMatch(/not a legal document/i);
    }
    for (const p of waters()) {
      expect(p.html).toMatch(/<h1>[^<]+ fishing regulations<\/h1>/);
      expect(p.html).toMatch(/class="app" href="https:\/\/canifishthis\.ca\/\?regs=/);   // the prominent Open in the app link
      expect(p.html).toContain("fishing_synopsis.pdf");
    }
  });

  it("the Vedder page carries the full rules in plain words, with the book page cited", () => {
    const v = waters().find((p) => p.path === "/water/vedder-river")!;
    const t = text(v.html);
    expect(t).toMatch(/Can I fish Vedder River today\?\s+Open to fishing today, with limits\./);
    expect(t).toContain("2 a day per angler from streams");
    expect(t).toContain("This river’s rule: up to 4 hatchery rainbow trout a day");
    expect(v.html).toMatch(/href="https:[^"]*fishing_synopsis\.pdf#page=24"[^>]*>Rule book p\.22</);
    expect(t).toMatch(/Region 2/);
  });

  it("each page's verdict is the app's answer for that water (and a planted wrong verdict is caught)", async () => {
    const store = await openRegsStore(openRegsFixture());
    const answerClass = (html: string) => /<p class="answer ?(\w*)">/.exec(html)?.[1] ?? "";
    const app = async (id: string, partIx: number): Promise<RegsPage> => buildRegsPage(
      await loadRegsInputs(store, { kind: "part", item: id, partIx }, TODAY.day, TODAY.weekday, TODAY.year),
      { today: TODAY.day, who: { age: "adult", residency: "resident" } });
    const mismatches = (html: string, statuses: string[]) => {
      const same = new Set(statuses).size === 1 ? statuses[0] : "";
      return answerClass(html) === (same === "open" || same === "closed" ? same : "") ? [] : [`${answerClass(html)} vs ${same}`];
    };
    let checked = 0;
    for (const m of built.build.models) {
      const page = waters().find((p) => p.path === `/water/${built.build.slugMap.waters[m.id]!.slug}`)!;
      const statuses = await Promise.all(m.parts.map(async (p) => (await app(m.id, p.partIx)).status));
      expect(mismatches(page.html, statuses), m.id).toEqual([]);
      checked++;
    }
    expect(checked).toBe(built.build.models.length);
    const vedder = waters().find((p) => p.path === "/water/vedder-river")!;
    expect(mismatches(vedder.html.replace('class="answer open"', 'class="answer closed"'), ["open"])).not.toEqual([]);
    expect(built.build.models.some((m) => m.status === "closed") || built.build.models.some((m) => m.status === "mixed")).toBe(true);
  }, 120_000);

  it("About names the sources and the contact only when there is one", () => {
    const about = built.build.pages.find((p) => p.path === "/about")!;
    expect(text(about.html)).toMatch(/free, non-profit/);
    expect(text(about.html)).toMatch(/Environment and Climate Change Canada/);
    expect(about.html).not.toMatch(/mailto:/);                 // site.json contact is null until the owner sets it
  });

  it("pages refresh with each release: a new release id and date are in the page", async () => {
    const next = await buildAll({ loader: openRegsFixture(), on: buildDayOf(new Date(2026, 10, 3, 12)) });
    const a = waters()[0]!, b = next.build.pages.find((p) => p.path === a.path)!;
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
    expect(waters().find((p) => p.path === "/water/vedder-river")!.html).toContain("<title>Vedder River fishing regulations (BC) — Can I Fish This?</title>");
  });

  it("the JSON-LD parses, has its required fields, and every fact in it is on the page; a planted fact is caught", () => {
    for (const p of built.build.pages) expect(checkJsonLd(p.html, site.origin), p.path).toEqual([]);
    const v = waters().find((p) => p.path === "/water/vedder-river")!.html;
    expect(v).toMatch(/"@type":"BreadcrumbList"/);
    expect(v).toMatch(/"@type":"FAQPage"/);
    const planted = v.replace('"name":"Vedder River"', '"name":"Vedder River","slogan":"The best fishing in BC"');
    expect(checkJsonLd(planted, site.origin).join("|")).toContain("The best fishing in BC");
    expect(checkJsonLd(v.replace(/<script type="application\/ld\+json">/, '<script type="application/ld+json">{'), site.origin)[0]).toMatch(/does not parse/);
    expect(checkJsonLd(v.replace('"dateModified"', '"dateModifiedX"'), site.origin).join("|")).toMatch(/lacks dateModified/);
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
    expect(again.files.get("_redirects")).toBe("/water/long-gone-creek /region/3 301\n");
    expect(again.build.pages.map((p) => p.path)).toEqual(built.build.pages.map((p) => p.path));
  }, 120_000);
});

describe("T195 hubs and dated pages", () => {
  it("every water is one click from its region hub and every hub is one click from the footer of any page", () => {
    const regionPages = built.build.pages.filter((p) => p.kind === "region");
    expect(regionPages.length).toBeGreaterThan(2);
    for (const m of built.build.models) {
      const path = `/water/${built.build.slugMap.waters[m.id]!.slug}`;
      expect(regionPages.some((r) => r.html.includes(`href="${path}"`)), path).toBe(true);
    }
    for (const r of regionPages) expect(waters()[0]!.html).toContain(`href="${r.path}"`);
  });

  it("the weekend and opening/closing pages state their release and dates, and link the waters they name", () => {
    const wk = built.build.pages.filter((p) => p.kind === "weekend"), cal = built.build.pages.filter((p) => p.kind === "calendar");
    expect(wk.length).toBe(built.build.pages.filter((p) => p.kind === "region").length);
    for (const p of [...wk, ...cal]) {
      expect(text(p.html)).toContain(built.build.snapshot.release);
      expect(p.html).toMatch(/<time datetime="2026-\d\d-\d\d">/);
    }
    expect(text(wk[0]!.html)).toMatch(/Weekend of 2026-10-31 and 2026-11-01/);
    expect(weekendDays(buildDayOf(new Date(2026, 10, 1, 12))).map((d) => d.iso)).toEqual(["2026-11-01"]);
    expect(weekendDays(buildDayOf(new Date(2026, 9, 31, 12))).map((d) => d.iso)).toEqual(["2026-10-31", "2026-11-01"]);
    expect(cal.some((p) => /<li><strong>[A-Z][a-z]{2} \d+<\/strong> (opens|closes): <a href="\/water\//.test(p.html))).toBe(true);
  });

  it("the changes page appears only against a previous release, and lists changed, new and removed waters", async () => {
    expect(built.build.pages.some((p) => p.kind === "changes")).toBe(false);
    const ids = Object.keys(built.build.snapshot.waters);
    const prev = {
      ...built.build.snapshot, release: "r-000000000000", asOf: "2026-01-01",
      waters: { ...built.build.snapshot.waters, [ids[0]!]: { ...built.build.snapshot.waters[ids[0]!]!, hash: "old" },
                "gnis:1": { slug: "gone", name: "Gone Creek", hash: "x" } },
    };
    delete (prev.waters as Record<string, unknown>)[ids[1]!];
    const next = await buildAll({ loader: openRegsFixture(), on: TODAY, previous: prev });
    const page = next.build.pages.find((p) => p.kind === "changes")!;
    const t = text(page.html);
    expect(t).toMatch(/Rules changed \(1\)/); expect(t).toMatch(/New waters \(1\)/); expect(t).toMatch(/no longer listed \(1\)/);
    expect(t).toContain("Gone Creek"); expect(t).toContain("r-000000000000");
    expect(next.files.get("sitemap-1.xml")).toContain("/changes");
  }, 120_000);
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
    expect(lh.ci.assert.assertions["largest-contentful-paint"]).toEqual(["error", { maxNumericValue: 1800 }]);
  });

  it("MUTATION: an oversized page, a fat stylesheet and a blocking script each fail the gate", () => {
    const htmlPaths = [...built.files.keys()].filter((k) => k.endsWith(".html") && k !== "404.html");
    const noise = Array.from({ length: 30000 }, (_, i) => `w${i}x${(i * 7919) % 10007}`).join(" ");
    const fat = new Map(built.files).set(htmlPaths[0]!, built.files.get(htmlPaths[0]!)! + noise);
    expect(checkBudget(measure(fat, htmlPaths, cssPath()), fat, htmlPaths).join("|")).toMatch(/over 30720/);
    const css = new Map(built.files).set(cssPath(), noise + noise);
    expect(checkBudget(measure(css, htmlPaths, cssPath()), css, htmlPaths).join("|")).toMatch(/stylesheet/);
    const js = new Map(built.files).set(htmlPaths[1]!, built.files.get(htmlPaths[1]!)!.replace("</head>", '<script src="/app.js"></script></head>'));
    expect(checkBudget(measure(js, htmlPaths, cssPath()), js, htmlPaths).join("|")).toMatch(/blocking JavaScript/);
    expect(blockingScripts('<script src="/a.js" defer></script><script type="module" src="/m.js"></script>')).toEqual([]);
  });
});

describe("T198 hosting config (code half)", () => {
  it("HSTS and the cache split are in _headers; the 404 page is a real page", () => {
    const h = built.files.get("_headers")!;
    expect(h).toContain(`Strict-Transport-Security: ${HSTS}`);
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

  it("verification tokens render only when the owner has set them", () => {
    expect(built.build.pages[0]!.html).not.toMatch(/site-verification|msvalidate/);
  });
});

describe("the static output", () => {
  it("is written to a gitignored dist folder, and refuses any other", () => {
    writeSite(OUT, built.files);
    expect(existsSync(join(OUT, "water/vedder-river/index.html"))).toBe(true);
    expect(readFileSync(join(OUT, "robots.txt"), "utf8")).toContain("Sitemap:");
    expect(existsSync(join(OUT, cssPath()))).toBe(true);
    expect(() => writeSite(join(APP, "seo-out"), built.files)).toThrow(/inside a dist folder/);
    expect(readFileSync(join(APP, "../.gitignore"), "utf8")).toMatch(/^app\/\*\*\/dist\/$/m);
  });
});
