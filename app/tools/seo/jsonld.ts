/**
 * T194 — STRUCTURED DATA, generated and held to the page. Every fact in the JSON-LD must be visible in the same page's HTML
 * (`checkJsonLd`): a breadcrumb, a title, a date, the question and its answer. A mismatch fails the build, and the FAQ markup
 * is the first thing to drop (it is low-value insurance; Google limits FAQ rich results).
 */
import type { Site } from "./html";
import type { WaterModel } from "./model";

type Json = null | boolean | number | string | Json[] | { [k: string]: Json };

/** The schema.org body-of-water type, only where the kind is certain. */
export function bodyOfWater(kind: string): string | null {
  return /^(river|stream|creek)$/i.test(kind) ? "RiverBodyOfWater" : /^lake$/i.test(kind) ? "LakeBodyOfWater" : null;
}

const script = (g: Json): string => JSON.stringify({ "@context": "https://schema.org", "@graph": g }).replace(/</g, "\\u003c");

const website = (site: Site): Json => ({ "@type": "WebSite", "@id": `${site.origin}/#website`, name: site.name, url: `${site.origin}/` });

/** WebPage + WebSite for any page; `crumbs` are [name, path] from Home down (none: no breadcrumb). */
function base(site: Site, url: string, name: string, asOf: string, crumbs: readonly (readonly [string, string])[]): Json[] {
  const page: { [k: string]: Json } = { "@type": "WebPage", "@id": `${url}#webpage`, url, name, inLanguage: "en-CA", dateModified: asOf,
                                         isPartOf: { "@id": `${site.origin}/#website` } };
  if (!crumbs.length) return [website(site), page];
  page.breadcrumb = { "@id": `${url}#breadcrumb` };
  return [
    website(site), page,
    { "@type": "BreadcrumbList", "@id": `${url}#breadcrumb`,
      itemListElement: crumbs.map(([n, p], i) => ({ "@type": "ListItem", position: i + 1, name: n, item: `${site.origin}${p}` })) },
  ];
}

/**
 * (T269) A water page is the app's regs pane, which draws no breadcrumb, region line, centre point, build date or FAQ:
 * its JSON-LD says only what the pane shows — the page (its URL and title) about the water, by its name (and its kind,
 * where certain). The site itself is described on every hub page.
 */
export function waterLd(m: WaterModel, path: string, title: string, site: Site): string {
  const url = `${site.origin}${path}`, type = bodyOfWater(m.kind);
  const place: { [k: string]: Json } = { "@type": type ? ["Place", type] : "Place", "@id": `${url}#place`, name: m.name };
  return script([{ "@type": "WebPage", "@id": `${url}#webpage`, url, name: title, inLanguage: "en-CA", about: { "@id": `${url}#place` } }, place]);
}

export function pageLd(site: Site, path: string, title: string, asOf: string, crumbs: readonly (readonly [string, string])[]): string {
  return script(base(site, `${site.origin}${path}`, title, asOf, crumbs));
}

export function aboutLd(site: Site, title: string, asOf: string): string {
  const org: { [k: string]: Json } = { "@type": "Organization", "@id": `${site.origin}/#org`, name: site.name, url: `${site.origin}/` };
  if (site.contact) org.email = site.contact;
  return script([...base(site, `${site.origin}/about`, title, asOf, [["Home", "/"], ["About", "/about"]]), org]);
}

// ── the gate ─────────────────────────────────────────────────────────────────────────────────────────────────────────
const decode = (s: string) => s.replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"').replace(/&amp;/g, "&");
const LD = /<script type="application\/ld\+json">([\s\S]*?)<\/script>/g;
const SKIP_KEYS = new Set(["@context", "@type", "@id", "inLanguage", "position"]);

/** What is wrong with a page's JSON-LD: it must parse, carry the required fields, and say only what the page shows. */
export function checkJsonLd(html: string, origin: string, need: readonly string[] = ["WebSite", "WebPage", "BreadcrumbList"]): string[] {
  const bad: string[] = [];
  const blocks = [...html.matchAll(LD)].map((m) => m[1]!);
  if (blocks.length !== 1) return [`${blocks.length} JSON-LD blocks, expected 1`];
  const visible = decode(html.replace(LD, " ").replace(/<[^>]+>/g, " ")).replace(/\s+/g, " ");
  const attrs = decode(html.replace(LD, " "));   // the page without its own JSON-LD
  let doc: { "@graph"?: { [k: string]: Json }[] };
  try { doc = JSON.parse(blocks[0]!.replace(/\\u003c/g, "<")); } catch (e) { return [`JSON-LD does not parse: ${String(e)}`]; }
  const graph = doc["@graph"] ?? [];
  const types = graph.map((n) => String(Array.isArray(n["@type"]) ? n["@type"][0] : n["@type"]));
  for (const t of need) if (!types.includes(t)) bad.push(`missing ${t}`);
  for (const n of graph) {
    const t = String(Array.isArray(n["@type"]) ? n["@type"][0] : n["@type"]);
    // a page of the site carries its date and its site; a water page (no WebSite node: T269) only its URL and title
    const page = types.includes("WebSite") ? ["url", "name", "dateModified", "isPartOf"] : ["url", "name"];
    const req: Record<string, string[]> = { WebPage: page, BreadcrumbList: ["itemListElement"],
      Place: ["name"], FAQPage: ["mainEntity"], Organization: ["name", "url"], WebSite: ["name", "url"] };
    for (const f of req[t] ?? []) if (n[f] === undefined) bad.push(`${t} lacks ${f}`);
  }
  const walk = (v: Json, key: string): void => {
    if (typeof v === "string") {
      if (SKIP_KEYS.has(key)) return;
      if (v.startsWith(origin)) { if (!attrs.includes(`"${v}"`) && !attrs.includes(`href="${v.slice(origin.length) || "/"}"`) && !attrs.includes(`"${v}?`)) bad.push(`URL ${v} is not on the page`); }
      else if (!visible.includes(v.replace(/\s+/g, " ")) && !attrs.includes(v)) bad.push(`"${v.slice(0, 60)}" is in the JSON-LD and not on the page`);
    } else if (typeof v === "number") { if (!SKIP_KEYS.has(key) && !visible.includes(String(v))) bad.push(`${key} ${v} is not on the page`); }
    else if (Array.isArray(v)) v.forEach((x) => walk(x, key));
    else if (v && typeof v === "object") for (const [k, x] of Object.entries(v)) walk(x, k);
  };
  walk(graph as Json, "");
  return bad;
}
