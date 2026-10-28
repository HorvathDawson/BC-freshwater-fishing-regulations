/**
 * T194 — STRUCTURED DATA, generated and held to the page. Every fact in the JSON-LD must be visible in the same page's HTML
 * (`checkJsonLd`): a breadcrumb, a title, a date, the question and its answer. A mismatch fails the build, and the FAQ markup
 * is the first thing to drop (it is low-value insurance; Google limits FAQ rich results).
 */
import type { Site } from "./html";
import { regionLabel } from "./html";
import type { WaterModel } from "./model";

type Json = null | boolean | number | string | Json[] | { [k: string]: Json };

/** The schema.org body-of-water type, only where the kind is certain. */
export function bodyOfWater(kind: string): string | null {
  return /^(river|stream|creek)$/i.test(kind) ? "RiverBodyOfWater" : /^lake$/i.test(kind) ? "LakeBodyOfWater" : null;
}

const script = (g: Json): string => JSON.stringify({ "@context": "https://schema.org", "@graph": g }).replace(/</g, "\\u003c");

const website = (site: Site): Json => ({ "@type": "WebSite", "@id": `${site.origin}/#website`, name: site.name, url: `${site.origin}/` });

/** WebPage + WebSite for any page; `crumbs` are [name, path] from Home down. */
function base(site: Site, url: string, name: string, asOf: string, crumbs: readonly (readonly [string, string])[]): Json[] {
  return [
    website(site),
    { "@type": "WebPage", "@id": `${url}#webpage`, url, name, inLanguage: "en-CA", dateModified: asOf,
      isPartOf: { "@id": `${site.origin}/#website` }, breadcrumb: { "@id": `${url}#breadcrumb` } },
    { "@type": "BreadcrumbList", "@id": `${url}#breadcrumb`,
      itemListElement: crumbs.map(([n, p], i) => ({ "@type": "ListItem", position: i + 1, name: n, item: `${site.origin}${p}` })) },
  ];
}

export function waterLd(m: WaterModel, site: Site, path: string, title: string, asOf: string, regionPath: string, extra: readonly { q: string; a: string }[] = []): string {
  const url = `${site.origin}${path}`, type = bodyOfWater(m.kind);
  const place: { [k: string]: Json } = {
    "@type": type ? ["Place", type] : "Place", "@id": `${url}#place`, name: m.name,
    containedInPlace: { "@type": "AdministrativeArea", name: regionLabel(m.region) },
  };
  if (m.mid) place.geo = { "@type": "GeoCoordinates", latitude: Number(m.mid[1].toFixed(4)), longitude: Number(m.mid[0].toFixed(4)) };
  const faq = { "@type": "FAQPage", "@id": `${url}#faq`, mainEntity: [{ "@type": "Question", name: `Can I fish ${m.name} today?`,
    acceptedAnswer: { "@type": "Answer", text: m.answer } },
    ...extra.map((e) => ({ "@type": "Question", name: e.q, acceptedAnswer: { "@type": "Answer", text: e.a } }))] };
  return script([...base(site, url, title, asOf, [["Home", "/"], [regionLabel(m.region), regionPath], [m.name, path]]), place, faq]);
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
export function checkJsonLd(html: string, origin: string): string[] {
  const bad: string[] = [];
  const blocks = [...html.matchAll(LD)].map((m) => m[1]!);
  if (blocks.length !== 1) return [`${blocks.length} JSON-LD blocks, expected 1`];
  const visible = decode(html.replace(LD, " ").replace(/<[^>]+>/g, " ")).replace(/\s+/g, " ");
  const attrs = decode(html.replace(LD, " "));   // the page without its own JSON-LD
  let doc: { "@graph"?: { [k: string]: Json }[] };
  try { doc = JSON.parse(blocks[0]!.replace(/\\u003c/g, "<")); } catch (e) { return [`JSON-LD does not parse: ${String(e)}`]; }
  const graph = doc["@graph"] ?? [];
  const types = graph.map((n) => String(Array.isArray(n["@type"]) ? n["@type"][0] : n["@type"]));
  for (const need of ["WebSite", "WebPage", "BreadcrumbList"]) if (!types.includes(need)) bad.push(`missing ${need}`);
  for (const n of graph) {
    const t = String(Array.isArray(n["@type"]) ? n["@type"][0] : n["@type"]);
    const req: Record<string, string[]> = { WebPage: ["url", "name", "dateModified", "isPartOf"], BreadcrumbList: ["itemListElement"],
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
