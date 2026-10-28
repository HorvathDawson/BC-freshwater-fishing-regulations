/**
 * T193/T194 — THE PAGES AS HTML. Plain, crawlable, readable without JS: no script, no map, no widget. Every word of a rule
 * is the presenter's (model.ts); this file adds only the frame around it (headings, links, the not-a-legal-document line).
 * One shared stylesheet (css below, served hashed), a tiny critical part inlined, system fonts.
 */
import { bookUrl, SYNOPSIS_PDF } from "@app/core";
import meta from "../../packages/map/style/style.meta.json" with { type: "json" };
import type { BookRef, Item, PartModel, WaterModel } from "./model";

export interface Site {
  readonly origin: string; readonly name: string; readonly contact: string | null;
  readonly verification: { readonly google: string | null; readonly bing: string | null };
}
export interface Release { readonly id: string; readonly validUntil: string | null }

export const esc = (s: string): string =>
  s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");

export const NOT_LEGAL = "This is not a legal document. Check the official BC Freshwater Fishing Regulations Synopsis before you fish.";
export const regionLabel = (n: number): string => `Region ${n}`;

/** Every colour the pages use is a token of the app's one palette (packages/map/style), read from the generated themes. */
const T = (theme: "light" | "dark", token: string): string => {
  const v = (meta.themes as Record<string, Record<string, unknown>>)[theme]?.[token];
  if (typeof v !== "string") throw new Error(`theme "${theme}" has no colour ${token}`);
  return v;
};
const vars = (th: "light" | "dark"): string =>
  `--ink:${T(th, "color.ui.ink")};--muted:${T(th, "color.ui.sub")};--line:${T(th, "color.ui.line2")};--accent:${T(th, "color.ui.accent")};` +
  `--accent-ink:${T(th, "color.ui.on-accent")};--open:${T(th, "color.status.open")};--closed:${T(th, "color.status.closed")};--soft:${T(th, "color.ui.wash")}`;
export const SHARE_GROUND = T("light", "color.ui.accent");
export const SHARE_INK = T("light", "color.ui.on-accent");

/** the shared stylesheet; `inlineCritical` is the part the first paint needs */
export const CRITICAL = `body{margin:0;font:16px/1.5 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;background:${T("light", "color.ui.card")};color:${T("light", "color.ui.ink")}}` +
  "main{max-width:42rem;margin:0 auto;padding:1rem}h1{font-size:1.6rem;line-height:1.2;margin:.5rem 0}" +
  `@media(prefers-color-scheme:dark){body{background:${T("dark", "color.ui.page")};color:${T("dark", "color.ui.ink")}}}`;
export const CSS = `${CRITICAL}
:root{${vars("light")}}
@media(prefers-color-scheme:dark){:root{${vars("dark")}}}
a{color:var(--accent)}
.top{border-bottom:1px solid var(--line)}.top div{max-width:42rem;margin:0 auto;padding:.6rem 1rem;display:flex;gap:1rem;flex-wrap:wrap;align-items:baseline}
.top a{font-weight:600;text-decoration:none}.crumbs{font-size:.9rem;color:var(--muted)}.crumbs a{color:var(--muted)}
h2{font-size:1.25rem;margin:1.6rem 0 .4rem}h3{font-size:1.05rem;margin:1.2rem 0 .3rem}
.answer{font-size:1.2rem;font-weight:600;margin:.2rem 0}.answer.open{color:var(--open)}.answer.closed{color:var(--closed)}
.asof,.note,.small{color:var(--muted);font-size:.9rem}
.app{display:inline-block;margin:.6rem 0;padding:.65rem 1rem;border-radius:.5rem;background:var(--accent);color:var(--accent-ink);font-weight:600;text-decoration:none}
ul{padding-left:1.1rem}li{margin:.25rem 0}li.sub{list-style:none;margin-left:-1.1rem;margin-top:.9rem;font-weight:600}
.bk{font-size:.85rem;white-space:nowrap}.part{border-top:1px solid var(--line);margin-top:1.4rem}
.legal{background:var(--soft);border-radius:.5rem;padding:.7rem 1rem;margin:1.5rem 0}
.cols{columns:2 14rem;list-style:none;padding:0}.cols li{break-inside:avoid}
.tag{font-size:.8rem;color:var(--muted)}footer{border-top:1px solid var(--line);margin-top:2rem;padding:1rem;text-align:center;font-size:.9rem;color:var(--muted)}
`;

/** The page title, 60 characters at most: "<Water> fishing regulations (BC) — Can I Fish This?", shortened by dropping words (the brand, then "fishing"); the name is never cut. */
export function titleOf(name: string, site: Site, qualifier = ""): string {
  const end = ` — ${site.name}`, q = qualifier ? ` (${qualifier})` : "";
  for (const mid of [" fishing regulations (BC)", " fishing regulations", " regulations"]) {
    const t = `${name}${q}${mid}${end}`;
    if (t.length <= 60) return t;
  }
  // too long with the brand: drop it, then the "fishing"; a name that is itself too long is never cut (a cut name is how two
  // waters end up with one title), so such a title runs past 60 and a search result shortens it on its own
  for (const mid of [" fishing regulations (BC)", " fishing regulations", " regulations"]) {
    const t = `${name}${q}${mid}`;
    if (t.length <= 60) return t;
  }
  return `${name}${q} regulations`;
}

/** At most 155 characters, cut at a word. */
export function clip(s: string, max = 155): string {
  const t = s.replace(/\s+/g, " ").trim();
  if (t.length <= max) return t;
  return `${t.slice(0, max - 1).replace(/\s+\S*$/, "").replace(/[,;:\s]+$/, "")}…`;
}

/** The meta description of a water page: its answer and its first rule. */
export function descriptionOf(m: WaterModel, label: string = m.name): string {
  const first = m.parts[0]?.sections.find((s) => s.title === "What you can keep")?.items.find((i) => !i.head)?.text;
  return clip(`${label}, ${regionLabel(m.region)}: ${m.answer}${first ? ` ${first}.` : ""}`);
}

const bookLinks = (book: readonly BookRef[]): string => book.map((b) => b.pdf === null
  ? ` <span class="bk">(rule book p.${b.printed})</span>`
  : ` <a class="bk" href="${esc(bookUrl(b.pdf))}" rel="noopener">Rule book p.${b.printed}</a>`).join("");
const itemHtml = (i: Item): string => i.head ? `<li class="sub">${esc(i.text)}</li>` : `<li>${esc(i.text)}${bookLinks(i.book)}</li>`;

export const appLink = (site: Site, id?: string, part?: number): string =>
  id === undefined ? `${site.origin}/` : `${site.origin}/?regs=${encodeURIComponent(id)}${part ? `&amp;part=${part}` : ""}`;

function partHtml(p: PartModel, multi: boolean, level: 2 | 3, id: string, site: Site): string {
  const h = (n: number, t: string) => `<h${n}>${esc(t)}</h${n}>`;
  const sections = p.sections.map((s) => `${h(level + (multi ? 1 : 0), s.title)}<ul>${s.items.map(itemHtml).join("")}</ul>`).join("");
  const turns = p.turns.length ? `<p class="note">${p.turns.map((t) => `${t.to === "open" ? "Opens" : "Closes"} ${t.on}`).join(" · ")}</p>` : "";
  return multi
    ? `<section class="part" id="part-${p.partIx}">${h(2, p.label)}<p class="answer ${p.status}">${esc(p.today)}</p>${turns}` +
      `<p><a href="${appLink(site, id, p.partIx)}">Open this part in the app</a></p>${sections}</section>`
    : `${turns}${sections}`;
}

export interface Links { readonly region: string; readonly neighbours: readonly { href: string; text: string; how: string }[]; readonly near: readonly { href: string; text: string }[] }

/** The body of a water page: one answer, the full rules by part, the links in and out. */
export function waterBody(m: WaterModel, site: Site, release: Release, asOf: string, links: Links): string {
  const multi = m.parts.length > 1;
  const q = `Can I fish ${m.name} today?`;
  const cls = m.status === "open" || m.status === "closed" ? m.status : "";
  const valid = release.validUntil ? ` Regulations valid until <time datetime="${esc(release.validUntil)}">${esc(release.validUntil)}</time>.` : "";
  const nav = [
    `<nav class="crumbs" aria-label="Breadcrumb"><a href="/">Home</a> › <a href="${esc(links.region)}">${esc(regionLabel(m.region))}</a> › ${esc(m.name)}</nav>`,
    `<h1>${esc(m.name)} fishing regulations</h1>`,
    `<p class="small">${esc(m.regions.map(regionLabel).join(", "))}</p>`,
    `<h2>${esc(q)}</h2><p class="answer ${cls}">${esc(m.answer)}</p>`,
    `<p class="asof">As of <time datetime="${esc(asOf)}">${esc(asOf)}</time>, from release ${esc(release.id)}.${valid}</p>`,
    `<p><a class="app" href="${appLink(site, m.id)}">Open ${esc(m.name)} in the app</a></p>`,
  ].join("");
  const parts = m.parts.map((p) => partHtml(p, multi, 2, m.id, site)).join("");
  const related = links.neighbours.length
    ? `<h2>Connected waters</h2><ul>${links.neighbours.map((n) => `<li>${esc(n.how)} <a href="${esc(n.href)}">${esc(n.text)}</a></li>`).join("")}</ul>` : "";
  const near = links.near.length
    ? `<h2>More in ${esc(regionLabel(m.region))}</h2><ul class="cols">${links.near.map((n) => `<li><a href="${esc(n.href)}">${esc(n.text)}</a></li>`).join("")}</ul>` : "";
  const where = m.mid ? `<p class="small">Approximate centre of the water: ${m.mid[1].toFixed(4)}, ${m.mid[0].toFixed(4)}.</p>` : "";
  const books = m.book.length
    ? `<p class="small">Synopsis pages cited: ${m.book.map((b) => `${b.printed}`).join(", ")} (printed page numbers).</p>` : "";
  const legal = `<div class="legal"><p>${esc(NOT_LEGAL)} <a href="${esc(SYNOPSIS_PDF)}" rel="noopener">Official synopsis (PDF)</a>.</p>` +
    `<p>Rules shown are for an adult resident angler; the licence you need does not change whether a water is open.</p></div>`;
  return `${nav}${parts}${related}${near}${where}${books}${legal}`;
}

export function aboutBody(site: Site, asOf: string): string {
  const contact = site.contact ? `<p>Contact: <a href="mailto:${esc(site.contact)}">${esc(site.contact)}</a></p>` : "";
  return `<nav class="crumbs"><a href="/">Home</a> › About</nav><h1>About ${esc(site.name)}</h1>` +
    `<p class="asof">Last built <time datetime="${esc(asOf)}">${esc(asOf)}</time>.</p>` +
    `<p>${esc(site.name)} is a free, non-profit app that tells you whether a BC freshwater water is open to fishing, and what you can keep, in plain words.</p>` +
    `<h2>Where the rules come from</h2><p>The rules are read from the Province of British Columbia’s Freshwater Fishing Regulations Synopsis. ` +
    `River gauges come from Environment and Climate Change Canada and the Water Survey of Canada; salmon notices from Fisheries and Oceans Canada; ` +
    `stocking records from the Freshwater Fisheries Society of BC. Their terms of use are respected.</p>` +
    `<p>Every page on this site is built from the same release as the app, so the two never disagree.</p>` +
    `<div class="legal"><p>${esc(NOT_LEGAL)} <a href="${esc(SYNOPSIS_PDF)}" rel="noopener">Official synopsis (PDF)</a>.</p></div>` +
    `<p>If a rule looks wrong, tell us; every report gets a reply and a fix.</p>${contact}` +
    `<p><a class="app" href="${esc(site.origin)}/">Open the app</a></p>`;
}

export function notFoundBody(site: Site, regions: readonly number[]): string {
  return `<h1>Page not found</h1><p>That page is not here. Try a region:</p><ul>` +
    regions.map((r) => `<li><a href="/region/${r}">${esc(regionLabel(r))}</a></li>`).join("") +
    `</ul><p><a class="app" href="${esc(site.origin)}/">Open the app</a></p>`;
}

export interface Shell {
  readonly title: string; readonly description: string; readonly path: string; readonly body: string;
  readonly jsonld?: string; readonly image?: string; readonly cssHref: string; readonly robots?: string;
}

/** the whole document */
export function shell(s: Shell, site: Site): string {
  const url = `${site.origin}${s.path === "/" ? "/" : s.path}`;
  const ver = [site.verification.google ? `<meta name="google-site-verification" content="${esc(site.verification.google)}">` : "",
               site.verification.bing ? `<meta name="msvalidate.01" content="${esc(site.verification.bing)}">` : ""].join("");
  const img = s.image ? `<meta property="og:image" content="${esc(site.origin + s.image)}"><meta name="twitter:card" content="summary_large_image"><meta name="twitter:image" content="${esc(site.origin + s.image)}">`
    : `<meta name="twitter:card" content="summary">`;
  return `<!doctype html><html lang="en-CA"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">` +
    `<title>${esc(s.title)}</title><meta name="description" content="${esc(s.description)}">` +
    `<link rel="canonical" href="${esc(url)}"><meta name="robots" content="${s.robots ?? "index,follow"}">` +
    `<meta property="og:type" content="website"><meta property="og:site_name" content="${esc(site.name)}">` +
    `<meta property="og:title" content="${esc(s.title)}"><meta property="og:description" content="${esc(s.description)}"><meta property="og:url" content="${esc(url)}">` +
    `${img}${ver}<style>${CRITICAL}</style><link rel="stylesheet" href="${esc(s.cssHref)}">` +
    `${s.jsonld ? `<script type="application/ld+json">${s.jsonld}</script>` : ""}</head><body>` +
    `<header class="top"><div><a href="/">${esc(site.name)}</a><a href="/about">About</a></div></header>` +
    `<main>${s.body}</main><footer>${esc(NOT_LEGAL)}</footer></body></html>`;
}
