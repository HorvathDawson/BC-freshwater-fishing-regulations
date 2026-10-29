/**
 * T193/T194 — THE HUB PAGES AS HTML (regions, letters, topics, weekend, season changes, changes, About, privacy). Plain,
 * crawlable, readable without JS: no script, no map, no widget. One shared stylesheet (css below, served hashed), a tiny
 * critical part inlined, system fonts. (T269) A WATER'S page is not drawn here: it is the app's own regs pane, prerendered
 * (pane.tsx, prerender.ts); this file gives it only its head (`headOf`).
 */
import { SYNOPSIS_PDF } from "@app/core";
import meta from "../../packages/map/style/style.meta.json" with { type: "json" };
import type { WaterModel } from "./model";

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

/** The page title, 60 characters at most: "<Water> fishing regulations, B.C. — Can I Fish This?", shortened by dropping words (the brand, then "fishing"); the name is never cut. */
export function titleOf(name: string, site: Site, qualifier = ""): string {
  const end = ` — ${site.name}`, q = qualifier ? ` (${qualifier})` : "";
  for (const mid of [" fishing regulations, B.C.", " fishing regulations", " regulations"]) {
    const t = `${name}${q}${mid}${end}`;
    if (t.length <= 60) return t;
  }
  // too long with the brand: drop it, then the "fishing"; a name that is itself too long is never cut (a cut name is how two
  // waters end up with one title), so such a title runs past 60 and a search result shortens it on its own
  for (const mid of [" fishing regulations, B.C.", " fishing regulations", " regulations"]) {
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
export function descriptionOf(m: WaterModel, label: string = m.name, town: string | null = null): string {
  const first = m.parts[0]?.sections.find((s) => s.title === "What you can keep")?.items.find((i) => !i.head)?.text;
  const where = town ? `near ${town}, ${regionLabel(m.region)}` : regionLabel(m.region);
  return clip(`${label} fishing regulations, B.C. (${where}): ${m.answer}${first ? ` ${first}.` : ""}`);
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

/** where privacy questions go: a placeholder until the owner sets the mailbox up (OWNER-TODO) */
export const PRIVACY_EMAIL = "privacy@canifishthis.ca";
export const PRIVACY_EFFECTIVE = "2026-10-29";

/** (owner 2026-10-29) THE PRIVACY POLICY. Every claim here is checked against the code (see T268): change the app, change this. */
export function privacyBody(site: Site, asOf: string): string {
  const mail = `<a href="mailto:${PRIVACY_EMAIL}">${PRIVACY_EMAIL}</a>`;
  return `<nav class="crumbs"><a href="/">Home</a> › Privacy policy</nav><h1>Privacy policy</h1>` +
    `<p class="asof">Effective ${PRIVACY_EFFECTIVE}. Page last built <time datetime="${esc(asOf)}">${esc(asOf)}</time>.</p>` +
    `<p>${esc(site.name)} is a free, non-profit app and website that shows BC freshwater fishing regulations. ` +
    `It has no accounts, no ads and no tracking. This page says what the app and this website do with your information.</p>` +
    `<h2>The short version</h2><ul>` +
    `<li>Your location, your photos and the spots you save stay on your device. We never receive them.</li>` +
    `<li>The app sends us two things: a problem report, only when you press Send, and, unless you switch it off, a short error report when the app itself fails.</li>` +
    `<li>We do not sell or share your information, and we use no advertising or analytics services.</li></ul>` +
    `<h2>Location</h2><p>If you allow it, the app uses your location while it is open, to show where you are on the map, ` +
    `to show what is near you, and to pin a spot where you are standing. It is used on your device and is not sent to us. ` +
    `The app never uses your location in the background. You can turn the permission off in your phone settings; the map still works.</p>` +
    `<h2>Camera and photos</h2><p>The app asks for the camera or your photos only so you can attach a picture to a spot you saved. ` +
    `The pictures stay on your device and are not uploaded.</p>` +
    `<h2>Saved spots</h2><p>Spots, notes and photos are stored only on your device, and there is no account. ` +
    `If you delete the app or clear its data, they are gone; we cannot recover them.</p>` +
    `<h2>Weather for a spot</h2><p>To show the weather at a spot you saved, the app sends that spot’s coordinates and the date to ` +
    `<a href="https://open-meteo.com" rel="noopener">Open-Meteo</a>, a weather service, and shows what comes back. ` +
    `If you pinned the spot where you were standing, those coordinates are where you were. ` +
    `Open-Meteo’s servers see your device’s IP address as any website would; we do not control their records. ` +
    `Nothing else about you is sent with the request.</p>` +
    `<h2>Downloads from our servers</h2><p>The app downloads the regulations, the map and river-gauge readings (including past days’ flow) and salmon notices from our servers, ` +
    `which run on Cloudflare. Each request carries the app’s version (so we can tell old versions from new) ` +
    `and, as with any web request, your IP address, which Cloudflare handles to serve the request. ` +
    `These downloads are the same for everyone and do not include your location, your spots or any identifier. ` +
    `Our servers, not the app, fetch the underlying data from Environment and Climate Change Canada and Fisheries and Oceans Canada.</p>` +
    `<h2>Map and satellite imagery</h2><p>The map is drawn from files on our servers. Its fonts and symbols come from our servers too. When you turn satellite imagery on, the app asks Esri for the picture tiles of the area ` +
    `you are looking at, so Esri sees those tile requests and your IP address. Imagery is shown only while online and is not stored.</p>` +
    `<h2>Reporting a problem</h2><p>Sending a report is your choice, and the form shows exactly what will go. A report holds:</p><ul>` +
    `<li>the kind of problem and the words you wrote;</li>` +
    `<li>the details under “Include with your report” that you leave switched on (the water and part you were viewing, the day, the screen, ` +
    `the map colouring, the app version, data release, platform and recent app errors);</li>` +
    `<li>your email address, only if you type one. It is used only to reply, and only if you tick “You may contact me”.</li></ul>` +
    `<p>Reports are stored privately on Cloudflare, are read only by the people who run the app, and are not published. ` +
    `We do not store your IP address with a report: to limit abuse we keep only a salted one-way hash of it, which cannot be turned back into an address. ` +
    `Reports are deleted automatically 180 days after they are received, including any email in them. ` +
    `To have your report or email deleted sooner, write to ${mail}.</p>` +
    `<h2>App errors (crash reports)</h2><p>When the app hits an error it did not expect, it sends one short report to our own server on Cloudflare. ` +
    `We use no third-party crash-reporting service. This is on by default and you can turn it off at any time in More → Data → “Send crash reports”; ` +
    `when it is off nothing is sent. An error report holds only:</p><ul>` +
    `<li>the app version, whether it is a test or live build, your phone’s system name and version (or “web”), and the data release the app was using;</li>` +
    `<li>the kind of screen you were on — map, conditions, runs, spots, menu or a water page — and never which water, where you were or what you searched for;</li>` +
    `<li>the error itself: its type, its message with any quoted text, web address, e-mail address, coordinates and long numbers removed, and the app’s own code trace (file names and line numbers);</li>` +
    `<li>whether the error stopped the app, and the time.</li></ul>` +
    `<p>It never includes your location, spots, photos, notes, searches, e-mail address or any device or advertising identifier. ` +
    `Each different error is sent once per time the app runs, and only while you are online; an error that happens offline is not saved to send later, ` +
    `except one that stops the app, which is kept on your device and sent the next time the app starts (and deleted from your device if you have switched crash reports off). ` +
    `We keep one stored record per kind of error, with a count and the app versions it was seen in; we do not store your IP address, ` +
    `and to limit abuse we keep only a salted one-way hash of it for a day’s counting. Error records are deleted automatically 90 days after the error was last seen.</p>` +
    `<h2>This website</h2><p>The website serves plain pages. It sets no cookies and runs no analytics, advertising or tracking scripts. ` +
    `Cloudflare, which hosts it, sees ordinary request information such as your IP address.</p>` +
    `<h2>No selling, no sharing</h2><p>We do not sell your information, and we do not share it for advertising. ` +
    `The services named above (Cloudflare, Open-Meteo, Esri) receive only the requests described here.</p>` +
    `<h2>Children</h2><p>The app is not aimed at children under 13 and does not knowingly collect their information. ` +
    `It asks for none beyond the optional report.</p>` +
    `<h2>Changes</h2><p>If this policy changes, this page will carry the new text and a new effective date.</p>` +
    `<h2>Contact</h2><p>Questions, or a request to delete a report: ${mail}.</p>` +
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

/** The head's page tags: title, description, canonical, robots, social cards, verification, JSON-LD. */
export function headOf(s: Omit<Shell, "body" | "cssHref">, site: Site): string {
  const url = `${site.origin}${s.path === "/" ? "/" : s.path}`;
  const ver = [site.verification.google ? `<meta name="google-site-verification" content="${esc(site.verification.google)}">` : "",
               site.verification.bing ? `<meta name="msvalidate.01" content="${esc(site.verification.bing)}">` : ""].join("");
  const img = s.image ? `<meta property="og:image" content="${esc(site.origin + s.image)}"><meta name="twitter:card" content="summary_large_image"><meta name="twitter:image" content="${esc(site.origin + s.image)}">`
    : `<meta name="twitter:card" content="summary">`;
  return `<title>${esc(s.title)}</title><meta name="description" content="${esc(s.description)}">` +
    `<link rel="canonical" href="${esc(url)}"><meta name="robots" content="${s.robots ?? "index,follow"}">` +
    `<meta property="og:type" content="website"><meta property="og:site_name" content="${esc(site.name)}">` +
    `<meta property="og:title" content="${esc(s.title)}"><meta property="og:description" content="${esc(s.description)}"><meta property="og:url" content="${esc(url)}">` +
    `${img}${ver}${s.jsonld ? `<script type="application/ld+json">${s.jsonld}</script>` : ""}`;
}

/** the whole document */
export function shell(s: Shell, site: Site): string {
  return `<!doctype html><html lang="en-CA"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">` +
    `${headOf(s, site)}<style>${CRITICAL}</style><link rel="stylesheet" href="${esc(s.cssHref)}"></head><body>` +
    `<header class="top"><div><a href="/">${esc(site.name)}</a><a href="/about">About</a></div></header>` +
    `<main>${s.body}</main><footer>${esc(NOT_LEGAL)} <a href="/privacy">Privacy policy</a></footer></body></html>`;
}
