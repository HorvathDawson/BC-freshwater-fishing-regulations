/**
 * T193/T194 — THE HUB PAGES AS HTML (regions, letters, topics, weekend, season changes, changes, About, privacy). Plain,
 * crawlable, readable without JS: no script, no map, no widget. One shared stylesheet (css below, served hashed), a tiny
 * critical part inlined, system fonts. (T269) A WATER'S page is not drawn here: it is the app's own regs pane, prerendered
 * (pane.tsx, prerender.ts); this file gives it only its head (`headOf`).
 */
import { BOOK_DAYS, IMAGERY_KEPT_PRIVACY, MONTHS, regionWords, SYNOPSIS_PDF } from "@app/core";
import meta from "../../packages/map/style/style.meta.json" with { type: "json" };
import type { PartModel, WaterModel } from "./model";

export interface Site {
  readonly origin: string; readonly name: string; readonly contact: string | null;
  readonly verification: { readonly google: string | null; readonly bing: string | null };
}
export interface Release { readonly id: string; readonly validUntil: string | null }

export const esc = (s: string): string =>
  s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");

export const NOT_LEGAL = "This is not a legal document. Check the official BC Freshwater Fishing Regulations Synopsis before you fish.";
/** a region HUB covers every code of its number ("Region 7" holds 7A and 7B) */
export const regionLabel = (n: number): string => `Region ${n}`;
/** (T622 G23) a WATER's region as the app names it: its own code, "Region 7A", not the hub's "Region 7" */
export const waterRegion = (m: Pick<WaterModel, "focus" | "region">): string => (m.focus ? regionWords(m.focus) : regionLabel(m.region));

/**
 * (T672 W4, owner 2026-11-12) EVERY region a water runs through, for its description: "Regions 2, 3, 5, 7" for a multi-region
 * water, else `waterRegion`'s one ("Region 7A"). Hub numbers, ascending, once each.
 */
export const waterRegions = (m: Pick<WaterModel, "focus" | "region" | "regions">): string => {
  const all = [...new Set(m.regions)].sort((a, b) => a - b);
  return all.length > 1 ? `Regions ${all.join(", ")}` : waterRegion(m);
};

/** Every colour the pages use is a token of the app's one palette (packages/map/style), read from the generated themes. */
const T = (theme: "light" | "dark", token: string): string => {
  const v = (meta.themes as Record<string, Record<string, unknown>>)[theme]?.[token];
  if (typeof v !== "string") throw new Error(`theme "${theme}" has no colour ${token}`);
  return v;
};
const vars = (th: "light" | "dark"): string =>
  `--ink:${T(th, "color.ui.ink")};--card:${T(th, "color.ui.card")};--scrim:color-mix(in srgb,${T(th, "color.ui.shadow")} ${Math.round(Number((meta.themes as Record<string, Record<string, unknown>>)[th]?.["opacity.ui.scrim"]) * 100)}%,transparent);--muted:${T(th, "color.ui.sub")};--line:${T(th, "color.ui.line2")};--accent:${T(th, "color.ui.accent")};` +
  `--accent-ink:${T(th, "color.ui.on-accent")};--open:${T(th, "color.status.open")};--closed:${T(th, "color.status.closed")};--soft:${T(th, "color.ui.wash")};`;
export const SHARE_GROUND = T("light", "color.ui.accent");
/** (T406) the share card: its size (the size Facebook and X ask for a large-image card) and its face (share.ts loads it) */
export const SHARE_W = 1200, SHARE_H = 630, SHARE_FONT = "Archivo";
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
.gv .gfoot{margin-top:2.5rem;padding-top:1.2rem;border-top:1px solid var(--line)}.gv .gfoot .gl{margin:0 0 .2rem;font-weight:600;color:var(--ink)}.gv .gfoot .app{display:inline-flex;align-items:center;box-sizing:border-box;min-height:44px;margin:.9rem 0 1.6rem}
.card{border:1px solid var(--line);padding:.8rem 1rem;margin:1rem 0}.card>p{margin:0 0 .5rem}.card .app{border-radius:0;margin:0}.opens{display:flex;flex-wrap:wrap;gap:.5rem}#gauge-get-app{margin-top:1rem}.card h2{margin-top:0}
.lead{display:grid;gap:1rem;align-items:start;margin:.6rem 0 1rem}@media(min-width:44rem){.lead{grid-template-columns:minmax(0,1fr) minmax(0,1fr)}}
.mapcard{position:relative;border:1px solid var(--line);background:var(--soft)}.mapcard .tag2{position:absolute;top:.4rem;left:.4rem;font-size:.8rem;background:var(--card);color:var(--muted);padding:.1rem .4rem}
.mapcard .dots{display:block;width:100%;height:auto;max-height:26rem}.mapcard .dots path{fill:none;stroke:var(--accent);stroke-width:2.4;stroke-linecap:round;opacity:.75}
.mapcard .app{display:block;margin:0;border-radius:0;text-align:center;padding:.85rem 1rem}
.find label{display:block;font-weight:600;margin-bottom:.3rem}.find input{box-sizing:border-box;width:100%;font:inherit;padding:.65rem .7rem;border:1.5px solid var(--ink);border-radius:0;background:var(--card);color:var(--ink)}
.sr{position:absolute;width:1px;height:1px;margin:-1px;padding:0;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0}.sugg{list-style:none;margin:0;padding:0;border:0}.sugg:not(:empty){border:1px solid var(--line);border-top:0}.sugg li{margin:0;border-top:1px solid var(--line)}.sugg li:first-child{border-top:0}.sugg a{display:block;padding:.6rem .7rem;text-decoration:none;color:var(--ink)}.gres .gh{font-size:.9rem;font-weight:600;color:var(--muted);margin:1.3rem 0 .4rem}.gres .sugg a:hover,.gres .sugg a:focus-visible{background:var(--soft)}.gres .sugg a{padding:.7rem .8rem .15rem}.gres .sugg li{background:var(--card)}.find input:focus{outline:2px solid var(--accent);outline-offset:2px}.find .row{margin-bottom:.2rem}.find .said{margin:.5rem 0 0}.gres .gsep{font-size:.8rem;color:var(--muted);padding:.4rem .8rem;background:var(--soft)}.gres .sugg .tag{display:block;padding:0 .8rem .7rem;font-size:.85rem;color:var(--muted)}.mapline{margin:1rem 0 1.4rem}.mapline p{color:var(--muted)}.mapline .app{margin:0;display:block;text-align:center;flex:1}
.yb svg{display:block;width:100%;height:auto}.yb text{font-size:10px;fill:var(--muted)}.yb .o{fill:var(--open)}.yb .c{fill:var(--closed)}.yb ul{margin:.4rem 0 0}
.gv details{margin:.5rem 0}.gv summary{cursor:pointer;font-weight:600}.gv .wxfold,.gv .keyfold,.gv .fdfold{border-top:1px solid var(--line);padding-top:.4rem}.gv .keyfold summary,.gv .fdfold summary{font-weight:400;font-size:.9rem;color:var(--muted)}
.gv h2{font-size:1.1rem;margin:1.2rem 0 .3rem}
.warn{border-left:3px solid var(--accent);padding:.3rem .7rem;background:var(--soft)}
.gv #gauge-chart{margin:.4rem 0;min-height:550px}
.wx{border-collapse:collapse;width:100%;font-size:.9rem}.wx th,.wx td{text-align:left;padding:.25rem .4rem;border-bottom:1px solid var(--line)}
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

/** (T1042) what a description says when the page holds no fact: shown in budget.json (`descriptionsWithoutFacts`), never silent */
export const FACTLESS_TAIL = "its season, what you can keep, gear and limits, from the B.C. synopsis.";
const FACTLESS_SHORT = "regulations from the B.C. synopsis.";
const DESC_MAX = 155;
/** a sentence naming the build day or a date: a description is written once a release (T622 G2), so these are never in it */
const DAY_WORDS = /\b(today|tomorrow|tonight|now|until|then)\b|\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.? \d/i;

/** day index of the calendar year (0 = Jan 1, the strip's own order) as "Jun 15": the calendar's own BOOK_DAYS and month words */
function mdOf(i: number): string {
  const d = BOOK_DAYS[((i % 365) + 365) % 365]!;
  return `${MONTHS[d.month - 1]!.slice(0, 3)} ${d.day}`;
}

/**
 * (T1042, owner 2026-11-23 "Season windows") THE WINDOWS A PART IS OPEN TO FISHING, from its year strip (the presenter's own
 * closed / open runs over the calendar year; a run that ends Dec 31 and one that starts Jan 1 are one): "Jun 1–Jul 14".
 * null: no strip, or a run the strip does not know. The strip is the water's, not a species'.
 */
export function openWindows(runs: PartModel["year"]): string[] | null {
  if (!runs || runs.length === 0 || runs.some((r) => r.closed === null)) return null;
  const spans: { open: boolean; a: number; b: number }[] = [];
  let at = 0;
  for (const r of runs) { spans.push({ open: !r.closed, a: at, b: at + r.days - 1 }); at += r.days; }
  if (spans.length > 1 && spans[0]!.open === spans[spans.length - 1]!.open) { const last = spans.pop()!; spans[0]!.a = last.a - 365; }
  return spans.filter((x) => x.open).map((x) => (x.b - x.a >= 364 ? "all year" : x.a === x.b ? mdOf(x.a) : `${mdOf(x.a)}–${mdOf(x.b)}`));
}

/** The marks the strip holds on EVERY open day of the year ("fly fishing only", "bait ban"): the legend's marks whose bit every open run carries. */
function steadyMarks(p: PartModel): string[] {
  if (!p.yearBits) return [];
  const open = p.yearBits.filter((r) => r.bits === null || (r.bits & 1) === 0);
  if (open.length === 0 || open.some((r) => r.bits === null)) return [];
  return p.marks.filter((k) => k.bit > 1 && open.every((r) => ((r.bits ?? 0) & k.bit) !== 0)).map((k) => k.label);
}

/**
 * (T1042) THE FACTS OF A PAGE, in the page's own words, each TRUE ON EVERY DAY OF THE YEAR (the independent review built the
 * fixture on 24 days: Other fish rows and the Hooks line are the build day's, so a description from them changed with the day).
 * What is year-stable and read here: the open windows (`openWindows`, the strip's year), the vessel rules (the Boats section,
 * the same on every day: site.test.ts sweeps a whole year to prove it) and the strip's marks that hold on every open day.
 * Only what every part says, no dated line, no day word.
 */
export function factsOf(m: Pick<WaterModel, "parts">): string[] {
  if (m.parts.length === 0) return [];
  const out: string[] = [];
  const trim = (t: string) => t.replace(/[.\s]+$/, "");
  const wins = m.parts.map((p) => openWindows(p.year));
  if (wins.every((w) => w !== null) && new Set(wins.map((w) => w!.join("|"))).size === 1) {
    const w = wins[0]!;
    if (w.length === 0) out.push("Closed all year");
    else if (w[0] === "all year") out.push("Open all year");
    else if (w.length <= 4) out.push(`Open ${w.join(", ")}`);
  }
  const boats = (p: PartModel) => p.sections.filter((s) => s.title === "Boats").flatMap((s) => s.items).filter((i) => !i.head && !i.dated && !DAY_WORDS.test(i.text)).map((i) => trim(i.text));
  const first = boats(m.parts[0]!).find((b) => m.parts.every((p) => boats(p).includes(b)));
  if (first) out.push(first);
  const marks = m.parts.map(steadyMarks);
  out.push(...marks[0]!.filter((k) => marks.every((x) => x.includes(k))));
  return out;
}

/**
 * The meta description of a water page (and its og:description). (T622 G2) NO DAY-RELATIVE WORD: it is written once a
 * release. (T1042) It was one template on every page, which Google ignores
 * (developers.google.com/search/docs/appearance/snippet: identical descriptions on every page are not helpful), so it took a
 * passage of the page instead, the licence quote. Now: the water's name and place, then `factsOf` while they fit in 155
 * characters, a whole fact or none. A long name drops "fishing regulations", then "B.C.", before it drops a fact; with no
 * fact the text says so in whole words (`descriptionHasFacts` is how the build counts them), and is never cut mid-sentence.
 */
export function descriptionOf(m: WaterModel, label: string = m.name, town: string | null = null): string {
  const where = town ? `near ${town}, ${waterRegions(m)}` : waterRegions(m);
  const heads = [`${label} fishing regulations, B.C. (${where}): `, `${label}, B.C. (${where}): `, `${label} (${where}): `];
  const facts = factsOf(m);
  for (const head of heads) {
    const got: string[] = [];
    for (const f of facts) if ((head + [...got, f].join("; ")).length + 1 <= DESC_MAX) got.push(f);
    if (got.length) return `${head}${got.join("; ")}.`;
  }
  for (const head of heads) for (const tail of [FACTLESS_TAIL, FACTLESS_SHORT]) if ((head + tail).length <= DESC_MAX) return head + tail;
  return clip(`${label} (${where})`);   // a name too long for any whole sentence: the name, cut at a word
}
/** does the description hold a fact (the build's count of those that do not) */
export const descriptionHasFacts = (m: Pick<WaterModel, "parts">): boolean => factsOf(m).length > 0;

export function aboutBody(site: Site, asOf: string): string {
  const contact = site.contact ? `<p>Contact: <a href="mailto:${esc(site.contact)}">${esc(site.contact)}</a></p>` : "";
  return `<nav class="crumbs"><a href="/">Home</a> › About</nav><h1>About ${esc(site.name)}</h1>` +
    `<p class="asof">Last built <time datetime="${esc(asOf)}">${esc(asOf)}</time>.</p>` +
    `<p>${esc(site.name)} is a free, non-profit app that tells you whether a BC freshwater water is open to fishing, and what you can keep, in plain words.</p>` +
    `<h2>Where the rules come from</h2><p>The rules are read from the Province of British Columbia’s Freshwater Fishing Regulations Synopsis. ` +
    `River gauges come from Environment and Climate Change Canada and the Water Survey of Canada; salmon notices from Fisheries and Oceans Canada; ` +
    `stocking records from the Freshwater Fisheries Society of BC. Their terms of use are respected.</p>` +
    `<p>Every page on this site is built from the same release as the app, so the two never disagree.</p>` +
    `<div class="legal" data-nosnippet><p>${esc(NOT_LEGAL)} <a href="${esc(SYNOPSIS_PDF)}" rel="noopener">Official synopsis (PDF)</a>.</p></div>` +
    `<p>If a rule looks wrong, tell me; every report gets a reply and a fix.</p>${contact}` +
    `<p><a class="app" href="${esc(site.origin)}/">Open the app</a></p>`;
}

/** where privacy questions go: a placeholder until the owner sets the mailbox up (an owner ask in its ticket) */
export const PRIVACY_EMAIL = "privacy@canifishthis.ca";
export const PRIVACY_EFFECTIVE = "2026-11-19";

/** (owner 2026-10-29) THE PRIVACY POLICY. Every claim here is checked against the code (see T268): change the app, change this. */
export function privacyBody(site: Site, asOf: string): string {
  const mail = `<a href="mailto:${PRIVACY_EMAIL}">${PRIVACY_EMAIL}</a>`;
  return `<nav class="crumbs"><a href="/">Home</a> › Privacy policy</nav><h1>Privacy policy</h1>` +
    `<p class="asof">Effective ${PRIVACY_EFFECTIVE}. Page last built <time datetime="${esc(asOf)}">${esc(asOf)}</time>.</p>` +
    `<p>${esc(site.name)} is a free, non-profit app and website that shows BC freshwater fishing regulations. ` +
    `It has no accounts, no ads and no tracking. This page says what the app and this website do with your information.</p>` +
    `<h2>The short version</h2><ul>` +
    `<li>Your photos and the spots you save stay on your device, and the app never sends me your GPS position. Two things that can show roughly where you were do leave it, both explained below: a saved spot’s place, rounded to about 1 km, goes to a weather service, and a problem report you choose to send includes the middle of the map you were looking at.</li>` +
    `<li>The app sends me three things: a problem report, only when you press Send; unless you switch it off, a short error report when the app itself fails; and, on an iPhone with notifications on, a push token so I can tell the app to check for new data.</li>` +
    `<li>I do not sell or share your information, and I use no advertising. The website counts visits with Cloudflare Web Analytics, which sets no cookies and records no identifier; the phone app does not use it.</li></ul>` +
    `<h2>Location</h2><p>If you allow it, the app uses your location while it is open, to show where you are on the map, ` +
    `to show what is near you, and to pin a spot where you are standing. It is used on your device and is not sent to me. Two things described below can show roughly where you were: the weather request for a spot you pinned where you stood, and the map centre in a problem report you choose to send. ` +
    `The app never uses your location in the background. You can turn the permission off in your phone settings; the map still works.</p>` +
    `<h2>Camera and photos</h2><p>The app asks for the camera or your photos only so you can attach a picture to a spot you saved. ` +
    `The pictures stay on your device and are not uploaded.</p>` +
    `<h2>Saved spots</h2><p>Spots, notes and photos are stored only on your device, and there is no account. ` +
    `If you delete the app or clear its data, they are gone; I cannot recover them.</p>` +
    `<h2>Weather for a spot</h2><p>To show the weather at a spot you saved, the app sends that spot’s coordinates, rounded to about 1 km, and the date to ` +
    `<a href="https://open-meteo.com" rel="noopener">Open-Meteo</a>, a weather service, and shows what comes back. ` +
    `If you pinned the spot where you were standing, that is roughly where you were. ` +
    `Open-Meteo’s servers see your device’s IP address as any website would; I do not control their records. ` +
    `Nothing else about you is sent with the request.</p>` +
    `<h2>Downloads from my servers</h2><p>The app downloads the regulations, the map and river-gauge readings (including past days’ flow) and salmon notices from my servers, ` +
    `which run on Cloudflare. Each request carries the app’s version and, from the phone app, your phone’s system name and version (so I can tell old versions from new) ` +
    `and, as with any web request, your IP address, which Cloudflare handles to serve the request. ` +
    `These downloads are the same for everyone and do not include your location, your spots or any identifier. ` +
    `My servers, not the app, fetch the underlying data from Environment and Climate Change Canada and Fisheries and Oceans Canada. ` +
    `Looking for a newer version of the app itself goes through the store it came from: on an iPhone from the App Store, the app asks Apple’s App Store which version is current, and Apple sees that request and your IP address as any website would; on Android from Google Play, the check is Google Play’s own.</p>` +
    `<h2>Visit counts on the website</h2><p>The website (not the phone app) loads Cloudflare Web Analytics, which counts page visits so I can see how the site is used. ` +
    `It sets no cookies, stores nothing on your device and records no identifier; Cloudflare sees your IP address as it does for any request. ` +
    `Cloudflare may also run a short bot check on the website to keep automated traffic away.</p>` +
    `<h2>Map and satellite imagery</h2><p>The map is drawn from files on my servers. Its fonts and symbols come from my servers too. When you turn satellite imagery on, the app asks Esri for the picture tiles of the area ` +
    `you are looking at, so Esri sees those tile requests and your IP address. Imagery is shown only while online. ${IMAGERY_KEPT_PRIVACY}</p>` +
    `<h2>Sharing and directions</h2><p>Share makes a link to the water you are viewing (the water, the view, the place on the map and the day) and hands it to the app you pick. Directions opens Apple Maps on an iPhone, and Google Maps on Android and the web, with a spot’s parking point. Both happen only when you tap them, and what the other app does with what it receives follows that app’s own policy.</p>` +
    `<h2>Reporting a problem</h2><p>Sending a report is your choice, and the form shows exactly what will go. A report holds:</p><ul>` +
    `<li>the kind of problem and the words you wrote;</li>` +
    `<li>the details under “Include with your report” that you leave switched on (the water, part and section you were viewing, the day, the screen, ` +
    `the map colouring, the map’s layer, forecast day, zoom and centre (the middle of the map you were looking at, to about 100 m; if the map was centred on you, that is about where you were), the app version, data release, platform, ` +
    `the install id described below, and recent app errors);</li>` +
    `<li>your email address, only if you type one. It is used only to reply, and only if you tick “You may contact me”. It is stored in the same record as the install id, so a report with an email can be connected to you (and, through the same install id, to the error reports that install sent).</li></ul>` +
    `<p>Reports are stored privately on Cloudflare, are read only by me, and are not published. ` +
    `I do not store your IP address with a report: to limit abuse I keep only a salted one-way hash of it, which cannot be turned back into an address. ` +
    `Reports are deleted automatically 180 days after they are received, including any email in them. ` +
    `To have your report or email deleted sooner, write to ${mail}.</p>` +
    `<h2>App errors (crash reports)</h2><p>When the app hits an error it did not expect, it sends one short report to my own server on Cloudflare. ` +
    `I use no third-party crash-reporting service. This is on by default and you can turn it off at any time in More → Settings → Privacy → “Send crash reports”; ` +
    `when it is off nothing is sent. An error report holds only:</p><ul>` +
    `<li>the app version, whether it is a test or live build, your phone’s system name and version (or “web”), and the data release the app was using;</li>` +
    `<li>the kind of screen you were on — map, conditions, runs, spots, menu or a water page — and, when a water was open, that water’s id number in my data ` +
    `and how its page was set (which view and part, whether the whole water was shown, whether a search or a spot was open); never the water’s name, where you were or what you searched for;</li>` +
    `<li>an install id: a random number the app makes once on your device, so I can tell whether several errors came from one install or from many. ` +
    `It is not your device’s identifier or advertising identifier and is made from nothing about you or your device; the same id goes with a problem report you send, and if you type an email address in that report it is stored with the id in one record (see Reporting a problem). ` +
    `Uninstalling the app (or clearing its data) deletes it, and a new one is made;</li>` +
    `<li>the error itself: its type, its message with any quoted text, web address, e-mail address, coordinates and long numbers removed, and the app’s own code trace (file names and line numbers);</li>` +
    `<li>whether the error stopped the app, whether the phone closed the app while it was in the background, and the time.</li></ul>` +
    `<p>On Android the app also asks the phone, the next time it starts, why its last run ended. If the answer was a crash, a freeze (“app not responding”), ` +
    `or the system closing the app for memory or resource use, the app sends that reason, how busy the app was (foreground or background), ` +
    `how much memory it was using, the exit code and the time. It reads no crash dump or memory contents, and no screen or water is named for it. ` +
    `When the phone closed the app in the background to free memory, the report says so and is not counted as a crash. ` +
    `This follows the same switch and the same rules below.</p>` +
    `<p>An iPhone keeps no such record for the app, so when the app itself crashes on an iPhone it writes a short note on the device at that moment: ` +
    `the kind of fault (its signal, or the error’s type and message), whether the app was in front, how much memory it was using and the time. ` +
    `The next time it starts, the app sends that note under the same switch and the same rules below; it holds no crash dump, memory contents, screen or water.</p>` +
    `<p>It never includes your location, spots, photos, notes, searches, e-mail address or any device or advertising identifier (only the random install id above). ` +
    `Each different error is sent once per time the app runs, and only while you are online; an error that happens offline is not saved to send later, ` +
    `except one that stops the app, which is kept on your device and sent the next time the app starts (and deleted from your device if you have switched crash reports off). ` +
    `I keep one stored record per kind of error, with a count, the app versions and the install ids it was seen in; I do not store your IP address, ` +
    `and to limit abuse I keep only a salted one-way hash of it for a day’s counting. Error records are deleted automatically 90 days after the error was last seen.</p>` +
    // (T739) the push token: the owner's 2026-11-15 line ("Yes to both"), then his 2026-11-20 09:39 pick "Claude's edit":
    // iPhone / Apple only (no Android push), "I/my", and what the server keeps said whole. Change the app, change this.
    `<h2>Push token</h2><p>On an iPhone, when notifications are on and at least one is set (a gauge alert or “New regulations data”), the app sends my server a push token for this phone: an address Apple issues so a message can reach the app. ` +
    `With it I keep only whether it is for Apple’s test or live push service and when it was registered. ` +
    `I use it only to send, through Apple’s push service, a silent signal telling the app to check for new data and, when a new data release is out and your download setting allows it, to start downloading it; ` +
    `the signal carries no words, no waters and no location. I do not receive your alerts, the waters you watch or where you are. ` +
    `I do not store your IP address with it: to limit abuse I keep only a salted one-way hash of it for a day’s counting. ` +
    `The token is deleted when you turn notifications off, or when Apple tells me it no longer works. ` +
    // (T1098 g) owner 2026-11-25: option 2, built by T1116 (PR #194): the page may go live only with or after the 0.9.1 app and the feeds worker deploy of T1116.
    `From version 0.10.0, the app sends the token only while this phone lets it show notifications and at least one is on; if you turn notifications off for the app in the phone’s settings, the app tells my server to delete the token the next time you open it. A token the app has not sent again for 30 days is deleted (the app sends it again about once a week when you open it), and it is not used after those 30 days. ` +
    `It is not linked to you or to an account and is not backed up.</p>` +
    `<h2>This website</h2><p>The website serves plain pages. It sets no cookies and runs no advertising or tracking scripts; the scripts it runs are these: the visit counter described above (Cloudflare Web Analytics); on a river gauge page, the page’s own script, which reads that gauge’s latest flow and the weather forecast when the page opens, the script for its chart, which reads that gauge’s readings from my server, and the “Join the test” card’s script; on the river gauges page, a script for its gauge search, which reads the list of gauges from this website and the gauges’ latest readings from my server; and on a region page, a script for its water search, which reads that region’s list of water names from this website when you start typing. None of them sets a cookie or sends what you type or where you are; if one of them fails, a short error record (never what you typed) may be sent to my server, handled as the app’s error records described above. ` +
    `Cloudflare, which hosts it, sees ordinary request information such as your IP address.</p>` +
    `<h2>No selling, no sharing</h2><p>I do not sell your information, and I do not share it for advertising. ` +
    `The services named above (Cloudflare, Open-Meteo, Esri, Apple for push and for the App Store version check, and Google Play for its update check) receive only the requests described here.</p>` +
    `<h2>Children</h2><p>The app is not aimed at children under 13 and does not knowingly collect their information. ` +
    `It asks for none beyond the optional report.</p>` +
    `<h2>Changes</h2><p>If this policy changes, this page will carry the new text and a new effective date.</p>` +
    // (owner 2026-10-29) Google Play's "delete data URL": the app's name, the steps, what is deleted and what is kept
    `<h2 id="delete">Deleting your data (Can I Fish This?)</h2>` +
    `<p>Spots, photos, notes and settings live only on your device: delete them in the app, or uninstall the app, and they are gone.</p>` +
    `<p>To delete a problem report you sent (and any email address in it): write to ${mail} from that address, or with the date ` +
    `and the words of the report, and ask for it to be deleted. I delete it within 30 days and reply to confirm.</p>` +
    `<p>Crash reports hold no name, email or place, only the random install id, which on its own I cannot connect to a person. The one way it can be connected is a problem report you send with your email address: that report holds the email and the same install id in one record. Crash reports are deleted automatically 90 days after the error was last seen. ` +
    `Problem reports I am not asked to delete are deleted automatically 180 days after they are received. I keep nothing else about you.</p>` +
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
  // (T406) a water's card is a 1200 x 630 PNG (share.ts): its size and type said, so a preview need not fetch it to lay it out
  const img = s.image ? `<meta property="og:image" content="${esc(site.origin + s.image)}">` +
      (s.image.endsWith(".png") ? `<meta property="og:image:type" content="image/png"><meta property="og:image:width" content="${SHARE_W}"><meta property="og:image:height" content="${SHARE_H}">` : "") +
      `<meta name="twitter:card" content="summary_large_image"><meta name="twitter:image" content="${esc(site.origin + s.image)}">`
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
    `<main>${s.body}</main><footer><span data-nosnippet>${esc(NOT_LEGAL)}</span> <a href="/privacy">Privacy policy</a></footer></body></html>`;
}
