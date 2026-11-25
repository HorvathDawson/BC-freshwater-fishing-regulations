/**
 * (T1107) THE /gauges SEARCH, in the browser: the form the site build writes (`gaugeFindForm`), filled by this script. The
 * search is core's index over the gauge file (gaugeSearch.ts); this file only reads the file on first use and draws the
 * answer. Ships as one plain script (gauges.ts `gaugeFindScript`: esbuild, with the page reporter written in before it).
 *
 * Failures are said on the page AND reported (W46): a file that cannot be read says so and points to the list below (reported
 * once; an environment failure, no connection, is only said); a 404 (a release was cut since the page loaded) says to reload
 * and is reported as "gauge search file out of date (HTTP 404)", a kind the worker classes as expected (as the region find's).
 */
import { answerGauges, DATA_WORDS, dataStateOf, gaugeFinder, kmWords, type DataState, type GaugeAnswer, type GaugeFinder, type GaugeHit, type GaugeSearchFile, type LiveIndex } from "./gaugeSearch";

type Cfg = { crash: string | null; env: string; release: string; live: string; app?: string; route?: string };
export const LIVE_FAILED = "Whether each gauge has current data could not be checked just now.";

export const STALE_FILE = "gauge search file out of date (HTTP 404)";
export const NO_MATCH = (q: string): string => `No gauge, river, lake or place matches “${q}”. Every gauge is in the list below.`;
export const LOAD_FAILED = (why: string): string => `The gauge search could not be loaded (${why}). Every gauge is in the list below.`;

/** the page's form markup: hidden until this script runs, so a browser without it sees the list alone */
export function gaugeFindForm(indexHref: string, cfg: Omit<Cfg, "app" | "route">): string {
  const a = (s: string) => s.replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;");
  return `<div class="find"><form data-index="${a(indexHref)}" data-cfg="${a(JSON.stringify(cfg))}" role="search" hidden>` +
    `<label for="gf-q">Find a gauge near a place, a river or a lake</label>` +
    `<div class="row"><input id="gf-q" type="search" autocomplete="off" placeholder="Town, river, lake or gauge number"></div>` +
    `<div class="gres"></div><p class="said small"></p><p class="sr" role="status" aria-live="polite"></p></form></div>`;
}

const el = (doc: Document, tag: string, text?: string, cls?: string): HTMLElement => {
  const e = doc.createElement(tag); if (text !== undefined) e.textContent = text; if (cls) e.className = cls; return e;
};

/** the tag line under a gauge's name: its number, its water, and how far it is (when a place or a water was asked) */
export function hitTag(h: GaugeHit, titleWater: string | null, state: DataState | null = null): string {
  return [h.station, h.water && h.water !== titleWater ? h.water : null, h.km === null ? null : `${kmWords(h.km)} away`, state ? DATA_WORDS[state] : null].filter(Boolean).join(" · ");
}

export function drawAnswer(doc: Document, into: HTMLElement, a: GaugeAnswer, live: LiveIndex | null = null): void {
  into.textContent = "";
  for (const g of a.groups) {
    into.appendChild(el(doc, "p", g.kind === "place" ? `Nearest gauges to ${g.title}` : g.kind === "water" ? `Gauges on ${g.title}` : g.title, "gh"));
    const ul = el(doc, "ul", undefined, "sugg");
    let othersLabelled = false;
    for (const h of g.hits) {
      if (g.kind === "water" && h.km !== null && !othersLabelled) {
        othersLabelled = true;
        const l = el(doc, "li", "Nearest others", "gsep"); ul.appendChild(l);
      }
      const li = doc.createElement("li"), a1 = el(doc, "a", h.name); a1.setAttribute("href", h.path);
      li.appendChild(a1); li.appendChild(doc.createTextNode(" ")); li.appendChild(el(doc, "span", hitTag(h, g.kind === "water" ? g.title : null, live ? dataStateOf(h.station, live) : null), "tag"));
      ul.appendChild(li);
    }
    into.appendChild(ul);
  }
}

/** mount the search; `report` is the page reporter (given the form's config); resolves when wired */
export function mountGaugeFind(doc: Document, fetchImpl: typeof fetch, reporter: (cfg: Cfg) => (e: unknown) => void,
                               loc: { href: string; reload: () => void }): void {
  const f = doc.querySelector<HTMLFormElement>("form[data-index]");
  if (!f) return;
  const cfg = JSON.parse(f.getAttribute("data-cfg")!) as Cfg;
  cfg.app = "gauges-page"; cfg.route = "other";
  const report = reporter(cfg);
  const q = f.querySelector("input")!, res = f.querySelector<HTMLElement>(".gres")!, said = f.querySelector<HTMLElement>(".said")!, sr = f.querySelector<HTMLElement>(".sr")!;
  let finder: GaugeFinder | null = null, live: LiveIndex | null = null, liveSaid = "", tried = false;
  f.hidden = false;
  const say = (t: string) => { said.textContent = t; sr.textContent = t; };
  const draw = () => {
    const v = q.value.trim(); res.textContent = ""; say("");
    if (v.length < 2 || !finder) return;
    const a = answerGauges(finder, v);
    if (!a.groups.length) { say(NO_MATCH(v)); return; }
    drawAnswer(doc, res, a, live);
    if (liveSaid) say(liveSaid);
    const n = a.groups.reduce((s, g) => s + g.hits.length, 0);
    sr.textContent = `${n} ${n === 1 ? "gauge" : "gauges"} found.`;
  };
  const stale = () => {
    said.textContent = ""; sr.textContent = "This page is out of date: reload it.";
    said.appendChild(doc.createTextNode("This page is out of date: "));
    const a = el(doc, "a", "reload it"); a.setAttribute("href", loc.href);
    a.addEventListener("click", (e) => { e.preventDefault(); loc.reload(); });
    said.appendChild(a); said.appendChild(doc.createTextNode("."));
  };
  const load = () => {
    if (tried) return; tried = true; say("Loading the gauges…");
    // the app's live index (the feed the Conditions tab reads): which gauges have current data. A failure is said and reported
    // (the results still show, without the words), never silently dropped
    const liveP = fetchImpl(cfg.live).then((r) => { if (!r.ok) throw new Error(`live gauge index ${r.status}`); return r.json() as Promise<LiveIndex>; })
      .then((j) => { if (!j || typeof j.fetchedAt !== "string" || !j.stations) throw new Error("live gauge index: no fetchedAt or stations"); live = j; },
            (e: Error) => { liveSaid = LIVE_FAILED; report(e); });
    fetchImpl(f.getAttribute("data-index")!)
      .then((r) => { if (r.status === 404) { const x = new Error("stale") as Error & { stale?: boolean }; x.stale = true; throw x; } if (!r.ok) throw new Error(`gauge search file ${r.status}`); return r.json() as Promise<GaugeSearchFile>; })
      .then((j) => { if (j.format !== "gauge-search/1") throw new Error(`gauge search file: format ${String(j.format)}, not gauge-search/1`); finder = gaugeFinder(j); say(""); return liveP.then(draw); })
      .catch((e: Error & { stale?: boolean }) => {
        if (e.stale) { stale(); report(new Error(STALE_FILE)); return; }
        say(LOAD_FAILED(e.message)); report(e);
      });
  };
  f.addEventListener("submit", (e) => { e.preventDefault(); const a = res.querySelector("a"); if (a) loc.href = a.getAttribute("href")!; });
  q.addEventListener("focus", load);
  q.addEventListener("input", () => { if (!finder) { load(); return; } draw(); });
}
