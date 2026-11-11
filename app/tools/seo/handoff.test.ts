// @vitest-environment jsdom
/**
 * T405 — THE HAND-OVER SCRIPT, run over a page that stands in for a water page: when the prerendered pane goes. It must
 * wait for the app's pane to be complete (every part the prerendered pane shows, then still), so the app's first frames —
 * a part arriving after the header — happen while it is drawn at 1 % (PRERENDER_CSS) and shift nothing the reader sees.
 * jsdom lays nothing out: a part's arrival is what changes the pane's boxes here (their count), frames are pumped by hand.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { afterPaint, HANDOFF, PRERENDER_CSS, QUIET_MS, SETTLE_FRAMES, SETTLE_MAX_MS } from "./prerender";

const BODY = /^<script type="module">([\s\S]*)<\/script>$/.exec(HANDOFF)![1]!;
let frames: FrameRequestCallback[] = [], clock = 0;
const pump = (n: number, ms = 16) => { for (let i = 0; i < n; i++) { clock += ms; const f = frames; frames = []; f.forEach((cb) => cb(clock)); } };
const settle = () => new Promise((r) => setTimeout(r, 0));   // the MutationObserver's callback is a microtask
const part = (id: string, inner = "") => `<div data-testid="${id}">${inner}</div>`;
const pane = (...parts: string[]) => part("regs-view", part("regs-header") + part("regs-scroll", parts.map((p) => part(p)).join("")));
const gone = () => !document.getElementById("prerender");

function page(prerendered: string, search = "") {
  history.replaceState(null, "", `/water/vedder-river${search}`);
  document.body.innerHTML = `<div id="prerender">${prerendered}</div><div id="root"></div>`;
  new Function(BODY)();
}
const app = (html: string) => { document.getElementById("root")!.innerHTML = html; };

beforeEach(() => {
  frames = []; clock = 0;
  vi.stubGlobal("requestAnimationFrame", (cb: FrameRequestCallback) => frames.push(cb));
  vi.spyOn(performance, "now").mockImplementation(() => clock);
  // jsdom has no layout: every element counts as drawn
  Object.defineProperty(HTMLElement.prototype, "offsetParent", { configurable: true, get() { return this.parentNode; } });
});
afterEach(() => { vi.unstubAllGlobals(); vi.restoreAllMocks(); delete (HTMLElement.prototype as { offsetParent?: unknown }).offsetParent; });

describe("T405: the prerendered pane goes when the app's pane is complete and still", () => {
  it("the app's root is drawn at 1 % until then (a transform: its layout untouched, its first paints a few pixels)", () => {
    expect(PRERENDER_CSS).toContain("#prerender~#root{transform:scale(.01);transform-origin:0 0}");
  });

  it("waits for every part the prerendered pane shows (the access strip arrives after the header), then SETTLE_FRAMES still frames", async () => {
    page(pane("keep-tab", "access-note"));
    app(pane("keep-tab"));
    await settle();
    pump(SETTLE_FRAMES + 5);
    expect(gone()).toBe(false);                       // still, but the access strip is not there yet
    app(pane("keep-tab", "access-note"));
    pump(SETTLE_FRAMES);
    expect(gone()).toBe(false);                       // it changed: the frames count again
    pump(1);
    expect(gone()).toBe(true);
  });

  it("a part the app does not draw (a live feed changed it) — handed over once the pane has been still for QUIET_MS", async () => {
    page(pane("keep-tab", "salmon-strip"));
    app(pane("keep-tab"));
    await settle();
    pump(Math.ceil(QUIET_MS / 16));                   // the first frame sees the pane; QUIET_MS later, less one frame
    expect(gone()).toBe(false);
    pump(1);
    expect(gone()).toBe(true);
  });

  it("a pane that never holds still is handed over after SETTLE_MAX_MS, said in the console", async () => {
    const said = vi.spyOn(console, "info").mockImplementation(() => {});
    page(pane("keep-tab", "salmon-strip"));
    app(pane("keep-tab"));
    await settle();
    for (let t = 0; t < SETTLE_MAX_MS - 32; t += 16) { app(pane("keep-tab", ...(t % 32 ? ["x"] : []))); pump(1); }
    expect(gone()).toBe(false);
    for (let i = 0; i < 4; i++) { app(pane("keep-tab", ...(i % 2 ? ["x"] : []))); pump(1); }
    expect([gone(), said.mock.calls.length]).toEqual([true, 1]);
  });

  it("nothing goes before the app's header is drawn", async () => {
    page(pane("keep-tab"));
    app(part("boot-slow"));
    await settle();
    pump(200);
    expect([gone(), frames.length]).toEqual([false, 0]);
  });

  it("the reader's first click (not on a link) hands over at once, once the app's pane is drawn; before it, nothing", async () => {
    page(pane("keep-tab", "access-note") + '<a href="/?show=gnis:3062" data-testid="water-back">Map</a>');
    const pre = document.getElementById("prerender")!;
    pre.querySelector('[data-testid="keep-tab"]')!.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    expect(gone()).toBe(false);
    app(pane("keep-tab"));
    await settle();
    pre.querySelector("a")!.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true }));
    expect(gone()).toBe(false);                       // the ‹ is a link to the map: it goes where it says
    pre.querySelector('[data-testid="keep-tab"]')!.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    expect(gone()).toBe(true);
  });

  it("a start on another view has no prerendered twin: it goes at once", () => {
    page(pane("keep-tab"), "?view=conditions");
    expect(gone()).toBe(true);
  });

  it("(T622 G6) nor has a start on a part, a place or a day the build did not draw; a bare or unrelated query keeps it", () => {
    for (const q of ["?date=2026-11-14", "?blk=360823421&m=1200", "?ix=2", "?view=regs&date=2026-12-01"]) {
      page(pane("keep-tab"), q);
      expect(gone(), q).toBe(true);
    }
    for (const q of ["", "?theme=dark", "?view=regs"]) {
      page(pane("keep-tab"), q);
      expect(gone(), q).toBe(false);
    }
  });
});

describe("T405: the app's script is asked for after the pane's first paint", () => {
  const run = (src: string) => new Function(/^<script type="module">([\s\S]*)<\/script>$/.exec(afterPaint(src))![1]!)();
  const asked = () => [...document.querySelectorAll("script[src]")].map((e) => e.getAttribute("src"));
  it("one frame after the first paint, once", async () => {
    document.body.innerHTML = "";
    run("/app.js");
    expect(asked()).toEqual([]);                      // not while the page parses: the pane paints without it
    pump(1);
    await new Promise((r) => setTimeout(r, 0));
    run("/app.js"); pump(1); await new Promise((r) => setTimeout(r, 0));
    expect(asked()).toEqual(["/app.js"]);
  });
  it("a page opened in a background tab (no frames) asks at once", () => {
    document.body.innerHTML = "";
    vi.spyOn(document, "visibilityState", "get").mockReturnValue("hidden");
    run("/app.js");
    expect(asked()).toEqual(["/app.js"]);
  });
});
