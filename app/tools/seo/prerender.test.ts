/**
 * T269 — the string work that makes the app's renders one water page: themes merged into CSS variables, the layouts into
 * one tree, the header's controls as links, the app's document around it. Each rule with the fault it must refuse.
 */
import { describe, expect, it } from "vitest";
import { LayoutStyles, mergeLayouts, parse } from "./layouts";
import { appDocument, asLink, HANDOFF, linkHeader, mergeThemes, ThemeVars } from "./prerender";
import { BARE_APP } from "./build";

describe("mergeThemes", () => {
  it("one markup: a colour the themes share stays, one they do not is a variable; paint attributes move into style", () => {
    const v = new ThemeVars();
    const light = '<div style="color:#111111;border-color:#cccccc">Open</div><svg><rect fill="#ffffff"></rect></svg>';
    const dark = '<div style="color:#eeeeee;border-color:#cccccc">Open</div><svg><rect fill="#000000"></rect></svg>';
    expect(mergeThemes(light, dark, v)).toBe('<div style="color:var(--c0);border-color:#cccccc">Open</div><svg><rect style="fill:var(--c1)"></rect></svg>');
    expect(v.css()).toBe(":root{--c0:#111111;--c1:#ffffff}@media (prefers-color-scheme:dark){:root{--c0:#eeeeee;--c1:#000000}}");
    expect(v.name("#111111", "#eeeeee")).toBe("--c0");   // the same pair is the same variable everywhere
  });
  it("MUTATION: renders that differ in words or structure are refused (the caller ships both)", () => {
    const v = new ThemeVars();
    expect(mergeThemes("<p>Open</p>", "<p>Closed</p>", v)).toBeNull();
    expect(mergeThemes('<p class="a">x</p>', '<p class="b">x</p>', v)).toBeNull();
    expect(mergeThemes("<p>x</p>", "<p>x</p><p>y</p>", v)).toBeNull();
  });
});

describe("mergeLayouts", () => {
  const P = '<div class="v"><div data-testid="head">N</div><div class="s"><p>a</p><p>b</p></div></div>';
  // the tablet wraps the page in a reading column and adds an overlay; the desktop also changes a style
  const T = '<div class="v"><div data-testid="head">N</div><div class="s"><div class="w" data-testid="reading"><p>a</p><p>b</p></div><div class="o"></div></div></div>';
  it("one tree: a wrapper only one layout draws is transparent on the others, a node only it draws hidden on the others", () => {
    const out = mergeLayouts(new Map([["phone", P], ["tablet", T]]), new LayoutStyles());
    expect(out).toBe('<div class="v"><div data-testid="head">N</div><div class="s"><div class="w vc-phone" data-testid="reading"><p>a</p><p>b</p></div>' +
      '<div class="o vh-phone"></div></div></div>');
  });
  it("a style that differs by layout becomes per-layout classes under that layout's width", () => {
    const s = new LayoutStyles();
    const out = mergeLayouts(new Map([["phone", '<p style="a:1">x</p>'], ["desktop", '<p style="a:2">x</p>']]), s);
    expect(out).toBe('<p class="ls0 ls1">x</p>');
    expect(s.css()).toBe("@media (max-width:899.98px){#prerender .ls0{a:1}}@media (min-width:1280px){#prerender .ls1{a:2}}");
  });
  it("text only one layout draws is wrapped to be hidden; React's text separators are kept bare", () => {
    expect(mergeLayouts(new Map([["phone", "<p>a<!-- -->b</p>"], ["desktop", "<p>a<!-- -->c</p>"]]), new LayoutStyles()))
      .toBe('<p>a<!-- --><span class="vh-desktop">b</span><span class="vh-phone">c</span></p>');
  });
  it("MUTATION: markup it cannot read is refused, not guessed", () => {
    expect(() => parse("<div><p>x</div>", "phone")).toThrow(/closes/);
    expect(() => parse("<div>", "phone")).toThrow(/never closed/);
  });
});

describe("the header's controls and the document", () => {
  it("‹ and × are links to the map at the water, every drawing of them; the rest untouched", () => {
    const h = '<div role="button" style="min-height:32px" data-testid="water-back"><div>Map</div></div><div role="button" data-testid="water-close"><svg></svg></div>' +
      '<div role="button" data-testid="water-back"><div>Map</div></div>';
    expect(linkHeader(h, "gnis:3062")).toBe('<a href="/?show=gnis:3062" style="min-height:32px;text-decoration:none" data-testid="water-back"><div>Map</div></a>' +
      '<a href="/?show=gnis:3062" data-testid="water-close" style="text-decoration:none"><svg></svg></a>' +
      '<a href="/?show=gnis:3062" data-testid="water-back" style="text-decoration:none"><div>Map</div></a>');
    expect(asLink("<p>none</p>", "water-back", "/")).toBe("<p>none</p>");
  });
  it("the app's own index.html: its head replaced, the map's stylesheets not blocking, the pane before its root, the hand-over", () => {
    const t = '<html lang="en"><head><title>canifishthis</title><link rel="preload" href="/m.css" as="style">\n<link rel="stylesheet" href="/m.css"></head>' +
      '<body><noscript>JS</noscript><div id="root"></div><script src="/app.js" defer></script></body></html>';
    const d = appDocument(t, { head: "<title>X</title>", critical: "#prerender{}", cssHref: "/assets/pane.1.css", preload: ["/f.ttf"], item: "gnis:1", pane: "<div>pane</div>" });
    expect(d).toBe('<html lang="en-CA"><head>\n<link rel="stylesheet" href="/m.css" media="print" onload="this.media=\'all\'"><title>X</title>' +
      '<meta name="cift:item" content="gnis:1"><link rel="preload" href="/f.ttf" as="font" type="font/ttf" crossorigin><style>#prerender{}</style>' +
      '<link rel="stylesheet" href="/assets/pane.1.css"></head><body><div id="prerender"><div>pane</div></div><div id="root"></div>' +
      `${HANDOFF}<script src="/app.js" defer></script></body></html>`);
    expect(() => appDocument("<html></html>", { head: "", critical: "", cssHref: "", preload: [], item: "", pane: "" })).toThrow(/no <title>/);
    expect(appDocument(BARE_APP, { head: "", critical: "", cssHref: "/c.css", preload: [], item: "gnis:1", pane: "" })).toContain('<div id="prerender"></div><div id="root"></div>');
  });
});
