// @vitest-environment jsdom
/**
 * T44 — ONE FRAME FOR THE WATER SCREEN'S VIEWS, measured (owner 2026-10-21: "in desktop switching between regulation and
 * condition tab makes the margin on the left change and everything move").
 *
 * The real Shell is mounted on a water (the fixture bundle and the committed regs release) and the view strip is pressed
 * Regulations → Conditions → Map, as a reader does. After each switch the DOM react-native-web drew — its own CSS rules
 * included — is laid out by a real browser (headless Chrome over the DevTools protocol, `water-frame-measure.mjs`; jsdom
 * has no layout) at the three sizes the ticket names, and four things both views draw are measured:
 *
 *   back     the ‹ button              strip   the view strip (Regulations · Conditions · Map)
 *   title    the water's name row      left    the content's left edge (the leftmost line of text under the title)
 *
 * They must sit at the same WHOLE-pixel positions on every view. A MUTANT — the Conditions view's body given its own
 * side gutter, which is exactly what a view must not do (`tools/water-frame.test.ts` pins the source) — must fail it.
 *
 * Only the map canvas is replaced (no WebGL here): a plain box with no words, so it cannot move "left".
 */
import "./render-setup.ts";   // the render project's setup
import { createElement as h, type ReactNode } from "react";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";

/** The mutant: the Conditions body inside a gutter of its own. Off unless a test turns it on. */
const MUTANT = { on: false };

vi.mock("../packages/ui-native/src/MapScreen", async () => {
  const { View } = await import("react-native");
  return { MapScreen: () => h(View, { testID: "map-canvas", style: { flex: 1 } }) };
});
// the Conditions view's route map and minimap are the same renderer: a box, sized as the view sizes it
vi.mock("@app/map", async (importOriginal) => {
  const real = await importOriginal<typeof import("@app/map")>();
  const { View } = await import("react-native");
  return { ...real, Map: () => h(View, { testID: "map-canvas", style: { flex: 1 } }) };
});
vi.mock("../packages/ui-native/src/ConditionsPanel", async (importOriginal) => {
  const real = await importOriginal<typeof import("../packages/ui-native/src/ConditionsPanel")>();
  const { View } = await import("react-native");
  return { ...real, ConditionsPanel: (p: Parameters<typeof real.ConditionsPanel>[0]) => MUTANT.on
    ? h(View, { style: { flex: 1, paddingHorizontal: 16 } }, h(real.ConditionsPanel, p))
    : h(real.ConditionsPanel, p) };
});

const { GestureHandlerRootView } = await import("react-native-gesture-handler");
const { makeFixtureSource } = await import("@app/data/fixture");
const { openRegsFixture } = await import("@app/data/release/node");
const { HOME } = await import("@app/ui");
const { Shell } = await import("../packages/ui-native/src/Shell");
const { LIGHT } = await import("../packages/ui-native/src/theme");

const SIZES = [[390, 844], [768, 1024], [1440, 900]] as const;
const VIEWS = ["Regulations", "Conditions", "Map"] as const;
const NAME = "Chilliwack River";

/** The page as react-native-web drew it: its CSS rules (inserted through the CSSOM, so not in any textContent) + DOM. */
function snapshot(): string {
  const css = [...document.styleSheets].flatMap((s) => [...s.cssRules].map((r) => r.cssText)).join("\n");
  return `<!doctype html><html><head><meta charset="utf-8"><style>html,body,#root{margin:0;height:100%}`
    + `#root{display:flex;flex-direction:column}\n${css}</style></head>`
    + `<body><div id="root">${document.body.firstElementChild!.innerHTML}</div></body></html>`;
}

type Where = { back: number[] | null; strip: number[] | null; title: number[] | null; left: number | null };

/** How many view strips each view drew (the header is the frame's, drawn ONCE). */
const strips: Record<string, number> = {};

/** Mount the water on Regulations, press the strip through every view, and snapshot each. */
async function shoot(): Promise<Record<(typeof VIEWS)[number], string>> {
  const source = makeFixtureSource();
  const water = await source.water("gnis:8634" as never);
  render(h(GestureHandlerRootView, { style: { flex: 1 } } as never, h(Shell, {
    source, palette: LIGHT, theme: "light", themeName: "light", onTheme: () => {},
    tiles: { atlas: "a", basemap: "b" } as never, loader: openRegsFixture(),
    initialRoute: { mode: "water", from: { ...HOME, tab: "map" },
                    stack: [{ item: "gnis:8634" as never, part: 0, tapSid: water!.sections[0]!, at: null, view: "regs" }] },
  } as never) as ReactNode));
  const out = {} as Record<(typeof VIEWS)[number], string>;
  // what says each view has drawn its body (Conditions: the panel's first words, then a moment for the rest)
  const ready = { Regulations: () => screen.queryByTestId("keep-tab"), Conditions: () => screen.queryByRole("radiogroup"),
                  Map: () => screen.queryByTestId("water-map-where") };
  for (const v of VIEWS) {
    if (v !== "Regulations") act(() => { fireEvent.click(screen.getByRole("tab", { name: v })); });
    await waitFor(() => expect([screen.getByRole("tab", { name: v }).getAttribute("aria-selected"), !!ready[v](),
                                screen.queryAllByText(NAME).length > 0]).toEqual(["true", true, true]), { timeout: 20_000 });
    await act(async () => { await new Promise((r) => setTimeout(r, 500)); });
    strips[v] = screen.getAllByRole("tablist", { name: "Views of this water" }).length;
    out[v] = snapshot();
  }
  cleanup();
  return out;
}

const dir = mkdtempSync(join(tmpdir(), "t44-frame-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));

/** Lay each snapshot out at each size, in Chrome (`water-frame-measure.mjs`, its own process): `{ "1440x900": { Regulations: Where, … } }`. */
function measure(pages: Record<string, string>): Record<string, Record<string, Where>> {
  const files = Object.fromEntries(Object.entries(pages).map(([view, html]) => {
    const file = join(dir, `${view}.html`);
    writeFileSync(file, html);
    return [view, file];
  }));
  const out = execFileSync(process.execPath, [join(process.cwd(), "tools", "water-frame-measure.mjs")   /* vitest runs in app/; import.meta.url is not a file: url under jsdom */,
                                              JSON.stringify({ sizes: SIZES, pages: files })], { encoding: "utf8", timeout: 60_000 });
  return JSON.parse(out) as Record<string, Record<string, Where>>;
}

/** Every disagreement, in words: "1440x900 Conditions left 112 ≠ Regulations 96"; a fractional px is one too. */
function disagreements(m: Record<string, Record<string, Where>>): string[] {
  const out: string[] = [];
  for (const [size, views] of Object.entries(m)) {
    const ref = views.Regulations!;
    for (const [view, at] of Object.entries(views)) {
      for (const k of ["back", "strip", "title", "left"] as const) {
        const v = JSON.stringify(at[k]), r = JSON.stringify(ref[k]);
        if (at[k] === null) out.push(`${size} ${view} ${k} missing`);
        else if ([at[k]].flat().some((n) => !Number.isInteger(n))) out.push(`${size} ${view} ${k} ${v} not whole px`);
        else if (view !== "Regulations" && v !== r) out.push(`${size} ${view} ${k} ${v} ≠ Regulations ${r}`);
      }
    }
  }
  return out;
}

describe("T44 the water frame: a view switch moves nothing the views share", () => {
  it("back, strip, title row and content left are at equal whole-pixel positions on Regulations, Conditions and Map, at 390, 768 and 1440", async () => {
    const m = measure(await shoot());
    expect(disagreements(m)).toEqual([]);
    expect(strips).toEqual({ Regulations: 1, Conditions: 1, Map: 1 });
    // and where they are: the gutter on a phone and a tablet; centred in the 1280 measure on a desktop ((1440-1280)/2 + 16)
    expect(Object.fromEntries(Object.entries(m).map(([s, v]) => [s, v.Regulations]))).toEqual({
      "390x844": { back: [16, 12], strip: [16, 48], title: [0, 94], left: 16 },
      "768x1024": { back: [16, 12], strip: [16, 48], title: [0, 94], left: 16 },
      "1440x900": { back: [96, 12], strip: [96, 48], title: [80, 94], left: 96 },
    });
  }, 120_000);

  it("MUTATION: a view that adds its own side gutter (the Conditions body, +16) fails it", async () => {
    MUTANT.on = true;
    try {
      const m = measure(await shoot());
      expect(disagreements(m)).toEqual([
        "390x844 Conditions left 32 ≠ Regulations 16",
        "768x1024 Conditions left 32 ≠ Regulations 16",
        "1440x900 Conditions left 112 ≠ Regulations 96",
      ]);
    } finally { MUTANT.on = false; }
  }, 120_000);
});
