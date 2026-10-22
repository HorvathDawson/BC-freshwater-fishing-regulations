/**
 * T46 — a fish's looks in its sp: sheet (owner: "for stuff that has ocean and spawning look they should be side by side
 * in desktop right now they become hugeeee"): side by side on a tablet and a desktop, stacked on a phone, each picture at
 * most FISH_PICTURE.maxWidth across, each with its caption (the fish manifest's caption · phase · credit, composed in
 * core's fishSheet — never written here). Through the real SheetHost over the frozen pages' sp: sheets.
 *
 * What jsdom cannot lay out (one row, the widths drawn) is measured in Chrome by tools/fish-looks-layout.test.ts; this
 * pins the arrangement and the computed styles, and the T39 picture rule: no ground behind a fish, light and dark.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import type { LayoutClass } from "@app/ui";
import type { RegsPage, SheetKey, SheetVM } from "@app/core";
import { DARK, LIGHT, type Palette } from "../../theme";
import { SheetHost } from "../SheetHost";
import type { FishImages } from "../types";
import vedder1010 from "../fixtures/vedder_1010.json";

afterEach(cleanup);

type Frozen = Omit<RegsPage, "sheet"> & { sheets: Record<string, SheetVM | null> };
const sheetPage = (f: unknown): RegsPage => { const p = f as Frozen; return { ...p, sheet: (k: SheetKey) => p.sheets[k] ?? null } as RegsPage; };
const VEDDER = sheetPage(vedder1010);
/** pictures registered as the app registers them (a source per manifest picture, manifest order) */
const FISH: FishImages = {
  ST: [{ uri: "steelhead-ocean-phase.webp" }, { uri: "steelhead-spawning-phase.webp" }],
  KO: [{ uri: "kokanee-spawning-phase.webp" }, { uri: "kokanee.webp" }],
  RB: [{ uri: "rainbow-trout.webp" }],
};

function open(key: SheetKey, layout: LayoutClass, palette: Palette = LIGHT, fish: FishImages | undefined = FISH) {
  render(<SheetHost page={VEDDER} open={key} onOpen={() => {}} onClose={() => {}} palette={palette} fish={fish} layout={layout} />);
}
/** a computed background that paints nothing (unset, transparent, or any colour at alpha 0) */
const clear = (bg: string) => !bg || bg === "transparent" || bg.replace(/\s/g, "").endsWith(",0)");
/** the looks as drawn: the row's direction and wrap, each look's measure and caption, in order */
function looks() {
  const row = screen.getByTestId("fish-looks");
  const r = getComputedStyle(row);
  return {
    row: `${r.flexDirection} ${r.flexWrap}`,
    looks: screen.getAllByTestId("fish-look").map((l) => {
      const s = getComputedStyle(l);
      return `${s.maxWidth} | ${l.textContent}`;
    }),
  };
}

describe("T46 a fish's looks: side by side when there is room, stacked on a phone, never wider than the token", () => {
  it("sp:ST on a desktop: the ocean and spawning looks in one row, each ≤ 360 px, captioned from the manifest", () => {
    open("sp:ST", "desktop");
    expect(looks()).toEqual({
      row: "row wrap",
      looks: [
        "360px | Steelhead · ocean · Freshwater Fisheries Society of BC (GoFishBC)",
        "360px | Steelhead · spawning · WDFW (eRegulations WA)",
      ],
    });
  });

  it("sp:ST on a tablet: the same row", () => {
    open("sp:ST", "tablet");
    expect(looks().row).toBe("row wrap");
  });

  it("sp:ST on a phone: stacked (column), the same looks and captions in the same order", () => {
    open("sp:ST", "phone");
    expect(looks()).toEqual({
      row: "column nowrap",
      looks: [
        "360px | Steelhead · ocean · Freshwater Fisheries Society of BC (GoFishBC)",
        "360px | Steelhead · spawning · WDFW (eRegulations WA)",
      ],
    });
  });

  it("sp:KO and a one-look fish (sp:RB) on a desktop", () => {
    open("sp:KO", "desktop");
    const ko = looks();
    cleanup();
    open("sp:RB", "desktop");
    expect([ko, looks()]).toEqual([
      { row: "row wrap", looks: ["360px | Kokanee · spawning · WDFW (sockeye male) (eRegulations WA)",
                                 "360px | Kokanee · Freshwater Fisheries Society of BC (GoFishBC)"] },
      { row: "row wrap", looks: ["360px | Rainbow trout · Freshwater Fisheries Society of BC (GoFishBC)"] },
    ]);
  });

  it("the sheet's blocks keep their order: the looks, then What to look for", () => {
    open("sp:ST", "desktop");
    const body = screen.getByTestId("sheet-body");
    expect([...body.children].map((c) => c.getAttribute("data-testid") ?? c.textContent?.slice(0, 16))).toEqual([
      "fish-looks", "What to look for", "A rainbow trout ",
    ]);
  });

  it("T39: no ground behind a fish (the look, its box, the row), light and dark, every class", () => {
    const grounds: string[] = [];
    for (const palette of [LIGHT, DARK])
      for (const layout of ["phone", "desktop"] as const) {
        open("sp:ST", layout, palette);
        const row = screen.getByTestId("fish-looks");
        for (const el of [row, ...row.querySelectorAll<HTMLElement>("*")]) {
          if (el.tagName === "IMG") continue;
          const bg = getComputedStyle(el).backgroundColor;
          if (!clear(bg)) grounds.push(`${palette === LIGHT ? "light" : "dark"} ${layout}: ${bg}`);
        }
        cleanup();
      }
    expect(grounds).toEqual([]);
  });

  it("a fish with no registered picture keeps its plain ground (the stand-in), at the same measure", () => {
    open("sp:ST", "desktop", LIGHT, {});
    const boxes = screen.getAllByTestId("fish-look-box").map((b) => getComputedStyle(b).backgroundColor);
    expect(boxes.length).toBe(2);
    expect(new Set(boxes).size).toBe(1);
    expect(clear(boxes[0]!)).toBe(false);
  });
});
