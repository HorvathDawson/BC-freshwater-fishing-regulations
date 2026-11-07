/**
 * T38 — the "Hatchery or wild?" sheet (regs-v11 sheetHtml hatch over page v36 adiposeHtml; T439: its lines are typed here, no longer read from golden.json):
 * the two rainbow-trout drawings (`AdiposeArt`), the plain line, then the hatchery_wild term with its closed fold and
 * its pages. Mounted over the FROZEN vedder_1010 page (fixtures/vedder_1010.json), the images handed down as the app
 * hands them (apps/mobile `ADIPOSE_IMAGES`; here two stand-in sources).
 *
 * Order inside a figure: the drawing, then its caption — the prototype (regs-v11.html sheetHtml hatch) puts the SVG at
 * the start of each `figure.fishart`, before the figcaption (screens/59-sheet-how-to-tell.png).
 *
 * react-native-web's Modal renders into a portal: queries go to `screen` (the document).
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { RegsPage, SheetKey, SheetVM } from "@app/core";
import { SheetHost } from "./SheetHost";
import { RegsScreen } from "./RegsScreen";
import { AdiposeArt } from "./AdiposeArt";
import { DARK, LIGHT } from "../theme";
import vedder1010 from "./fixtures/vedder_1010.json";

afterEach(cleanup);

type Frozen = Omit<RegsPage, "sheet"> & { sheets: Record<string, SheetVM | null> };
const sheetPage = (f: unknown): RegsPage => { const p = f as Frozen; return { ...p, sheet: (k: SheetKey) => p.sheets[k] ?? null } as RegsPage; };
const VEDDER = sheetPage(vedder1010);
const ADIPOSE = { wild: { uri: "adipose-wild.webp" }, clipped: { uri: "adipose-clipped.webp" } };
/** the sheet's lines, in order (the book's words are the glossary transcription: the rule book's p.13 and p.80 words) */
const G = {
  lines: [
    "Wild · adipose fin there",
    "Hatchery · clipped, a healed scar instead",
    "Look on the back, just in front of the tail. A small fin there means wild. Only a scar means hatchery. If you can’t tell, treat the fish as wild.",
    "Hatchery or wild (adipose fin)",
    "Hatchery trout and steelhead are marked before release by removing the adipose fin — the small fleshy fin on the back between the dorsal fin and the tail — so a hatchery fish has a healed scar there. A fish with its adipose fin (or a fresh, unhealed wound) is wild. Where only hatchery fish may be kept, every wild one goes back.",
    "In the book’s words",
    "hatchery trout: in some waters, hatchery trout may be harvested but wild trout must be released. In these waters, hatchery trout are marked before stocking by removal of their adipose fin (see centre colour section). Therefore, these hatchery trout must have a healed scar in place of the missing fin. wild trout: in some waters, hatchery trout may be harvested but wild trout must be released. In these waters, wild trout will not be marked as hatchery fish and will have a normal adipose fin (see illustration on page 13), or will have an unhealed scar in place of that fin, if missing.",
  ],
  pages: [{ text: "p.13 ↗", pdf: 15 }, { text: "p.80 ↗", pdf: 86 }],
};

function hatch({ adipose = ADIPOSE as typeof ADIPOSE | null, palette = LIGHT } = {}) {
  const onBook = vi.fn(), opened: SheetKey[] = [];
  render(<SheetHost page={VEDDER} open="hatch" onOpen={(k) => opened.push(k)} onClose={() => {}} onBook={onBook} palette={palette} adipose={adipose ?? undefined} />);
  return { onBook, opened };
}
const squash = (s: string | null | undefined) => (s ?? "").replace(/\s+/g, " ").trim();
/** the source an RN-web Image draws (its inner <img>) */
const srcOf = (fig: HTMLElement) => fig.querySelector("img")?.getAttribute("src") ?? null;

describe("T38 How to tell: the hatch sheet", () => {
  it("1. two figures, the drawing then its caption: labelled by their captions, in order, each with its own drawing", () => {
    hatch();
    const figs = screen.getAllByTestId("adipose-figure");
    // (T40) the drawing is the image, named by the caption; the figure carries no label (it would hide the caption's text)
    const pic = (f: HTMLElement) => f.querySelector<HTMLElement>('[data-testid="adipose-picture"]')!;
    expect(figs.map((f) => [f.getAttribute("aria-label"), pic(f).getAttribute("role"), pic(f).getAttribute("aria-label"), srcOf(f)])).toEqual([
      [null, "img", "Wild · adipose fin there", "adipose-wild.webp"],
      [null, "img", "Hatchery · clipped, a healed scar instead", "adipose-clipped.webp"],
    ]);
    // inside each figure: the picture box first, the caption after it (the prototype's figure.fishart order)
    expect(figs.map((f) => [...f.children].map((c) => [c.getAttribute("data-testid"), squash(c.textContent)]))).toEqual([
      [["adipose-picture", ""], ["adipose-caption", "Wild · adipose fin there"]],
      [["adipose-picture", ""], ["adipose-caption", "Hatchery · clipped, a healed scar instead"]],
    ]);
  });

  it("the caption's name is bold, the rest plain (adiposeHtml `<b>Wild</b> · adipose fin there`)", () => {
    hatch();
    const caps = screen.getAllByTestId("adipose-caption");
    const bold = (el: Element) => [...el.querySelectorAll("*")].map((c) => c.textContent).filter((t, i, a) => a.indexOf(t) === i);
    expect(caps.map((c) => [c.textContent, bold(c)])).toEqual([
      ["Wild · adipose fin there", ["Wild"]],
      ["Hatchery · clipped, a healed scar instead", ["Hatchery"]],
    ]);
    expect(caps.map((c) => c.querySelector("*")!.getAttribute("style")!.includes("font-family"))).toEqual([true, true]);
  });

  it("2. the sheet's lines, in order", () => {
    hatch();
    expect(G.lines.slice(0, 4)).toEqual([
      "Wild · adipose fin there",
      "Hatchery · clipped, a healed scar instead",
      "Look on the back, just in front of the tail. A small fin there means wild. Only a scar means hatchery. If you can’t tell, treat the fish as wild.",
      "Hatchery or wild (adipose fin)",
    ]);
    const body = squash(screen.getByTestId("sheet-body").textContent);
    let at = 0;
    const found = G.lines.slice(0, 5).map((l) => { const i = body.indexOf(squash(l), at); if (i >= 0) at = i + squash(l).length; return i >= 0; });
    expect(found).toEqual([true, true, true, true, true]);
    expect(body.indexOf("Wild · adipose fin there")).toBe(0);   // the drawings lead the sheet
    expect(G.lines[4]!.startsWith("Hatchery trout and steelhead are marked before release by removing the adipose fin")).toBe(true);
  });

  it("3. the term's fold starts closed: the book's words appear only when pressed", () => {
    hatch();
    const quote = G.lines[6]!;
    expect(quote.startsWith("hatchery trout: in some waters, hatchery trout may be harvested")).toBe(true);
    const fold = screen.getByRole("button", { name: "In the book’s words" });
    expect([fold.getAttribute("aria-expanded"), screen.queryByText(quote)]).toEqual(["false", null]);
    fireEvent.click(fold);
    expect([fold.getAttribute("aria-expanded"), screen.getByText(quote).textContent]).toEqual(["true", quote]);
  });

  it("4. pages: 'Rule book' then p.13 ↗ and p.80 ↗, each opening its PDF page", () => {
    const h = hatch();
    expect(G.pages).toEqual([{ text: "p.13 ↗", pdf: 15 }, { text: "p.80 ↗", pdf: 86 }]);
    const body = squash(screen.getByTestId("sheet-body").textContent);
    expect(body.endsWith("Rule book p.13 ↗ p.80 ↗")).toBe(true);
    const links = screen.getAllByRole("link");
    expect(links.map((l) => l.textContent)).toEqual(["p.13 ↗", "p.80 ↗"]);
    links.forEach((l) => fireEvent.click(l));
    expect(h.onBook.mock.calls).toEqual([[15], [86]]);
  });

  it("5. no tile: neither picture nor its two ancestors paints a ground (D4, r20h-picture-surface), light and dark", () => {
    for (const palette of [LIGHT, DARK]) {
      hatch({ palette });
      const grounds = screen.getAllByTestId("adipose-figure").map((f) => {
        const img = f.querySelector("img")!;
        const chain = [img.parentElement!, img.parentElement!.parentElement!, img.parentElement!.parentElement!.parentElement!];
        return chain.map((e) => e.style.backgroundColor);
      });
      expect(grounds).toEqual([["", "", ""], ["", "", ""]]);
      cleanup();
    }
  });

  it("each figure keeps the drawing's own proportions (800×300) and the fish picture's measure", () => {
    hatch();
    const boxes = screen.getAllByTestId("adipose-picture").map((b) => [b.style.aspectRatio, b.parentElement!.style.maxWidth]);
    expect(boxes).toEqual([[`${800 / 300} / 1`, "360px"], [`${800 / 300} / 1`, "360px"]]);
  });

  it("an app that registered no drawings still draws both figures, captioned, with an empty ground of the same size", () => {
    hatch({ adipose: null });
    expect(screen.getAllByTestId("adipose-picture").map((f) => [f.getAttribute("aria-label"), srcOf(f)])).toEqual([
      ["Wild · adipose fin there", null], ["Hatchery · clipped, a healed scar instead", null]]);
  });

  it("6. How to tell opens it: the ST kind's 'How to tell hatchery ›' on the Vedder Keep tab opens the hatch sheet (ur3-howtell-steelhead)", () => {
    const onSheet = vi.fn();
    render(<RegsScreen page={VEDDER} palette={LIGHT} adipose={ADIPOSE} actions={{ onSheet, onDay() {}, onBook() {} }} onDate={() => {}} />);
    const tells = screen.getAllByText("How to tell hatchery ›");
    expect(tells.length).toBeGreaterThan(0);
    fireEvent.click(tells[0]!);
    expect(onSheet.mock.calls).toEqual([["hatch"]]);
    expect(screen.getByRole("heading", { name: "Hatchery or wild?" })).toBeTruthy();
    expect(screen.getAllByTestId("adipose-figure").map(srcOf)).toEqual(["adipose-wild.webp", "adipose-clipped.webp"]);
  });
});

describe("AdiposeArt on a wide sheet", () => {
  const block = VEDDER.sheet("hatch" as SheetKey)!.blocks.find((b) => b.k === "adipose");
  const figures = block?.k === "adipose" ? block.figures : [];
  it("phone: one above the other; tablet / desktop: side by side, wrapping (as a fish's looks, T46)", () => {
    expect(figures.map((f) => f.variant)).toEqual(["wild", "clipped"]);
    const dir = (layout: "phone" | "tablet" | "desktop") => {
      render(<AdiposeArt figures={figures} images={ADIPOSE} palette={LIGHT} layout={layout} />);
      const s = screen.getByTestId("adipose-art").style; const out = [s.flexDirection, s.flexWrap]; cleanup(); return out;
    };
    expect([dir("phone"), dir("tablet"), dir("desktop")]).toEqual([["column", ""], ["row", "wrap"], ["row", "wrap"]]);
  });

  it("the sheet hands its layout down: a tablet's hatch sheet sets the two side by side", () => {
    render(<SheetHost page={VEDDER} open="hatch" onOpen={() => {}} onClose={() => {}} palette={LIGHT} adipose={ADIPOSE} layout="tablet" />);
    expect(screen.getByTestId("adipose-art").style.flexDirection).toBe("row");
  });

  it("a figure is never laid out reversed: the drawing stays above its caption", () => {
    hatch();
    expect(screen.getAllByTestId("adipose-figure").map((f) => getComputedStyle(f).flexDirection)).toEqual(["column", "column"]);
  });
});
