/**
 * T40 (C) — the Regulations view with a screen reader and at the OS's largest text: roles (tablist / tab, radiogroup /
 * radio, heading, button, link, image), every press named and reaching 44 × 44, no labelled container swallowing its
 * children, text that scales (numbers and the header stop at 2×), and the anchors measured again after a font-scale
 * change. Mounted as the app draws it — `RegsScreen` in the water frame (T44), with the Gear / Methods / Licence tabs
 * (T34/T35) — over the frozen golden pages, on a phone, a tablet and a desktop (T45), and every sheet the pages open
 * (T37). The audits are `testing/a11yAudit.ts`; each rule is pinned below by a planted violation it must catch.
 *
 * The Text spy: react-native-web drops `allowFontScaling` / `maxFontSizeMultiplier` (the browser scales by its own
 * setting), so this file's `react-native` Text writes them to `data-afs` / `data-mfm` for the font-scaling assertions.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { createElement, forwardRef, useState } from "react";
import { PixelRatio, Pressable, Text, View } from "react-native";
import type { LicenceVM, RegsPage, SheetKey, SheetVM, Who } from "@app/core";
import type { LayoutClass, MenuControls } from "@app/ui";
import { MenuContext } from "../menu/MenuContext";
import { LIGHT } from "../theme";
import { WaterFrame } from "../WaterFrame";
import { GearTab } from "./GearTab";
import { LicenceTab } from "./LicenceTab";
import { MethodsTab } from "./MethodsTab";
import { lineRun } from "./hit";
import { RegsPress } from "./RegsPress";
import { RegsScreen } from "./RegsScreen";
import { SheetBody } from "./SheetBody";
import { SheetHost } from "./SheetHost";
import { createTabStore } from "./tabStore";
import { labelAudit, pressAudit, scaleOf } from "./testing/a11yAudit";
import vedder1010 from "./fixtures/vedder_1010.json";
import klwa1010 from "./fixtures/klwa_1010.json";
import bc1501 from "./fixtures/bc1_501.json";
import glenn501 from "./fixtures/glenn_501.json";
import vedder615 from "./fixtures/vedder_615.json";
import licences from "./fixtures/licence_vedder_1010.json";
import { TEST_SOURCES, TEST_TROUT_SOURCES } from "./testing/sources";

vi.mock("react-native", async (importOriginal) => {
  const rn = await importOriginal<typeof import("react-native")>();
  type P = React.ComponentProps<typeof rn.Text> & { dataSet?: Record<string, string> };
  const Spy = forwardRef<unknown, P>(function Text(p, ref) {
    return createElement(rn.Text as never, { ...p, ref, dataSet: { ...p.dataSet, afs: String(p.allowFontScaling !== false),
      ...(p.maxFontSizeMultiplier != null ? { mfm: String(p.maxFontSizeMultiplier) } : {}) } });
  });
  return { ...rn, Text: Spy };
});

afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.useRealTimers(); });

type Frozen = Omit<RegsPage, "sheet"> & { sheets: Record<string, SheetVM | null> };
const pageOf = (f: unknown): RegsPage => { const p = f as Frozen; return { ...p, sheet: (k: SheetKey) => p.sheets[k] ?? null } as RegsPage; };
const PAGES: Record<string, RegsPage> = { vedder_1010: pageOf(vedder1010), klwa_1010: pageOf(klwa1010), bc1_501: pageOf(bc1501),
  glenn_501: pageOf(glenn501), vedder_615: pageOf(vedder615) };
const LIC = licences as unknown as Record<string, LicenceVM>;
const NOOP = { onSheet() {}, onDay() {}, onBook() {} };
/** vedder_1010's runs inside a sentence, every fold open: a new one (or a press that turned back into a run) shows here */
const VEDDER_INLINE: string[] = [
  "button What does Anglers under 16 mean?", "button What does Bait ban mean?", "button What does Catch and release (release) mean?",
  "button What does Conservation Surcharge Stamp for salmon mean?", "button What does Fly fishing only mean?",
  "button What does No fishing for (closed) mean?", "button What does Single barbless hook mean?",
  "button What does Steelhead Stamp mean?",
  // (T810 cutover) gone with the old trout card: "What does Steelhead mean?", "Fishing more than one water today? ›" (the
  // table's "Fished elsewhere today? ›" is a press: TroutCard.test), "How this limit is shared", "How to tell hatchery ›"
  // (the table's "How to tell a hatchery fish ›" press), "B.C. Synopsis page 21 / 22" (the fish sheet's Sources), "Steelhead stamp ›"
  "link B.C. Synopsis page 9, opens the PDF", "link White Sturgeon Conservation Licence ›",
];

/** the Licence tab as the water screen holds it: a pick rebuilds the answer for that angler (the frozen six) */
function Licence({ actions }: { actions: Parameters<typeof LicenceTab>[0]["actions"] }) {
  const [who, setWho] = useState<Who>({ age: "adult", residency: "resident" });
  return <LicenceTab vm={LIC[`${who.age}|${who.residency}`]!} who={who} palette={LIGHT} actions={actions} onWho={setWho} />;
}

/** the Shell's menu, so the page's foot draws its "Report a problem with this page" link as the app does */
const MENU = { report() {} } as unknown as MenuControls;

function mount(id = "vedder_1010", layout: LayoutClass = "phone", store = createTabStore()) {
  const page = PAGES[id]!;
  return render(
    <MenuContext.Provider value={MENU}>
    <WaterFrame palette={LIGHT} face="regulations" onBack={() => {}} backLabel="Map">
      <RegsScreen troutSources={TEST_TROUT_SOURCES} sources={TEST_SOURCES} page={page} palette={LIGHT} actions={NOOP} onDate={() => {}} layout={layout} store={store}
                  gear={page.gear ? (a) => <GearTab vm={page.gear!} palette={LIGHT} actions={a} /> : undefined}
                  methods={page.methods ? (a) => <MethodsTab vm={page.methods!} palette={LIGHT} actions={a} /> : undefined}
                  licence={id === "vedder_1010" ? (a) => <Licence actions={a} /> : undefined} />
    </WaterFrame>
    </MenuContext.Provider>);
}

/** open every fold on the screen (the in-part caveat, "How this was decided", sheet folds) so their insides are audited */
function openFolds(root: HTMLElement) {
  for (let i = 0; i < 20; i++) {
    const shut = [...root.querySelectorAll('[aria-expanded="false"]')];
    if (!shut.length) return;
    for (const f of shut) fireEvent.click(f);
  }
}

describe("roles", () => {
  it("1. tabs: one tablist of four tabs named as the page says, exactly one selected (r20e-four-tabs)", () => {
    mount();
    const list = screen.getByRole("tablist", { name: "Section" });
    const tabs = within(list).getAllByRole("tab");
    expect(tabs.map((t) => t.textContent)).toEqual(["What you can keep", "Gear", "Methods", "Licence"]);
    expect(tabs.map((t) => t.getAttribute("aria-selected"))).toEqual(["true", "false", "false", "false"]);
  });

  it("2. headings: the water's name and the group's title are headings", () => {
    mount();
    const names = screen.getAllByRole("heading").map((h) => h.textContent);
    expect(names).toContain("Vedder River");
    expect(names).toContain("Trout and char");
  });

  it("3. who selectors: Licence holds two radiogroups; picking Under 16 checks it and only it", () => {
    mount();
    const groups = within(screen.getByTestId("licence-tab")).getAllByRole("radiogroup");
    // each named by the page (LicenceVM axes[].aria — the prototype's own label, the same question for both)
    expect(groups.map((g) => g.getAttribute("aria-label"))).toEqual(["Who is fishing?", "Who is fishing?"]);
    fireEvent.click(within(groups[0]!).getByRole("radio", { name: "Under 16" }));
    const age = within(within(screen.getByTestId("licence-tab")).getAllByRole("radiogroup")[0]!).getAllByRole("radio");
    expect(age.map((r) => [r.getAttribute("aria-label"), r.getAttribute("aria-checked")])).toEqual([["16 or older", "false"], ["Under 16", "true"]]);
  });

  it("the strip's bar is an image named by the status line; the legend stays text", () => {
    mount();
    const bar = within(screen.getByTestId("keep-strip")).getByRole("img");
    expect(bar.getAttribute("aria-label")).toBe("Open today");
    expect(screen.getByTestId("keep-strip").textContent).toContain("Open");
  });

  it("a trout table row is a button saying the whole row (its picture inside it); an ⓘ asks what its word means; a book link says it opens the PDF", () => {
    mount();
    // (T810 cutover) the table's row is the press its picture opens through (build spec P), named by its one label (N's
    // golden): the fish's name first; the old card's "<fish> — what it looks like" pictures went with it
    const rows = within(screen.getByTestId("keep-trout")).getAllByTestId("keep-row");
    expect(rows.length).toBeGreaterThan(3);
    expect(rows.filter((r) => r.getAttribute("role") !== "button"
      || !r.getAttribute("aria-label")!.startsWith(within(r).getByTestId("keep-name").textContent!))).toEqual([]);
    expect(rows.every((r) => r.querySelector("[data-testid^='pic:']") !== null)).toBe(true);
    expect(screen.getByRole("button", { name: /^Rainbow trout\b/ })).toBeTruthy();
    const terms = screen.getAllByRole("button").map((b) => b.getAttribute("aria-label")).filter((l) => l?.startsWith("What"));
    expect(terms).toContain("What does Bait ban mean?");
    expect(screen.getAllByRole("link", { name: /^B.C. Synopsis page \d+, opens the PDF$/ }).length).toBeGreaterThan(0);
  });
});

describe("4. every press is named and reaches 44 × 44", () => {
  for (const layout of ["phone", "tablet", "desktop"] as const)
    for (const id of Object.keys(PAGES))
      it(`${id} on a ${layout}: the whole screen, every fold open`, () => {
        const { container } = mount(id, layout);
        openFolds(container);
        const { bad, inline, sized } = pressAudit(container);
        expect(bad).toEqual([]);
        // not vacuous: the frame's back and view strip, the header (date, Map, four tabs) and the page's own presses
        expect(sized).toBeGreaterThanOrEqual(id === "vedder_615" ? 10 : 20);
        // the key: only runs inside a sentence (LineText's link / ⓘ / book runs), each named — a nested Text press that
        // is not one of LineText's runs is in `bad` above ("not a line run")
        expect(inline.every((x) => x.role === "link" || x.role === "button")).toBe(true);
        // the menu's report link at the page's foot (T41 Q3: once) is audited like any press (it is in `sized`, not exempt)
        expect(container.querySelectorAll('[data-testid="report-link"]').length).toBe(1);
      });

  it("vedder_1010: exactly these runs ride on the key — the ⓘs, the book pages and the line's own links", () => {
    const { container } = mount();
    openFolds(container);
    const names = [...new Set(pressAudit(container).inline.map((x) => `${x.role} ${x.name}`))].sort();
    expect(names).toEqual(VEDDER_INLINE);
  });

  for (const id of ["vedder_1010", "klwa_1010", "bc1_501", "glenn_501"] as const)
    it(`${id}: every sheet the page opens, on a phone and as a wide dialog`, () => {
      const page = PAGES[id]!, keys = Object.keys((({ vedder_1010: vedder1010, klwa_1010: klwa1010, bc1_501: bc1501, glenn_501: glenn501 })[id] as unknown as Frozen).sheets);
      const offences: string[] = [];
      let sized = 0;
      for (const layout of ["phone", "desktop"] as const)
        for (const k of keys) {
          const { baseElement, unmount } = render(<SheetHost page={page} open={k as SheetKey} onOpen={() => {}} onClose={() => {}} palette={LIGHT} layout={layout} />);
          openFolds(baseElement);
          const a = pressAudit(baseElement);
          sized += a.sized;
          for (const b of a.bad) offences.push(`${layout} ${k}: ${b.role} "${b.name}" ${b.why}`);
          for (const l of labelAudit(baseElement)) offences.push(`${layout} ${k}: label "${l.label}" swallows "${l.text}"`);
          unmount();
        }
      expect(offences).toEqual([]);
      expect(sized).toBeGreaterThan(keys.length * 2);   // at least Done on each, and the presses inside
    // (T250) ~2 s alone (every sheet of a page, twice), past vitest's 5 s when the machine is busy: load, not a regression
    // (the reason the logic project's timeout is 30 s, vitest.config.ts). A hung walk still fails, just later.
    }, 30_000);

  it("the planted mutants are caught: a press with no slop record, one too small, one unnamed", () => {
    const { container } = render(
      <View>
        <Pressable accessibilityRole="button" onPress={() => {}}><Text>Plain pressable</Text></Pressable>
        <RegsPress accessibilityRole="link" box={{ w: 44, h: 20 }} onPress={() => {}}><Text>ok</Text></RegsPress>
        <Pressable accessibilityRole="button" onPress={() => {}} {...{ dataSet: { hitslop: "30 30 2 2 2 2" } }}><Text>Too small</Text></Pressable>
        <Pressable accessibilityRole="tab" onPress={() => {}} {...{ dataSet: { hitslop: "44 44 0 0 0 0" } }} />
        <Text>A sentence with <Text accessibilityRole="link" onPress={() => {}} {...lineRun}>a run</Text> in it.</Text>
        <Text><Text accessibilityRole="link" onPress={() => {}}>How to tell ›</Text></Text>
      </View>);
    expect(pressAudit(container)).toEqual({
      bad: [
        { role: "button", name: "Plain pressable", why: "no data-hitslop: not a RegsPress / hitProps press" },
        { role: "button", name: "Too small", why: "reaches 34×34" },
        { role: "tab", name: "", why: "no accessible name" },
        { role: "link", name: "How to tell ›", why: "a nested Text press that is not a line run (LineText): give it a RegsPress" },
      ],
      inline: [{ role: "link", name: "a run" }],
      sized: 3,
    });
  });
});

describe("5. no container labels", () => {
  for (const layout of ["phone", "desktop"] as const)
    for (const id of Object.keys(PAGES))
      it(`${id} on a ${layout}: no labelled element swallows text outside a press`, () => {
        const { container } = mount(id, layout);
        openFolds(container);
        expect(labelAudit(container)).toEqual([]);
      });

  it("the planted mutant is caught: a labelled View over text", () => {
    const { container } = render(
      <View>
        <View accessibilityLabel="Allowed: Spinners"><Text>✓</Text><Text>Spinners</Text></View>
        <View accessibilityRole="radiogroup" accessibilityLabel="Age">
          <Pressable accessibilityRole="radio" aria-checked onPress={() => {}}><Text>Under 16</Text></Pressable>
        </View>
        <Text accessibilityLabel="Mouth → Vedder Crossing Bridge">Mouth → Vedder Crossing</Text>
      </View>);
    expect(labelAudit(container)).toEqual([{ label: "Allowed: Spinners", text: "✓" }]);
  });
});

describe("6. font scaling", () => {
  it("every Text on the Keep tab scales; the numbers and the header's words stop at 2×", () => {
    mount();
    const keep = scaleOf(screen.getByTestId("keep-tab"));
    expect(keep.length).toBeGreaterThan(50);
    expect(keep.filter((t) => t.afs !== "true")).toEqual([]);
    // (T810 cutover) the Keep tab's numbers (`n:` runs) went with the old trout card: the table's numbers are in its
    // payload sentences, which grow like every sentence (below). The page's numbers still stop at 2× where it draws them:
    // the group cards of the Other fish sheets (the Vedder's Whitefish and Bass)
    const sheetNumbers = (["other:o1", "other:o2"] as const).flatMap((k) => {
      const { container, unmount } = render(<SheetBody vm={PAGES.vedder_1010!.sheet(k)!} palette={LIGHT} onOpen={() => {}} onBook={() => {}} />);
      const out = scaleOf(container).filter((t) => t.id.startsWith("n:"));
      unmount();
      return out;
    });
    const numbers = [...keep.filter((t) => t.id.startsWith("n:")), ...sheetNumbers];
    // (T883) the sheets' group cards say their number once (the kind line's "Up to 15"), not again in a head
    expect(numbers.length).toBeGreaterThan(1);
    expect(numbers.filter((t) => t.mfm !== "2")).toEqual([]);
    // the header: the name (the frame's WaterTitle) and every word of the regs header
    const header = scaleOf(screen.getByTestId("regs-header"));
    expect(header.map((t) => t.id)).toContain("Vedder River");
    expect(header.filter((t) => t.mfm !== "2")).toEqual([]);
    // and nothing else is capped: the page's sentences grow as far as the reader asked
    expect(keep.filter((t) => !t.id.startsWith("n:") && t.mfm !== "")).toEqual([]);
  });

  it("the header's place row grows with the text — as many lines as the name needs, never cut (T517: the Vedder's part " +
     "name takes two at 1×, four at 2×) — and the label fit re-runs at the scaled line height", () => {
    vi.spyOn(PixelRatio, "getFontScale").mockReturnValue(1);
    const r = mount();
    const place = () => (screen.getByTestId("regs-place") as HTMLElement).style.height;
    expect(place()).toBe("32.44px");
    vi.spyOn(PixelRatio, "getFontScale").mockReturnValue(2);
    act(() => { window.dispatchEvent(new Event("resize")); });
    expect(place()).toBe("129.76px");
    r.unmount();
  });
});

// (T810 cutover) retired: "7. re-measure on a font-scale change — useAnchors measures again on Dimensions change, and the
// pin bar reads the new anchors". Only a Keep-tab group card registers pin anchors, and no Keep tab draws one since the
// cutover (the trout table has no pin bar): useAnchors.test "the Vedder's Keep tab, both days, registers no pin anchor".
// No table test covers it: the pin bar has no subject on a real page.
