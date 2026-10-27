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
import type { LicenceVM, PinAnchor, RegsPage, SheetKey, SheetVM, Who } from "@app/core";
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
import { SheetHost } from "./SheetHost";
import { createPinStore } from "./pinStore";
import { labelAudit, pressAudit, scaleOf } from "./testing/a11yAudit";
import vedder1010 from "./fixtures/vedder_1010.json";
import klwa1010 from "./fixtures/klwa_1010.json";
import bc1501 from "./fixtures/bc1_501.json";
import glenn501 from "./fixtures/glenn_501.json";
import vedder615 from "./fixtures/vedder_615.json";
import licences from "./fixtures/licence_vedder_1010.json";

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
  "button What does Anglers under 16 mean?", "button What does Catch and release (release) mean?",
  "button What does Conservation Surcharge Stamp mean?", "button What does Fly fishing only mean?",
  "button What does No fishing for (closed) mean?", "button What does Single barbless hook mean?",
  "button What does Steelhead Stamp mean?", "button What does Steelhead mean?",
  "link How to tell ›",   // the page's own link run in a hatchery-only line (Line `link`), not GroupCard's HowTell press
  "link Rule book page 21, opens the PDF", "link Rule book page 22, opens the PDF", "link Rule book page 9, opens the PDF",
  "link Steelhead stamp ›", "link White Sturgeon Conservation Licence ›",
];

/** the Licence tab as the water screen holds it: a pick rebuilds the answer for that angler (the frozen six) */
function Licence({ actions }: { actions: Parameters<typeof LicenceTab>[0]["actions"] }) {
  const [who, setWho] = useState<Who>({ age: "adult", residency: "resident" });
  return <LicenceTab vm={LIC[`${who.age}|${who.residency}`]!} who={who} palette={LIGHT} actions={actions} onWho={setWho} />;
}

/** the Shell's menu, so the page's foot draws its "Report a problem with this page" link as the app does */
const MENU = { report() {} } as unknown as MenuControls;

function mount(id = "vedder_1010", layout: LayoutClass = "phone", store = createPinStore()) {
  const page = PAGES[id]!;
  return render(
    <MenuContext.Provider value={MENU}>
    <WaterFrame palette={LIGHT} face="regulations" onBack={() => {}} backLabel="Map">
      <RegsScreen page={page} palette={LIGHT} actions={NOOP} onDate={() => {}} layout={layout} store={store}
                  gear={page.gear ? (a) => <GearTab vm={page.gear!} palette={LIGHT} actions={a} /> : undefined}
                  methods={page.methods ? (a) => <MethodsTab vm={page.methods!} palette={LIGHT} actions={a} referenceOpen /> : undefined}
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

  it("a fish picture is a button saying what it shows; an ⓘ asks what its word means; a book link says it opens the PDF", () => {
    mount();
    expect(screen.getAllByRole("button", { name: /— what it looks like$/ }).length).toBeGreaterThan(0);
    expect(screen.getByRole("button", { name: "Rainbow trout — what it looks like" })).toBeTruthy();
    const terms = screen.getAllByRole("button").map((b) => b.getAttribute("aria-label")).filter((l) => l?.startsWith("What"));
    expect(terms).toContain("What does Steelhead mean?");
    expect(screen.getAllByRole("link", { name: /^Rule book page \d+, opens the PDF$/ }).length).toBeGreaterThan(0);
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
      const page = PAGES[id]!, keys = Object.keys((({ vedder_1010: vedder1010, klwa_1010: klwa1010, bc1_501: bc1501, glenn_501: glenn501 })[id] as Frozen).sheets);
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
    });

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
    const numbers = keep.filter((t) => t.id.startsWith("n:"));
    expect(numbers.length).toBeGreaterThan(3);
    expect(numbers.filter((t) => t.mfm !== "2")).toEqual([]);
    // the header: the name (the frame's WaterTitle) and every word of the regs header
    const header = scaleOf(screen.getByTestId("regs-header"));
    expect(header.map((t) => t.id)).toContain("Vedder River");
    expect(header.filter((t) => t.mfm !== "2")).toEqual([]);
    // and nothing else is capped: the page's sentences grow as far as the reader asked
    expect(keep.filter((t) => !t.id.startsWith("n:") && t.mfm !== "")).toEqual([]);
  });

  it("the header's place row grows with the text (one line at 1×; at 2× this label wraps to two), and the label fit re-runs at the scaled line height", () => {
    vi.spyOn(PixelRatio, "getFontScale").mockReturnValue(1);
    const r = mount();
    const place = () => (screen.getByTestId("regs-place") as HTMLElement).style.height;
    expect(place()).toBe("16.22px");
    vi.spyOn(PixelRatio, "getFontScale").mockReturnValue(2);
    act(() => { window.dispatchEvent(new Event("resize")); });
    expect(place()).toBe("64.88px");
    r.unmount();
  });
});

describe("7. re-measure on a font-scale change", () => {
  it("useAnchors measures again on Dimensions change, and the pin bar reads the new anchors", () => {
    vi.useFakeTimers();
    let scale = 1;
    vi.spyOn(PixelRatio, "getFontScale").mockImplementation(() => scale);
    // jsdom has no layout: every node measures as its document order × the font scale (text twice as tall at 2×) —
    // react-native-web's measureLayout reads offsetTop / offsetHeight
    const order = new Map<Element, number>();
    const at = (el: Element) => { if (!order.has(el)) order.set(el, order.size); return order.get(el)!; };
    const proto = (globalThis as unknown as { HTMLElement: { prototype: { offsetTop: number; offsetHeight: number } } }).HTMLElement.prototype;
    vi.spyOn(proto, "offsetTop", "get").mockImplementation(function (this: Element) {
      return this.getAttribute("data-testid") === "regs-content" ? 0 : (100 + at(this) * 40) * scale;
    });
    vi.spyOn(proto, "offsetHeight", "get").mockImplementation(function (this: Element) { at(this); return 40 * scale; });
    const store = createPinStore();
    const set = vi.spyOn(store, "setAnchors");
    mount("vedder_1010", "phone", store);
    act(() => { vi.advanceTimersByTime(50); });
    const before = set.mock.calls.length;
    expect(before).toBeGreaterThan(0);
    const first = set.mock.calls[before - 1]![0] as readonly PinAnchor[];
    scale = 2;
    act(() => { window.dispatchEvent(new Event("resize")); vi.advanceTimersByTime(50); });
    expect(set.mock.calls.length).toBeGreaterThan(before);
    const next = set.mock.calls[set.mock.calls.length - 1]![0] as readonly PinAnchor[];
    expect(next.map((a) => [a.top, a.height])).toEqual(first.map((a) => [a.top * 2, a.height * 2]));
    // the pin bar draws from the new anchors: a scroll inside the group as it now stands pins its line (under the old,
    // half-height anchors the same offset is past the group, and nothing pins)
    act(() => { store.setScroll(next[0]!.top + 50); });
    expect(screen.getAllByTestId("pin-group").length).toBe(1);
  });
});
