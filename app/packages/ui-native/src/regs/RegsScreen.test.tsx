/**
 * T23 — the Regulations screen (mounted in the water frame, T44, which draws the water's header row): the regs header FIXED above ONE ScrollView of the four tab
 * sections (header-one-shape, one scroll, licence-last, no part strip), the date pill (D7 a; T49: it asks the water
 * screen for "Which day?", the map's own calendar sheet — WaterScreen.regs-props.test), and (T29) a tab press scrolling to its section. Mounted over the frozen golden page
 * (fixtures/vedder_1010.json, buildRegsPage over the parity store).
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { Text } from "react-native";
import type { RegsPage } from "@app/core";
import { LIGHT } from "../theme";
import { WaterFrame } from "../WaterFrame";
import { RegsScreen, type RegsScreenProps } from "./RegsScreen";
import { createPinStore } from "./pinStore";
import vedder1010 from "./fixtures/vedder_1010.json";

afterEach(() => { cleanup(); vi.restoreAllMocks(); });

const page = vedder1010 as unknown as RegsPage;
const NOOP = { onSheet() {}, onDay() {}, onBook() {} };
/** The screen as the app draws it: inside the water frame (T44), which draws the back and the view strip above it. */
function mount(p: Partial<RegsScreenProps> = {}, onBack?: () => void) {
  return render(<WaterFrame palette={LIGHT} face="regulations" onBack={onBack}>
                  <RegsScreen page={page} palette={LIGHT} actions={NOOP} onDate={() => {}}
                              gear={<Text>gear section</Text>} methods={<Text>methods section</Text>}
                              licence={<Text>licence section</Text>} {...p} />
                </WaterFrame>);
}
const scrollEl = () => screen.getByTestId("regs-scroll");

describe("<RegsScreen>", () => {
  it("header-one-shape: the header is not in the scroll, and a scroll leaves its words and test ids as they were", () => {
    mount();
    const header = screen.getByTestId("regs-header");
    const before = { text: header.textContent, ids: [...header.querySelectorAll("[data-testid]")].map((e) => e.getAttribute("data-testid")) };
    expect(scrollEl().contains(header)).toBe(false);
    fireEvent.scroll(scrollEl(), { target: { scrollTop: 900 } });
    expect({ text: header.textContent, ids: [...header.querySelectorAll("[data-testid]")].map((e) => e.getAttribute("data-testid")) })
      .toEqual(before);
    // (the place label twice: the shown label, then its invisible measuring twin)
    expect(before.text).toBe("Vedder RiverToday ▾Oct 10Mouth → Vedder Crossing BridgeMouth → Vedder Crossing BridgeWhat you can keepGearMethodsLicence");
  });

  it("one scroll: exactly one vertical scroll container on the screen", () => {
    const { container } = mount();
    const scrolls = [...container.querySelectorAll<HTMLElement>("*")].filter((e) => /auto|scroll/.test(getComputedStyle(e).overflowY));
    expect(scrolls).toEqual([scrollEl()]);
  });

  it("licence-last: Keep, Gear, Methods, Licence, then the footer, in the one scroll; absent sections are not drawn", () => {
    mount({ footer: <Text testID="foot">foot</Text> });
    expect([...screen.getByTestId("regs-content").children].map((e) => e.getAttribute("data-testid")))
      .toEqual(["keep-tab", "regs-gear", "regs-methods", "regs-licence", "foot"]);
    cleanup();
    mount({ gear: undefined, methods: undefined, licence: undefined });
    expect([...screen.getByTestId("regs-content").children].map((e) => e.getAttribute("data-testid"))).toEqual(["keep-tab"]);
    expect(within(screen.getByRole("tablist", { name: "Section" })).getAllByRole("tab").map((t) => t.textContent))
      .toEqual(["What you can keep", "Gear", "Methods", "Licence"]);
  });

  it("no part strip: nothing between the header and the Keep tab's year strip", () => {
    mount();
    const keep = screen.getByTestId("keep-tab");
    expect(keep.children[0]!.getAttribute("data-testid")).toBe("keep-strip");
    expect(screen.queryAllByRole("radiogroup").filter((r) => !/Regulations|Conditions/.test(r.textContent ?? ""))).toEqual([]);
  });

  it("the water's header row is the frame's (T44): back and the view strip (FaceBar), Conditions offered; the screen draws neither", () => {
    const back = vi.fn();
    mount({}, back);
    const frameHeader = screen.getByTestId("water-frame-header");
    expect([frameHeader.contains(screen.getByRole("button", { name: "Back" })),
            screen.getByTestId("regs-view").contains(screen.getAllByRole("tablist")[0]!)]).toEqual([true, false]);
    const strip = screen.getAllByRole("tablist")[0]!;
    expect(within(strip).getAllByRole("tab").map((t) => [t.getAttribute("aria-label"), t.getAttribute("aria-selected")]))
      .toEqual([["Regulations", "true"], ["Conditions", "false"]]);
    fireEvent.click(screen.getByRole("button", { name: "Back" }));
    expect(back).toHaveBeenCalledTimes(1);
  });

  it("the Map button frames the water's map when the host can (onMap); otherwise it is not drawn", () => {
    const map = vi.fn();
    mount({ onMap: map });
    fireEvent.click(screen.getByRole("button", { name: "View on map: where these rules apply, Mouth → Vedder Crossing Bridge" }));
    expect(map).toHaveBeenCalledTimes(1);
    cleanup();
    mount();
    expect(screen.queryByTestId("regs-action")).toBeNull();
  });

  it("the pill asks for 'Which day?' (T49: the water screen's sheet, so a reload does not close it); no sheet of its own", () => {
    const onDate = vi.fn();
    mount({ onDate });
    fireEvent.click(screen.getByRole("button", { name: "Change date, now Oct 10" }));
    expect([onDate.mock.calls.length, screen.queryByText("Which day?")]).toEqual([1, null]);
  });

  it("(T29) a tab press selects it at once and scrolls to its section top + 2 (prototype 2000); Keep scrolls to 0", () => {
    const store = createPinStore();
    mount({ store });
    // react-native-web's ScrollView.scrollTo is the DOM node's scroll(): smooth when animated
    const to = vi.fn();
    (scrollEl() as unknown as { scroll: typeof to }).scroll = to;
    act(() => store.setSections({ gear: 1500, methods: 2100, methodsHeight: 400, licence: 2700 }));
    fireEvent.click(screen.getByRole("tab", { name: "Gear" }));
    expect(screen.getByRole("tab", { name: "Gear" }).getAttribute("aria-selected")).toBe("true");
    fireEvent.click(screen.getByRole("tab", { name: "What you can keep" }));
    expect(to.mock.calls).toEqual([[{ top: 1502, left: 0, behavior: "smooth" }], [{ top: 0, left: 0, behavior: "smooth" }]]);
  });

  it("(T29) the tab follows the scroll: past the Gear section's top the Gear tab is selected", () => {
    vi.useFakeTimers();
    try {
      const store = createPinStore();
      mount({ store });
      act(() => {
        store.setViewport(584); store.setContentHeight(3300);
        store.setSections({ gear: 1500, methods: 2100, methodsHeight: 400, licence: 2700 });
      });
      const sel = () => screen.getAllByRole("tab").filter((t) => t.getAttribute("aria-selected") === "true").map((t) => t.textContent);
      act(() => { fireEvent.scroll(scrollEl(), { target: { scrollTop: 1488 } }); vi.advanceTimersByTime(200); });
      expect(sel()).toEqual(["Regulations", "Gear"]);
      act(() => { fireEvent.scroll(scrollEl(), { target: { scrollTop: 0 } }); vi.advanceTimersByTime(200); });
      expect(sel()).toEqual(["Regulations", "What you can keep"]);
    } finally { vi.useRealTimers(); }
  });
});
