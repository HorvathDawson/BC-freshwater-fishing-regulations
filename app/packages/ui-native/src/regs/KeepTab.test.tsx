/**
 * T21 render: the Keep tab mounted over the FROZEN golden pages (fixtures/*.json, written by `UPDATE_REGS_FIXTURES=1` from
 * buildRegsPage over the parity store — never hand-edited). The blocks draw in `keep.order`; every block's words are the
 * VM's; links say where they go. jsdom cannot open a store, so these are the frozen VMs.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { KeepVM, RegsPage, SheetKey } from "@app/core";
import { KeepTab } from "./KeepTab";
import { LIGHT } from "../theme";
import type { KeepActions } from "./LineText";
import vedder1010 from "./fixtures/vedder_1010.json";
import vedder615 from "./fixtures/vedder_615.json";
import glenn501 from "./fixtures/glenn_501.json";

afterEach(cleanup);

const keepOf = (f: unknown) => (f as RegsPage).keep as KeepVM;
const TEST_ID = { strip: "keep-strip", inherited: "keep-inherited", banner: "keep-banner", otherFish: "keep-otherfish",
  possession: "keep-possession", whereNot: "keep-wherenot", outside: "keep-outside" } as const;
const idOf = (b: KeepVM["order"][number]) => typeof b === "string" ? TEST_ID[b] : "group" in b ? "keep-group" : "plain" in b ? "keep-plain" : "keep-caveat";

function mount(keep: KeepVM) {
  const sheets: SheetKey[] = [], books: number[] = [];
  const actions: KeepActions = { onSheet: (k) => sheets.push(k), onDay() {}, onTab() {}, onBook: (p) => books.push(p) };
  const r = render(<KeepTab keep={keep} dateLabel="Oct 10" palette={LIGHT} actions={actions} />);
  const drawn = [...r.container.querySelectorAll("[data-testid^='keep-']")].map((e) => e.getAttribute("data-testid")).filter((t) => t !== "keep-tab");
  return { r, sheets, books, drawn };
}

describe("<KeepTab> over the frozen pages", () => {
  it("vedder_1010: the blocks in keep.order — strip, the group, Other fish, possession, where-not", () => {
    const keep = keepOf(vedder1010), { drawn } = mount(keep);
    expect(drawn).toEqual(["keep-strip", "keep-group", "keep-otherfish", "keep-possession", "keep-wherenot"]);
    expect(drawn).toEqual(keep.order.map(idOf));
    expect(screen.getByTestId("keep-possession").textContent).toBe("Possession2 days’ quota›");
  });

  it("vedder_1010 (rows 32): the white sturgeon row's stored stamp drawn under it, its document a link to the Licence tab", () => {
    const tabs: string[] = [];
    render(<KeepTab keep={keepOf(vedder1010)} dateLabel="Oct 10" palette={LIGHT}
                    actions={{ onSheet() {}, onDay() {}, onTab: (t) => tabs.push(t), onBook() {} }} />);
    expect(screen.getAllByTestId("otherfish-stamp").map((e) => e.textContent)).toEqual([
      "You need White Sturgeon Conservation Licence › only if you fish for white sturgeon, even to release them"]);
    fireEvent.click(screen.getByText("White Sturgeon Conservation Licence ›"));
    expect(tabs).toEqual(["licence"]);
  });

  it("vedder_615 (closed): strip, banner, where-not; the banner's words are the VM's", () => {
    const keep = keepOf(vedder615), { drawn } = mount(keep);
    expect(drawn).toEqual(["keep-strip", "keep-banner", "keep-wherenot"]);
    expect(screen.getByTestId("keep-banner").textContent).toBe(
      "No fishing for any game fish Jun 1–30.Jun 1–30. This waterSourceRule book p.22 ↗See the rules from then ›");
  });

  it("glenn_501: the inherited line right after the strip, with its caution", () => {
    const keep = keepOf(glenn501), { drawn } = mount(keep);
    expect(drawn.slice(0, 2)).toEqual(["keep-strip", "keep-inherited"]);
    expect(screen.getByTestId("keep-inherited").textContent).toBe(
      "By another water’s ruleOpen Apr 1–Jun 14: the Duncan River’s exemption from the spring stream closure covers its tributaries. Rule book p.36 ↗"
      + "Trout Lake’s row, nearer this stream, prints no such exemption; the Duncan River’s is applied here because its row covers all its tributaries.");
  });

  it("glenn_501: the group's year window under its head block, before the tree (prototype .gyear)", () => {
    const keep = keepOf(glenn501);
    mount(keep);
    const y = screen.getByTestId("keep-group-year");
    expect(y.textContent).toBe("Through the yearYou can keep them until Oct 31. Then release only from Nov 1.");
    expect(keep.groups[0]!.year).toEqual({ label: "Through the year", says: "You can keep them until Oct 31. Then release only from Nov 1." });
    const vedder = keepOf(vedder1010);
    mount(vedder);
    expect(vedder.groups[0]!.year).toBeNull();
    expect(screen.getAllByTestId("keep-group-year")).toHaveLength(1);   // only glenn's
  });

  it("links say where they go: a Rule book page, an Other fish row, 'See every regulation line ›'", () => {
    const { sheets, books } = mount(keepOf(vedder1010));
    fireEvent.click(screen.getAllByText("Rule book p.21 ↗")[0]!);
    fireEvent.click(screen.getByText("Kokanee"));
    fireEvent.click(screen.getByText("See every regulation line ›"));
    expect([books, sheets]).toEqual([[23], ["other:o0", "src:r0"]]);
  });
});
