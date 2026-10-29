/**
 * (T235 / T236) The legal surfaces: the regs page's foot (not legal advice + how current its sources are), the
 * first-run notice (once per device until accepted), and the licences list on Sources and licences.
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, fireEvent, render, renderHook, screen } from "@testing-library/react";
import { Text } from "react-native";
import { FIRST_RUN_VERSION, LEGAL_WORDS, LICENCE_ROWS, licencesVM, type RegsPage } from "@app/core";
import { FirstRunNotice, useFirstRun, type FirstRunStore } from "./FirstRunNotice";
import { LicencesList } from "./menu/Licences";
import { RegsScreen } from "./regs/RegsScreen";
import vedder1010 from "./regs/fixtures/vedder_1010.json";
import { LIGHT } from "./theme";

afterEach(cleanup);
const NOOP = { onSheet() {}, onDay() {}, onBook() {} };
const memory = (v: string | null = null): FirstRunStore & { v: string | null } => {
  const s = { v, read: () => s.v, write: (t: string) => { s.v = t; } };
  return s;
};

describe("the regs page's foot (T236)", () => {
  it("says it is not legal advice, then how current the page's sources are", () => {
    const asOf = "Regulations as of Oct 27, 2026 · salmon feed 3h ago · in-season 1h ago";
    render(<RegsScreen page={vedder1010 as unknown as RegsPage} palette={LIGHT} actions={NOOP} onDate={() => {}}
                       licence={<Text>licence</Text>} asOf={asOf} />);
    expect(screen.getByTestId("regs-legal").textContent).toBe(LEGAL_WORDS.footer + asOf);
  });
  it("with no as-of line, the disclaimer alone", () => {
    render(<RegsScreen page={vedder1010 as unknown as RegsPage} palette={LIGHT} actions={NOOP} onDate={() => {}} />);
    expect(screen.getByTestId("regs-legal").textContent).toBe(LEGAL_WORDS.footer);
    expect(screen.queryByTestId("regs-as-of")).toBeNull();
  });
});

describe("the first-run notice (T236)", () => {
  it("opens on a device that has not accepted it; I understand stores the version and closes it", () => {
    const store = memory();
    const { result } = renderHook(() => useFirstRun(store));
    render(<FirstRunNotice state={result.current} palette={LIGHT} />);
    expect(screen.getByTestId("first-run-notice").textContent).toContain("not legal advice");
    act(() => { fireEvent.click(screen.getByText(LEGAL_WORDS.firstRun.accept)); });
    expect(store.v).toBe(FIRST_RUN_VERSION);
    expect(result.current.open).toBe(false);
  });
  it("stays down once accepted, and with no store (a test, a screen alone)", () => {
    expect(renderHook(() => useFirstRun(memory(FIRST_RUN_VERSION))).result.current.open).toBe(false);
    expect(renderHook(() => useFirstRun(null)).result.current.open).toBe(false);
  });
  it("closed without accepting: down for this run, stored nothing", () => {
    const store = memory("disclaimer-0");
    const { result } = renderHook(() => useFirstRun(store));
    expect(result.current.open).toBe(true);
    act(() => result.current.close());
    expect([result.current.open, store.v]).toEqual([false, "disclaimer-0"]);
  });
});

describe("Sources and licences (T235)", () => {
  it("lists every dataset with its licence and the words it asks for", () => {
    render(<LicencesList vm={licencesVM()} palette={LIGHT} />);
    expect(screen.getAllByTestId(/^licence-(?!words-)/).map((e) => e.getAttribute("data-testid")))
      .toEqual(LICENCE_ROWS.map((r) => `licence-${r.k}`));
    expect(screen.getByTestId("licence-words-osm").textContent).toBe("© OpenStreetMap contributors");
    expect(screen.queryByTestId("sources-odbl")).toBeNull();
  });
});
