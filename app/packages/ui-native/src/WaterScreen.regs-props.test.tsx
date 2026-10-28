/**
 * T01 → T21: the water screen draws the Regulations page from what it is handed — the release loader, the one day, the
 * part. No loader (the screen mounted alone): "Regulations unavailable", and nothing asked of a loader. A loader whose
 * store cannot open: "Regulations unavailable" with the reason, never a blank or a guess. The committed fixture release
 * (T10): the Keep tab of the part named, with its header tabs; Gear / Methods / Licence keep a placeholder.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId, SectionId } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import { openRegsFixture } from "@app/data/release/node";
import { WaterScreen } from "./WaterScreen";
import { LIGHT } from "./theme";

afterEach(cleanup);

describe("<WaterScreen> regulations", () => {
  it("no loader: says the regulations are unavailable", async () => {
    render(<WaterScreen source={makeFixtureSource()} item={"gnis:8634" as ItemId} palette={LIGHT} />);
    await waitFor(() => expect(screen.getByText("Chilliwack River")).toBeTruthy());
    expect(screen.getByText("Regulations unavailable: no data release is open")).toBeTruthy();
    expect(screen.queryByTestId("keep-tab")).toBeNull();
  });

  it("a loader whose files cannot be fetched: says the regulations are unavailable, and why", async () => {
    const broken = { release: "r-x", manifest: {}, regionsOf: () => [], regionDb: () => Promise.reject(new Error("offline")),
                     db: () => Promise.reject(new Error("offline")) } as unknown as ReleaseLoader;
    render(<WaterScreen source={makeFixtureSource()} item={"gnis:8634" as ItemId} palette={LIGHT} loader={broken} />);
    await waitFor(() => expect(screen.getByText(/^Regulations unavailable: /)).toBeTruthy(), { timeout: 10_000 });
    expect(screen.queryByTestId("keep-tab")).toBeNull();
  });

  it("the fixture release (T23/T34/T35): the regs header fixed above ONE scroll — the Chilliwack's closed-all-year part on Oct 10; Gear, Methods and Licence draw theirs in the same scroll", async () => {
    render(<WaterScreen source={makeFixtureSource()} item={"gnis:8634" as ItemId} palette={LIGHT}
                        loader={openRegsFixture()} day={new Date(2026, 9, 10, 12)} part={0} />);
    await waitFor(() => expect(screen.getByTestId("keep-tab")).toBeTruthy(), { timeout: 20_000 });
    const header = screen.getByTestId("regs-header");
    expect(within(header).getByRole("heading").textContent).toBe("Chilliwack River");
    expect(within(header).getByRole("button", { name: "Change date, now Oct 10" }).textContent).toBe("SatOct 10");
    const tabs = within(screen.getByRole("tablist", { name: "Section" }));
    expect(tabs.getAllByRole("tab").map((t) => [t.textContent, t.getAttribute("aria-selected")]))
      .toEqual([["What you can keep", "true"], ["Gear", "false"], ["Methods", "false"], ["Licence", "false"]]);
    // the header is not in the scroll; no "unavailable" line
    expect(screen.getByTestId("regs-scroll").contains(header)).toBe(false);
    expect(screen.queryByText(/^Regulations unavailable/)).toBeNull();
    expect(screen.getByTestId("keep-strip").textContent).toMatch(/^✕Closed/);
    expect(screen.getByTestId("keep-banner").textContent).toMatch(/^Closed all year/);
    // one scroll: Keep, then the sections; (T34) Gear draws its VM — on a closed-all-year part, its one sentence
    expect(screen.getByTestId("gear-tab").textContent).toBe("No fishing here on any day of the year.");
    expect(screen.getByTestId("methods-tab").textContent).toBe("No fishing here on any day of the year.");
    // (T35) on a closed day the Licence section is its heading (D16 a)
    expect(screen.getByTestId("licence-tab").textContent).toBe("Licence");
    expect([...screen.getByTestId("regs-content").children].map((e) => e.getAttribute("data-testid")))
      .toEqual(["keep-tab", "regs-gear", "regs-methods", "regs-licence"]);
  }, 30_000);

  it("a release past its valid_until: the page leads with core's out-of-date sentence (none on an unstamped release)", async () => {
    const live = openRegsFixture();
    const lapsed: ReleaseLoader = Object.assign(Object.create(Object.getPrototypeOf(live)), live, {
      manifest: { ...live.manifest, regulations: { ...live.manifest.regulations, valid_until: "2020-03-31" } } });
    render(<WaterScreen source={makeFixtureSource()} item={"gnis:8634" as ItemId} palette={LIGHT}
                        loader={lapsed} day={new Date(2026, 9, 10, 12)} part={0} />);
    await waitFor(() => expect(screen.getByTestId("keep-tab")).toBeTruthy(), { timeout: 20_000 });
    expect(screen.getByTestId("regs-lapsed").textContent).toBe("Out of date: these regulations were valid to Mar 31, " +
      "2020. Check the current Synopsis and the Province's notices before you fish.");
    expect(screen.getByRole("alert").textContent).toBe(screen.getByTestId("regs-lapsed").textContent);
  }, 30_000);

  /*
   * (T147 review) THE PAGE PATH IS WIRED: a water opened with no tapped section (search, a start link) leads with its
   * part's no-access block. `pageAccessNotice` and core's words are pinned in @app/ui and @app/core; this pins that
   * WaterScreen hands `useAccessNotice` the store and the page — dropping them (the T54 call, section only) left every
   * other ui-native test green.
   */
  it("(T147) opened with no tapped section: the part's no-access block leads the page", async () => {
    const live = openRegsFixture();
    const inside = [{ area: { tier: "no_access", name: "Test Watershed" }, share: 1 }];
    // the fixture release carries no paint file: a paint whose no-access rows put every section inside closed land
    const closedLand: ReleaseLoader = Object.assign(Object.create(Object.getPrototypeOf(live)), live, {
      paint: async () => ({ access: { of: () => inside } }) });
    render(<WaterScreen source={makeFixtureSource()} item={"gnis:8634" as ItemId} palette={LIGHT}
                        loader={closedLand} day={new Date(2026, 9, 10, 12)} part={0} />);
    await waitFor(() => expect(screen.getByTestId("access-banner")).toBeTruthy(), { timeout: 20_000 });
    expect(screen.getByTestId("access-banner").textContent).toBe("No public accessThis part of the water is inside Test "
      + "Watershed. The public may not go there, so you can't fish here. The regulations below are the Province's; they "
      + "don't give you access.");
    expect([...screen.getByTestId("regs-content").children].map((e) => e.getAttribute("data-testid")).slice(0, 2))
      .toEqual(["access-banner", "keep-tab"]);
  }, 30_000);

  /*
   * (Q140, owner 2026-10-28) A TAPPED SECTION WHOLLY OUTSIDE no-access land, on a part that runs through it, leads with
   * the PART's banner in the same block — the page asks its part beside the tapped section (`useAccessNotice`).
   */
  it("(Q140) a tapped section outside no-access land: its part's banner leads the page", async () => {
    const live = openRegsFixture();
    const TAPPED = 987654321;
    const inside = [{ area: { tier: "no_access", name: "Test Watershed" }, share: 1 }];
    const partlyClosed: ReleaseLoader = Object.assign(Object.create(Object.getPrototypeOf(live)), live, {
      paint: async () => ({ access: { of: (sid: unknown) => (Number(sid) === TAPPED ? [] : inside) } }),
      access: async () => null });
    render(<WaterScreen source={makeFixtureSource()} item={"gnis:8634" as ItemId} palette={LIGHT}
                        loader={partlyClosed} day={new Date(2026, 9, 10, 12)} part={0}
                        section={TAPPED as SectionId} />);
    await waitFor(() => expect(screen.getByTestId("access-banner")).toBeTruthy(), { timeout: 20_000 });
    expect(screen.getByTestId("access-banner").textContent).toBe("No public accessThis part of the water is inside Test "
      + "Watershed. The public may not go there, so you can't fish here. The regulations below are the Province's; they "
      + "don't give you access.");
  }, 30_000);

  it("(T35) the Licence tab's pick reaches the page: Under 16 rebuilds it for that angler (useAnglerProfile, D8 a)", async () => {
    render(<WaterScreen source={makeFixtureSource()} item={"gnis:8634" as ItemId} palette={LIGHT}
                        loader={openRegsFixture()} day={new Date(2026, 9, 10, 12)} part={1} />);
    await waitFor(() => expect(screen.getByTestId("keep-tab")).toBeTruthy(), { timeout: 20_000 });
    fireEvent.click(within(screen.getByTestId("regs-view")).getByRole("tab", { name: "Licence" }));
    const checked = () => screen.getAllByRole("radio").filter((r) => r.getAttribute("aria-checked") === "true").map((r) => r.textContent);
    expect(checked()).toEqual(["16 or older", "B.C. resident"]);
    fireEvent.click(screen.getByRole("radio", { name: "Under 16" }));
    await waitFor(() => expect(checked()).toEqual(["Under 16", "B.C. resident"]));
    expect(screen.getByText("B.C. resident, under 16")).toBeTruthy();
    fireEvent.click(screen.getByRole("radio", { name: "16 or older" }));   // the profile is the Shell's, in memory: put it back
    await waitFor(() => expect(checked()).toEqual(["16 or older", "B.C. resident"]));
  }, 30_000);
});
