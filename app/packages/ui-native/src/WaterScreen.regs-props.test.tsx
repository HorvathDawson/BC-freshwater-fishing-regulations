/**
 * T01 → T21: the water screen draws the Regulations page from what it is handed — the release loader, the one day, the
 * part. No loader (the dev bundle holds no regulations): the placeholder, and nothing asked of a loader. A loader whose
 * store cannot open: "Regulations unavailable" with the reason, never a blank or a guess. The committed fixture release
 * (T10): the Keep tab of the part named, with its header tabs; Gear / Methods / Licence keep a placeholder.
 */
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import { openRegsFixture } from "@app/data/release/node";
import { WaterScreen } from "./WaterScreen";
import { LIGHT } from "./theme";

afterEach(cleanup);

describe("<WaterScreen> regulations", () => {
  it("no loader: the placeholder stands", async () => {
    render(<WaterScreen source={makeFixtureSource()} item={"gnis:8634" as ItemId} palette={LIGHT} />);
    await waitFor(() => expect(screen.getByText("Chilliwack River")).toBeTruthy());
    expect(screen.getByRole("heading", { name: "Regulations are coming" })).toBeTruthy();
    expect(screen.queryByTestId("keep-tab")).toBeNull();
  });

  it("a loader whose files cannot be fetched: says the regulations are unavailable, and why", async () => {
    const broken = { release: "r-x", manifest: {}, regionsOf: () => [], regionDb: () => Promise.reject(new Error("offline")),
                     db: () => Promise.reject(new Error("offline")) } as unknown as ReleaseLoader;
    render(<WaterScreen source={makeFixtureSource()} item={"gnis:8634" as ItemId} palette={LIGHT} loader={broken} />);
    await waitFor(() => expect(screen.getByText(/^Regulations unavailable: /)).toBeTruthy(), { timeout: 10_000 });
    expect(screen.queryByTestId("keep-tab")).toBeNull();
  });

  it("the fixture release (T23): the regs header fixed above ONE scroll — the Chilliwack's closed-all-year part on Oct 10; Gear, Methods and Licence are placeholders in the same scroll", async () => {
    render(<WaterScreen source={makeFixtureSource()} item={"gnis:8634" as ItemId} palette={LIGHT}
                        loader={openRegsFixture()} day={new Date(2026, 9, 10, 12)} part={0} />);
    await waitFor(() => expect(screen.getByTestId("keep-tab")).toBeTruthy(), { timeout: 20_000 });
    const header = screen.getByTestId("regs-header");
    expect(within(header).getByRole("heading").textContent).toBe("Chilliwack River");
    expect(within(header).getByRole("button", { name: "Change date, now Oct 10" }).textContent).toBe("Sat ▾Oct 10");
    const tabs = within(screen.getByRole("tablist", { name: "Section" }));
    expect(tabs.getAllByRole("tab").map((t) => [t.textContent, t.getAttribute("aria-selected")]))
      .toEqual([["What you can keep", "true"], ["Gear", "false"], ["Methods", "false"], ["Licence", "false"]]);
    // the header is not in the scroll; the old frame's placeholder is gone
    expect(screen.getByTestId("regs-scroll").contains(header)).toBe(false);
    expect(screen.queryByRole("heading", { name: "Regulations are coming" })).toBeNull();
    expect(screen.getByTestId("keep-strip").textContent).toMatch(/^✕Closed/);
    expect(screen.getByTestId("keep-banner").textContent).toMatch(/^Closed all year/);
    // one scroll: Keep, then the three sections T34/T35 will fill, each saying it is not in the app yet
    expect(screen.getAllByTestId("regs-tab-pending").map((e) => e.textContent)).toEqual(["Gear", "Methods", "Licence"].map((t) =>
      `${t}: not in the app yet. Check the B.C. Freshwater Fishing Regulations Synopsis before you fish.`));
    expect([...screen.getByTestId("regs-content").children].map((e) => e.getAttribute("data-testid")))
      .toEqual(["keep-tab", "regs-gear", "regs-methods", "regs-licence"]);
  }, 30_000);
});
