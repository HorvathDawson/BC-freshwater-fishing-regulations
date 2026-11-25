/**
 * (T1049 cold start) The water screen tells the Regulations page that its salmon card is still COMING (`coming`): on the
 * 0.9.0 device pass the card waited on its feed and landed ~200 dp above the sections after a start link's follow had
 * stopped, so Gear and Licence were left short. While the card is on its way the page's start keeps following; once it is
 * in (or said as failed), it may settle.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import type { SalmonFacts } from "@app/core";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId } from "@app/data";
import type { ReleaseLoader } from "@app/data/release";
import { openRegsFixture } from "@app/data/release/node";
import { LIGHT } from "./theme";
import { InSeasonContext } from "@app/ui";
import type { RegsScreenProps } from "./regs/RegsScreen";

const coming: boolean[] = [];
vi.mock("./regs/RegsScreen", async (orig) => {
  const m = await orig<typeof import("./regs/RegsScreen")>();
  return { ...m, RegsScreen: (p: RegsScreenProps) => { coming.push(!!p.coming); return m.RegsScreen(p); } };
});
const { WaterScreen } = await import("./WaterScreen");

afterEach(() => { cleanup(); coming.length = 0; });
const LOADED = { timeout: 20_000 } as const;

/** The fixture release as one that ships salmon (its manifest names the file), the salmon read as given. */
function withSalmon(salmon: () => Promise<SalmonFacts | null>): ReleaseLoader {
  const live = openRegsFixture();
  return Object.assign(Object.create(Object.getPrototypeOf(live)), live, {
    manifest: { ...live.manifest, files: { ...live.manifest.files, salmon: { url: "salmon.bin.gz" } } },
    salmon, method: async () => null, access: async () => null,
  });
}

describe("<WaterScreen> (T1049 cold start) the page is told its salmon card is still coming", () => {
  it("coming while the card waits on its feed; not once it has landed (here: failed, said in its place)", async () => {
    let land: (e: Error) => void = () => {};
    const salmon = () => new Promise<SalmonFacts | null>((_, no) => { land = no; });
    render(<WaterScreen source={makeFixtureSource()} item={"gnis:8634" as ItemId} palette={LIGHT} glance
                        loader={withSalmon(salmon)} day={new Date(2026, 9, 10, 12)} part={0} />);
    await waitFor(() => expect(screen.getByTestId("keep-tab")).toBeTruthy(), LOADED);
    const whileWaiting = coming.at(-1);
    land(new Error("salmon: damaged"));
    await waitFor(() => expect(coming.at(-1)).toBe(false), LOADED);
    expect({ whileWaiting, pending: !!screen.queryByTestId("salmon-pending") }).toEqual({ whileWaiting: true, pending: true });
  }, 30_000);

  it("(T1049 review) the in-season feed's first answer not in yet: coming (its notices are drawn first on the page); in: not", async () => {
    const base = { matched: [], unmatched: [], ended: [], data: null, release: null };
    const tree = (pending: boolean) => (
      <InSeasonContext.Provider value={{ ...base, pending }}>
        <WaterScreen source={makeFixtureSource()} item={"gnis:8634" as ItemId} palette={LIGHT} loader={openRegsFixture()} day={new Date(2026, 9, 10, 12)} part={0} />
      </InSeasonContext.Provider>);
    const r = render(tree(true));
    await waitFor(() => expect(screen.getByTestId("keep-tab")).toBeTruthy(), LOADED);
    const whileWaiting = coming.at(-1);
    r.rerender(tree(false));
    await waitFor(() => expect(coming.at(-1)).toBe(false), LOADED);
    expect(whileWaiting).toBe(true);
  }, 30_000);

  it("a release that ships no salmon: never coming", async () => {
    render(<WaterScreen source={makeFixtureSource()} item={"gnis:8634" as ItemId} palette={LIGHT}
                        loader={openRegsFixture()} day={new Date(2026, 9, 10, 12)} part={0} />);
    await waitFor(() => expect(screen.getByTestId("keep-tab")).toBeTruthy(), LOADED);
    expect(coming.includes(true)).toBe(false);
  }, 30_000);
});
