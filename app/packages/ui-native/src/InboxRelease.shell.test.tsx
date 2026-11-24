/**
 * (T1030) THE INBOX'S DATA-UPDATE ROW SAYS HOW MANY WATERS THE RELEASE CHANGED — what the release in use states
 * (`manifest.changed_waters`), passed by the real Shell; a release that states none says nothing of it (rule 32), never 0.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import { HOME, LayoutWidthContext } from "@app/ui";
import { inboxVM, parseInbox } from "@app/core";
import { LIGHT } from "./theme";

vi.mock("@app/map", async (orig) => ({ ...(await orig<Record<string, unknown>>()), Map: () => null }));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
vi.mock("./BarControls", async () => await import("./BarControls.native"));
const { Shell } = await import("./Shell");
const { openRegsFixture } = await import("@app/data/release/node");
afterEach(cleanup);

const memory = (v: string | null = null) => ({ v, read() { return this.v; }, write(t: string) { this.v = t; } });
/** a device that last saw another release: the one in use now is a data-update row */
const SEEN_BEFORE = JSON.stringify({ format: "inseason-inbox/1", rev: 2, items: [], base: { release: "r-older", app: null } });

async function rowAfterMount(changed: number | undefined) {
  const inbox = memory(SEEN_BEFORE);
  const loader = openRegsFixture({ manifest: (m) => (changed === undefined ? m : { ...m, changed_waters: changed }) });
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 844 });
  render(<LayoutWidthContext.Provider value={390}><GestureHandlerRootView>
    <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
           tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={HOME} spots={[]} onSaveSpot={() => {}}
           locate={null} loader={loader} inSeasonInbox={inbox} inSeasonSeen={null} />
  </GestureHandlerRootView></LayoutWidthContext.Provider>);
  await act(async () => { await new Promise((r) => setTimeout(r, 300)); });
  const state = parseInbox(inbox.v);
  const ev = state?.events?.find((e) => e.kind === "data");
  const row = ev ? inboxVM(state!, ev.at).groups.flatMap((g) => g.rows).find((r) => r.id === ev.id) : undefined;
  return { changed: ev?.changed, text: row?.text, release: loader.manifest!.data_release, id: ev?.id };
}

describe("(T1030) the data-update row's count is the release's own", () => {
  it("a release that states 12 says \"12 waters changed\"; one water, \"1 water changed\"", async () => {
    const r = await rowAfterMount(12);
    expect([r.id, r.changed, r.text]).toEqual([`data:${r.release}`, 12, "12 waters changed"]);
    cleanup();
    const one = await rowAfterMount(1);
    expect([one.changed, one.text]).toEqual([1, "1 water changed"]);
  });
  it("a release that states no count says nothing of it, never 0", async () => {
    const r = await rowAfterMount(undefined);
    expect([r.id, r.changed, r.text]).toEqual([`data:${r.release}`, null, ""]);
  });
});
