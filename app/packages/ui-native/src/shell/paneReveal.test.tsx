/**
 * (T498) A WATER'S PANE IS SHOWN WHOLE. Two halves:
 *   - the hold (`usePaneHold`): a pane opened over the map is held (the map stays) until its view says it is drawn whole,
 *     or until `PANE_HOLD_MS`; no hold at a landing, over a spot, or between two waters;
 *   - the word (`usePaneReady`) from the REAL Regulations view on the regs fixture release: when it says "ready", what it
 *     draws is what it draws once everything has arrived — the Salmon card and the access notices in, nothing still
 *     coming. Measured before the fix (testbed `pane-open`, a cold tap on Norrish Creek): five screens in 250 ms — the
 *     map, the frame with only the name, the page without its Salmon card, with it, then pushed down by the access banner.
 *
 * MUTATION PIN: a ready said at the page's first draw (`drawn` alone, the open probe's "commit") fails the second test:
 * the Salmon card is still its pending face then.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render, screen } from "@testing-library/react";
import { Text } from "react-native";
import type { ItemId, SectionId } from "@app/data";
import { makeReleaseSource } from "@app/data/release";
import { openRegsFixtureWithLines, sectionNames } from "@app/data/release/node";
import { LIGHT } from "../theme";
import { WaterScreen } from "../WaterScreen";
import { PANE_HOLD_MS, PaneReadyContext, usePaneHold, usePaneReady } from "./paneReveal";

afterEach(() => { cleanup(); vi.useRealTimers(); });

/** The Shell's use of the hold, in miniature: what is on screen, and whether the pane is held. */
function Host({ open, overMap, ready }: { open: boolean; overMap: boolean; ready: boolean }) {
  const pane = usePaneHold(open, overMap);
  return (
    <>
      <Text testID="state">{`${open ? "open" : "browse"} ${pane.holding ? "held" : "shown"}`}</Text>
      {open && <PaneReadyContext.Provider value={pane.holding ? pane.ready : null}><View ready={ready} /></PaneReadyContext.Provider>}
    </>
  );
}
function View({ ready }: { ready: boolean }) { usePaneReady(ready); return null; }
const state = () => screen.getByTestId("state").textContent;

describe("(T498) the hold: a pane opened over the map waits, hidden, until it is whole", () => {
  it("held from the very render that opens it, shown when the view says ready; never held at a landing or not over the map", () => {
    vi.useFakeTimers();
    const r = render(<Host open={false} overMap ready={false} />);
    r.rerender(<Host open overMap ready={false} />);
    expect(state()).toBe("open held");                 // the first render of the open pane: already held
    act(() => { vi.advanceTimersByTime(PANE_HOLD_MS - 50); });
    expect(state()).toBe("open held");
    r.rerender(<Host open overMap ready />);
    expect(state()).toBe("open shown");
    // back to the map, and open again over a spot (no map shown): not held
    r.rerender(<Host open={false} overMap ready={false} />);
    r.rerender(<Host open overMap={false} ready={false} />);
    expect(state()).toBe("open shown");
    cleanup();
    // a landing: the app starts on the water, nothing to hold
    render(<Host open overMap ready={false} />);
    expect(state()).toBe("open shown");
  });

  it("a pane that never says ready is shown after PANE_HOLD_MS, with its own waiting state (never a map that ignores the reader)", () => {
    vi.useFakeTimers();
    const r = render(<Host open={false} overMap ready={false} />);
    r.rerender(<Host open overMap ready={false} />);
    act(() => { vi.advanceTimersByTime(PANE_HOLD_MS); });
    expect(state()).toBe("open shown");
  });
});

const lined = openRegsFixtureWithLines();
const source = makeReleaseSource(lined);
/**
 * The fixture release as a phone meets it: its Salmon file and its access check answer a little after the regulations
 * (the cold tap measured: page 45 ms, salmon 226 ms, access 276 ms). The regs fixture ships neither a Salmon file nor a
 * paint file, so here the manifest names a Salmon file that answers nothing (the card's failed face, after its loading
 * face) and the access check fails late (its failure is drawn) — what matters is that both come AFTER the page.
 */
const late = <T,>(ms: number, f: () => Promise<T>) => new Promise<T>((ok, no) => setTimeout(() => f().then(ok, no), ms));
const slow = new Proxy(lined, {
  get(t, k) {
    const v = Reflect.get(t, k, t) as unknown;
    if (k === "manifest") { const m = v as typeof lined.manifest; return m && { ...m, files: { ...m.files, salmon: m.files?.salmon ?? "salmon.json" } }; }
    if (typeof v !== "function") return v;
    const call = (...a: unknown[]) => (v as (...x: unknown[]) => Promise<unknown>).apply(t, a);
    if (k === "salmon") return () => late(150, async () => null);
    if (k === "paint" || k === "access") return (...a: unknown[]) => late(250, () => call(...a));
    return (v as (...x: unknown[]) => unknown).bind(t);
  },
});
const C = "gnis:19986" as ItemId;                 // the Coquitlam: its Mary Hill → Lougheed part (salmon, access)
const TAP = await sectionNames(lined).stream(356364584, 312) as SectionId;
const DAY = new Date(2026, 6, 15, 12);

async function until(ok: () => boolean, ms = 30_000): Promise<void> {
  const end = Date.now() + ms;
  while (!ok()) {
    if (Date.now() > end) throw new Error(`until: not settled in ${ms} ms:\n${document.body.textContent?.slice(0, 400)}`);
    await act(() => new Promise((d) => setTimeout(d, 25)));
  }
}

describe("(T498) the Regulations view says ready only once it is drawn whole", () => {
  it("what it draws at its ready is what it draws when everything is in (Salmon card, access notices)", async () => {
    let atReady: string | null = null;
    const say = () => { atReady ??= screen.queryByTestId("regs-view")?.textContent ?? "(no page)"; };
    render(
      <PaneReadyContext.Provider value={say}>
        <WaterScreen source={source} loader={slow} item={C} section={TAP} part={2} day={DAY} palette={LIGHT} />
      </PaneReadyContext.Provider>);
    await until(() => atReady !== null);
    // settled: the page unchanged for 600 ms after the last late answer (salmon 150 ms, access 250 ms) could land
    await act(() => new Promise((d) => setTimeout(d, 400)));
    let was = "", since = Date.now();
    await until(() => { const now = screen.queryByTestId("regs-view")?.textContent ?? "";
                        if (now !== was) { was = now; since = Date.now(); } return now !== "" && Date.now() - since > 600; });
    expect(atReady).not.toBe("(no page)");
    expect(atReady).not.toMatch(/Loading salmon|salmon.*loading/i);
    expect(atReady).toBe(screen.getByTestId("regs-view").textContent);
  }, 60_000);
});
