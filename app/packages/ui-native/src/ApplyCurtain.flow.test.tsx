/**
 * (T746) ONE CURTAIN FOR EVERY WAY AN APPLY STARTS — the app's own wiring (`useReleaseBoot` → `useApplyCurtain` →
 * `ApplyCurtain`, as apps/mobile App.tsx composes them), the release boot mocked at the data seam. The safe moment on a
 * still map, the update sheet's "Apply now" and the return from the background each call the app's `apply()`, which is
 * one generation bump: each is driven here as that bump, and each must draw the same curtain. A failed apply is said on
 * it and reported, and the set in use stays. A cold start that applies says the curtain's line on its boot screen
 * (`startStage("apply")`, pinned in core install.test.ts and the boot's stage in tools/release-boot.test.ts).
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { setFailureSink, APPLY_WORDS, applyLine } from "@app/core";
import type { BootOptions, Booted } from "@app/data/release";

const boots: (() => Promise<Booted>)[] = [];
vi.mock("@app/data/release", async (orig) => ({
  ...(await orig<typeof import("@app/data/release")>()),
  bootRelease: () => boots.shift()!(),
  warmLike: async () => {},
}));

import { useApplyCurtain, useReleaseBoot } from "@app/ui";
import { ApplyCurtain } from "./ApplyCurtain";
import { LIGHT } from "./theme";

const set = (name: string) => ({ loader: { release: name, retire: async () => 0 } }) as unknown as Booted;
const OPTS = {} as BootOptions;

/** `target`: the release being applied, as App.tsx's `apply()` reads it off the update service */
function AppLike({ generation, target = null }: { generation: number; target?: string | null }) {
  const boot = useReleaseBoot(OPTS, generation);
  const c = useApplyCurtain(boot, target);
  return <>
    <span data-testid="on-screen">{boot.state === "ready" ? (boot.booted.loader as unknown as { release: string }).release : boot.state}</span>
    <ApplyCurtain palette={LIGHT} applying={c.applying} failed={c.failed} onClose={c.close} line={c.line} />
  </>;
}

const flush = async (ms = 0) => { await act(async () => { await vi.advanceTimersByTimeAsync(ms); }); };

describe("the apply curtain, every way in (T746)", () => {
  let reported: unknown[] = [];
  let unsink = () => {};
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ["Date", "performance", "setTimeout", "clearTimeout"] });
    reported = []; unsink = setFailureSink((e) => reported.push(e)); boots.length = 0;
  });
  afterEach(() => { unsink(); cleanup(); vi.useRealTimers(); vi.restoreAllMocks(); });

  for (const way of ["the safe moment (map still 3 s)", "Apply now", "back from the background"]) {
    it(`${way}: the curtain takes taps at once, says the line after 300 ms, and lifts once the new set is in`, async () => {
      let open = () => {};
      boots.push(async () => set("r-a"), () => new Promise<Booted>((done) => { open = () => done(set("r-b")); }));
      const { rerender } = render(<AppLike generation={0} />);
      await flush();
      expect(screen.getByTestId("on-screen").textContent).toBe("r-a");
      expect(screen.queryByTestId("apply-curtain")).toBeNull();
      rerender(<AppLike generation={1} target="release-four" />);   // apply(): one generation bump, every way in
      await flush();
      expect(screen.getByTestId("apply-curtain")).toBeTruthy();
      expect(screen.queryByTestId("apply-line")).toBeNull();
      await flush(300);
      // the line is the release's own (core applyLine), the same a cold start's boot screen says for it
      expect(screen.getByTestId("apply-line").textContent).toBe("Casting the new regulations into place…");
      expect(screen.getByTestId("apply-line").textContent).toBe(applyLine("release-four"));
      await flush(1000);
      expect(screen.getByTestId("apply-line").textContent).toBe("Casting the new regulations into place…");   // one line, start to end
      expect(screen.getByTestId("on-screen").textContent).toBe("r-a");   // the set in use stays under it
      open();
      await flush();
      expect(screen.getByTestId("on-screen").textContent).toBe("r-b");
      await flush(400);
      expect(screen.queryByTestId("apply-curtain")).toBeNull();
      expect(reported).toEqual([]);
    });
  }

  it("a failed apply: said on the curtain in plain words, reported, the old set kept; closed by the reader", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    boots.push(async () => set("r-a"), async () => { throw new Error("core: open failed"); });
    const { rerender } = render(<AppLike generation={0} />);
    await flush();
    rerender(<AppLike generation={1} />);
    await flush();
    expect(screen.getByTestId("apply-failed").textContent).toBe(APPLY_WORDS.failed);
    expect(reported.map(String)).toEqual(["Error: core: open failed"]);
    expect(screen.getByTestId("on-screen").textContent).toBe("r-a");
    fireEvent.click(screen.getByTestId("apply-failed-ok"));
    await flush(400);
    expect(screen.queryByTestId("apply-curtain")).toBeNull();
  });
});
