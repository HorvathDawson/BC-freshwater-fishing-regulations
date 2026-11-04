/**
 * (T385) THE WATER HEADER'S SHARE: drawn beside × when the Shell hands one (`WaterViews.share`), said aloud as
 * "Share this water", pressed once per tap; what it said ("Link copied") sits in the same row. No share: not drawn.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { SHARE_WORDS } from "@app/core";
import type { ShareControl } from "@app/ui";
import { FaceBar } from "./Faces";
import { WaterViewsContext, type WaterViews } from "./ViewSwipe";
import { LIGHT } from "./theme";

afterEach(cleanup);

const views = (share: ShareControl | null): WaterViews => ({
  views: ["regs", "conditions", "map"], on: "regs", onView: () => {}, onBack: () => {}, backLabel: "Map",
  close: () => {}, closeLabel: "Close", share, swipe: null, bodySwipe: null,
});
const mount = (share: ShareControl | null) => render(
  <WaterViewsContext.Provider value={views(share)}>
    <FaceBar palette={LIGHT} face="regulations" onFace={() => {}} />
  </WaterViewsContext.Provider>);

describe("(T385) Share on the water header", () => {
  it("drawn beside ×, named, pressed; what it said in the row", () => {
    const press = vi.fn();
    mount({ label: SHARE_WORDS.label, said: SHARE_WORDS.copied, busy: false, press });
    fireEvent.click(screen.getByLabelText(SHARE_WORDS.label));
    expect([press.mock.calls.length, screen.getByTestId("water-share-said").textContent,
            !!screen.getByTestId("water-close")]).toEqual([1, "Link copied", true]);
  });

  it("not drawn without one", () => {
    mount(null);
    expect([screen.queryByTestId("water-share"), !!screen.getByTestId("water-close")]).toEqual([null, true]);
  });
});
