/**
 * Leaving a screen by ANY back (G1 review): the spot screen's ‹ ran its own cleanup (end the edit, discard the photos
 * it added) before popping, but Android's system back and the browser's back pop the route directly — so an edit
 * outlived its spot and the next spot opened in edit mode with the old text, and Save wrote it onto the wrong spot.
 * The cleanup now follows the ROUTE (`useOnLeave` on the open spot), so every input runs it, once.
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import { useState } from "react";
import { useBack, type SystemBack } from "./gestures";
import { useOnLeave } from "./leave";
import { useRoute, type RouteAction, type SpotId } from "./route";

afterEach(cleanup);
const A = "spot-a" as SpotId, B = "spot-b" as SpotId;

function androidBack() {
  let handler: (() => boolean) | null = null;
  const system: SystemBack = (back) => { handler = back; return { sync: () => {}, dispose: () => { handler = null; } }; };
  return { system, press: () => act(() => { handler!(); }) };
}

interface Out { go?: (a: RouteAction) => void; edit?: string | null; spot?: SpotId | null;
                setEdit?: (e: string | null) => void; close?: () => void; discarded: number }

/** The Shell's spot screen in miniature: a Shell-level edit, a ‹ that ends it then pops, and the route-driven leave. */
function SpotProbe({ system, out, wired }: { system: SystemBack; out: Out; wired: boolean }) {
  const [route, go] = useRoute();
  const [edit, setEdit] = useState<string | null>(null);
  const { back } = useBack(route, go, system);
  const spot = route.mode === "browse" ? route.spot : null;
  const endEdit = () => { if (edit !== null) out.discarded++; setEdit(null); };
  useOnLeave(wired ? spot : null, endEdit);
  Object.assign(out, { go, edit, spot, setEdit, close: () => { endEdit(); back(); } });
  return null;
}

function editThenOpenB(press: () => void, out: Out) {
  act(() => { out.go!({ type: "openSpot", id: A }); });
  act(() => { out.setEdit!("A's title"); });
  press();
  const afterBack = { spot: out.spot, edit: out.edit, discarded: out.discarded };
  act(() => { out.go!({ type: "openSpot", id: B }); });
  return [afterBack, { spot: out.spot, edit: out.edit, discarded: out.discarded }];
}

describe("useOnLeave: a spot's edit ends however the spot is left", () => {
  it("Android's system back from an editing spot ends the edit; the next spot opens in view mode", () => {
    const a = androidBack();
    const out: Out = { discarded: 0 };
    render(<SpotProbe system={a.system} out={out} wired />);
    expect(editThenOpenB(a.press, out)).toEqual([
      { spot: null, edit: null, discarded: 1 }, { spot: B, edit: null, discarded: 1 }]);
  });
  it("without it (the defect): the edit survives the system back and B opens holding A's title", () => {
    const a = androidBack();
    const out: Out = { discarded: 0 };
    render(<SpotProbe system={a.system} out={out} wired={false} />);
    expect(editThenOpenB(a.press, out)[1]).toEqual({ spot: B, edit: "A's title", discarded: 0 });
  });
  it("the ‹ button still ends it exactly once (its own cleanup, then the leave finds nothing to end)", () => {
    const a = androidBack();
    const out: Out = { discarded: 0 };
    render(<SpotProbe system={a.system} out={out} wired />);
    expect(editThenOpenB(() => act(() => { out.close!(); }), out)).toEqual([
      { spot: null, edit: null, discarded: 1 }, { spot: B, edit: null, discarded: 1 }]);
  });
  it("moving straight from one spot to another ends the first one's edit", () => {
    const a = androidBack();
    const out: Out = { discarded: 0 };
    render(<SpotProbe system={a.system} out={out} wired />);
    act(() => { out.go!({ type: "openSpot", id: A }); });
    act(() => { out.setEdit!("A's title"); });
    act(() => { out.go!({ type: "openSpot", id: B }); });
    expect({ spot: out.spot, edit: out.edit, discarded: out.discarded }).toEqual({ spot: B, edit: null, discarded: 1 });
  });
});
