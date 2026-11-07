/**
 * (T369 D14) A PHONE'S COLOURING CLEAR, THEN ITS RELABEL, IN ONE FRAME, ENDS RELABELLED. MapLibre Native's feature-state
 * queue (`source_feature_state.cpp`) applies a frame's sets first and its removals after: a whole-layer
 * `removeFeatureState({sourceLayer})` sent beside the relabelling wiped the new labels (8a5b5bd fixed the highlight's
 * per-key removals the same way). The engine's queue (`StateQueue`) clears by NULL writes, in order with the sets.
 */
import { describe, expect, it } from "vitest";
import { StateQueue } from "./native/useColouring";

type State = Record<string, unknown>;
/** The native layer's per-frame queue: `setFeatureStates` in order, then any whole-layer removal. */
function mbgl() {
  const cur = new Map<string, State>();
  let sets: [string, State][] = [];
  let cleared: string[] = [];
  return {
    setFeatureStates: (layer: string, ids: string[], states: State[]) => {
      ids.forEach((id, i) => sets.push([`${layer}/${id}`, states[i]!]));
    },
    removeLayer: (layer: string) => { cleared.push(layer); },
    frame() {
      for (const [k, s] of sets) cur.set(k, { ...(cur.get(k) ?? {}), ...s });
      for (const l of cleared) for (const k of [...cur.keys()]) if (k.startsWith(`${l}/`)) cur.delete(k);
      sets = []; cleared = [];
    },
    /** what a paint reads (a null value is unset) */
    on: (layer: string, id: string) =>
      Object.fromEntries(Object.entries(cur.get(`${layer}/${id}`) ?? {}).filter(([, v]) => v !== null)),
  };
}
const send = (q: StateQueue, m: ReturnType<typeof mbgl>) => { for (const [l, ids, st] of q.drain()) m.setFeatureStates(l, ids, st); };

describe("(T369 D14) the native colouring queue", () => {
  it("a clear then the relabel in ONE frame: the new labels stand, the old keys are gone", () => {
    const m = mbgl(), q = new StateQueue();
    q.set("stream", "1", { status: 2 }); q.set("stream", "2", { status: 3, salmon: 1 });
    send(q, m); m.frame();
    q.clear("stream");
    q.set("stream", "1", { status: 5 });
    send(q, m); m.frame();
    expect([m.on("stream", "1"), m.on("stream", "2")]).toEqual([{ status: 5 }, {}]);
  });

  it("MUTATION PIN: the old whole-layer removal, sent beside the relabel, wipes it (the bug this queue fixes)", () => {
    const m = mbgl(), q = new StateQueue();
    q.set("stream", "1", { status: 2 });
    send(q, m); m.frame();
    m.removeLayer("stream");
    q.set("stream", "1", { status: 5 });
    send(q, m); m.frame();
    expect(m.on("stream", "1")).toEqual({});
  });

  it("a clear forgets what was queued for the layer, and nothing is sent for a layer that wears nothing", () => {
    const q = new StateQueue();
    q.clear("lake");
    expect(q.drain()).toEqual([]);
    q.set("lake", "9", { survey: 1 });
    q.clear("lake");
    expect(q.drain()).toEqual([["lake", ["9"], [{ survey: null }]]]);
  });

  it("(T451) a section cleared then relabelled in one burst is ONE write: no frame between them draws it bare", () => {
    const q = new StateQueue();
    q.set("stream", "1", { p: 2, rs: 4 }); q.set("stream", "2", { p: 3 });
    q.drain();
    q.clear("stream");
    q.set("stream", "1", { p: 2 }); q.set("stream", "2", { p: 3 });
    // a renderer that may draw between ANY two writes of the batch: every write is a frame
    const cur = new Map<string, State>([["1", { p: 2, rs: 4 }], ["2", { p: 3 }]]);
    const bare: string[] = [];
    for (const [, ids, states] of q.drain()) ids.forEach((id, i) => {
      cur.set(id, { ...cur.get(id), ...states[i] });
      for (const [k, v] of cur) if (v.p === null || v.p === undefined) bare.push(k);
    });
    expect(bare).toEqual([]);
    expect(Object.fromEntries(cur)).toEqual({ 1: { p: 2, rs: null }, 2: { p: 3, rs: null } });
  });
});
