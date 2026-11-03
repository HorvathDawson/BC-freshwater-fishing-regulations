/**
 * (T345) THE SELECTION MOVED ON A PHONE ENDS WHERE IT WAS SENT. The Coquitlam's Map view, part 3 → "Show all 3 parts" →
 * part 1 (Mary Hill → Lougheed, sections 316504 + 316505), lit only 316505 — the "sliver" — while the view model and
 * the highlight call both named the two (emulator, `getFeatureState` after the pick: 316504 {water}, 316505
 * {water, part}). The cause is MapLibre Native's feature-state queue (`SourceFeatureState`): within one frame it
 * applies every set first and the removals after, and it keeps only the FIRST per-feature removal per source-layer.
 * The fake atlas below queues exactly so; the renderer must reach the state the highlight asked for through it.
 */
import { describe, expect, it } from "vitest";
import { nativeAdapter, nativeRenderer, type NativeRefs } from "./adapters/native";

type State = Record<string, unknown>;
/** An atlas source with MapLibre Native's per-frame queue (`source_feature_state.cpp`). */
function mbglAtlas() {
  const current = new Map<string, State>();
  let sets: [string, State][] = [];
  let removed = new Map<string, Map<string, string>>();       // source-layer → feature → key (the first only)
  const at = (sl: string, id: string | number) => `${sl}/${String(id)}`;
  return {
    current: (sl: string, id: number) => {
      const s = current.get(at(sl, id)) ?? {};
      // a null value is no state: every paint reads it as unset (`["boolean", ["feature-state", k], false]`)
      return Object.fromEntries(Object.entries(s).filter(([, v]) => v !== null && v !== undefined));
    },
    /** The frame is drawn: sets in order, then the removals. */
    frame() {
      for (const [k, s] of sets) current.set(k, { ...(current.get(k) ?? {}), ...s });
      for (const [sl, byId] of removed) for (const [id, key] of byId) {
        const s = current.get(at(sl, id));
        if (s) delete s[key];
      }
      sets = []; removed = new Map();
    },
    ref: {
      setFeatureState: async (f: { id: string | number; sourceLayer: string }, s: State) => { sets.push([at(f.sourceLayer, f.id), s]); },
      removeFeatureState: async (f: { id: string | number; sourceLayer: string }, key?: string) => {
        // `if (deletedStates.count(sourceLayer) == 0 && …)`: a second removal in the layer this frame is dropped
        if (!removed.has(f.sourceLayer) && key !== undefined) removed.set(f.sourceLayer, new Map([[String(f.id), key]]));
      },
      querySourceFeatures: async () => [],
    },
  };
}

// the Coquitlam's three parts on release r-4df5bccc9360 (`ui/src/waterMap.test.ts` T345)
const P1 = [316504, 316505];
const P2 = [316506];
const P3 = [60536, 62474, 62912, 63390, 63452, 316507, 316508, 316509, 316510];
const ALL = [...P1, ...P2, ...P3];

function phone() {
  const atlas = mbglAtlas();
  const refs: NativeRefs = { map: () => null, atlas: () => atlas.ref as never, camera: () => null };
  const r = nativeRenderer(refs, (e) => { throw e; });
  const a = nativeAdapter();
  let was: number[] = [];
  const show = (sel: number[]) => {
    const water = ALL.filter((s) => !sel.includes(s));
    a.highlight(r.states(() => was), sel, water, "dim");
    was = [...sel, ...water];
  };
  return { atlas, show };
}

describe("(T345) a phone's highlight moved between parts ends as sent", () => {
  it("part 2 → ▲ part 3 → the list's part 1 (sent twice in one frame, as the screen does): both its sections lit", () => {
    const { atlas, show } = phone();
    show(P2); atlas.frame();
    show(P3); atlas.frame();
    show(P1); show(P1); atlas.frame();       // the pick: the frame's part follows, the view renders again
    const lit = ALL.filter((s) => atlas.current("stream", s).part === true);
    const water = ALL.filter((s) => atlas.current("stream", s).water === true);
    expect(lit).toEqual(P1);
    expect(water).toEqual([...P2, ...P3]);
  });

  it("the tapped way in: part 1 → ▲ part 2 lights 316506 alone, never the 440 m below the bridge (\"past its bounds\")", () => {
    const { atlas, show } = phone();
    show(P1); atlas.frame();
    show(P2); atlas.frame();
    expect(ALL.filter((s) => atlas.current("stream", s).part === true)).toEqual(P2);
    expect(ALL.filter((s) => atlas.current("stream", s).water === true)).toEqual([...P1, ...P3]);
  });
});
