/**
 * (T566) THE KEY FOLLOWS THE MAP (`heldLook.ts`): a new colouring's key is shown in the same step as the map says it drew
 * it, never before — the owner's "layers stay wrong for half a second" was the new key over the old colours. Pure steps,
 * no clocks.
 */
import { describe, expect, it } from "vitest";
import { heldLook, HeldLookStore } from "./heldLook";

describe("the key under the map is the colouring the map drew (T566)", () => {
  it("a press asks for salmon: the key stays trout until the map says salmon is drawn", () => {
    let s = heldLook.start("trout key", "trout");
    s = heldLook.asked(s, "salmon key", "salmon");
    expect([s.shown, s.drawn === s.want]).toEqual(["trout key", false]);
    s = heldLook.asked(s, "salmon key", "salmon");          // a render while the map lays out: still trout
    expect(s.shown).toBe("trout key");
    s = heldLook.drawn(s, "salmon");
    expect([s.shown, s.drawn === s.want]).toEqual(["salmon key", true]);
  });

  it("the map saying an OLD look is drawn moves nothing", () => {
    let s = heldLook.start("trout key", "trout");
    s = heldLook.asked(s, "salmon key", "salmon");
    s = heldLook.asked(s, "runs key", "runs");
    expect(heldLook.drawn(s, "salmon")).toBe(s);
    expect(s.shown).toBe("trout key");                      // salmon was never drawn: the key is still trout's
    s = heldLook.drawn(s, "runs");
    expect(s.shown).toBe("runs key");
  });

  it("back to the drawn look before the new one is drawn: the key waits for the map's word on it again", () => {
    let s = heldLook.start("trout key", "trout");
    s = heldLook.asked(s, "salmon key", "salmon");
    s = heldLook.asked(s, "trout key 2", "trout");
    expect([s.shown, s.drawn === s.want]).toEqual(["trout key", false]);
    s = heldLook.drawn(s, "trout");
    expect(s.shown).toBe("trout key 2");
  });

  it("a drawn look's own updates (a legend tap, a new day) show at once", () => {
    let s = heldLook.start("trout key", "trout");
    s = heldLook.asked(s, "trout key, closed only", "trout");
    expect(s.shown).toBe("trout key, closed only");
  });
});

describe("the store tells only its readers, and only on the map's word (T566)", () => {
  it("asking a new look notifies nobody; the map's word on it does, once", () => {
    const st = new HeldLookStore<string>();
    let told = 0;
    st.subscribe(() => { told++; });
    st.ask("trout key", "trout");
    st.ask("salmon key", "salmon");
    expect([st.get().shown, told]).toEqual(["trout key", 0]);
    st.drawn("salmon");
    st.drawn("salmon");
    expect([st.get().shown, told]).toEqual(["salmon key", 1]);
  });

  it("a render asking for the same thing keeps the same snapshot (no re-render loop)", () => {
    const st = new HeldLookStore<string>();
    st.ask("trout key", "trout");
    const a = st.get();
    st.ask("trout key", "trout");
    expect(st.get()).toBe(a);
  });
});
