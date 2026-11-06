import { describe, expect, it } from "vitest";
import { NATIVE_SENT_MAX, TileHold } from "./tile-hold";

/** The binding's half (MLRNVectorSource.kt `sent`): an access-ordered LRU of the tiles whose ids went to JS. */
class NativeSent {
  private m = new Map<string, true>();
  /** One request: true when its ids are sent (first time in this memory), false when only the repeat goes. */
  request(key: string): boolean {
    if (this.m.has(key)) { this.m.delete(key); this.m.set(key, true); return false; }
    this.m.set(key, true);
    if (this.m.size > NATIVE_SENT_MAX) this.m.delete(this.m.keys().next().value!);
    return true;
  }
}

describe("TileHold (T420: a tile's ids once per watch)", () => {
  it("a repeat moves its tile to the end; a tile never put is not held", () => {
    const h = new TileHold();
    h.put({ tag: 1, z: 1, x: 0, y: 0, layers: { stream: [1] } });
    h.put({ tag: 1, z: 1, x: 1, y: 0, layers: { stream: [2] } });
    expect(h.touch({ z: 1, x: 0, y: 0 })).toBe(true);
    expect(h.all().map((t) => t.stream)).toEqual([[2], [1]]);
    expect(h.touch({ z: 9, x: 9, y: 9 })).toBe(false);
  });

  it("refuses a hold too small to cover what the binding remembers sending", () => {
    expect(() => new TileHold(NATIVE_SENT_MAX)).toThrow();
  });

  it("every repeat the binding sends finds its tile, over a long random pan", () => {
    const h = new TileHold();
    const n = new NativeSent();
    let seed = 7;
    const rnd = () => (seed = (seed * 1103515245 + 12345) % 2 ** 31) / 2 ** 31;
    let cx = 0, cy = 0, repeats = 0;
    for (let i = 0; i < 50_000; i++) {
      // a wandering camera: mostly nearby tiles, sometimes a jump
      if (rnd() < 0.01) { cx = Math.floor(rnd() * 400); cy = Math.floor(rnd() * 400); }
      cx += Math.floor(rnd() * 3) - 1; cy += Math.floor(rnd() * 3) - 1;
      const x = cx + Math.floor(rnd() * 8), y = cy + Math.floor(rnd() * 8);
      const key = `12/${x}/${y}`;
      if (n.request(key)) h.put({ tag: 1, z: 12, x, y, layers: { stream: [i] } });
      else { repeats += 1; expect(h.touch({ z: 12, x, y })).toBe(true); }
    }
    expect(repeats).toBeGreaterThan(10_000);
  });
});
