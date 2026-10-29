/**
 * (T230) The in-season notice opens by itself once per row per device, re-opens from the bell, and drops a row once it
 * is matched or out of effect (a new unmatched list). Mounted (jsdom) because it is a hook.
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import type { InSeasonChange } from "@app/core";
import { useInSeasonNotice, type InSeasonNoticeState } from "./inseason";

afterEach(cleanup);
const ch = (id: string): InSeasonChange =>
  ({ id, region: "4", water: "Kaslo River", text: "No fishing", from: null, to: null, source: "s", extents: [] });
const mem = () => { let t: string | null = null; return { read: () => t, write: (x: string) => { t = x; } }; };

function Probe({ rows, store, out }: { rows: InSeasonChange[]; store: ReturnType<typeof mem>; out: { s?: InSeasonNoticeState } }) {
  out.s = useInSeasonNotice(rows, store);
  return null;
}

describe("useInSeasonNotice", () => {
  it("opens for an unseen row, once per device; the bell re-opens it; a matched row leaves it", () => {
    const store = mem(), out: { s?: InSeasonNoticeState } = {};
    const a = [ch("is-a"), ch("is-b")];
    const { rerender, unmount } = render(<Probe rows={a} store={store} out={out} />);
    expect([out.s!.open, out.s!.count, out.s!.label]).toEqual([true, 2, "2 in-season changes not on the map yet"]);
    act(() => out.s!.close());
    expect([out.s!.open, store.read()]).toEqual([false, JSON.stringify(["is-a", "is-b"])]);
    unmount();
    // the next launch: both seen — closed, the bell still counts them
    const again = render(<Probe rows={a} store={store} out={out} />);
    expect([out.s!.open, out.s!.count]).toEqual([false, 2]);
    act(() => out.s!.show());
    expect(out.s!.open).toBe(true);
    act(() => out.s!.close());
    // is-a matched: it leaves; a new row opens the notice again
    again.rerender(<Probe rows={[ch("is-b"), ch("is-c")]} store={store} out={out} />);
    expect([out.s!.open, out.s!.rows.map((r) => r.id)]).toEqual([true, ["is-b", "is-c"]]);
    void rerender;
  });
  it("nothing unmatched: no badge, never open", () => {
    const out: { s?: InSeasonNoticeState } = {};
    render(<Probe rows={[]} store={mem()} out={out} />);
    expect([out.s!.open, out.s!.count]).toEqual([false, 0]);
  });
});
