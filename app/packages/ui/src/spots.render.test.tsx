/** P01: useSpots awaits every write and hands a failure back, said whole; the list never empties on a bad file. */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import type { Spot } from "@app/data/spots";
import { spotSaveFailed, useSpots, type SpotsStoreLike } from "./spots";

afterEach(cleanup);
const settle = () => act(async () => { for (let i = 0; i < 4; i++) await Promise.resolve(); });

function Probe({ store, out }: { store: SpotsStoreLike; out: { api?: ReturnType<typeof useSpots> } }) {
  out.api = useSpots(store);
  const s = out.api.spots;
  return <p>{s.state}:{s.state === "ready" ? s.value.length : ""}|{out.api.error ?? ""}</p>;
}

const store = (over: Partial<SpotsStoreLike> = {}): SpotsStoreLike => {
  let all: Spot[] = [];
  return { list: async () => all, put: async (s) => { all = [s, ...all]; }, remove: async () => {},
           patch: async () => {}, ...over };
};

describe("useSpots (P01)", () => {
  it("a rejected save rejects its caller and says why, whole", async () => {
    const out: { api?: ReturnType<typeof useSpots> } = {};
    const { container } = render(<Probe store={store({ put: async () => { throw new Error("disk full"); } })} out={out} />);
    await settle();
    let thrown: unknown = null;
    await act(async () => { await out.api!.save({ id: "a" } as Spot).catch((e) => { thrown = e; }); });
    await settle();
    expect((thrown as Error).message).toBe("disk full");
    expect(container.textContent).toBe("ready:0|This spot was not saved (disk full). It is still on screen; try again.");
  });

  it("a saved spot is in the list after the save resolves", async () => {
    const out: { api?: ReturnType<typeof useSpots> } = {};
    const { container } = render(<Probe store={store()} out={out} />);
    await settle();
    await act(async () => { await out.api!.save({ id: "a" } as Spot); });
    await settle();
    expect(container.textContent).toBe("ready:1|");
  });

  it("a corrupt store is an error the reader sees, never an empty list", async () => {
    const corrupt = Object.assign(new Error("The saved spots could not be read. The file was kept as s.corrupt-1 and nothing was overwritten."), { name: "SpotsCorrupt" });
    const { container } = render(<Probe store={store({ list: async () => { throw corrupt; } })} out={{}} />);
    await settle();
    expect(container.textContent).toBe("failed:|" + corrupt.message);
    expect(spotSaveFailed(corrupt)).toBe(corrupt.message);
  });
});
