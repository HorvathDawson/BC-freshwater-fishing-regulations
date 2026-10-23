/**
 * useCaptureSpot (P12b): the visit's day and hour, the capture and the save, out of the SpotCapture component.
 *
 * The clamp is the part that matters: a spot records weather and flow for an instant, and an instant that has not
 * happened yet has neither. Every timestamp is built from local parts so the test holds in any time zone.
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import type { RegsSource, SectionId } from "@app/data";
import type { Spot } from "@app/data/spots";
import { useCaptureSpot, type CaptureOptions } from "./capture";

afterEach(cleanup);
const settle = () => act(async () => { for (let i = 0; i < 6; i++) await Promise.resolve(); });

const NOW = new Date(2026, 7, 30, 14, 20).getTime();

const source = (over: Partial<RegsSource> = {}): RegsSource => ({
  gaugeForSection: async () => null, gaugeNow: async () => null, traceToGauge: async () => [],
  panelsFor: async () => new Map(), ...over,
} as unknown as RegsSource);

type Api = ReturnType<typeof useCaptureSpot>;
function Probe({ opts, out }: { opts: CaptureOptions; out: { api?: Api } }) {
  out.api = useCaptureSpot(opts);
  const a = out.api;
  return <p>{a.steps.dayText} {a.steps.hourLabel}|{a.steps.dayOffset}|{a.draft?.title ?? ""}|{a.error ?? ""}</p>;
}

const mount = (over: Partial<CaptureOptions> = {}) => {
  const out: { api?: Api } = {};
  const r = render(<Probe opts={{ source: source(), now: () => NOW, ...over }} out={out} />);
  return { out, r };
};
const water = { section: 7 as SectionId, item: null, name: "Chilliwack River" };
const point = { lat: 49.0974, lon: -121.9675 };

describe("useCaptureSpot", () => {
  it("opens on now, and will not step to a later day", () => {
    const { out, r } = mount();
    act(() => out.api!.shiftDay(1));
    expect(r.container.textContent).toBe(`${new Date(NOW).toDateString()} 14:00|0||`);
  });

  it("steps back a day, and a quick chip goes to that day from wherever it is", () => {
    const { out, r } = mount();
    act(() => out.api!.shiftDay(-1));
    expect(r.container.textContent).toBe(`${new Date(2026, 7, 29).toDateString()} 14:00|-1||`);
    act(() => out.api!.toDay(-2));
    expect(r.container.textContent).toBe(`${new Date(2026, 7, 28).toDateString()} 14:00|-2||`);
  });

  it("clamps an hour later than now to now", () => {
    const { out } = mount();
    act(() => out.api!.setHour(16));
    expect(out.api!.visitedAt).toBe(NOW);
  });

  it("captures for the chosen instant, and the draft carries no section id", async () => {
    const { out, r } = mount();
    act(() => out.api!.shiftDay(-1));
    let ok = false;
    await act(async () => { ok = await out.api!.capture(water, point, ""); });
    await settle();
    expect(ok).toBe(true);
    expect(out.api!.draft!.visitedAt).toBe(new Date(2026, 7, 29, 14, 20).getTime());
    expect("section" in out.api!.draft!).toBe(false);
    expect(r.container.textContent).toBe(`${new Date(2026, 7, 29).toDateString()} 14:00|-1|Chilliwack River|`);
  });

  it("a capture that fails says why, whole, and leaves no draft", async () => {
    const { out, r } = mount({ source: source({ gaugeForSection: async () => { throw new Error("offline"); } }) });
    let ok = true;
    await act(async () => { ok = await out.api!.capture(water, point, ""); });
    expect(ok).toBe(false);
    expect(r.container.textContent)
      .toBe(`${new Date(NOW).toDateString()} 14:00|0||This spot was not saved (offline). It is still on screen; try again.`);
  });

  it("saves the draft with the title and notes typed, and a failed save is said whole", async () => {
    const { out, r } = mount();
    await act(async () => { await out.api!.capture(water, point, ""); });
    const saved: Spot[] = [];
    await act(async () => { await out.api!.save("Tamihi run", "two on a bead", async (s) => { saved.push(s); }); });
    expect([saved[0]!.title, saved[0]!.notes, saved[0]!.updatedAt]).toEqual(["Tamihi run", "two on a bead", NOW]);
    await act(async () => { await out.api!.save("", "", async () => { throw new Error("disk full"); }); });
    expect(r.container.textContent).toBe(
      `${new Date(NOW).toDateString()} 14:00|0|Chilliwack River|This spot was not saved (disk full). It is still on screen; try again.`);
  });
});

describe("useCaptureSpot: the draft's photographs (S1)", () => {
  const photos = () => {
    const removed: string[] = [];
    return {
      removed,
      photos: {
        sources: ["library"] as const,
        pick: async () => ({ uri: "file:///cache/a.jpg", mimeType: "image/jpeg" }),
        store: {
          add: async (id: string, _p: unknown, taken: readonly string[]) => `photos/${id}-${taken.length + 1}.jpg`,
          remove: async (ref: string) => { removed.push(ref); },
          uri: async () => null,
        },
      },
    };
  };

  it("saves the photos added to the draft, as relative references, with no section id", async () => {
    const f = photos();
    const { out } = mount({ photos: f.photos });
    await act(async () => { await out.api!.capture(water, point, ""); });
    const id = out.api!.draft!.id;
    await act(async () => { await out.api!.photos.add("library"); });
    const saved: Spot[] = [];
    await act(async () => { await out.api!.save("", "", async (s) => { saved.push(s); }); });
    expect(saved[0]!.photos).toEqual([`photos/${id}-1.jpg`]);
    expect(JSON.stringify(saved[0])).not.toMatch(/"section"|file:\/\//);
  });

  it("keeps the photos when the visit is changed and the water read again", async () => {
    const f = photos();
    const { out } = mount({ photos: f.photos });
    await act(async () => { await out.api!.capture(water, point, ""); });
    await act(async () => { await out.api!.photos.add("library"); });
    const refs = [...out.api!.photos.refs];
    act(() => out.api!.shiftDay(-1));
    await act(async () => { await out.api!.capture(water, point, ""); });
    expect(out.api!.photos.refs).toEqual(refs);
  });

  it("a draft read again keeps its id, so the saved spot's photos are named after the spot (T80 1)", async () => {
    const f = photos();
    const { out } = mount({ photos: f.photos });
    await act(async () => { await out.api!.capture(water, point, ""); });
    const first = out.api!.draft!.id;
    await act(async () => { await out.api!.photos.add("library"); });
    act(() => out.api!.shiftDay(-1));
    await act(async () => { await out.api!.capture(water, point, ""); });
    const saved: Spot[] = [];
    await act(async () => { await out.api!.save("", "", async (s) => { saved.push(s); }); });
    expect({ id: saved[0]!.id, photos: saved[0]!.photos }).toEqual({ id: first, photos: [`photos/${first}-1.jpg`] });
  });

  it("a draft cancelled and captured again is a new spot with a new id", async () => {
    const { out } = mount({ photos: photos().photos });
    await act(async () => { await out.api!.capture(water, point, ""); });
    const first = out.api!.draft!.id;
    await act(async () => { await out.api!.cancel(); });
    await act(async () => { await out.api!.capture(water, point, ""); });
    expect(out.api!.draft!.id).not.toBe(first);
  });

  it("cancelling the draft deletes the photos it added", async () => {
    const f = photos();
    const { out } = mount({ photos: f.photos });
    await act(async () => { await out.api!.capture(water, point, ""); });
    await act(async () => { await out.api!.photos.add("library"); });
    const refs = [...out.api!.photos.refs];
    await act(async () => { await out.api!.cancel(); });
    expect(f.removed).toEqual(refs);
  });
});
