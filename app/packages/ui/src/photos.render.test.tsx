/**
 * S1: adding and removing a spot's photographs, as a hook — the draft and the edit of a saved spot.
 *
 * The rule that matters: a photo is a FILE the moment it is picked, but it belongs to the spot only once the spot is
 * saved. So a photo added and then removed (or the whole edit discarded) is deleted at once; a photo the saved spot
 * already had is deleted only when the spot is saved without it (by the store, `openSpotsWith`).
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import type { PhotoSource, PhotoStore, PickedPhoto } from "@app/data/spots";
import { usePhotoEdit, usePhotoUris, type PhotoEdit, type SpotPhotos } from "./photos";

afterEach(cleanup);

function fakePhotos(picks: (PickedPhoto | null | Error)[] = [{ uri: "file:///cache/a.jpg" }]) {
  const kept = new Set<string>(["photos/s1-1.jpg"]);
  const removed: string[] = [];
  const store: PhotoStore = {
    add: async (id, p, taken) => {
      const n = Math.max(0, ...taken.map((r) => Number(/-(\d+)\./.exec(r)?.[1] ?? 0))) + 1;
      const ref = `photos/${id}-${n}.jpg`;
      void p;
      kept.add(ref);
      return ref;
    },
    remove: async (ref) => { removed.push(ref); kept.delete(ref); },
    uri: async (ref) => (kept.has(ref) ? `file:///doc/spots/${ref}` : null),
  };
  const asked: PhotoSource[] = [];
  const photos: SpotPhotos = {
    store, sources: ["library", "camera"],
    pick: async (from) => {
      asked.push(from);
      const next = picks.shift() ?? null;
      if (next instanceof Error) throw next;
      return next;
    },
  };
  return { photos, removed, asked, kept };
}

function Probe({ photos, out }: { photos?: SpotPhotos; out: { api?: PhotoEdit } }) {
  out.api = usePhotoEdit(photos);
  return <p>{out.api.refs.join(",")}|{out.api.error ?? ""}|{out.api.sources.join(",")}</p>;
}
const mount = (photos?: SpotPhotos) => {
  const out: { api?: PhotoEdit } = {};
  const r = render(<Probe photos={photos} out={out} />);
  return { out, r };
};

describe("usePhotoEdit", () => {
  it("adds a picked photo to the spot being edited, from the source asked for", async () => {
    const f = fakePhotos();
    const { out, r } = mount(f.photos);
    act(() => out.api!.begin("s1", ["photos/s1-1.jpg"]));
    await act(async () => { await out.api!.add("camera"); });
    expect(f.asked).toEqual(["camera"]);
    expect(r.container.textContent).toBe("photos/s1-1.jpg,photos/s1-2.jpg||library,camera");
  });

  it("a picker closed without a choice changes nothing", async () => {
    const f = fakePhotos([null]);
    const { out, r } = mount(f.photos);
    act(() => out.api!.begin("s1", []));
    await act(async () => { await out.api!.add("library"); });
    expect(r.container.textContent).toBe("||library,camera");
  });

  it("a photo that cannot be added is said whole, and nothing else changes", async () => {
    const f = fakePhotos([new Error("camera permission refused")]);
    const { out, r } = mount(f.photos);
    act(() => out.api!.begin("s1", ["photos/s1-1.jpg"]));
    await act(async () => { await out.api!.add("camera"); });
    expect(r.container.textContent).toBe(
      "photos/s1-1.jpg|The photo was not added (camera permission refused). Nothing else changed.|library,camera");
  });

  it("removing a photo added in this edit deletes its file at once", async () => {
    const f = fakePhotos();
    const { out, r } = mount(f.photos);
    act(() => out.api!.begin("s1", ["photos/s1-1.jpg"]));
    await act(async () => { await out.api!.add("library"); });
    await act(async () => { await out.api!.remove("photos/s1-2.jpg"); });
    expect(f.removed).toEqual(["photos/s1-2.jpg"]);
    expect(r.container.textContent).toBe("photos/s1-1.jpg||library,camera");
  });

  it("removing a photo the saved spot has keeps its file — the save deletes it, a discard brings it back", async () => {
    const f = fakePhotos();
    const { out, r } = mount(f.photos);
    act(() => out.api!.begin("s1", ["photos/s1-1.jpg"]));
    await act(async () => { await out.api!.remove("photos/s1-1.jpg"); });
    expect(f.removed).toEqual([]);
    expect(r.container.textContent).toBe("||library,camera");
    await act(async () => { await out.api!.discard(); });
    expect(r.container.textContent).toBe("photos/s1-1.jpg||library,camera");
  });

  it("discarding the edit deletes the photos it added, and only those", async () => {
    const f = fakePhotos([{ uri: "a" }, { uri: "b" }]);
    const { out } = mount(f.photos);
    act(() => out.api!.begin("s1", ["photos/s1-1.jpg"]));
    await act(async () => { await out.api!.add("library"); });
    await act(async () => { await out.api!.add("library"); });
    await act(async () => { await out.api!.discard(); });
    expect(f.removed).toEqual(["photos/s1-2.jpg", "photos/s1-3.jpg"]);
  });

  it("once the spot is saved, the photos it added are the spot's: a later discard deletes nothing", async () => {
    const f = fakePhotos();
    const { out } = mount(f.photos);
    act(() => out.api!.begin("s1", []));
    await act(async () => { await out.api!.add("library"); });
    act(() => out.api!.committed());
    await act(async () => { await out.api!.discard(); });
    expect(f.removed).toEqual([]);
    expect(out.api!.refs).toEqual(["photos/s1-1.jpg"]);
  });

  it("offers nothing when the app wired no photos", () => {
    const { r } = mount(undefined);
    expect(r.container.textContent).toBe("||");
  });
});

describe("usePhotoUris", () => {
  function Uris({ store, refs }: { store?: Pick<PhotoStore, "uri">; refs: string[] }) {
    const got = usePhotoUris(store, refs);
    return <p>{refs.map((r) => `${r}=${got.get(r) === undefined ? "…" : got.get(r) ?? "gone"}`).join(" ")}</p>;
  }

  it("resolves each reference to something an <Image> can draw, and a missing photo to null", async () => {
    const f = fakePhotos();
    const r = render(<Uris store={f.photos.store} refs={["photos/s1-1.jpg", "photos/s9-1.jpg"]} />);
    await act(async () => { for (let i = 0; i < 4; i++) await Promise.resolve(); });
    expect(r.container.textContent)
      .toBe("photos/s1-1.jpg=file:///doc/spots/photos/s1-1.jpg photos/s9-1.jpg=gone");
  });

  it("draws an entry from before S1 as it is, and a reference with no store as gone", async () => {
    const r = render(<Uris refs={["file:///old.jpg", "photos/s1-1.jpg"]} />);
    await act(async () => { await Promise.resolve(); });
    expect(r.container.textContent).toBe("file:///old.jpg=file:///old.jpg photos/s1-1.jpg=gone");
  });
});
