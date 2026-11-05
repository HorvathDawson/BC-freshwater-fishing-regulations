/**
 * (T403) `readsRelease`: a release made for an older app is refused before any reader opens it. Measured 2026-11-05: a
 * phone updated to 0.5.0 opened the r-4e709418ebd8 it held from 0.4.0 (salmon/3, no `lines`), and "not a salmon/4
 * file" and "this release has no section_line" were thrown on the map and the regs page.
 */
import { describe, expect, it } from "vitest";
import { readsRelease, type Manifest } from "./manifest";
import { PACK_FILES, PACK_OPTIONAL, PACK_WEB_ONLY } from "./pack.generated";

const spec = (format: string) => ({ url: "x", bytes: 1, gz: 1, sha256: "s", format, section_handles: "h" });
/** A whole published release in the contract's formats (`drop`: files it leaves out). */
function release(drop: readonly string[] = [], published: string | null = "2026-11-04"): Manifest {
  const files = Object.fromEntries(Object.entries(PACK_FILES).filter(([n]) => !drop.includes(n))
    .map(([n, f]) => [n, spec(f.format)]));
  return { format: "app-manifest/2", data_release: "r-test", tiles: { compatible: [], current: null, assets: null, mask: null },
           basemap: null, files, regulations: { valid_until: null, versions: null }, published } as Manifest;
}

describe("readsRelease", () => {
  it("reads a whole release in the contract's formats, the optional files left out", () => {
    expect(readsRelease(release(PACK_OPTIONAL), "web")).toEqual({ ok: true });
    expect(readsRelease(release(PACK_OPTIONAL), "phone")).toEqual({ ok: true });
  });

  it("refuses a file in a format its reader does not take (the 0.4.0 salmon/3)", () => {
    const m = release();
    m.files.salmon = spec("salmon/3");
    expect(readsRelease(m, "phone")).toEqual({ ok: false,
      reason: "release r-test is not one this app reads: salmon is salmon/3, not salmon/4" });
  });

  it("takes paint/4 beside the contract's paint/5 (the decoder reads both), and no other paint", () => {
    const m = release();
    m.files.paint = spec("paint/4");
    expect(readsRelease(m, "web").ok).toBe(true);
    m.files.paint = spec("paint/3");
    expect(readsRelease(m, "web").ok).toBe(false);
  });

  it("refuses a PUBLISHED release missing a contract file (the 0.4.0 one had no lines)", () => {
    expect(readsRelease(release(["lines"]), "phone")).toEqual({ ok: false,
      reason: "release r-test is not one this app reads: it has no lines" });
  });

  it("on a phone, the web's own files (the answer slices) are not missed — the held manifest has none", () => {
    expect(PACK_WEB_ONLY.length).toBeGreaterThan(0);
    expect(readsRelease(release(PACK_WEB_ONLY), "phone")).toEqual({ ok: true });
    expect(readsRelease(release(PACK_WEB_ONLY), "web").ok).toBe(false);
  });

  it("an unpublished release (a fixture, a local build) is a partial set: only its formats are held to", () => {
    expect(readsRelease(release(["lines", "salmon", "geo"], null), "phone")).toEqual({ ok: true });
  });

  it("a file the contract does not know is a newer release's: it passes", () => {
    const m = release();
    m.files["future-thing"] = spec("future/9");
    expect(readsRelease(m, "phone")).toEqual({ ok: true });
  });
});
