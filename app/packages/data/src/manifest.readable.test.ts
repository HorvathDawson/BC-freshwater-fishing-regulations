/**
 * (T403) `readsRelease`: a release made for an older app is refused before any reader opens it. Measured 2026-11-05: a
 * phone updated to 0.5.0 opened the r-4e709418ebd8 it held from 0.4.0 (salmon/3, no `lines`), and "not a salmon/4
 * file" and "this release has no section_line" were thrown on the map and the regs page.
 */
import { describe, expect, it } from "vitest";
import { KNOWN_VERSIONS, SECTION_NAMES } from "@app/core";
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

  it("takes paint/4 beside the contract's paint/6 (the frozen fixture releases' paint), and no other paint", () => {
    const m = release();
    expect(m.files.paint!.format).toBe("paint/6");
    m.files.paint = spec("paint/4");
    expect(readsRelease(m, "web").ok).toBe(true);
    for (const old of ["paint/3", "paint/5"]) {
      m.files.paint = spec(old);
      expect(readsRelease(m, "web").ok).toBe(false);
    }
  });

  it("(T975) refuses a release cut before the packed formats: near/1, down/1, climate/1 and paint/5 are not read", () => {
    const m = release();
    m.files.near = spec("near/1");
    m.files.down = spec("down/1");
    m.files.climate = spec("climate/1");
    m.files.paint = spec("paint/5");
    expect(readsRelease(m, "phone")).toEqual({ ok: false, reason: "release r-test is not one this app reads: "
      + "climate is climate/1, not climate/2; down is down/1, not down/2; near is near/1, not near/2; paint is paint/5, not paint/6" });
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

  /** (T810 M0) the section versions are held at install, not by the regs store on every page */
  describe("the regulations' section versions", () => {
    const REQUIRED: Record<string, number> = Object.fromEntries(SECTION_NAMES.map((n) => [n, KNOWN_VERSIONS[n]]));
    const said = (versions: Record<string, number> | null): Manifest =>
      ({ ...release(), regulations: { valid_until: null, versions } }) as Manifest;
    const TROUT = KNOWN_VERSIONS.trout;

    it("no versions named passes (the store still holds the water file to them); this app's own pass", () => {
      expect(readsRelease(said(null), "phone")).toEqual({ ok: true });
      expect(readsRelease(said(REQUIRED), "phone")).toEqual({ ok: true });
    });

    it("a required section at another version, or left out, is refused", () => {
      expect(readsRelease(said({ ...REQUIRED, rows: KNOWN_VERSIONS.rows + 1 }), "phone")).toEqual({ ok: false,
        reason: `release r-test is not one this app reads: regulations rows is ${KNOWN_VERSIONS.rows + 1}, not ${KNOWN_VERSIONS.rows}` });
      const noGear = Object.fromEntries(Object.entries(REQUIRED).filter(([n]) => n !== "gear"));
      expect(readsRelease(said(noGear), "web")).toEqual({ ok: false,
        reason: `release r-test is not one this app reads: regulations gear is absent, not ${KNOWN_VERSIONS.gear}` });
    });

    it("(T810 S) the trout section is required: left out, or at another version, is refused", () => {
      expect(readsRelease(said({ ...REQUIRED, trout: TROUT + 1 }), "phone")).toEqual({ ok: false,
        reason: `release r-test is not one this app reads: regulations trout is ${TROUT + 1}, not ${TROUT}` });
      const noTrout = Object.fromEntries(Object.entries(REQUIRED).filter(([n]) => n !== "trout"));
      expect(readsRelease(said(noTrout), "phone")).toEqual({ ok: false,
        reason: `release r-test is not one this app reads: regulations trout is absent, not ${TROUT}` });
    });

    it("a section this app knows neither way is a newer release's: it passes", () => {
      expect(readsRelease(said({ ...REQUIRED, bait: 3 }), "phone")).toEqual({ ok: true });
    });
  });

  it("a file the contract does not know is a newer release's: it passes", () => {
    const m = release();
    m.files["future-thing"] = spec("future/9");
    expect(readsRelease(m, "phone")).toEqual({ ok: true });
  });
});
