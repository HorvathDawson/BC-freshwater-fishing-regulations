/**
 * The store pairing the parity suites hang on (`provinceStoreAt`): a stale or absent store must come back as a skip WITH its
 * reason (never a silent pass, never a fail), and REGS_STORE_REQUIRED=1 must turn that skip into a failure, so a machine that
 * is meant to prove parity cannot exit 0 on a rebuilt store.
 */
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { afterAll, describe, expect, it } from "vitest";
import { KNOWN_VERSIONS } from "@app/core";
import { liveReleaseAt } from "../fixtureRelease";
import { paritySkip, provinceStoreAt, taps } from "./golden";

const dir = mkdtempSync(join(tmpdir(), "regs-pairing-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));
const storeWith = (name: string, digest: string | null): string => {
  const p = join(dir, name), db = new DatabaseSync(p);
  db.exec("CREATE TABLE meta (k TEXT PRIMARY KEY, v TEXT)");
  if (digest !== null) db.prepare("INSERT INTO meta VALUES ('store_digest', ?)").run(digest);
  db.close();
  return p;
};
const want = taps().meta.store.store_digest;

describe("provinceStoreAt: which store, or why none", () => {
  it("a store at the oracles' digest is the pairing", () => {
    const p = storeWith("paired.sqlite", want);
    expect(provinceStoreAt([p])).toEqual({ path: p, reason: null });
  });

  it("a store at another digest is a skip naming both digests and the re-cut", () => {
    const p = storeWith("stale.sqlite", "0000000000000000");
    const at = provinceStoreAt([p]);
    expect(at.path).toBeNull();
    expect(at.reason).toContain(p);
    expect(at.reason).toContain("0000000000000000");
    expect(at.reason).toContain(want);
    expect(at.reason).toMatch(/taps\.py/);
  });

  it("a store with no digest at all is a skip, not a pairing", () => {
    const at = provinceStoreAt([storeWith("nodigest.sqlite", null)]);
    expect(at.path).toBeNull();
    expect(at.reason).toMatch(/store undefined|no store_digest/);
  });

  it("the first candidate that exists is the one judged: a stale first store is not skipped over for a paired one", () => {
    const stale = storeWith("first-stale.sqlite", "1111111111111111"), paired = storeWith("second-paired.sqlite", want);
    const at = provinceStoreAt([undefined, join(dir, "missing.sqlite"), stale, paired]);
    expect(at.path).toBeNull();
    expect(at.reason).toContain(stale);
  });

  it("no candidate on disk is a skip with the places looked", () => {
    const at = provinceStoreAt([undefined, join(dir, "missing.sqlite")]);
    expect(at).toEqual({ path: null, reason: expect.stringMatching(/no regs store on this machine/) });
  });
});

describe("paritySkip: a skip is announced, and REGS_STORE_REQUIRED makes it a failure", () => {
  const stale = { path: null, reason: "x.sqlite is store 00, the oracles were cut from 11" } as const;

  it("paired: run, nothing to say", () => {
    expect(paritySkip({ path: "/s.sqlite", reason: null }, {})).toEqual({ skip: false, fail: null });
  });

  it("unpaired: skip, carrying the reason", () => {
    expect(paritySkip(stale, {})).toEqual({ skip: true, fail: null, reason: stale.reason });
  });

  it("unpaired with REGS_STORE_REQUIRED=1: a failure carrying the reason", () => {
    expect(paritySkip(stale, { REGS_STORE_REQUIRED: "1" })).toEqual({ skip: true, fail: expect.stringContaining(stale.reason) });
  });

  it("REGS_STORE_REQUIRED=0 or empty does not require", () => {
    expect(paritySkip(stale, { REGS_STORE_REQUIRED: "0" }).fail).toBeNull();
    expect(paritySkip(stale, { REGS_STORE_REQUIRED: "" }).fail).toBeNull();
  });
});

describe("liveReleaseAt: a release built for another port is a skip with its reason, never tested as if paired", () => {
  const release = (name: string, versions: Record<string, number>): string => {
    const d = join(dir, name);
    mkdirSync(d, { recursive: true });
    writeFileSync(join(d, "manifest.json"), JSON.stringify({ regulations: { valid_until: null, versions } }));
    return d;
  };
  const { glossary: _g, ...sections } = KNOWN_VERSIONS;

  it("a release at this app's section versions is the live release", () => {
    const d = release("r-now", { ...sections });
    expect(liveReleaseAt({ RELEASE_DIR: d })).toEqual({ dir: d, reason: null });
  });

  it("a rows-29 release (before the year strip) is a skip naming the version and the rebuild", () => {
    const d = release("r-rows29", { ...sections, rows: 29 });
    expect(liveReleaseAt({ RELEASE_DIR: d })).toEqual({ dir: null,
      reason: `release ${d} is rows 29 (this app reads 30): rebuild it (deliver all), then re-cut the goldens (sync.mjs)` });
  });

  it("with REGS_STORE_REQUIRED=1 it throws that reason", () => {
    const d = release("r-rows29b", { ...sections, rows: 29 });
    expect(() => liveReleaseAt({ RELEASE_DIR: d, REGS_STORE_REQUIRED: "1" })).toThrow(
      `REGS_STORE_REQUIRED is set: release ${d} is rows 29 (this app reads 30): rebuild it (deliver all), then re-cut the goldens (sync.mjs)`);
  });

  it("no release: the reason says so", () => {
    expect(liveReleaseAt({ RELEASE_DIR: join(dir, "nowhere") })).toEqual({ dir: null, reason: "no release on this machine (data/release/r-*)" });
  });
});
