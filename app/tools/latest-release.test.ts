/** (T601 item 2) The JS readers' one `latest()`: LATEST names the release; no mtime fallback; a pointer to nothing is an error. */
import { mkdirSync, mkdtempSync, rmSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { latest, latestId } from "../conformance/src/latestRelease.mjs";

const roots: string[] = [];
afterEach(() => { for (const r of roots.splice(0)) rmSync(r, { recursive: true, force: true }); });
const root = () => { const r = mkdtempSync(join(tmpdir(), "latest-")); roots.push(r); return r; };
const release = (r: string, id: string, mtime: number) => {
  mkdirSync(join(r, id), { recursive: true });
  const m = join(r, id, "manifest.json");
  writeFileSync(m, "{}");
  utimesSync(m, mtime, mtime);
};

describe("latest()", () => {
  it("LATEST wins over a newer folder", () => {
    const r = root();
    release(r, "r-old", 1000); release(r, "r-new", 2000);
    writeFileSync(join(r, "LATEST"), "r-old\n");
    expect(latestId(r)).toBe("r-old");
    expect(latest(r)).toBe(join(r, "r-old"));
  });
  it("no pointer is null, never the newest by modified time", () => {
    const r = root();
    release(r, "r-a", 1000); release(r, "r-b", 2000);
    expect(latest(r)).toBeNull();
    expect(latest(join(r, "nothing-here"))).toBeNull();
  });
  it("a pointer naming a folder with no manifest (or nothing) is an error, not another release", () => {
    const r = root();
    release(r, "r-a", 1000);
    writeFileSync(join(r, "LATEST"), "r-gone\n");
    expect(() => latest(r)).toThrow(/names r-gone, which has no manifest/);
    writeFileSync(join(r, "LATEST"), "\n");
    expect(() => latest(r)).toThrow(/is empty/);
  });
});
