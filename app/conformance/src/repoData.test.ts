import { describe, expect, it } from "vitest";
import { repoData } from "./repoData";

describe("repoData (T667): a committed data file by name", () => {
  it("a file that is there is its repo-relative path", () => {
    for (const rel of ["curated/waters/cuts.json", "source/bc_hydrometric_stations.json"]) expect(repoData(rel)).toBe(`data/${rel}`);
  });
  it("a file that is not there, or a name outside curated/ and source/, throws naming it", () => {
    expect(() => repoData("curated/waters/no-such-file.json")).toThrow(/curated\/waters\/no-such-file\.json is not in this checkout/);
    expect(() => repoData("release/LATEST")).toThrow(/under curated\/ or source\//);
    expect(() => repoData("../app/package.json")).toThrow(/under curated\/ or source\//);
  });
});
