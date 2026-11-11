/**
 * (T235, owner 2026-10-28; moved out of packages/core/src/menu/legal.test.ts by T667) The credits page links the ODbL
 * files on GitHub: the list names exactly these repo files, and each of them is committed (`repoData` checks it is there —
 * `packages/core` reads no repo file, so this half lives where files can be read).
 */
import { describe, expect, it } from "vitest";
import { repoData } from "../conformance/src/repoData";
import { ODBL_FILES, REPO_BLOB } from "../packages/core/src/menu/licences";

describe("the ODbL files the credits page links (T235)", () => {
  it("are the committed ODbL licence and the OpenStreetMap-derived data files, each in the repo", () => {
    const paths = ODBL_FILES.map((f) => f.url.slice(REPO_BLOB.length + 1));
    for (const p of ["data/LICENSE-ODbL.md", repoData("source/bc_places.json"), repoData("curated/waters/found_names.json"),
      repoData("curated/waters/added_lakes.geojson"), repoData("curated/waters/added_streams.json"),
      repoData("source/municipal/osm_streams.geojson"), repoData("curated/waters/cuts.json")]) expect(paths).toContain(p);
  });
});
