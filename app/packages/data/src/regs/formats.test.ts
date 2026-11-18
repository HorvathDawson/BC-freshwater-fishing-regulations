/**
 * (T810 S, trout table contract 2.7) THE REGS STORE'S FORMATS ARE THE CONTRACT'S. The store checks each file it opens
 * against core's hand-typed `REGS_FORMATS` (`store.ts`), while install and the held set (`readsRelease`, `boot.ts`) read
 * the generated contract (`PACK_FILES`, emitted from `pipeline/deliver/release/contract.py`). Two copies of one fact:
 * at the trout cutover the contract moved every `regs-<region>` file to `regs-region/2`, and a copy left at `/1` would
 * install the new set, then refuse every regs page. This holds every `REGS_FORMATS` entry equal to the format of each
 * file it names in `PACK_FILES`, so neither moves alone.
 */
import { describe, expect, it } from "vitest";
import { REGS_FORMATS } from "@app/core";
import { PACK_FILES, REGIONS } from "../pack.generated";

/** The contract's files each `REGS_FORMATS` entry is the format of (store.ts: water, every region file, regs-sid). */
const FILES_OF: Readonly<Record<keyof typeof REGS_FORMATS, readonly string[]>> = {
  water: ["water"],
  region: REGIONS.map((r) => `regs-${r}`),
  sid: ["regs-sid"],
};

describe("(T810 S) REGS_FORMATS = the generated contract's formats (PACK_FILES)", () => {
  it("every entry names its files, and every one of them is in the contract", () => {
    expect(Object.keys(FILES_OF).sort()).toEqual(Object.keys(REGS_FORMATS).sort());
    for (const names of Object.values(FILES_OF)) for (const n of names) expect(PACK_FILES[n], n).toBeDefined();
  });
  it.each(Object.keys(REGS_FORMATS) as (keyof typeof REGS_FORMATS)[])("%s: each of its files is at REGS_FORMATS' format", (k) => {
    expect(FILES_OF[k].map((n) => [n, PACK_FILES[n]?.format])).toEqual(FILES_OF[k].map((n) => [n, REGS_FORMATS[k]]));
  });
});
