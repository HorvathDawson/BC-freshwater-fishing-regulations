/**
 * A literal subset of the store's `static export.species` (regs store, rule ids sha 16be720be08b0fca) for the
 * store-free regs tests: the fish and groups T06's cases name. Copied from the data, not written: names, members and
 * the open flags are the export's. `SA` (and its members) is the one hand-built group — the export has no `SA` today, and page v36's
 * groupFor skips it by code; the test that pins that skip needs one to exist.
 */
import type { SpeciesBook } from "../names";

export const BOOK: SpeciesBook = {
  fish: {
    RB: { name: "Rainbow trout" }, ST: { name: "Steelhead" }, CT: { name: "Cutthroat trout" }, GB: { name: "Brown trout" },
    DV: { name: "Dolly Varden/bull trout" }, LT: { name: "Lake trout" }, EB: { name: "Brook trout" },
    LMB: { name: "Largemouth bass" }, SMB: { name: "Smallmouth bass" }, KO: { name: "Kokanee" },
  },
  groups: {
    ALL_FIN_FISH: { name: "All fish", members: [], open: true },
    ALL_GAME_FISH: { name: "All game fish", members: ["RB", "ST", "CT", "GB", "DV", "LT", "EB", "LW", "MW", "LMB", "SMB", "KO", "GR", "BB", "WSG", "BCB", "NP", "YP", "WP", "GE", "IN", "CRA"] },
    BASS: { name: "Bass", members: ["LMB", "SMB"] },
    CHAR: { name: "Char", members: ["DV", "LT", "EB"] },
    SA: { name: "Pacific salmon", members: ["CH", "CULTUS_LAKE_SCULPIN"] },
    TROUT_CHAR: { name: "Trout and char", members: ["RB", "ST", "CT", "GB", "DV", "LT", "EB"] },
  },
  protected: { CULTUS_LAKE_SCULPIN: { name: "Cultus Lake sculpin" }, GREEN_STURGEON: { name: "Green sturgeon" } },
  salmon: { CH: { name: "Chinook" } },
};
