// T671 D2 (the Worker half of pipeline/tests/test_t671_salmon_rows.py — the same rows, the same answers): a DFO row
// nobody could type keeps the species its cell names. "Chum | To be determined" reads `check` for chum only; "Sockeye |
// Apr 1 to Mar 31 | To be determined" reads `check` for sockeye on its days (it answered for no fish); a cell naming no
// salmon the table knows reads for every salmon, never none. Both modes: the Python's byte-for-byte one and robust.
import { describe, expect, it } from "vitest";
import { DAYS, Interner, locationCalendar, type Verdict } from "../src/dfo/salmon";
import { GEAR_PERMISSION, TBD, toRules, type TypedRule } from "../src/dfo/typed";
import { REG } from "./wording.fixture";

const SPECIES = ["CH", "CO", "SK", "PK", "CM"];
type Row = [species: string, dates: string, limits: string];

const rulesOf = (rows: Row[], robust: boolean): TypedRule[] =>
  rows.flatMap(([species, dates, limits_gear], i) => toRules({ species, dates, limits_gear }, "t671", i, robust, REG));

/** the place's own answer for a species on MMDD: its status, or null where the place says nothing that day */
function status(rows: Row[], robust: boolean, sp: string, md: number): string | null {
  const verdicts = new Interner<unknown>();
  const cal = locationCalendar(rulesOf(rows, robust) as never[], verdicts, new Interner<unknown>(), robust);
  const runs = cal.sp[SPECIES.indexOf(sp)]!;
  const day = DAYS.indexOf(md);
  let v = -1;
  for (let k = 0; k < runs.length; k += 2) if (runs[k]! <= day) v = runs[k + 1]!;
  return v >= 0 ? (verdicts.items[v] as Verdict).s : null;
}

for (const robust of [false, true])
  describe(`(T671 D2) a row nobody could type keeps its species — ${robust ? "robust" : "pythonParity"}`, () => {
    it("a to-be-determined row is typed with the species its cell names", () => {
      const [chum] = rulesOf([["Chum", "To be determined", "To be determined"]], robust);
      expect(chum!.type).toBe("advisory");
      expect(chum!.species).toEqual(["CM"]);
      expect(chum!.review_reason!.startsWith(TBD)).toBe(true);
      expect(rulesOf([["Sockeye, pink and chum", "Apr 1 to Mar 31", "To be determined"]], robust)[0]!.species).toEqual(["SK", "PK", "CM"]);
      expect(rulesOf([["All", "To be determined", "To be determined"]], robust)[0]!.species).toEqual(["SA"]);
      expect(rulesOf([["Steelhead", "To be determined", "To be determined"]], robust)[0]!.species).toBeUndefined();
    });

    it("Courtenay: chum reads check, chinook and coho keep their openings, the species it does not name are not spoken for", () => {
      const rows: Row[] = [["Chinook", "Sep 1 to Dec 31", "2 per day"], ["Coho", "Sept 1 to Dec 31", "4 hatchery-marked per day"],
                           ["Chum", "To be determined", "To be determined"]];
      expect(status(rows, robust, "CM", 601)).toBe("check");
      expect(status(rows, robust, "CM", 1001)).toBe("check");
      expect(status(rows, robust, "CH", 1001)).toBe("open");
      expect(status(rows, robust, "CO", 1001)).toBe("open");
      expect(["CH", "CO", "SK", "PK"].map((sp) => status(rows, robust, sp, 601))).toEqual([null, null, null, null]);
    });

    it("Campbell: a place printing only an undetermined chinook says nothing for coho or pink (the wider place answers)", () => {
      const rows: Row[] = [["Chinook", "To be determined", "To be determined"]];
      expect(status(rows, robust, "CH", 1001)).toBe("check");
      expect(["CO", "SK", "PK", "CM"].map((sp) => status(rows, robust, sp, 1001))).toEqual([null, null, null, null]);
    });

    it("Kamloops Lake: to be determined with readable dates reads check on those days, never nothing", () => {
      const rows: Row[] = [["Chinook", "Aug 28 to Sep 22", "4 per day, only 2 over 50cm"], ["Pink", "Apr 1 to Mar 31", "No fishing for pink"],
                           ["Coho", "Apr 1 to Mar 31", "No fishing for coho"], ["Sockeye", "Apr 1 to Mar 31", "To be determined"]];
      expect(status(rows, robust, "SK", 715)).toBe("check");
      expect(status(rows, robust, "PK", 715)).toBe("closed");
      expect(status(rows, robust, "CO", 715)).toBe("closed");
      expect(status(rows, robust, "CH", 901)).toBe("open");
      expect(status(rows, robust, "CH", 715)).toBe(null);
      expect(status(rows, robust, "CM", 715)).toBe(null);
    });

    it("an undetermined row speaks only inside its own dates", () => {
      const rows: Row[] = [["Sockeye", "Aug 1 to Aug 31", "To be determined"]];
      expect(status(rows, robust, "SK", 815)).toBe("check");
      expect(status(rows, robust, "SK", 915)).toBe(null);
    });

    it("a species cell the table does not know reads check for every salmon; another fish and a gear permission do not", () => {
      expect(SPECIES.map((sp) => status([["Kokanee salmon", "Apr 1 to Mar 31", "2 per day"]], robust, sp, 601))).toEqual(Array(5).fill("check"));
      expect(SPECIES.map((sp) => status([["All", "Apr 1 to Mar 31", "No fishing for eulachon"]], robust, sp, 601))).toEqual(Array(5).fill(null));
      const bait = rulesOf([["Coho", "Apr 1 to Mar 31", "Natural bait allowed"]], robust);
      expect(bait[0]!.review_reason!.startsWith(GEAR_PERMISSION)).toBe(true);
      expect(status([["Coho", "Apr 1 to Mar 31", "Natural bait allowed"]], robust, "CO", 601)).toBe(null);
    });
  });
