/**
 * (T1135) The release pins: the app helper (`conformance/src/pins.ts`) and the re-cut's writer (`pins.mjs`), on scratch
 * files — never the committed pins, which only the re-cut writes (and which must be in the writer's one spelling).
 */
import { mkdtempSync, readFileSync, writeFileSync, mkdirSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { makePin, readPins } from "../../conformance/src/pins";
import { SUITES, format, merge, pinTests, recorded } from "./pins.mjs";

const REPO = new URL("../../../", import.meta.url).pathname;
const scratch = () => mkdtempSync(join(tmpdir(), "pins-test-"));
function pinsFile(dir: string, pins: Record<string, { answers: boolean; value: unknown }>): string {
  const f = join(dir, "release-pins.json");
  writeFileSync(f, format(pins));
  return f;
}

describe("pin(name, actual), the app helper", () => {
  const PINS = { "a.count": { answers: false, value: { n: 67, v: 5 } }, "a.words": { answers: true, value: "From the table." } };

  it("compare: an equal value passes; another value fails naming the pin, the file and the re-cut", () => {
    const pin = makePin(pinsFile(scratch(), PINS), undefined);
    pin("a.count", { v: 5, n: 67 });
    pin("a.words", "From the table.");
    expect(() => pin("a.count", { v: 5, n: 68 })).toThrow(/pin a\.count \(.*release-pins\.json\).*node app\/tools\/regs-recut\/sync\.mjs/);
    expect(() => pin("a.words", "From the table. Covers it.")).toThrow(/pin a\.words/);
  });

  it("compare: an unknown name FAILS (never a pass), and so does a bad name or no value", () => {
    const pin = makePin(pinsFile(scratch(), PINS), undefined);
    expect(() => pin("a.nope", 1)).toThrow(/pin a\.nope: not in .*release-pins\.json — add its row by hand/);
    expect(() => pin("a b", 1)).toThrow(/a pin name is/);
    expect(() => pin("a.count", undefined)).toThrow(/no value/);
  });

  it("record: writes the value, asserts nothing, and refuses one name recorded with two values", () => {
    const d = scratch(), rec = join(d, "rec");
    const pin = makePin(pinsFile(d, PINS), rec);
    pin("a.count", { v: 5, n: 68 });                    // a moved value: recorded, no failure
    pin("a.new", [1, 2]);                               // a name the file lacks: recorded (the writer refuses it)
    pin("a.count", { n: 68, v: 5 });                    // the same value again: fine
    expect(recorded(join(rec, "app"))).toEqual({ "a.count": { n: 68, v: 5 }, "a.new": [1, 2] });
    expect(() => pin("a.count", { v: 5, n: 69 })).toThrow(/recorded twice with different values/);
    expect(readPins(join(d, "release-pins.json"))).toEqual(PINS);   // the file itself is never touched by a test
  });
});

describe("the re-cut's writer (pins.mjs)", () => {
  const OLD = { "x.count": { answers: false, value: 67 }, "x.words": { answers: true, value: "a" }, "x.gone": { answers: false, value: 1 } };

  it("moves values, keeps each answers flag, drops and names a stale pin when the run passed", () => {
    const got = merge(OLD, { "x.count": 68, "x.words": "a" }, true);
    expect(got.pins).toEqual({ "x.count": { answers: false, value: 68 }, "x.words": { answers: true, value: "a" } });
    expect(got.said).toEqual(["moved: x.count", "STALE, dropped: x.gone (no test asked for it)"]);
    expect(got.refused).toEqual([]);
  });

  it("keeps (and names) an unrecorded pin when the run failed; refuses a recorded name with no row", () => {
    const got = merge(OLD, { "x.count": 67, "x.new": 3 }, false);
    expect(got.pins["x.gone"]).toEqual(OLD["x.gone"]);
    expect(got.said).toContain("not recorded, KEPT: x.gone (its suite's run failed before it, above)");
    expect(got.refused).toEqual(["x.new"]);
  });

  it("one spelling: keys sorted at every depth, one pin per line, no clock", () => {
    const a = format({ "b": { value: { z: 1, a: [2, 1] }, answers: true }, "a": { answers: false, value: 0 } });
    expect(a).toBe(format({ "a": { value: 0, answers: false }, "b": { answers: true, value: { a: [2, 1], z: 1 } } }));
    expect(a.split("\n").filter((l) => l.startsWith("    "))).toEqual([
      '    "a": {"answers":false,"value":0},', '    "b": {"answers":true,"value":{"a":[2,1],"z":1}}']);
  });

  it("the committed pins files are in that spelling, and every pin has a boolean answers flag", () => {
    for (const { file } of Object.values(SUITES)) {
      const text = readFileSync(join(REPO, file), "utf8"), pins = readPins(join(REPO, file));
      expect(text, `${file}: written by node app/tools/regs-recut/sync.mjs, never by hand`).toBe(format(pins));
      for (const [n, p] of Object.entries(pins)) expect(typeof p.answers, `${file} ${n}`).toBe("boolean");
    }
  });

  it("finds the pin-bearing tests by their import of pin (this file imports the helper's maker, not pin)", () => {
    const d = scratch();
    const put = (rel: string, body: string) => { mkdirSync(join(d, rel, ".."), { recursive: true }); writeFileSync(join(d, rel), body); };
    put("app/x/a.test.ts", 'import { pin } from "../pins";\n');
    put("app/x/b.test.tsx", 'import { taps, pin as p } from "../../conformance/src/pins";\n');
    put("app/x/c.test.ts", 'import { makePin } from "../pins";\n');
    put("app/node_modules/d.test.ts", 'import { pin } from "../pins";\n');
    put("pipeline/tests/test_a.py", "from pipeline.tests.pins import pin\n");
    put("pipeline/tests/test_b.py", "from pipeline.tests.pins import make_pin\n");
    expect(pinTests("app", d)).toEqual(["app/x/a.test.ts", "app/x/b.test.tsx"]);
    expect(pinTests("pipeline", d)).toEqual(["pipeline/tests/test_a.py"]);
    expect(pinTests("app")).toEqual(["app/conformance/src/regs/reads.release.test.ts",
      "app/conformance/src/regs/sourceLine.fixture.test.ts"]);
    expect(pinTests("pipeline")).toEqual(["pipeline/tests/test_answers_26.py"]);
    expect(existsSync(join(REPO, SUITES.app.file)) && existsSync(join(REPO, SUITES.pipeline.file))).toBe(true);
  });
});
