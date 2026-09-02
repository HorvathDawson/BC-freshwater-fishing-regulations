/**
 * Water the app has said nothing about must not be drawn as an answer.
 *
 * The `missing` colour of a categorical mode fires when NO feature-state has been pushed
 * for a feature — the app has not evaluated it, or the viewport query has not returned yet.
 * Every closure mode had `missing: color.status.open`, so the whole province rendered as
 * open until data arrived, and any feature the app never evaluated stayed that way.
 *
 * That is the failure AGENTS rule 29 exists for, and it is the worst one this app can
 * produce: "we have not checked" and "you may fish here" are not the same sentence.
 *
 * "Open under the general rules" IS a real answer — it is computed by `evaluate()` and
 * pushed as a status. This test is about the absence of any answer at all.
 */
import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const meta = JSON.parse(readFileSync(
  fileURLToPath(new URL("../packages/map/style/style.meta.json", import.meta.url)), "utf8"));
const themes = ["light", "dark"].map((n) => JSON.parse(readFileSync(
  fileURLToPath(new URL(`../packages/map/style/themes/${n}.json`, import.meta.url)), "utf8")));

/** Tokens that assert something about the world rather than reporting an absence. */
const ANSWERS = /^color\.(status|flow|stock)\./;

describe("no data is not an answer", () => {
  it("no colour mode falls back to a token that states an outcome", () => {
    for (const [layer, modes] of Object.entries<Record<string, {
      scale: string; missing?: { token: string };
    }>>(meta.colorModes)) {
      for (const [name, mode] of Object.entries(modes)) {
        if (mode.scale === "static") continue;
        expect(mode.missing, `${layer}.${name} has no missing colour`).toBeTruthy();
        expect(mode.missing!.token, `${layer}.${name} paints "no data" as an outcome`)
          .not.toMatch(ANSWERS);
      }
    }
  });

  it("the no-data colour is distinct from every outcome colour, in every theme", () => {
    // If it merely looks like "open" it is just as wrong, whatever the token is called.
    for (const t of themes) {
      const missing = new Set(Object.values<Record<string, { missing?: { token: string } }>>(
        meta.colorModes).flatMap((modes) =>
          Object.values(modes).map((m) => m.missing?.token).filter(Boolean) as string[]));
      for (const token of missing) {
        const hex = t.values[token];
        if (typeof hex !== "string") continue;
        for (const [n, v] of Object.entries<string>(t.values))
          if (ANSWERS.test(n) && typeof v === "string")
            expect(hex.toLowerCase(), `${t.name}: ${token} equals ${n}`).not.toBe(v.toLowerCase());
      }
    }
  });
});
