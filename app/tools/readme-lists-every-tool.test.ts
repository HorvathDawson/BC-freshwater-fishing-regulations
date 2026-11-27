/**
 * (T1193) THE TOOLS README NAMES EVERY TOP-LEVEL RUNNABLE. A person finds a tool in `tools/README.md`; a new `.mjs` that
 * the page does not name fails here, so the list cannot rot silently. Add a line to the README (the group it belongs to).
 */
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, it } from "vitest";

const here = dirname(fileURLToPath(import.meta.url));

it("every top-level .mjs in app/tools is named in app/tools/README.md", () => {
  const readme = readFileSync(join(here, "README.md"), "utf8");
  const missing = readdirSync(here).filter((f) => f.endsWith(".mjs") && !readme.includes(`\`${f}\``));
  expect(missing, `name each in app/tools/README.md, in its group, with one line from its header: ${missing.join(", ")}`).toEqual([]);
});
