// T47: write one candidate's tokens into this checkout's theme files, for a render or a gate run. NEVER COMMIT THE RESULT.
//
//   node handoff/PLAN/app-data/status-colours/scripts/apply-palette.mjs <candidate id>    # then: pnpm -C app style:build
//   node handoff/PLAN/app-data/status-colours/scripts/apply-palette.mjs --restore         # today's theme files back
//                                                                                          # (then style:build again)
// The first apply keeps the theme files as they were in $TMPDIR/t47-themes-backup/<checkout>; --restore writes them back
// and removes the backup. (Checking out app/packages/map/style/ from the index does the same.)
// Only the values the candidate names change; key order and every other token stay as they are.
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { layered, REPO } from "./colour-math.mjs";

const id = process.argv[2];
const THEMES = ["light", "dark", "cvd"];
const themeFile = (t) => join(REPO, `app/packages/map/style/themes/${t}.json`);
const BACKUP = join(tmpdir(), "t47-themes-backup", REPO.replace(/[^\w]+/g, "_"));

if (id === "--restore") {
  if (!existsSync(BACKUP)) { console.log("nothing to restore"); process.exit(0); }
  for (const t of THEMES) writeFileSync(themeFile(t), readFileSync(join(BACKUP, `${t}.json`)));
  rmSync(BACKUP, { recursive: true });
  console.log("restored");
  process.exit(0);
}

const all = JSON.parse(readFileSync(join(REPO, "handoff/PLAN/app-data/status-colours/candidates.json"), "utf8"));
const cand = all[id];
if (!cand || id.startsWith("$")) { console.error(`no candidate ${id}; have ${Object.keys(all).filter((k) => !k.startsWith("$"))}`); process.exit(2); }
if (!existsSync(BACKUP)) {
  mkdirSync(BACKUP, { recursive: true });
  for (const t of THEMES) writeFileSync(join(BACKUP, `${t}.json`), readFileSync(themeFile(t)));
}
for (const [theme, values] of Object.entries(layered(all, id))) {   // the baseline under the candidate
  // always from the backup (today's palette), so applying B after A never inherits A
  const doc = JSON.parse(readFileSync(join(BACKUP, `${theme}.json`), "utf8"));
  for (const [k, v] of Object.entries(values)) {
    if (!(k in doc.values)) throw new Error(`${theme}: ${k} is not a token of this theme`);
    doc.values[k] = v;
  }
  writeFileSync(themeFile(theme), `${JSON.stringify(doc, null, 2)}\n`);
  console.log(`${theme}: ${Object.keys(values).length} tokens`);
}
