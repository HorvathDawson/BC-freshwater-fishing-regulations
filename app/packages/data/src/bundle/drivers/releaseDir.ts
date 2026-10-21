/**
 * NODE ONLY (tests and tools; never the app's path): a release FOLDER read the way the app reads a release, and THE REGS
 * FIXTURE RELEASE (T10).
 *
 * The regs fixture is the regulations half of a real release — `water`, `regs-<region>`, `regs-sid` and the `core` rows
 * `itemOrd` reads — cut to the waters the golden states name by `python -m pipeline.tools.regs_fixture_release` (the
 * release builder's own copy/ship code: every row is the release's bytes, blobs keep their codec) and committed under
 * `packages/data/dev/regs-release/<release id>/`. The regs conformance suite and the ui regs hooks' tests open it when this
 * machine has no live release (CI). Its manifest names no tile set: `openRegsStore(loader)` holds it to no atlas.
 *
 * Lives in the drivers layer because it is a node:sqlite driver over files (layers.json); the temp folder comes from
 * $TMPDIR (node:os is not a driver import).
 */
import { existsSync, mkdtempSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { verify } from "../../fetchVerified";
import { parseManifest } from "../../manifest";
import { releaseLoader, type Opener, type ReleaseLoader, type ReleaseSource } from "../../release/loader";
import { openBundle } from "./node";

/** Where the committed regs fixture release lives (the folder holds exactly one `r-*` release). */
export const REGS_FIXTURE_ROOT = fileURLToPath(new URL("../../../dev/regs-release", import.meta.url));

/** The committed fixture release's folder (`<root>/<release id>`); throws when it is missing or not alone. */
export function regsFixtureDir(root: string = REGS_FIXTURE_ROOT): string {
  const dirs = existsSync(root)
    ? readdirSync(root).filter((n) => /^r-[0-9a-f]{12}$/.test(n) && existsSync(join(root, n, "manifest.json"))) : [];
  if (dirs.length !== 1)
    throw new Error(`regs fixture: ${root} holds ${dirs.length} releases, not 1 — run pnpm regs-fixture`);
  return join(root, dirs[0]!);
}

/** node:sqlite over a verified file: the bytes written to a temp file and opened read-only. */
export function nodeOpener(): Opener {
  const tmp = mkdtempSync(join(process.env.TMPDIR || "/tmp", "release-open-"));
  return async (name, bytes) => {
    const p = join(tmp, `${name}.sqlite`);
    writeFileSync(p, await bytes());
    return openBundle(p);
  };
}

/**
 * A `ReleaseLoader` over a release folder, the way the app reads one: each file's bytes held to the manifest
 * (`verify`: the decoded length and sha256) and opened in node:sqlite from a temp copy.
 */
export function openReleaseDir(dir: string): ReleaseLoader {
  const m = parseManifest(JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8")));
  const source: ReleaseSource = {
    async bytes(name) {
      const f = m.files[name];
      if (!f) throw new Error(`release ${m.data_release} has no file ${name}`);
      return verify(f.url, new Uint8Array(readFileSync(join(dir, "..", f.url))), f);
    },
  };
  return releaseLoader(m, source, nodeOpener());
}

/** The regs fixture release, opened as the app opens a release. */
export const openRegsFixture = (): ReleaseLoader => openReleaseDir(regsFixtureDir());
