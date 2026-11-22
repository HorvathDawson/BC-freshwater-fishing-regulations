/**
 * (T978, owner 2026-11-22: "Pinned upstream main (Recommended)") EVERY ANDROID BUILD SHIPS ONE PINNED UPSTREAM MAPLIBRE
 * COMMIT, NO PATCH. The pin is named once (app/tools/maplibre-android/pin.env); the AAR build applies nothing and refuses
 * a changed tree; every build path reads the pin and nothing reads the archived T420 patches. A mutation of any of these
 * lines fails here, in seconds, without a native build.
 */
import { readFileSync, readdirSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

const root = resolve(__dirname, "../..");
const read = (p: string) => readFileSync(resolve(root, p), "utf8");
/** a shell file without its comment lines */
const code = (p: string) => read(p).split("\n").filter((l) => !/^\s*#/.test(l)).join("\n");

function pin(): Record<string, string> {
  const out: Record<string, string> = {};
  for (const line of read("app/tools/maplibre-android/pin.env").split("\n")) {
    if (/^\s*(#|$)/.test(line)) continue;
    const m = /^([A-Z_]+)=(\S+)$/.exec(line);
    if (!m) throw new Error(`pin.env: not KEY=value: ${line}`);
    out[m[1]] = m[2];
  }
  return out;
}

describe("pin.env", () => {
  const p = pin();
  it("names one full upstream commit and a local version that names it", () => {
    expect(Object.keys(p).sort()).toEqual(["MLN_COMMIT", "MLN_VERSION"]);
    expect(p.MLN_COMMIT).toMatch(/^[0-9a-f]{40}$/);
    expect(p.MLN_VERSION).toMatch(/^\d+\.\d+\.\d+-main\.[0-9a-f]{7}$/);
    expect(p.MLN_VERSION.endsWith(p.MLN_COMMIT.slice(0, 7))).toBe(true);
  });
  it("says why main and not a tag, and when to move", () => {
    const t = read("app/tools/maplibre-android/pin.env");
    expect(t).toMatch(/WHY UPSTREAM MAIN, NOT A RELEASE TAG/);
    expect(t).toMatch(/WHEN TO MOVE: to the first android-v\* release tag that contains 9cf8446d/);
  });
});

describe("build.sh builds the pin with no patch", () => {
  const sh = code("app/tools/maplibre-android/build.sh");
  it("reads the pin and checks out that commit", () => {
    expect(sh).toMatch(/source "\$HERE\/pin\.env"/);
    expect(sh).toMatch(/checkout -q "\$MLN_COMMIT"/);
    expect(sh).toMatch(/\[\[ \$got == "\$MLN_COMMIT" \]\] \|\|/);
  });
  it("applies nothing and refuses a changed source tree, untracked files and submodules included (the version file aside)", () => {
    expect(sh).not.toMatch(/git apply|git am|\bpatch -p|MLN_PATCH/);
    expect(sh).toMatch(/status --porcelain --untracked-files=all --ignore-submodules=none -- \. ':\(exclude\)platform\/android\/VERSION'/);
    expect(sh).toMatch(/\[\[ -z \$dirty \]\] \|\| \{/);
  });
  it("brings every submodule (nested too) up on EVERY run, not only after the first clone (T978 review)", () => {
    const clone = sh.slice(sh.indexOf("if [[ ! -d $SRC/.git ]]; then"), sh.indexOf("\nfi\n", sh.indexOf("if [[ ! -d $SRC/.git ]]; then")));
    expect(clone).not.toMatch(/submodule/);
    expect(sh).toMatch(/git -C "\$SRC" submodule update --init --recursive --depth 1 --jobs 8/);
    expect(sh.indexOf("submodule update --init --recursive")).toBeLessThan(sh.indexOf("dirty=$(git"));
  });
  it("refuses a commit whose Versions.kt asks for another NDK (a pin move can change it)", () => {
    expect(sh).toMatch(/grep -q "ndkVersion = \\"\$NDK\\"" "\$VERSIONS_KT" \|\| \{/);
    expect(read("app/tools/maplibre-android/pin.env")).toMatch(/check build\.sh's NDK and\s*\n# CMAKE lines against the new commit's/);
  });
  it("publishes to cache/maplibre/<MLN_VERSION>, the path the builds default to, with the commit beside it", () => {
    expect(sh).toMatch(/OUT=\$\{MLN_OUT:-\$ROOT\/cache\/maplibre\/\$MLN_VERSION\}/);
    expect(sh).toMatch(/-PpublicationRepositoryUrl="file:\/\/\$OUT"/);
    expect(sh).toMatch(/"\$MLN_COMMIT" "\$MLN_VERSION" .*>"\$OUT\/SOURCE\.txt"/);
  });
});

describe("every build path reads the pin; none reads the archived patches", () => {
  it("build-apk.sh defaults to the pinned repo, takes only the pinned AAR, names it in the build-id line, has no stock path", () => {
    const sh = code("deploy/staging/build-apk.sh");
    expect(sh).toMatch(/source app\/tools\/maplibre-android\/pin\.env/);
    expect(sh).toMatch(/export MLN_LOCAL_MAVEN="\$REPO\/cache\/maplibre\/\$MLN_VERSION"/);
    expect(sh).toMatch(/MLN_AAR=\$\(mln_pinned_aar "\$MLN_LOCAL_MAVEN"\)/);
    expect(sh).toMatch(/MLN_WHAT="pinned upstream \$MLN_VERSION"/);
    expect(sh).toMatch(/--stock-maplibre\|--stock-maplibre=\*\)\s+echo "build-apk: --stock-maplibre is refused/);
    expect(sh).not.toMatch(/STOCK_MLN|MLN_WHAT=stock/);
  });
  it("build-links.sh takes the pinned AAR only: its file name AND SOURCE.txt's commit", () => {
    const sh = code("deploy/lib/build-links.sh");
    expect(sh).toMatch(/source "\$BUILD_LINKS_DIR\/\.\.\/\.\.\/app\/tools\/maplibre-android\/pin\.env"/);
    expect(sh).toMatch(/elif \[\[ \$\(basename "\$found"\) != "android-sdk-vulkan-\$MLN_VERSION\.aar" \]\]; then\s+bad /);
    expect(sh).toMatch(/elif \[\[ \$src_commit != "\$MLN_COMMIT" \]\]; then\s+bad /);
    expect(sh).toMatch(/\[\[ \$src_commit == "\$MLN_COMMIT" \]\] \|\| return 0\s+echo "\$found"/);   // mln_pinned_aar
    expect(sh).toMatch(/read -r _ src_commit _ <"\$dir\/SOURCE\.txt"/);
  });
  it("no build file names the lean patch, its repo or the old tool folder", () => {
    const files = ["deploy/staging/build-apk.sh", "deploy/lib/build-links.sh", "app/tools/testbed/session.sh",
      "app/tools/maplibre-android/build.sh", "app/tools/maplibre-android/check-loaded.sh", "app/apps/mobile/app.config.js",
      "app/apps/mobile/plugins/with-local-maplibre.js", "app/apps/mobile/plugins/with-pinned-maplibre.js",
      "scripts/fast-fail.sh", "scripts/heavy.sh"];
    for (const f of files) expect(read(f), f).not.toMatch(/fslean|maplibre-patch|feature-state-deltas/);
  });
  it("the archive holds the old patches and says no build reads it", () => {
    const dir = resolve(root, "app/tools/maplibre-android/archive-t420");
    expect(readdirSync(dir)).toEqual(expect.arrayContaining(["feature-state-deltas-lean.patch", "README.md"]));
    expect(read("app/tools/maplibre-android/archive-t420/README.md")).toMatch(/not used by any build/);
  });
});
