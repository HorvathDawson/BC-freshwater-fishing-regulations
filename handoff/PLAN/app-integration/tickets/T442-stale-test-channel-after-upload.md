# T442 — A staging upload left the test channel on a cleared release (fresh test-bed installs 404)

Status: DONE in code (2026-11-06); staging test/ itself still needs one `reset` (an R2 write: owner/coordinator).

Source: walk-063 (0/4) and probe-063. A fresh install of the test-bed APK (channel `test`) showed "The map data could
not be loaded" on every scenario. Cause: `test/release/manifest.json` still named r-5a5999c94588 (the app logged
`mobile data: go (483577736 bytes)` = that manifest's phone install size exactly; the live r-4230bc0c1dd5 is 474827955),
while every file is fetched from the live `release/` prefix, which `deploy/staging/upload.sh` (T433) clears of every
release but its own: r-5a59…'s files and its tiles 822db… all answer 404. The test bed resets test/ before `install`,
but `run.mjs --no-r2` refuses the reset and the step failed without stopping the run. Not an app bug: the live channel
(every store build, incl. 0.6.3 on Play) reads `release/manifest.json`, and all its phone files answer 200.

## Done
1. `upload.sh` runs `test-updates.mjs reset` after its clear (test/ is spared, so it is re-pointed).
2. Test bed: `ctx.resetChannel()` = reset, or with `--no-r2` a read-only check (`lib/channel.mjs`, pinned by
   `lib/channel.test.mjs`) that test/ is the live release; `install` and `bg-install` STOP on it.
