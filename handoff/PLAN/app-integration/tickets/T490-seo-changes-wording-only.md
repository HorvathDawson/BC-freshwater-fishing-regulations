# T490 — /changes counts a wording-only release as every water changed (over budget; staging site not deployed)

Status: DONE — 3c12dc0. /changes hashes each water's rules (ids + bound values); r-1b4f vs r-67b4: 17 rule changes + "Wording updated on 16482 waters", 4.2 KB. Staging site deployed (r-f56b90ad2540, smoke 17 pass 0 fail); the release ledger closed stage 6 on r-f56b90ad2540 (the rerun built a new release, same bindings). Old snapshot upgraded by --snapshot-only --keep-pages.

`build-seo.mjs --release-dir data/release/r-1b4f54fb6ed9 --app app/dist/web-staging` stopped:
"/changes is 154515 B gzip, over 30720". The snapshot's per-water "rules hash" moves when the WORDS move, so T475/T479/
T481 (quota wording, no rule moved) make thousands of waters "changed". The staging site still runs the older web build
(walkthrough-3), which is also why the release smoke's unnamed-stream tap failed (the walkthrough card covers it: the
smoke seeds the repo's walkthrough-4).

## Do
- /changes lists REGULATION changes: hash what an angler must do (status, seasons, quotas, sizes, gear — the rule ids and
  their bound values), not the rendered words. A wording-only release says "Wording updated on N waters" once.
- sitemap lastmod: decide whether wording moves it (probably yes — the page did change).
- Then: export web staging, build-seo, `deploy/staging/site.sh`; rerun `python -m pipeline release --staging
  --min-app-version 0.5.0` so the ledger records stage 6 (its smoke then passes the tap).
- Fast-fail (AGENTS 20a): build the /changes page alone (cheap) before the 19k-page build.
