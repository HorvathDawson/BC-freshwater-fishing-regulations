# T919 — 0.8.3 leftovers

Status: OPEN (found by Claude 2026-11-19, during the 0.8.3 integration).

Small finds that did not block 0.8.3. Batch them into the next release.

- (a) K_DOWN near-duplicate labels: "Lake Revelstoke's Tributaries · its tributaries" and "Chilko Lake's tributary streams'
  tributaries" say the same thing twice. Make each read once.
- (b) Elaho River shows rule book p.23 twice: once under Gear sources and once as the T916 page-foot line. Show it once.
- (c) Map assets (776 glyph and sprite files) are still downloaded in-app. Bundle them or fetch them from our servers once,
  per the self-host rule.
- (d) T912 follow-up: a cut data-file system download restarts from zero. The data worker fix (T912 round 3) is deployed on
  staging AND production as of 2026-11-19, so check that the app can now resume a cut download, then close this or fix it.
