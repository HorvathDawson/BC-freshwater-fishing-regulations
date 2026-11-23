# T1016 — Web: a tab kept open across two releases (or a site deploy) meets 404s on files production no longer serves

Status: OPEN — next release batch.

Source: T1005 triage. Production keeps the live release and the previous one (`deploy/prod/upload.sh` prunes the rest
24 h on). The 404 kinds of T1005 (`3d5ded1b72ff865f`, `2c766955748940d9`, `f6d0af3f051de23a`, `bb863fcb6535d696`, …)
were all a crawler holding a cached r-fc137c4040fa; but a READER's tab opened before two releases and still open after
the prune reads the same pruned files (a region's regs / water slice, the in-season section lines, the tile folder's
mask and sprites), and one opened before a site deploy loads a chunk that is gone (`AsyncRequireError`, kind
`240b0d41d6fc2c88`). Each is reported as a defect and shown as a failed read.

## Work
1. Reproduce on the web (local server): boot on release A, publish B, then C and prune A, then read a slice not yet
   opened; and a later chunk after the site's export changes.
2. A 404 on an immutable release file of a release the live pointer no longer names is a known state ("this page's data
   was replaced"): say so with one action (reload onto the live release, or apply the staged one), report it as
   `expected`, never as a defect. A missing chunk: the same, reload. One load path (the app's in-session apply).
3. Tests for both, against a fake origin.

Depends: none. Size: M. Owner: no.
