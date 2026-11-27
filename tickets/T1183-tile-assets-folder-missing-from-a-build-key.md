# T1183 — A reused tile version folder keeps its old map assets

Status: OPEN — a #286 follow-up; scoped 2026-11-26: real on main

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1183 | A reused tile version folder keeps its old map assets | OPEN — a #286 follow-up; scoped 2026-11-26: real on main | — | a lane: on reuse, compare the folder's map-assets to what `assets.write` would write now; a difference refuses (or writes the next assets folder beside it); a test |

Source: PR #286's third review (M2a, "Not fixed, pre-existing") and the comment at `pipeline/common/stepcache.py:104-107`: "an existing tile version folder
whose tile bytes are unchanged is reused, assets and all (a follow-up ticket: the assets need their own key)". Scoped by the planning chat's docs agent, 2026-11-26.

## The defect (on main, 1e2dc8a3b)
- `pipeline/deliver/tiles/version.py:111-115` (`to_release`): when `data/release/tiles/<version>/` exists and its archive carries the same stamp, it returns
  "reused, not rewritten". `assets.write(stage)` (L123-124) runs only for a NEW folder.
- The version is the tiles' content hash (`stamp`, L87-99: section handles + the archive's sha256). The map assets are not in it: `assets.wanted()`
  (`assets.py:73-76`) takes every font the style names (`style_fonts`, `app/packages/map/style/style.json`) plus the basemap fonts and the sprites, at `PIN`.
- So a font added to the style (or a `PIN` / `ASSETS_VERSION` / `SPRITES` change) reruns the tiles stage (#286 keyed the style and `assets.py`), the tiles come out
  byte-identical, the folder is reused, and `map-assets-1/` ships without the new font's glyphs: labels in that font are not drawn, with no error anywhere.
- Nothing downstream catches it: `release.pack_problems_of` (`pipeline/release.py:981-1002`) checks the pack against the folder, never the folder against `wanted()`.

## Proof (no build needed)
A test: `to_release` into a temp release root with a stub archive; then point `assets.STYLE` at a copy that names one more font and call `to_release` again
with the same archive: today it prints "reused" and the folder's `assets.json` lacks `fonts/<new font>/0-255.pbf`.

## Fix (one sentence)
On reuse, compare the folder's `map-assets-<n>/assets.json` (pin and file list) with what `assets.write` would write now; when they differ, refuse naming the
missing files, or (better, a published folder is immutable) write `map-assets-<n+1>/` beside it with a bumped `ASSETS_VERSION` and let the manifest name it.

## Acceptance
The test above fails today and passes after; a mutation (the check removed) brings the failure back. Severity: medium (labels silently missing on the map).
Model: Sonnet. Size: S.
