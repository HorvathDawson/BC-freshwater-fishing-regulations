# T505 — A crayfish picture in the same textbook look

Status: DONE — 17ec1d9b0; owner approved the picture 2026-11-08; in build 22 (3d807829d).
Earlier: IN REVIEW — 17ec1d9b0; owner approved the picture 2026-11-08 ("The Cray fish is good"); in the app from the next build.

The fish pictures share one textbook (scientific illustration) look; the crayfish does not match. Find a crayfish
(signal crayfish, Pacifastacus leniusculus — B.C.'s native one) illustration in the same style, with a licence that allows
shipping it in the app (public domain / CC BY, credited like the others), and swap it in.

## Do
- Look where the current fish set came from first (same source / artist / collection if it has a crayfish).
- Owner picks from 2–3 candidates (send the images); credit added to the picture credits.

## Candidates (2026-11-08 night) — all public domain, all the signal crayfish (Pacifastacus leniusculus)
No colour painting of this species was found (the fish are colour paintings by USFWS artists; USFWS has crayfish
photos only). The best match is a 19th-century stipple engraving, top view (turned on its side it fills the 2:1 thumb):
- **A — Faxon 1885, "Astacus leniusculus Dana, Male"** (Review of the Astacidae; Freshwater and Marine Image Bank,
  Univ. of Washington, FMIB 53513). Clean white ground, finest detail. Recommended.
  https://commons.wikimedia.org/wiki/File:FMIB_53513_Astacus_leniusculus_Dana_Male.jpeg
- **B — Hagen 1870, Plate X "A. Trowbridgii Female"** (Monograph of the North American Astacidæ; BHL). Lighter.
  https://commons.wikimedia.org/wiki/File:Monograph_of_the_North_American_Astacid%C3%A6_(Plate_X)_(6810343304).jpg
- The same Hagen plate from Iconographia Zoologica (UvA) — a yellowed scan of B.
Options for the owner: A as is (grey engraving among colour fish), or A tinted to a natural brown-olive (a derived
image, still PD), or keep looking for a colour plate (European noble crayfish plates exist; wrong species).
Shots in the session scratchpad t505/.

## Done (17ec1d9b0)
Owner picked the 1885 engraving, tinted. `signal-crayfish.webp` (code CRA, credit "Walter Faxon 1885 / FMIB", Commons),
800×400 RGBA like the fish, thumbnail made, manifest + generated files rebuilt; the Crayfish row now shows it instead of
the silhouette. Made by `app/tools/fish-sources/make-signal-crayfish.py` from FMIB 53513 (cut from the paper, shell
filled, brown-olive gradient map, head left).
