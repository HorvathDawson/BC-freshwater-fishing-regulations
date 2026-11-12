# T387 — Every tap on a part gives the same page (access warnings first)

Status: DONE (owner 2026-11-04, in 0.5.0) — 2a9bd4aed (app + net), 47c5b66f0 (pipeline: section lengths, rides the 0.5.0 rebuild).

## Ask (owner 2026-11-04)
"the issue with nicomen slough is not all of them when clicked will get the land use warnings so yes it is one section
but in the one section not all clicks are equal." — "we need to make sure similar things dont happen for other items
other then landuse."

## Found
Access is per SECTION (`section_access`: sid, area, permille). Nicomen Slough (gnis:21105, one part, 9 sections: 5 line,
4 polygon) has Skumalasph 16 (reserve) and a private parcel on 4 of its 9 sections; a tap on the other 5 shows none.
Province-wide (bundle 2026-11-04): 20,700 parts, 7,101 touch an access area, 1,972 of them on only some of their
sections. Pooled per part: median 1 area, p99 3, max 7 (Similkameen, Fraser part 3) — no bloat.

## Work
1. Audit every per-section fact a Regulations page (and its sheets) shows on a tap: access (restricted note, no-access
   banner), licensing, region / MU / place words, tidal, salmon section, "Tributary of" / down, steelhead, closures by
   section, the sun line's point, … — which vary between sections of ONE part.
2. Each becomes PART-level on the page (the same for every tap on the part; access: the part's areas, each with how much
   of the part it covers) unless it is truly about the tapped point (list those with why; the owner sees the list).
3. A net on the live release: for every part with more than one section, the Regulations page built from each of its
   sections is equal (minus the listed point-bound facts) — with mutation pins.
4. App-side where the release has the data (no rebuild); anything needing the pipeline rides the one 0.5.0 rebuild.

## Done (2026-11-04)

### What a tap entered, audited (every per-section fact a Regulations page or its sheets reads)
- **The page's inputs** (`targetFor` → `loadRegsInputs` → `buildRegsPage`, sheets included): the tapped section only
  PICKS the part; everything after is keyed by the part's answer key — licensing, region / MU / place words, tidal,
  steelhead, closures and every frame of the year, the header's place, the Parts sheet, the in-season banners (by part).
  Already part-level by construction; net (1) proves every section of every part picks its own part.
- **Access** (the banner and the gentle note): was the TAPPED section's (`sectionAccessNotices`) — the Nicomen bug.
  **Now the part's**: `@app/ui` `regsAccessNotices(loader, store, subject)` takes NO section — the page's subject is read
  off its inputs (`pageSubjectOf`: a named water's part; an unnamed reach's section). Core `partAccess` pools every
  section of the part (grazes under TOUCH_ONLY dropped per section, as before), each area with the share of the WHOLE
  part it covers, the sections weighed by their length along the water; `pageAccessNotices` words it: "About 40% of this
  part of the water is in Coquitlam Watershed…", "This water crosses an Indian reserve (Skumalasph 16, about 6% of it)
  and private land (under 1% of it)." The gentle note now shows from search too (one page, whatever the way in).
- **The Salmon card** (and its provincial bait / fly marks): was the tapped section's chain; **now the part's** sections
  (`salmonCardFor(loader, store, {subject, akey})`, no section argument).
- **The Conditions view's place + access** (`useWaterPlace`, T367): the same subject, so it says what the page says.
- **"Tributary of …"**: an unnamed reach only, whose page IS its section's (the release holds no unnamed-part grouping:
  regs-sid maps section → key / place); unchanged.
- **The map's no-access colour** stays per section (the map, not the page).

### Weights: what the release holds (and why a pipeline change)
Neither per-section length in the live release covers the parts: `section_down.len` holds gauge-shed members only (33,025
of the 65,119 part sections have no row), `lines` has no lake / polygon and no length for a one-section stream. So the
app weighs by `part_section.m` (store `partLengths`), written by the pipeline for the 0.5.0 rebuild:
`bundle/access.py` measures `section_length` (sid, metres) for every named section from the same tile geometry as the
shares — a line's length, a polygon-only section's half perimeter — `derived.py` copies it into `part_section.m`, the
release ships it in the water file (contract + `pack.generated.ts`; additive, still water/1). Tests:
`test_section_access.py::test_section_lengths_along_the_water`, `test_derived_tables.py::test_part_section_carries_each_sections_length`.
**Until that rebuild** (and on any release without `m`): where a part's sections differ, no share is claimed — the
banner says "Some of this part of the water is in …" and the note names the causes without a share; where they are
alike the share is exact ("All of this water is in …"). Never a guessed weight.

### The net — `app/tools/one-part-one-page.test.ts` (live release)
(1) every section of all 6,816 multi-section parts opens its own part; (2) the WHOLE page (VM + every sheet it names,
access, Salmon card) built afresh from one tap per distinct per-section signature (paint + access rows, salmon chain,
bait/fly class, answer key) + first + last section, for the 2,630 parts that differ + every 25th other (9,990 pages, ~45 s):
all equal. `T387_EVERY=1`: every section of every part — 51,235 pages, all equal (226 s, run 2026-11-04). MUTATION PIN: the T54 read (access and salmon by
the tapped section) gives Nicomen Slough different pages; the honest page names Skumalasph 16. Core / ui pins:
`access.test.ts` (Nicomen shape), `regs.test.ts` (part weights, no-length path, failure reporting).

Found on the way (owner report 2026-11-04, "the Salmon card no longer shows next to trout"): the live release
r-ac5a4e4f734c predates T372's in-force days (`n`), so every water with an undrawn DFO place (the Vedder, Capilano,
Nicomen Slough …) FAILS its Salmon card ("has no in-force days") — before and after this ticket alike (the page's
subject was right; probed on the live release). The 0.5.0 rebuild carries `n`. What made it invisible: the Keep tab's
at-a-glance table folded the card in only when it had the card's row, and drew nothing while the card was coming or had
failed. Now (`RegsScreen`) the card's slot is drawn whenever the glance has no salmon row, and (`WaterScreen`) a release
that ships salmon always draws the card or its pending / failed face. Pinned: `WaterScreen.salmon.test.tsx` (card, and
failed face, in both layouts; the glance-failed case fails on the old code).

### POINT-BOUND (left per tap — the owner decides)
1. **The sun line**: sunrise / sunset at the TAPPED point (`useSunLine(source, item, at)`), else the water's point. Why:
   it is about where you stand; across a long part it moves by minutes. Alternative: the part's midpoint (`part_geo.mid`).
2. **Scroll memory** (`regsScrollKey(item, part, tapSid)`): where the reader left the page, kept per tapped reach. Not a
   fact on the page; keying it by part would make every tap on a part reopen at the same place.

## Owner 2026-11-04: the point-bound list
Agreed: the sun line (at the tapped point) and the scroll memory (per tapped reach) stay per-spot. Nothing else is.

## Owner 2026-11-12
Point-bound facts (the sun line and scroll memory): leave per tapped spot, accepted.
