# T1140 part names: before and after (round 2, after the owner's 17:00–17:31 answers)

Web app, Nov 25 2026, each water opened by its plain link (`?regs=<item>`, the way `/water/<slug>` opens it).

- **before:** origin/main a5f404c2c on the candidate release r-71d09ad82e81.
- **after:** this branch on a side release built from the same bundle with this branch's deliver code (r-2617d36e2bf9:
  answers, card check, store and release reran; atlas, reach and tiles did not).

Each picture is `<before|after>-<water>-<size>-<theme>[-map].png`; `-map` is the Map tab with its list of parts open.

| what | pictures |
|---|---|
| Squamish River (a side-channel part inside an ecological reserve) | `*-squamish-web390-light.png`, `*-squamish-web390-light-map.png`, dark: `*-squamish-web390-dark.png`, desktop: `*-squamish-desktop-light.png` |
| Gordon River (a side-channel part inside a national park reserve) | `*-gordon-web390-light.png`, `*-gordon-web390-light-map.png` |
| Comox Creek and Rees Creek (two of the 18 parts with no real-world name: the data's own label) | `*-comox-web390-light(-map).png`, `*-rees-web390-light(-map).png` |
| Capilano River (unchanged apart from the count gone) | `*-capilano-web390-light(-map).png` |
| Bella Coola River (one part: unchanged) | `*-bella-coola-one-part-web390-light(-map).png` |

## What changed

- **No part is numbered anywhere.** On the Map tab, "PART 1 OF 2 · SQUAMISH RIVER" now reads "SQUAMISH RIVER", over the
  part's name. The screen reader's list rows give names, not numbers.
- **Every part has a real name.**
  - Inside an area: "Squamish River inside Baynes Island Ecological Reserve", "Gordon River inside Pacific Rim National
    Park Reserve".
  - No real-world name: the data's own label, e.g. "Side channel, joining 12 km from the mouth".
  - Outside B.C.: "Kettle River outside B.C."
- **The line "Other parts of this water have different rules" stays**, without a number, under the part's name on the
  Regulations and Conditions tabs. It opens the Map tab with its list of parts open. It is still a proposal (your call):
  it is the one thing that says other parts exist.
- **New on the Squamish and Gordon maps:** the ring where the reserve's channels meet the main stem is labelled
  "Inside Baynes Island Ecological Reserve". Before, there was no label there. It is the part's own lower end, said the
  way its name says it.

## What each of the 26 parts with no name of its own now reads (r-2617d36e2bf9)

| water, part | now | why |
|---|---|---|
| Squamish River p1 | Squamish River inside Baynes Island Ecological Reserve | its channels run reserve cut to reserve cut |
| Gordon River p0 | Gordon River inside Pacific Rim National Park Reserve | same, the park reserve |
| Okanagan River Oxbows p1 | 2 side channels | one channel runs from its source to the reserve's boundary, the other from source to mouth: not provably inside, so the data's label |
| Burman River p0 | 5 side channels, joining 1.1–5.5 km from the mouth | data's label |
| Bedwell River p0 | 3 side channels, joining 4.4 km from the mouth | data's label |
| Thelwood Creek p0 | 2 side channels, joining 0.4 km from the mouth | data's label |
| Rees Creek p0 | 4 side channels, joining 1.4 km from the mouth | data's label |
| Comox Creek p0 | Side channel, joining 12 km from the mouth | data's label |
| Campbell River p6 | Side channel, joining 16.9 km from the mouth | data's label |
| Forsyth Creek p1 | Side channel (from the mouth up to the top of the river) | data's label (another lane's walk fix may merge it away) |
| Mahood Creek p1 | Side channel (from the mouth up to the top of the river) | data's label |
| Cottonwood River p1 | Side channel, joining 5.7 km from the mouth | data's label |
| Koksilah River p1 | Side channel (from the mouth up to the top of the river) | data's label (may merge away) |
| Stirling Creek p1 | Side channel (from Acme Lake up to the top of the river) | data's label |
| Theodosia River p1 | Side channel (from the mouth up to the top of the river) | data's label |
| Noons Creek p4 | Side channel (from the mouth up to the top of the river) | data's label |
| Robertson Creek p1 | Side channel (from the mouth up to the Boot Lagoon outlet) | data's label |
| West Road (Blackwater) River p3 | 2 side channels, joining 227.4–234.4 km from the mouth | data's label |
| Snehumption Creek p1 | 2 side channels | data's label |
| Skeena River p4 | Side channel (from the Exchamsiks River confluence up to Skeena River Ecological Reserve boundary) | data's label; it runs TO the reserve, not inside it |
| Halfway River p2 | 11 side channels | data's label |
| MacMillan Creek p2 | Side channel (from the mouth up to the top of the river) | data's label (may merge away) |
| Seymour River p3 | Side channel (from the mouth up to the top of the river) | data's label |
| Manson River p0 | 3 side channels, joining 86 km from the mouth | data's label |
| Shelagyote River p0 | Side channel, joining 48.6 km from the mouth | data's label (may merge away) |
| Omineca River p0 | Side channel, joining 134.4 km from the mouth | data's label (may merge away) |

Parts outside B.C. (56) read "<water> outside B.C.", e.g. "International Creek outside B.C.". Tidal pieces are never
listed.

## Your questions

1. Keep the line "Other parts of this water have different rules" (no number)?
2. The new ring label "Inside Baynes Island Ecological Reserve" on the Squamish and Gordon maps: keep it?
3. Five labels say only "Side channel (from the mouth up to the top of the river)", e.g. Seymour p3, which is next to
   its river's own part. Is that enough? The data has nothing more specific.
