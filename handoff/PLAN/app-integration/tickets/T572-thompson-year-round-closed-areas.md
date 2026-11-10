# T572 — Thompson River: the three year-round closed areas inside one DFO place (owner's problem report)

Status: IN REVIEW — owner looks (a proposal waits in the review app)

Owner, problem report saved in the review app 2026-11-09 (marks.jsonl, DFO place
`3:thompson-river:from-kamloops-lake-outlet-downstre`, rows 2, 4, 5, 6, 7 picked wrong, problem "split missing"):
"need to figure out how to represent all of these".

DFO's words (his note quotes them): the place runs "From Kamloops Lake outlet downstream to the fishing boundary signs
just downstre…" and "no fishing is allowed in the following areas, year-round":
- Deadman — from the fishing boundary signs approximately 1 km downstream of the Hwy No. 1 Bridge at Savona to the CN
  rail bridge approximately 500 m downstream of the confluence with Deadman Creek.
- Juniper — from the fishing boundary signs approximately 1.5 km downstream from Juniper Beach Park downstream
  approximately 4 km to the fishing boundary signs located at 50°46.893' N and 121°08.110' W.
- Ashcroft — from the fishing boundary sign at the upstream side of the mouth of the Bonaparte River to Hwy 97C bridge
  in Ashcroft.

Ours today: the place has no extent, and the three exceptions are "not cut out".
To decide and build: how one place holds a reach MINUS three stretches (three exception pieces on the place, or three
closure places of their own that override it), the six-plus cuts they need (one is given as a coordinate), and how
the checklist rows read it. Everything proposed stays a proposal for the owner.

## 2026-11-10 — built as a proposal with the pieces machinery
The place binds the FOUR OPEN STRETCHES between the three closed areas (so its rules never land inside one):
1. Kamloops Lake outlet (existing cut) → the Deadman upper signs: NEW cut 1 km down the river from the Hwy 1 bridge at
   Savona (BC MoTI structure "SAVONA", 50.75654, -120.87265) — medium.
2. The CN rail bridge below Deadman Creek (existing cut `thompson_river__cnr_bridge`, about 330 m below the Deadman
   River's mouth) → the Juniper upper signs: NEW cut — low, see the choice below.
3. The Juniper lower signs: NEW cut at DFO's own coordinate 50°46.893' N 121°08.110' W (111 m from the line) — high →
   the Bonaparte River's mouth: NEW confluence cut — high.
4. The Hwy 97C bridge in Ashcroft: NEW cut (BC MoTI structure "ASHCROFT", 50.72701, -121.28188) — high → the signs just
   below Gold Pan (the cut the owner's own save proposed).
Every new cut was placed by the server's own anchor check on the Thompson. The proposal is on both wordings
(`…downstre` and `…downstre~2`); "Save this extent for all 2 wordings" covers both and REPLACES the whole-stretch
extent he saved on the live wording.
THE ONE CHOICE LEFT TO THE OWNER: the Juniper upper signs. DFO says "approximately 1.5 km downstream from Juniper Beach
Park downstream approximately 4 km to [the coordinate]". Measured on the river, 4 km above the coordinate and 1.5 km
below the park's downstream boundary are 2.8 km apart (the park runs 4 km along the river). The proposal uses 4 km
above the coordinate — both numbers DFO's, and it matches 1.5 km below the park's upstream part.
NOT DONE: the three closed areas are not places of their own, so the app does not SHOW them as closed year-round;
whether it should (three closure places, or the synopsis' own rows) is a separate decision.

## 2026-11-10 (later) — the owner's direction: two entries → T573
"it is like it should become 2 entries?" The four-open-stretches proposal above is REPLACED: entry A is the whole
stretch (one piece), entry B the three closed areas (T573). Corrections to the text above:
- THE CUT COUNT: six cuts are not built — five new ones for the closed areas plus the Gold Pan signs the owner's own
  save proposed. ("5 new cuts" above counted only ours; the panel rightly listed six.)
- MEASURED ALONG THE RIVER (FWA measure, downstream): outlet → Deadman upper signs 1,507 m · DEADMAN closed 3,616 m ·
  open 15,976 m · JUNIPER closed 4,000 m · open 12,333 m · ASHCROFT closed 2,342 m · open 52,290 m to the Gold Pan
  signs. Every piece's two cuts are in river order and no open piece overlaps a closed area. Whole stretch 92,064 m.
- "now: none" was TRUE for the wording he was on (`…downstre`, no longer live, no extent); the live wording
  (`…downstre~2`) holds his save of the whole stretch, waiting for the Gold Pan cut. The headline now reads "SETS this
  place's extent (it has none now)".
- Tributaries: OFF for this place (no call on the place, none in DFO's heading; the builder does not walk them).
