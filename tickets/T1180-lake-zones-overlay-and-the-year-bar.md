# T1180 — Lake zones: a design chat researches a clean way and brings proposals with pictures

Status: OPEN — DESIGN only; round 2 done, the mix confirmed; nothing is built now except the year-bar run Z1 in the re-cut (owner 2026-11-26); owner: see ## Owner

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1180 | Lake zones: a design chat researches a clean way and brings proposals with pictures | OPEN — design only, build nothing | — | round 2 done (43 zones counted); the mix confirmed; more design on the two maps and the flow, then his picks; Z1 (year-bar run) rides the re-cut |

Source: the owner, 2026-11-26: "i think all this lake stuff we need to just leave it how it is now and really have a few agents look into a clean way to do
this and plan it out and figure out the best way so it isnt janky." The inputs: the planning chat's notes `lake-zones-design` (and `lake-zones-inventory`:
43 closures on 35 lakes; Babine's circles are already an overlay, only the Gullwing zone is a cut). Related: T812, T885, T281.

## His leanings (verbatim in `docs/OWNER-RULINGS.md`; INPUTS, not decisions)
- A mix: cuts for the larger divisions with a full closure (Gullwing stays a cut), overlays for the small ones (stream-mouth closures); Shuswap map A
  probably a real cut. The card's map must be clear whether the user arrives from the map or from a website link.
- Year bar: a new "partial closure" line type, half red, half the other type; the fix goes "in recut", its look from the design chat first.
- A zone off its closed days: not drawn until close to the change; maybe a "changing soon" colour.
- Sign-only zones: proposals with examples of options on how each should look.

## Round 2 (2026-11-26, planning chat; notes `lake-zones-design-2`, sheets A1-A3, B1-B3, C1-C3, S1-S3, R2-0..R2-5)
- **His words (verbatim):** on the models: "can you explain the differences a bit more i like a lot from each proposal you
  sent. we need to make the difference between the main map and the map in the card a bit better defined and to know the
  flow of a user ? i like the search in proposal b ith the multiple parts showing up under the name etc... i think we
  need more work from agents on this design before we consider it. i think we do want some to be full cuts because they
  are big sectiosn liek obvious kooteay lake. however smaller areas should be marks only so i think the mix is the
  correct way". On the cut rule: "can we find the data for how this changes the numbers? i think all line cuts make
  sense to do. i like the no point badge and the we should attempt to get approximate area if it sounds doable but
  otherwise we could look at points could we do analysis on the numbers etc? we want to figure out how many of each
  there would be etc". Lakes partly in a cut area: "this similar to the naming etc it could be a tag saying partly in
  eco reserve? then later can also be in the cut rule options in the later releases?" (the chip: T1191).
- **Counts of the 43 zones by how they are bounded:** 9 line from placeable points, 3 book-map line, 6 sign line, 11
  radius at a held point, 4 radius at an outlet sign or weir, 5 bay, 4 signs only, 1 reserve. Cuts: round 1's size rule
  gives 2; **all line cuts** (his "all line cuts make sense") give 18 cuts on 15 lakes (smallest 0.025 km2); the design
  recommends lines plus bay mouths with both pieces at 0.5 km2 or more = 11 cuts on 8 lakes. Marks: 16 exact, 10
  approximate (dashed edge), 3 pins, 2 "no point" badges.
- **The mix is confirmed as the direction:** big divisions are cuts, small areas marks. The main map and the card's map
  have defined roles (the card's map shows this water only, with numbered parts and the list as its key); search lists
  the lake with its parts and marks under it, each with its answer and dates.
- **His later picks (evening, verbatim):** cuts: "Every line or bay mouth, both pieces at least 0.5 km²: 11 cuts (Recommended)"; approximate edges: "Keep them as approximate marks until located" (only the 4 exact cuts now; 7 stay dashed marks); the card map and search are NOT approved: "more design please just this changes a lot so i want to make it more clear because it effects streams too..." (round 3: the card's map for lakes and streams as whole options). Nothing that changes a look merges without him.
- **Timing:** cuts wait for 0.11.0 (heavy: atlas to release, a binding diff per rule, an owner check of each edge; 2-3 agent-days per batch). Z1, the year-bar
  "part closed" run (about 35 lakes' answers change, no bindings), rides the 0.10.0 re-cut; Z2-Z8 are 0.11.0 and later.
- **Open picks (9, recommended first):** which zones become cuts (all lines no floor: 18 / lines and bay mouths with both pieces at 0.5 km2 or more: 11 / only lines from exact points / round 1's rule: 2); the floor (0.5 km2 / 0.1 / none); cuts set by signs (a dashed "approximate edge · signs decide" / approximate marks until located); signs near a point with no distance (a pin / an approximate area); Dragon Lake (look for the Hallis Creek mouth / the badge); the card's map (this water only / today's Map view); search (marks under their lake / cut parts only); inside pieces for lakes partly in an area (the chip only, decide after 0.11.0 / 8 lakes / 19 lakes); the round-1 picks still open (Y1 year bar, O2 "closes soon", the "part closed today" suffix).

## Work
1. The design chat researches how other maps show a part-closed water, reads `lake-zones-design`, and brings 2-3 options per question WITH PICTURES
   (light, dark, satellite; arriving from the map and from a web link).
2. He picks; the build tickets are written from his picks (year-bar line type, zones file, overlay layer, the zones we can draw first).
3. Nothing is built before step 2, except what he says rides the re-cut.

## Acceptance
Proposals with pictures for every open question; his picks recorded in the rulings page. Severity: medium (a lake answers "open" while part is closed;
the lake page already says "Open, except some parts"). Model: Opus. Size: M (design).

## Owner

- [ ] 2026-11-26 — Round 2's nine picks (listed above; sheets B1/B2/B3/S1/S2/S3 and R2-0..R2-5). Earlier: pick from the design chat's pictured proposals (cuts vs overlays, the year-bar line type, the off-day look, sign-only zones, Shuswap map A, the search suffix). Default: leave as it is now.
