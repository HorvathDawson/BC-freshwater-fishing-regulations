# T1176 — Side channels: fewer on the map, and parts that name themselves

Status: PARTIAL — superseded by the water options; the design lane and the water options answered it: the work moved to T1199 (cut rule), T1240 (folds), T1241 (map calming), T1242 (loop-removal trial) and T1191 (names). left (Claude): the naming fix (T1172) and the side-part report.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1176 | Side channels: fewer on the map, and parts that name themselves | PARTIAL — the rest went to T1240, T1241, T1242, T1199 and T1191 (water options, 2026-11-26) | — | nothing new here; the owner picked "D: simplify the water itself"; follow T1199, T1240, T1241, T1242 |

Source: the owner, 2026-11-26, after the gnis:2123 stop (T1172), verbatim: "mnaybe this is a natural point to
simplify braids too by reducing the amount of braiding on the map and masking it more single lines we could
probably prevenmt most cases of this sort of stuff. (we still need a real fix though with the name generation like
you are working on. otherwise it is just a bandaid." And, on what he meant: "do all first items but also geometry
simplifcation is what i meant by simplification. basically we want to reduce some of the sidechannels on the map to
make a cleaner design (this will happen in the build step etc) does that make sense. that way some more loops or
multiple stream braids entering a lake etc will be simplified makign the idea of a cut and sections easier to
define". The planning chat's notes: `T1176-braids-scoping`, `side-channels-step-back`. Related: T1151 (braids in the
atlas), T1146 (#264), T1140 (#265), T1149, T1172.

## What the scoping measured (clean clone, policy 51, read-only, 2026-11-26)
- Folding same-rule side channels into their river's part changes nothing: the bundle already does it (95.1% of
  channel groups are in their river's part). The 26 side-channel-only parts all differ from their river by a rule,
  licence, salmon chain, tidal water or area (Old Kootenay's channels carry the Creston WMA rules). So only the naming
  fix (T1172) prevents the stop; 181 waters share the no-join risk.
- Fewer parts needs a rule change (a channel takes its river's rules, T1151 option c): a policy bump and a binding
  diff, after T1149. Not for the re-cut.
- The tiles cannot tell a closure from a safe line (Gordon's closed channel is 46 m), so lines can only fade, never
  hide, until the tiles carry a "same answer" flag.
- The step back: 14 of the 26 parts are binding artefacts, 3 are the owner's call, 9 are real; 13,751 braid loops
  survive the atlas prune, reason not yet measured.

## Superseded by the water options (2026-11-26, planning chat)
The design lane (`braids-design`) and the combined options (`water-options`: A smallest change, B tidy the rules, C tidy
the rules and calm the map, D simplify the water itself) answered this ticket. The owner picked, verbatim: "D: simplify
the water itself"; late folds: "Hold the re-cut for all of them"; and, when put to him that D plus "hold" makes 0.10.0 wait
for an atlas change with unmeasured unknowns: "Run the loop-removal trial first, then I decide". Of the 23 side-channel-only
parts, 6 are real and 17 are artefacts of how a rule binds. The work now lives in:
- **T1199** the reserve and park cut rule (the Skeena example he called "crazy"), in the re-cut;
- **T1240** the side-channel folds, by class (including the 3 wrong answers: Cottonwood, West Road, Campbell);
- **T1241** same-answer side channels not drawn below zoom 12 (about 4,005 lines);
- **T1242** the loop-removal trial (why 11,875 loops survive the prune; 5,209 loops under 100 m; 558 creek mouths);
- **T1191** the part names, and "every part says why it is separate".
This ticket closes when T1172's naming fix and the side-part report are done; its owner line below is answered by those.

## Work
1. For the re-cut: the naming fix (T1172), a bundle report of every side-channel-only part and what it differs by
   (a report, not a stop), and a low-zoom fade of side channels (below z12; closures never faded) after his pictures.
2. Geometry simplification in the build step (his meaning): first measure why 13,751 loops survive the prune; then a
   side-atlas trial that removes loops and braids entering a lake, never a channel with its own name, its own rule or
   a closure; lists and pictures to him before anything ships. Needs an atlas build: a data build of its own.
3. Later, decided with the audit review: channels take their river's rules (T1151 option c), the "same answer" flag.

## Acceptance
The re-cut builds with the report and no stop; the fade is on only after his look; the trial's lists name every
removed line and none changes an answer (checked by a binding diff). Severity: medium. Model: Opus. Size: M (1),
L (2).

## Owner

- [x] 2026-11-26 — What to simplify → "Parts and map lines" (2026-11-26)
- [x] 2026-11-26 — When → "Before the re-cut" (2026-11-26)
- [x] 2026-11-26 — What goes in the re-cut → "do all first items but also geometry simplifcation is what i meant by simplification ..." (2026-11-26; full words in Source)
- [x] 2026-11-26 — The later braid items → "Decide with the audit" (2026-11-26)
- [x] 2026-11-26 — Decide the later braid items; look at the fade pictures → "A design lane first, then decide", then "D: simplify the water itself" and "Run the loop-removal trial first, then I decide" (2026-11-26); the open lines are in T1240 and T1242
