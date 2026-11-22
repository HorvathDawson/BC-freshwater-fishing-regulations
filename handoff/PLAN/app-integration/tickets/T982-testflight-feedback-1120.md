# T982 — TestFlight tester feedback, 2026-11-20 (0.8.3, iPhone): 6 items

Status: DONE — shipped 0.8.5 (eee8ae839, 2026-11-21).

Source: TestFlight feedback from an external tester (iPhone15,4, iOS 26.6.1), read 2026-11-21 with
`node deploy/staging/testflight-feedback.mjs --days 30 --shots <dir>` (owner: "is there a way to have an agent get it?").
Screenshots are in `T982-shots/`.

## Items (oldest first)
1. 2026-11-20T05:06Z, shot `T982-shots/AKPiT9NQT1GRXg5T4-Z-87E-1.png`. Verbatim: "What does % of this estimate mean?"
2. 2026-11-20T05:10Z, shot `T982-shots/AGU1zAiWiZOF8aZKcs2YCVc-1.png`. Verbatim: "Do the boxes with the  percentages coincide with where the gauges are? Could probably just have a symbol for gauges that tells you percentage on top when you click. Or you could just have the symbol different shades of color representing each interval of percentage - I don’t think most people can’t tell difference between 10% and say 13% in the context of this estimate - at least to me, the most valuable info from this would be which gauge is contributing most to the estimate like 70% would be very dark blue and 10% is lighter blue"
3. 2026-11-20T05:13Z, shot `T982-shots/AHeRvYu9rz5TsIdAKU8lnAE-1.png`. Verbatim: "Do you say “much below normal” because that is how you set the intervals relative to normal? You can simplify language to be “lower than” or “higher than” normal, unless I’m missing the point?"
4. 2026-11-20T05:19Z, shot `T982-shots/AFOc2tEyUVLoMa8dUCyHdgA-1.png`. Verbatim: "Replace “add from phone” and “take a photo” with symbols (something like what you see when attaching media in a messaging app)"
5. 2026-11-20T05:21Z, shot `T982-shots/AESP9xaJHS_PAy0HlgFfj6M-1.png`. Verbatim: "I’m ESL what does this mean? Can you simplify the language to say that conditions weren’t recorded because gauge was absent? Especially that second sentence… what do you mean?"
6. 2026-11-20T05:23Z, shot `T982-shots/ANJDU7QLExzIK8BT7ivyyPM-1.png`. Verbatim: "When I open the app, it doesn’t default to where I’m located?"

## Owner 2026-11-21: proposals, not decisions
Owner, verbatim: "We don't blindly take it". Every item is one tester's suggestion. The owner decides each one; build
nothing until then. Look at each screenshot first. Main chat's first read, still to confirm with the owner:
1. "% of this estimate": likely a real labelling problem. Reword or add a one-line explanation.
2. Gauge % boxes → symbol or colour shades: mixed. The numbers are deliberate, and colour alone fails colourblind users
   (T467). Owner pick with pictures; default: keep.
3. "Much below normal" → "lower/higher": the five steps carry meaning (two steps would merge "a little" and "very").
   Default: keep the steps, maybe use plainer words; owner pick.
4. Photo buttons as icons: text is clearer and more accessible. Default: keep; owner pick.
5. The gauge-absent sentence (ESL reader): agree. Rewrite it in short, plain words.
6. The app doesn't open at my location: reproduce first (permission timing on first launch, or the last place
   remembered on purpose), then decide.

## Work
- Triage each item into a fix or a reply. Several are plain-language wording: the "% of this estimate" label, "much
  below normal" vs "lower than normal", and the gauge-absent sentence for an ESL reader.
  - Wording fixes go through the copy rules: short sentences, plain words.
  - The design items need owner picks with pictures: photo buttons as icons, gauge % boxes on the map, the map opening
    at the user's location.
- Split them into their own tickets if they grow. Note here which ticket took which item.

## Acceptance
Every item is fixed, ticketed, or answered with the owner's decision, and each is noted here.

Depends: —. Size: M. Owner: yes (the design picks).

## Owner picks 2026-11-21 (dialog with pictures)
- #2: C, a dot with shade + size + the number inside, no boxes (per-gauge colour becomes a ring or goes).
- #3: B, same five steps: "Far lower than normal" / "Lower than normal" / "Normal" / "Higher than normal" / "Far higher than normal".
- #4: B, icon + one word ("Photos" / "Camera").
- #6: after a dismissed or denied first location ask, a one-line "Show my location" prompt on the map instead of a silent default (cause read from code: `useOpenAtMe` asks once, never again).
