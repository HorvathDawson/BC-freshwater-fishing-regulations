# Keep tab: the app vs the prototype (T21)

2026-10-21. These shots compare the app's "What you can keep" tab with the prototype (`app/design/regs-v11.html`, regs v38) on the same water and day.

**How they were taken**
- Day: Oct 10 2026, the prototype's "today". The app's clock was shifted to that day; time still runs.
- Prototype: served from `app/` on a local port and opened in shot mode (`#shot=1`, a 390 × 844 phone). The state was set with `go({w, d, p, sec:'keep'})`.
- App: Expo web on its own port, reading the owner's data server at :39217 (release r-7b6e802bf22b, read-only).
  - The water was opened straight onto its Regulations view through `Shell`'s existing `initialRoute`.
  - A one-line local hook in `apps/mobile/src/App.tsx` passed that route in. **The hook was not committed.**
  - Today, a water opens from a map or Conditions tap only; there is no URL route to a water.
- Both were driven by headless Chrome over CDP (`app/design/regs-v11/tools/cdp.mjs`).
- Theme: `prefers-color-scheme` was emulated as light, then dark.

**What each folder holds.** One folder per state. Each folder has:
- `{phone,desktop}-{light,dark}-{proto,app}.png`: the first viewport.
  - phone is 390 × 844; desktop is 1280 × 900.
- `phone-light-{proto,app}-scrollN.png`: the rest of the tab, one viewport at a time.

| state | water / part | why it is here |
|---|---|---|
| `vedder_1010` | Vedder River | a stream group in form a (share lead over the region total), the size band, a duty line, How it adds up, Other fish |
| `klwa_1010` | Kootenay Lake — Lower West Arm | a lake; "This water" scope kicker; the Other fish weekday split (Kokanee) |
| `chw0_1010` | Chilliwack River, the closed-all-year part | closed all year: strip, banner with its chain and the merged stretches, where-not |
| `x2p_1010` | Okanagan Lake | a lake with a region-wide total and the lake's own tier |
| `x31p_1010` | Nitinat Lake | tidal: the tidal banner alone |
| `cow1_1010` | Cowichan River, part 1 | a plain row ("Char ↩ Release") with the folded "How this was decided" |
| `x4p_1010` | Shuswap Lake | the in-part caveat ("Fishing is different in some spots today") |

## Every visible difference

The words, numbers, block order and Rule book pages are equal. The Keep parity test proves it on all 134 golden states: `conformance/src/regs/keep.parity.test.ts` checks order, every block, and every drawn line. The differences below are layout and chrome only.

### Outside the Keep tab: the header and shell (T23)
1. **The header.**
   - The prototype has one compact header: back ‹, the name (17 px), the date pill, and Map.
   - The app keeps its water header: the Regulations · Conditions · Map strip (`FaceBar`), then the name at 30 px.
   - Below that, T21 draws a minimal row: the place label on the left, the day ("Today"/weekday) with the status dot and date on the right. The four tabs follow.
   - The app has no "▾" on the date and no Map/Parts button in that row. The date is not a control yet (D7, T23).
2. **The place label on one-part waters.**
   - The app shows "Whole water" (Nitinat, Okanagan, Shuswap); the prototype shows nothing.
   - This is T16's rule. It is an open owner question in implement_progress.
3. **The bottom bar.**
   - The prototype keeps Map · Search · Conditions · Spots under the page.
   - The app hides it in water mode (owner decision D1).
4. **Gear / Methods / Licence.**
   - The prototype draws all four sections in one scroll, and the tabs jump to them.
   - In the app the other three tabs switch to a placeholder: "Gear: not in the app yet. Check the B.C. Freshwater Fishing Regulations Synopsis before you fish." These are T30–T35.
   - There is no pin bar while scrolling (T28/T29).
5. **The Expo dev-tools button.** The app shots show it bottom left (⚡) on scroll pages. It is the dev server's overlay, not the app.
6. **Desktop.**
   - The prototype is a 390 px phone at every width.
   - The app is full width, so the year strip and rows stretch across 1280 px. There is no desktop layout (not in scope).

### Inside the Keep tab
7. **Year strip.**
   - The patterns are the same: closed hatch, fly stripes, the bait band.
   - The closed hatch is drawn with an SVG pattern, so its stripe phase differs by a pixel or two.
   - The prototype's "✓" is a heavier glyph.
8. **Group head.**
   - Same words and numbers.
   - In the prototype, "How to tell ›" sits at the end of the origin sentence; the Rule book link wraps under it. In the app, both links sit on their own row under the sentence.
9. **Size band** ("Of these, only 1 may be over 50 cm"). Matches: the slate side bar and slate head line (`--rule`, round UR7).
   - T21 added `color.ui.rule` to the theme for this: light #46627F, dark #93A8BE, taken from `port/golden/tokens.json`.
   - Both draw the band's 1 px `line2` top and bottom rules.
   - The prototype's band has no gap between its head line and its first kind. The app's tier padding leaves about 4 px more.
10. **Also-count chip** ("Only 1 cutthroat or rainbow trout over 50 cm"). The prototype has a 3 px ink left bar and full width. The app draws a tint chip sized to its text.
11. **Closed banner.**
    - The chain's "Source" chip and the merged stretches' level chips ("This water", "Federal / parks") are drawn.
    - The prototype bullets the merged list. The app indents it with no bullet.
    - "Source" does nothing yet: the rule sheet is T36/T37.
12. **Plain row** (Cowichan "Char ↩ Release ⓘ").
    - Same words, with the pill right-aligned.
    - "How this was decided" is a ▶/▼ fold, closed by default. "See every regulation line ›" is inside the fold, as in the golden.
    - The prototype uses `<details>`; the app uses a Pressable.
13. **In-part caveat** (Shuswap). Folded by default, as in the prototype. The app's fold marker is ▶ in text; the prototype's is the browser's disclosure triangle.
14. **Other fish.**
    - The status key under the list ("release You may fish for it… ⓘ", "closed …") is drawn.
    - It was missing from T19. T21 added `KeepVM.otherFishKey` from the glossary's own words.
    - The row chevron is a text "›"; the prototype uses an SVG chevron.
    - No thumbnail is a 40 × 20 `line2` block, as the prototype's silhouette.
15. **Stamp lines are not drawn.**
    - These are the prototype's "You need Steelhead Conservation Surcharge Stamp › only if…", under a fish and under an Other fish row ("White Sturgeon Conservation Licence").
    - They are a licence answer the pipeline lane is moving into the store.
    - Parity drops them by a keyed exclusion: 19 states.
16. **The group's `.gyear` line is not drawn** ("Through the year You can keep them until Oct 31…"; glenn / hamill / x9p… — 11 states). Keyed exclusion (pending).
17. **klmb_1010's pooled "Rainbow trout" row is not drawn** (decisions D19: page v36's legacy card path). Keyed exclusion. No shot here.
18. **Links.**
    - "Rule book p.N ↗" opens the synopsis PDF at its page.
    - Every sheet link (ⓘ, fish captions, Other fish rows, Possession, "See every regulation line ›", "How to tell ›", "Source") is a no-op until T36/T37 build the sheets.
    - "See the rules from then ›" moves the one day (the map's too).

## Re-taking them
- Start the app on a free port with `EXPO_PUBLIC_DATA_BASE=http://localhost:39217 npx expo start --web --port <p>` (from `app/apps/mobile`, not in CI mode, so Metro watches).
- Serve `app/` for the prototype.
- Run the CDP shot scripts: the session scratchpad `proto.mjs` / `app.mjs` / `shots.sh`, rebuilt from the description above. The app needs the `initialRoute` hook (above) to open a water directly.
