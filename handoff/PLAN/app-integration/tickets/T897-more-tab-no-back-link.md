# T897 — The More tab's back says "Back", not a tab's name; its head stays put while it scrolls

Status: DONE (1468f8bf0, merged into 0.8.2; review blocker (every menu page said ‹ More) fixed in a3ec100f4).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T897 | The More tab's back says "Back", not a tab's name; its head stays put while it scrolls (owner) | DONE | 1468f8bf0 | owner 2026-11-19 ~01:04, ~01:05 |

Owner 2026-11-19 ~01:04 (app chat), desktop web shot of the More tab headed "‹ Spots / More", verbatim: "why does more
link to spots?"

Why: T48 gave the More tab a ‹ back to the tab it was opened from (BrowseHost.tsx, `back={TABS.find(... browse.prev
...)}`), from when More was the hamburger's page. Now More is a tab like the others, and the tab bar (phone) or the rail
(desktop) is always there, so the link only names whichever tab was last open — it reads as if More belonged to Spots.
Build: the More tab draws no back link on any layout (title "More" alone); the pages opened FROM More (Help, About, …)
keep their ‹ More. Android's back button still steps back as the back layers say. Tests: the More tab's head has no back
press at 390 and 1440; a page opened from it still has ‹ More; the surfaces test (one way per action) stays green.

## Owner ~01:05, verbatim: "should just be back and the header in more should be sticked i think"
So, replacing the build above: the More tab KEEPS its back press, labelled plain "‹ Back" (never a tab's name), and
it still returns to the tab More was opened from. The More head (Back + the "More" title) is sticky: it stays at the top
while the page scrolls, on every layout. The pages opened from More keep ‹ More.

## What was built (branch app/t897-more-back)
- The More head (‹ Back, "More", its rule line) is drawn OUTSIDE the ScrollView in `MoreScreen.tsx` (new testID `more-scroll` on the scroll view), so it stays put on phone, narrow web and desktop; the desktop two columns scroll under it.
- The ‹ reads plain "Back" (`MENU_WORDS.back`), a11y name "Back", still `onBack` (returns to the tab More was opened from). `menuVM` and `MoreScreen` no longer take a tab name (`back` prop and `MORE_WORDS.back` removed).
- Pages opened FROM More: they read plain "Back" before, which would now be a second "Back" (surfaces test: duplicate control). New `MenuPage` prop `from`: the ‹ reads "More", said "Back to More" (MenuHost pages, Glossary, Settings); desktop card unchanged (no ‹, the ✕).
- Tests: Menu.test.tsx (head is Back for the previous tab, outside `more-scroll`; a page reads "‹ More"), more.test.ts; surfaces.test.tsx green.
- Pictures: scratchpad/t897-shots/ (390 and 1440, light, top and scrolled).
