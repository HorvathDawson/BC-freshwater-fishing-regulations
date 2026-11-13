# T703 — More: a Settings screen, the Freshwater Atlas is not "out of date" by age, no update lines repeating the table

Status: IN PROGRESS — left (Claude): commit and push; owner: judge the Settings layout on the next build

Owner 2026-11-13, phone (0.7.7 build 29), the More tab, verbatim: "Fresh water atlas rarely changes it isn't stale. Also all
the toggles for settings like crash reports or download source etc should be in a setting smenu in here. Also all the
update info at top is redundant since it is in the table formatted nicer below it?"

## 1. Staleness, per source (`core/src/staleness.ts`, THE ONE TABLE)
Before: Freshwater Atlas `age` 730 d (the owner's Mar 26, 2024 atlas was 2.6 years old: "Likely out of date"); Basemap
`age` 365 d; HYDAT `notice` (the feed's word) with 548 d as the fallback; Synopsis `release` (in force through
`valid_until`); Map tiles `release` (the proven pair).
After: Freshwater Atlas and Basemap are `dated`: the row gives its date and the words "Changes rarely" (a new
`SourceFreshness` kind `dated`, a filled `faint` dot, Help row), never a verdict from age and never from "a newer data
release" (that is the regulations' and the map's). No date recorded: unknown, as before. Unchanged and true for what they
are: Synopsis (the lapse is a fact: its last day passed), Map tiles (the pair is a fact), HYDAT (the feed says when a newer
release is listed; the 18-month fallback is a missed yearly release, only when no word reaches the app), feeds by age.
Is there a real signal that a newer atlas exists? No. The feed's notices cover HYDAT only; the release's `sources.fwa`
holds a date, nothing says a newer atlas is published. Only such a signal could mark it out of date; none is invented.
Owner's choice: the basemap is ours to rebuild yearly; if you want a reminder when it is old, that is a to-do for you, not
a word on the phone.

## 2. Settings
A "Settings" row, first on More, opens a Settings screen (`SettingsPage`, a menu page: back layer, ‹ / system back /
browser back). Under square-cornered headings: DATA (Download over mobile data on a phone; the low-bandwidth mode on the
website), PRIVACY (Send crash reports), DISPLAY (the three palette tiles and the colour-blind note). Same controls, same
words (the crash-report explanation word for word), same testIDs, same stored keys; a heading with nothing under it is not
drawn. Everything found below the fold on More was exactly these four controls. The words that named their home now say
"More → Settings" (the first-download answer, the confirm, the crash line on the website's privacy page). Not a
`MenuPageKey`: More opens it itself (no Shell change). No start link or route ever pointed at one of these controls.

## 3. Update information above the table
Above the table were: the refresh button with "Refresh data: up to date"; "Updates: Checked 3 min ago"; one line per
source ("Live gauge readings: Checked 3 min ago · updated 2h ago", ...); a "newer data release" banner; the in-season
card. Removed: the "Checked ..." line and every healthy source's line (the table has the ages). Kept, because the table
does not say it: the button (the one control) with its name, and its state in words only when it asks something
(checking, downloading, a failure, a newer release, feed not reached); a source that FAILED, in words; a release this app
cannot use or refused; a download line and the new-release banner only where there is no button to say them; the
in-season card and its link to the Province's notices (nowhere else on More). The summary badge on the "Data" heading is
the table's total and stays.

## Tests
core: menu.test (the table, the atlas at every age, no date, a newer release), more.test (rows aloud, `syncRowWords`,
Settings row), updates.test (what More keeps). ui-native: Menu.test (Settings opens, headings, controls where they were,
back closes it; no control left on More), OneSync.test (no repeats, failures kept), MobileDataRow / CrashSwitch / NetBanner
(open Settings first). The testbed scenario bg-install opens Settings for the mobile-data row.
