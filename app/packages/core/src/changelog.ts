/**
 * (T234 v1.1, owner 2026-11-05) THE APP'S CHANGELOG — THE ONE SOURCE. What a reader sees for each version: 3–5 plain
 * bullets for anglers (never commit subjects). Read by:
 *   - More → About → "What's new" (every version, newest first: `changelogVM`, appUpdate.ts), bundled with the app;
 *   - the update prompt (the bell's "App update", T390) and the new-version notification's tap: the INCOMING version's
 *     bullets travel in the version file our server publishes (`deploy/staging/build-apk.sh` copies this version's entry
 *     into `latest.json` / `play.json`, `build-ios-testflight.sh` into `appstore.json`), so a prompt shows them before the
 *     update is installed.
 * A RELEASE ADDS ITS ENTRY HERE before its build (build-apk.sh warns when app.json's version has none).
 *
 * NO IMPORTS: build-apk.sh reads this file with node (`node -e "import('./…/changelog.ts')"`, type stripping).
 *
 * `draft: true` — drafted by Claude from handoff/PLAN/STATUS.md for the owner to review (everything added is a proposal);
 * the owner clears it once read. Dates are the day the version was cut (null: not cut yet / not recorded).
 */
export interface ChangelogEntry {
  readonly version: string;
  readonly date: string | null;
  readonly notes: readonly string[];
  readonly draft?: boolean;
  /**
   * (T723 G11) A note true on ONE platform only, keyed by its exact text. Every other note is every platform's. What's new
   * shows a reader only their own (`changelogVM`: an iPhone is not told about Google Play); `notes` is unchanged.
   * (T888) The VERSION FILES carry the platform's own list (`notesOn`): build-apk.sh writes Android's into `latest.json`,
   * `play.json` and the Play notes, build-ios-testflight.sh writes the iPhone's into `appstore.json`. A newer version is
   * never in the bundled log, so this tag must travel in the file's list, not be looked up here.
   */
  readonly only?: Readonly<Record<string, "android" | "ios">>;
}

/** (T723 G11, T888) The notes a reader on `os` is shown: every note but one tagged for another platform (the web: untagged
 *  only). The one filter: What's new (`changelogVM`), the prompt's bundled fallback, and both build scripts' version files. */
export function notesOn(e: ChangelogEntry, os: "android" | "ios" | "web"): readonly string[] {
  return e.notes.filter((n) => { const o = e.only?.[n]; return !o || o === os; });
}

export const CHANGELOG: readonly ChangelogEntry[] = [
  {
    // (T998) opened by `python -m pipeline release --cut`: write 3-5 plain bullets for anglers (never commit
    // subjects); check-changelog refuses the entry until it has one, and the owner clears `draft`
    version: "0.9.0", date: "2026-11-24", notes: [
      "Regulations: each tab names the rule-book pages behind it, a fish's sheet has its own Sources tab, and the Class II line sits right under the water's name",
      "Conditions: a river with no normal range is charted against its own year, and a level-only gauge like Capilano's now shows its live level",
      "Map: lake shores are softly shaded, boundary labels no longer pile on each other, and a selected slough is outlined once, not boxed",
      "The bell shows only news since you installed, marks it read once you open it, and lists data and app updates",
      "Shared links open the same part, tab and day they were sent from, and the day picker runs past New Year",
    ],
  },
  {
    version: "0.8.5", date: "2026-11-21", notes: [
      "Regulations: one Sources button at the foot of the page lists every source behind the rules",
      "Conditions: plainer words for how a river compares with normal, and a clear note where no gauge can speak for the water",
      "In-season changes that already ended no longer come back as New after an update",
      "Opening a water from a web link and going back now returns to that water, not the last place you looked",
      "The guided tour only shows the way, and Lost Lagoon in Stanley Park shows as closed, with the park bylaw named",
    ],
  },
  {
    version: "0.8.4", date: null, draft: true, notes: [
      "Licence: a creek under a classified river shows that river's Classified Waters licence, with a note on why it may not apply where the creek has its own line in the book",
      "Salmon: where two regions' limits mix, each number names its region",
      "Conditions: the flow estimate always follows Flow, and gauges count as on the water across small unnamed ponds",
      "iPhone: Save rides above the keyboard on a spot, the map no longer sits off to the side after a swipe back, and swiping photos keeps the buttons",
      "Smaller data updates: town search, river flow charts and the walk downstream from a stream take about 2 MB less to download",
    ],
  },
  {
    version: "0.8.3", date: null, draft: true, notes: [
      "Smoother pinch-zoom and zoom-out on the map",
      "Data updates keep downloading after you leave the app",
      "What you can keep: a simpler trout table, one value per fish, with a quiet note where a tributary rule applies",
      "The day calendar keeps one size from month to month",
      "The rule-book page link for a water goes to that water's own page",
    ],
  },
  {
    version: "0.8.2", date: null, draft: true, notes: [
      "Smoother pinch-zoom on the map, and a braided river's side channels draw thinner than its main channel",
      "What you can keep: one table for every fish, each row marked DFO or B.C. or naming the tributary rule it comes from; Gear and Methods are easier to read, each source said once",
      "Salmon: the yearly limit counts adult chinook (with the size an adult is on that river), and Source lists every DFO row behind the card",
      "Sunrise and sunset sit under the date, daylight-only rules show that day's times, and each river forecast model has its own colour",
      "When a new version is out, a note says what's new, then waits under the bell",
    ],
  },
  {
    version: "0.8.1", date: null, draft: true, notes: [
      "The map draws faster, most of all the Salmon map when you zoom out",
      "Route maps on the phone show the gauge dots and where you are",
      "A sharper swimming coho while data loads, with a cleaner line under it",
      "On a computer, the Trout / Salmon switch sits beside the day, and the side bar lets the map show through",
    ],
  },
  {
    version: "0.8.0", date: null, notes: [
      "Trout and char: one table per water: each fish's size and what it counts toward, shared limits boxed together",
      "Tap a fish for how to tell it apart, what applies to it here today, and the book's own lines",
      "Classified waters show their class and dates beside the water's name",
      "Licence: closures for non-resident aliens on the waters they apply to; each youth licence with its own price",
      "The swimming coho now greets you when the app opens, on the phone and the web",
    ],
  },
  {
    version: "0.7.12", date: "2026-11-16", draft: true, notes: [
      "A swimming coho while data loads; the first download resumes in the background",
      "Applying new data shows progress; the bar never goes backwards",
      "Salmon: a stretch the book closes shows closed; a DFO rule I cannot place stays on its river with a note",
      "Level-only waters open Conditions on Level; a small stream on a big river's gauge is flagged",
      "Dark mode: the part of a river you are on stands out",
    ],
  },
  {
    version: "0.7.11", date: "2026-11-15", draft: true, notes: [
      "Android: code shrinker (R8) on for this build; also carries T737, T747, T750 from the shared branch.",
      "A water's map opened from search now frames the part you picked, not the whole water",
      "Upstream and Downstream follow one line through every part of a water, with no branch to pick",
      "Tapping an unnamed stretch opens its regulations sooner",
    ],
    only: { "Android: code shrinker (R8) on for this build; also carries T737, T747, T750 from the shared branch.": "android" },
  },
  {
    version: "0.7.10", date: "2026-11-14", draft: true, notes: [
      "Going back from a water's page no longer flashes while the map comes back",
      "The map's credits card opens above the legend, where it can be read",
      "Search: swipe down to close it, recent searches take less room, and the map stays where search took it",
      "Stepping upstream or downstream on a water no longer redraws the bar, and a Flow or Level pick in a gauge card leaves the map alone",
      "Fixed a rare crash when opening a water",
    ],
  },
  {
    version: "0.7.9", date: "2026-11-13", draft: true, notes: [
      "Faster opening when you tap a water: the stretch you tapped lights up at once",
      "Gauge and wording fixes",
    ],
  },
  {
    version: "0.7.8", date: "2026-11-13", notes: [
      "A pinch zoom keeps gliding after you lift your fingers, as a quick flick should",
      "The bell keeps an inbox of in-season notices: new ones are marked New, changed ones Updated, and Mark all as read clears them",
      "More has a Settings screen for mobile data, crash reports and map colours; the Freshwater Atlas and basemap show their date instead of an age warning",
      "On the Regulations page the salmon block comes first, salmon closed all year get a year bar like other fish, and the heading names the part you opened",
      "Where rivers meet is placed more exactly, so a few side channels and small creeks follow the right part of their river; Gagnon Creek's mouth reads 'at Williston Lake', and part ends name the real boundary (a weir, the source) instead of 'a lake outlet'",
    ],
    only: { "A pinch zoom keeps gliding after you lift your fingers, as a quick flick should": "android" },
  },
  {
    version: "0.7.7", date: "2026-11-12", draft: true, notes: [
      "The key for gauges that report only flow or only level sits on the Conditions map, out of the bottom bar",
      "Tidal water ends at DFO's written boundary; where there is none, a river is fresh to the shore and a creek is tidal only inside a tidal river. Deas, Woodward, McDonald, Tamboline and London sloughs are tidal",
      "A small unnamed pond a river runs through follows that river's rules",
      "Searching \"Dewdney\" finds Nicomen Slough",
      "Nakina River: the salmon card says DFO's rule covers the river from the centre line to the northern bank",
    ],
  },
  {
    version: "0.7.6", date: "2026-11-12", draft: true, notes: [
      "The Keep and Gear tabs show what the current data release holds for a water",
      "A tidal part of a water says it is tidal water, never outside B.C.",
      "The salmon card draws the period limits of each bucket",
      "A gauge card names which reading was ranked and when, and names the data release behind it",
      "Every day shown is re-read at once when you come back to the app",
    ],
  },
  {
    version: "0.7.5", date: "2026-11-11", notes: [
      "A water with a closure on a part the map does not draw carries a clear warning on its card",
      "A rule the book prints for parts of a water keeps those words in its title",
      "Over satellite, a stream opens into two thin edges when you zoom in, so the water shows through",
      "The key under the map always matches the map's colours when you switch Trout, Salmon or Runs",
      "A gauge that reports only the other of flow and level shows as a ringed grey dot on the map",
    ],
  },
  {
    version: "0.7.4", date: "2026-11-11", notes: [
      "Runs keeps its own day and says which day it shows",
      "Sharing a water sends a link to that water's own page",
      "A current notice for a water shows at the top of its Regulations page",
      "Gauge alerts follow the day: set them against past years for that date, and a gauge that has not reported for six hours shows quiet with its last report time",
      "The update row says plainly when a new release was refused or is ready",
    ],
  },
  {
    version: "0.7.3", date: "2026-11-10", notes: [
      "Gauge standings read in plain words, counted in days out of 10, wherever you see them",
      "Flow leads: one gauge per river and day is chosen in flow, and Level never switches it; a gauge that reports only level says so",
      "The gauge map stays above the chart when you switch between Flow and Level",
      "Conditions and the map say which day they show; the app opens on today",
      "Report a problem from a water's Conditions or Map, already filled with the gauge and chart you were looking at",
    ],
  },
  {
    version: "0.7.2", date: "2026-11-09", notes: [
      "Each calendar sits with its own fish: salmon under DFO, the rest under B.C. rules",
      "Calendar legends are easier to read",
    ],
  },
  {
    version: "0.7.1", date: "2026-11-09", notes: [
      "Share a river's flow from Conditions: the link opens a live page for that gauge",
    ],
  },
  {
    version: "0.7.0", date: "2026-11-09", notes: [
      "Flow alerts work like stock alerts: pick a gauge, a level or a usual level for the day, and get one alert when it crosses",
      "A river's parts follow its rules: stretches with the same rules join into one, and each is named by where it starts and ends",
      "Map lines are easier to see in dark mode",
      "Salmon rules from DFO show as out of date when they have not been checked for a day",
    ],
  },
  {
    version: "0.6.9", date: "2026-11-07", notes: [
      "The map pans more smoothly, and stays smooth the longer you use it",
      "The daily limit stays at the top as you scroll: this water's number, and how many may be over a size",
      "Quotas say when a water's limit is an exception to the region's, and how to tell a hatchery fish",
      "Sizes are written one way everywhere",
    ],
  },
  {
    version: "0.6.8", date: "2026-11-06", notes: [
      "A new walkthrough that shows you how to use each control and how to read the rules and conditions",
      "Pinch to zoom and drag to look around a spot's photos",
      "Stepping along a river greys out other waters and never skips a part; no grey flash switching to the map",
      "Lakes have a clearer shoreline, tidal water a quieter edge, and park borders show only when zoomed in",
      "Fixed: a fresh install could save empty files",
    ],
  },
  {
    version: "0.6.7", date: "2026-11-06", notes: [
      "A flick of the map glides more smoothly and starts at the speed of your finger",
      "Streams no longer flicker when you switch back to the map",
      "A small part of a river is shown large enough to read when you step to it",
    ],
  },
  {
    version: "0.6.6", date: "2026-11-06", notes: [
      "The map pans more smoothly zoomed out and draws less work on high-resolution screens",
      "Stepping up or down a river no longer jumps the map from far out to very close",
      "Fixed a problem when moving from one river's map to the river it flows into",
    ],
  },
  {
    version: "0.6.5", date: "2026-11-06", notes: [
      "The app says a new version is out only once Google Play has it for your phone",
    ],
    only: { "The app says a new version is out only once Google Play has it for your phone": "android" },
  },
  {
    version: "0.6.4", date: "2026-11-06", notes: [
      "Panning the map is smoother: the map no longer re-reads every tile each time it stops",
      "Screen readers can read the first-run notice and press \"I understand\" on Android",
    ],
    only: { "Screen readers can read the first-run notice and press \"I understand\" on Android": "android" },
  },
  {
    version: "0.6.3", date: "2026-11-06", notes: [
      "Map credits sit in one small button, off the scale bar, and show in full when the map opens",
      "More → Updates syncs the regulations, the same as the sync button on the map",
      "Winding rivers draw smoother when zoomed out",
      "Changing the update channel says what happens and tells you if it could not be saved",
    ],
  },
  {
    version: "0.6.2", date: "2026-11-05", notes: [
      "A new version of the app shows in the bell as soon as it is out, not a day later",
      "Watershed conditions no longer go blank while you pan; they follow the watershed most on screen",
      "The map keeps its labels and symbols while new regulations are applied",
    ],
  },
  {
    version: "0.6.1", date: "2026-11-05", notes: [
      "Stepping up or down a river no longer draws its highlighted part as two crossing lines",
    ],
  },
  {
    version: "0.6.0", date: "2026-11-05", notes: [
      "Gauge alerts you set yourself: when a river rises or falls past a level, or by a % in a day",
      "Choose how often an alert may ring; by default at most once a day per river",
      "A notice when a newer version of the app is out, with what's new in it",
      "Tap a class in the regulations key to show only it on the map",
      "Closed streams get a dashed stripe, so a closure never relies on colour alone",
    ],
  },
  {
    version: "0.5.0", date: "2026-11-04", notes: [
      "The rule book opens inside the app, right at the page for your water",
      "The forecast lists each of the next 10 days, grouped by watershed",
      "Dates and sunrise/sunset follow B.C. time, whatever your phone's clock says",
      "Fewer crashes and less memory use on Android; the map glides farther",
      "\"Non-retention until further notice\" now reads as closed",
    ],
    only: { "Fewer crashes and less memory use on Android; the map glides farther": "android" },
  },
  {
    version: "0.4.0", date: "2026-10-31", notes: [
      "Gauge charts keep the last week on your phone, so they work offline",
      "Saved spots can have a parking pin, with directions",
      "Provincial parks are drawn on the map",
      "Salmon rules are laid out by species, with each species' limits",
    ],
  },
  {
    version: "0.3.0", date: "2026-10-30", notes: [
      "Your location dot stays put as you move the map",
      "Smoother map drawing",
    ],
  },
  {
    version: "0.2.0", date: "2026-10-29", notes: [
      "Regulations open the moment you tap a water",
      "The rule book downloads with the offline maps",
      "A Glossary from A to Z, with a quick letter strip",
      "New name on the home screen, Can I Fish?, with a new icon",
    ],
  },
  {
    version: "0.1.0", date: null, notes: [
      "First test build: the map, regulations for each water, gauges and saved spots",
    ],
  },
];
