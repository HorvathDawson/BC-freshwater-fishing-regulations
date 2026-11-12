/**
 * (T234 v1.1, owner 2026-11-05) THE APP'S CHANGELOG — THE ONE SOURCE. What a reader sees for each version: 3–5 plain
 * bullets for anglers (never commit subjects). Read by:
 *   - More → About → "What's new" (every version, newest first: `changelogVM`, appUpdate.ts), bundled with the app;
 *   - the update prompt (the bell's "App update", T390) and the new-version notification's tap: the INCOMING version's
 *     bullets travel in the version file our server publishes (`deploy/staging/build-apk.sh` copies this version's entry
 *     into `latest.json` / `play.json`), so a prompt shows them before the update is installed.
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
}

export const CHANGELOG: readonly ChangelogEntry[] = [
  {
    version: "0.7.7", date: "2026-11-12", draft: true, notes: [
      "The key for gauges that report only flow or only level sits on the Conditions map, out of the bottom bar",
      "A river's tidal part ends at DFO's written boundary or where the river meets the shore",
      "A small unnamed pond a river runs through follows that river's rules",
      "Searching \"Dewdney\" finds Nicomen Slough",
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
  },
  {
    version: "0.6.4", date: "2026-11-06", notes: [
      "Panning the map is smoother: the map no longer re-reads every tile each time it stops",
      "Screen readers can read the first-run notice and press \"I understand\" on Android",
    ],
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
