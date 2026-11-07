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
