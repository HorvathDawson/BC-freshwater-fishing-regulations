/**
 * THE APP'S ICON SET (owner 2026-10-22, decisions.md "icons: Lucide, rounded 1.75"): Lucide, ISC licence.
 *
 * The drawings are copied into `@app/ui-native` `icons.tsx` (no runtime dependency); this is the one record of where
 * they came from — the version they were copied at, and the notice the ISC licence asks every copy to carry. The menu's
 * Data sources page shows `ICON_CREDIT` (`sourcesVM().icons`), and `app/LICENSES/lucide.txt` holds the full licence.
 * Bump `version` only together with re-copying the paths from that release.
 */
export const ICON_SET = {
  name: "Lucide",
  version: "0.460.0",
  licence: "ISC",
  url: "https://lucide.dev",
  /** The licence's copyright lines, verbatim (lucide-static@0.460.0 LICENSE). */
  copyright: "Copyright (c) for portions of Lucide are held by Cole Bemis 2013-2022 as part of Feather (MIT). "
    + "All other copyright (c) for Lucide are held by Lucide Contributors 2022.",
} as const;

/** What the Data sources page lists under "Icons". */
export const ICON_CREDIT = {
  label: "Icons",
  lines: [
    `${ICON_SET.name} v${ICON_SET.version} (${ICON_SET.url}), ${ICON_SET.licence} licence.`,
    ICON_SET.copyright,
  ],
} as const;
