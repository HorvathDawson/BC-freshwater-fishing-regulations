# menu-info — the hamburger menu (owner 2026-10-21)

Expo web, 390×844, the live release r-0efa5d05fc92 (it predates the `sources` block, so most dates say
"Date not recorded in this release" until the next `deliver all`).

| shot | what |
|---|---|
| 01-map-hamburger-light.png | the hamburger top-left on the Map tab (also Conditions, Search, Spots) |
| 02-drawer-light.png | the drawer: Glossary · Data sources & dates · About · Report a problem · Contact; the sync slot is the empty top (`MenuDrawer` `sync`) |
| 03-glossary.png / 04-glossary-term.png | the release's own glossary terms (the `gl:` sheets' terms, `glossBlocks`), searchable; a term opens its definition |
| 05-sources.png | every source with its date, from the manifest's `sources` (pipeline) and what the app fetched |
| 06-about-dark.png | About, dark theme: what the app is, the not-official disclaimer, the Synopsis link, versions |
| 07-regs-report-link.png | "Report a problem with this page" at the foot of each Regulations tab |
| 08-report-prefilled.png | the report from the Gear tab: water, part, day and tab prefilled; every diagnostic visible and switchable; the exact message shown |

Walked on web: the browser's back closes a glossary term, then the page, then the drawer (one history entry per level);
picking a page closes the drawer; the drawer closes on ✕, the scrim, a leftward swipe and back.

## Where things are
- words + VMs: `app/packages/core/src/menu.ts` (`MENU_WORDS`, `sourcesVM`, `aboutVM`, `reportMessage`, …)
- state: `app/packages/ui/src/menu.ts` (`useMenu`, `useGlossary`, `routeWhere`); error ring `ui/src/errorLog.ts`
- drawn: `app/packages/ui-native/src/menu/*` (`MenuHost` is mounted once by `Shell`)
- dates: `pipeline/deliver/release/manifest.py` `sources_block` (read from the inputs, never the run's clock)
- config: `app/apps/mobile/src/config.ts` `MENU_INFO` — `EXPO_PUBLIC_CONTACT` (email or https link) and
  `EXPO_PUBLIC_APP_BUILD`; both unset today.
