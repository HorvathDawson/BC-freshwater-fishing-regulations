order: 770

## 2026-11-25 morning: the release is 0.10.0; what is yours today (planning chat)

- **The number.** Asked 03:00, your pick: "0.10.0, follow the rule (Recommended)". The release that was called 0.9.1
  is cut as 0.10.0. Renamed with it: the hardening release "0.9.2" is now 0.10.1, the feature release "0.10" (stocked
  lakes, bathymetry, corridors, boat launches, Mapbox) is now 0.11. The roadmap's top says so; ticket bodies and your
  quoted words keep the old names.
- **Do (you): the production commands for 0.10.0**, in the order the 0.10.0 chat hands them to you with the dry runs:
  data, then the site, then the feeds worker. `pipeline release --prod` refuses in every Claude session by code, so
  no chat ran them. The site deploy is what puts the new privacy page (T1098) live.
- **Do (you): clear the What's-new words** the 0.10.0 chat drafted as a proposal (its morning summary).
- **Do (you): the App Store submission**, with the iOS chat's walk-through (batch 1 started 02:55). The picture set
  is on main and passes Apple's sizes. The final submit waits on the 0.10.0 iPhone build number and the privacy page
  live on production.
- **Closed:** your ponds answer (Q124 / T153) is built as a test in PR #166, merged 02:50. The head-lakes list, the
  privacy sentences, the gauges card and the stripe look were chosen by the 0.10.0 chat on your word ("I told the
  agent they could do these without me"); what it chose is in the item of 2026-11-24 (planning chat) and its morning
  summary, each one a proposal you can change in 0.10.1.
- **The one-time permission of 00:30 was not used by the planning chat:** it ran no production command. It ends
  with this release.
