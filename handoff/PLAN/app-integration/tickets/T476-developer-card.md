# T476 — One Developer card behind the credits long-press; dev/staging builds only

Status: OPEN 2026-11-07 (owner: "lets move it over… only ship it in dev/staging no prod and just be a long press to get").

Today the developer tools are scattered:
- Map lab (`MapLab.tsx` / `MapLabModal.tsx`): long-press on the map credits, `EXPO_PUBLIC_MAP_LAB=1` and not production.
- About (`InfoPages.tsx`): data-source/channel picker (T232), Updates log link (T251), env badge.

Do:
- The credits long-press opens ONE "Developer" card with these sections: Map lab · Data source / channel · Updates log ·
  Build info (version, data release, env, origin, channel).
- Gate: dev + staging builds only. Production has no long-press, no card and no dev rows. Nothing hidden: no tap-code,
  no runtime unlock.
- About drops its dev rows (picker, Updates log link) and reads exactly like production. The small env badge stays in
  About so the build is visible at a glance.
- Keep the back layer (`useBackLayer`) and clock rules. Lab switches stay session-only, defaults = today's map.

Tests: production env → no `onOpenLab`/card/dev rows in About; staging → card opens, each section is reachable.
Shots 390 + 1440 (staging web).
