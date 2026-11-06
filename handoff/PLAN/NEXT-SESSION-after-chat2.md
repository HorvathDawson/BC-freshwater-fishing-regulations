# After chat 2 (launch and hygiene): start here

Written 2026-11-05 (late), as chat 2 closed. It replaces `NEXT-SESSION-after-0.6.0.md` for what is left.
**DRAFT — finished when the audit (T333–T335) and the combined heavy run land; until then the sections marked
PENDING are not final.**

## Paste this to start the chat

> Read `handoff/PLAN/NEXT-SESSION-after-chat2.md` and the STATUS.md top entry. Do the list in order. Start ticket work
> without asking; ask me only for decisions. Keep replies short and STATUS.md current.

## Where chat 2 left things
- **App:** 0.6.2 (versionCode 11) on Play internal since 11-05. Closed testing (T371) after 2–3 clean days: the owner's
  Console steps, same build.
- **CI green on both** (app + pipeline) for the first time since 10-20: Node pinned (`app/.nvmrc`), PNGs via fflate,
  tests in BC time, data tests marked, full history on checkout.
- **Done in chat 2:** T86 contact guard · T240 secrets list · T244/T371 drafts current · T252 (upper Pitt) · T304 (code)
  · T319 generator · T331 · T366 · T409 lazy map (all widths) · T410 (partial) · T414 · T415 · T421 · T422 · T425 ·
  T426 · T427 · Heber carry (reflowed). T416: causes found and fixed (build path in about.bundle, synopsis cache key,
  DFO typed inputs) — final proof in the heavy run.
- **Not deployed:** the feeds worker (T366 row digest) ships with the next app; the staging site needs a FULL SEO build
  before its next deploy (T304: site.sh refuses pages without `cift:region`).

## The heavy run (one agent, one job at a time) — PENDING
1. T416 final proof + the release refuses a missing basemap
2. the Mac's deliver (on-disk export matches the new code)
3. full suites once (pipeline fast + slow, `pnpm -C app check`)
4. one web export → T409 Lighthouse (390 + 1440), T410 bundle listings, T304 SEO build + Fast-3G
5. T305 tile_version proof
6. `graphify update .`
Results: (to fill in)

## The audit (T333–T335) — PENDING
Scope approved 11-05 (in the tickets). Report: (to fill in) · fix tickets: (to fill in) · then T335 deletes archive/.

## Next, in order (to finalise)
1. The audit's fix tickets, in the order the owner picks.
2. T335: move what live code reads from archive/, delete archive/.
3. Whatever the heavy run left (see its results).
4. With the next APK ("build" from the owner): T250 Expo/dependency upgrade first, then the device checks — T420/T401
   pan, T242 crash test bed, T261, T293, T315, T354, T252 lower Pitt, T319 saved-spot shot, T410 phone pass; deploy
   the feeds worker with it.
5. T404 (prod switch) when the owner says prod is near.

## Waiting on the owner
- Look at: T425 shot (`.playwright-mcp/t425-phone-light.png`), T427 shots (`.playwright-mcp/T427-after-*-390.png`),
  the store shots + titles (T319, `app/tools/store-shots.json`; images in `store/graphics/store/light/`).
- T371 Closed testing (Console).
- OWNER-TODO has the rest.

## Decided in chat 2 (don't re-ask)
- Switches: ink fill everywhere (T260). Trout/Salmon on the map rail, fish art + word (T425).
- Map waits until first shown at every width on water pages (T409).
- CF token kept until prod; Apple install id declared as Device ID; carry ignores whitespace-only changes.
- Audits: all areas, report + fix tickets, medium depth; delete archive/.
