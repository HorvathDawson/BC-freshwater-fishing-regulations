# After chat 6 (0.6.9): start here

Written 2026-11-07, as chat 6 closed. It replaces `NEXT-SESSION-after-chat5.md`.

## Paste this to start the chat

> Read `handoff/PLAN/START-HERE.md`, the restart page it names, and the STATUS.md top entries. Do the list in order.
> Start ticket work without asking; ask me only for decisions. Keep replies short and STATUS.md current.

## Where chat 6 left things
- **Data:** release **r-1b4f54fb6ed9** is LATEST locally (T468 Powell River / Little Slocan: 15 rules gain, 0 lose,
  owner approved; T479 quota wording; T481 pin-bar fields). The staging upload was blocked by an open.canada.ca outage
  (the run-timing workflow's NuSEDS fetch timed out twice); see STATUS.md top entry for whether the retry landed. If not:
  `.venv/bin/python -m pipeline release --staging --min-app-version 0.5.0` (pass the min app explicitly — T484).
  Prod is the owner's step.
- **App 0.6.9:**
  - **Play internal, versionCode 19:** built with the **patched lean MapLibre** (T420; libmaplibre.so verified
    byte-identical to `13.6.1-fslean`). This is the phone perf test.
  - **Staging APK page:** the stock-MapLibre build of the same code, for the A/B.
  - Holds: T479 (quota wording), T480 (one size style; access banner back in place), T481 (sticky header: here-max line +
    one breadcrumb; "Fishing more than one water today? ›" sheet). With pre-r-1b4f data the bar shows the region total
    (no `water_max`); correct once the release is on staging.
- **Fixtures** re-cut to r-1b4f54fb6ed9 (48133455c); `pnpm check` green (6860).

## The list, in order
0. **Owner's phone A/B (T420):** Play (patched) vs staging APK (stock): pan Coquitlam z13, Fraser Valley z11.
   If the patch wins on the Pixel 9, this chat's last job is the owner posting the MapLibre PR
   (`app/tools/maplibre-patch/upstream/`, owner posts in their own words). If not, back to T420's traces.
1. **T482 salmon fold (next release):** code is on worktree branch `t482-salmon-fold` (a7235f996). **Only after
   r-1b4f54fb6ed9 is on staging** (its pipeline code would make the pending release rebuild), merge into
   redesign/stream-sections, set IN REVIEW, send the owner the shots (scratchpad `salmonfold/` may be gone — re-shoot).
   It also fixes a DATA regression: R1/R2/R6 DFO feeds were labelled with today's header format, so those regions ship
   no salmon sizes/hours in r-1b4f (pipeline bump to `dfo-header/3`). Needs a release after merge.
2. **T483 fast-fail** (AGENTS 20a): build `scripts/fast-fail.sh`, wire into release + build-apk. Add the two late
   failures from chat 6: branch not pushed (found at stage 6), resumed release losing min app (T484).
3. **T484:** release resume keeps the ledger's min app version; push check into preflight.
   **T485** steps 2–3: run-timing sources cached on R2, refresh as its own job (owner: no outside site may block a
   release).
4. Carried from chat 5 (details in each ticket):
   - **T478:** 0.6.8 walk failures — Updates log flow stale after T476; a Coquitlam River tap opens Coquitlam Lake.
   - Remove T459's test-bed-only all-grey switch (owner picked A); move T451/T443/T459/T456 to DONE as confirmed.
   - **T466** walkthrough follow-ups, **T467** lake shoreline (owner question open in the ticket), then **T465**.
   - **T351:** staging `/crashes` since 0.6.6. **T462** audit (owner starts with "use a workflow"); **T463** owner runs
     `/code-review ultra`, Claude triages.
   - With the next data release: T461 polygon simplification, T164 after the owner's DFO review. **T453** waits on T234.
   - Not yet ticketed: the "lIARD RIVER WATERSHED" name defect (T481 study).

## Owner items (not Claude's)
- The phone A/B above; posting the MapLibre PR.
- A better place for the access notice (T480 reverted the header chip).
- OWNER-TODO.md for the rest (T470 dark-mode call, T467, prod T404, Play closed testing T371).

## Notes
- `build-apk.sh --play` commits app.json; a release queued behind it then fails "push first". Push, rerun.
- Both T481 and T482 agents needed `basemap`/`tiles` links in their scratch out-dir before vintage passed — worth a
  fast-fail or a scratch-build helper.
- Never commit `data/generated/review/*`.
