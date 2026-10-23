# T122 — One station liveness: "active", "realtime" and "ECCC-listed" used interchangeably

Status: DONE — 9fe57691, review 0f918ea0 (lane-T118). Ships with the next deliver (bundle code) and the next feed run
(`pipeline.gauges.feed.publish`); measured bundle output unchanged, no station's liveness changes.

Source: owner 2026-10-22 (unify checks that decide the same thing); unification audit 2026-10-22, finding gauges 5
(PLAUSIBLE).

## Decision being unified
"May this station speak at all, and is it speaking now?"

## Where it is decided today
- `pipeline/deliver/bundle/build.py:619-625`: "active, not traffic" (deliberate; stays the rule).
- Elsewhere in `pipeline/gauges/`: `realtime` flags and the ECCC station list are each used as "live".

How they differ: a station can be active but missing from the ECCC list, or flagged realtime but inactive, and the
readers disagree on whether it speaks.

## Work
- `pipeline/gauges/liveness.py` with `may_speak(station)` (the build.py rule) and `speaks_now(station, feed)`; every
  reader calls them.
- Pin: a grep test that `realtime` / the ECCC list are read only inside `liveness.py`.

## Acceptance
List the stations whose liveness changes (expected few, each named). Bundle diff on the gauge tables reviewed.

Rebuild: deliver. Depends: none. Size: S. Owner: no.

## As built (2026-10-23, lane-T118)

- `pipeline/gauges/liveness.py`: `may_speak(station)` (ECCC status Active — the `build.py` rule, unchanged) and
  `speaks_now(station, feed=None)` (on ECCC's real-time list this run when `feed` is given, else the roster's
  `realtime` snapshot of that list). Independent facts: `speaks_now` is NOT gated on `may_speak` (the feed asks every
  listed station, including ones the roster has never seen). Also moved here from `feed/publish.py`: `STATION_LIST`,
  `listed_stations` (the list's parser), `stations_to_ask`, `roster_drift` (publish imports them; same objects).
- Every reader calls them: `deliver/bundle/build.py` (shed filter → `may_speak`; the "transmitting" report →
  `speaks_now`), `consume/shed.py` (equal-ratio tie-break → `speaks_now`), `consume/cuts.py` (`live_only` →
  `speaks_now`), `generate/promote.py` (transmitting counts), `feed/publish.py` (stations to ask, drift),
  `data/fetch_data.py` (the roster writer now parses ECCC's list with `listed_stations` and counts with `speaks_now`;
  its own copy of the URL and parser removed). `shed.load_stations(live_only=…)` had no caller — a third spelling —
  and is gone.
- Tests: `pipeline/tests/test_liveness.py` (whole values on four rows — transmitting, seasonal-silent, discontinued,
  listed-while-discontinued; the list parser; stations to ask + drift; publish and fetch_data use these objects; the
  grep pin over `pipeline/**.py` + `data/*.py` for `.get("realtime"|"active")`, `["realtime"]`, the list URL and
  `Prov/Terr`, with a test that the pin sees each spelling). Pin in `smoke.txt` (guard).
- Mutations run by hand, each failing the named test: `may_speak` reading `realtime` →
  `test_may_speak_is_ecccs_status_not_todays_traffic`; `speaks_now` ignoring the feed →
  `test_speaks_now_is_todays_list_else_the_snapshot`; `speaks_now` gated on `may_speak` → that test and
  `test_the_feed_asks_todays_list_and_says_how_far_the_roster_drifted`; `listed_stations` returning an empty set →
  `test_the_list_is_none_not_empty_when_it_says_nothing`; `shed.py` back to `.get("realtime")` and `build.py` back to
  `s.get("active")` → `test_nothing_else_reads_liveness`.

### Proof (acceptance)

Stations whose liveness changes: **none**. Roster (`data/source/bc_hydrometric_stations.json`, 2,324 stations): 466
active, 428 realtime, realtime ⊆ active (0 realtime-but-inactive); 38 active-but-silent (08CG001, 08DA012, 08EG012,
08FE002, 08HDX05, 08JA013, 08JA015, 08JB013, 08KB006, 08KE018, 08KH030, 08LG028, 08MC049, 08MF035, 08MF038, 08MF072,
08MF073, 08MF074, 08MF075, 08MF076, 08MH152, 08NB016, 08ND017, 08ND025, 08NE008, 08NE126, 08NH016, 08NH115, 08NH126,
08NH138, 08NJ158, 08NM240, 08NM241, 08NN028, 08NP003, 08OA003, 08PA004, 10CD003) — each keeps its shed (may speak)
and is not cut at (does not speak now), exactly as before. ECCC's list fetched once 2026-10-23: 432 BC stations = the
428 + 07FA008, 08DA013, 08DB015, 08FB012 (not in the roster: the feed asks them, as before); none listed-but-inactive.
No station is demoted or promoted, so there is no station-by-station demotion list.

Bundle diff: scratch bundle built with this code (same atlas + batch-4 reach run, via `scripts/heavy.sh`) against the
live bundle of 06:37: `gauge`, `section_gauge`, `lake_gauge`, `section_down`, `gauge_stats`, `gauge_clim`,
`section_panel`, `panel_member` — and every other table — identical.

## Owner questions
- The atlas cuts rivers at stations that SPEAK NOW (`cuts.split_defs`, the roster's realtime snapshot), while sheds are
  built from stations that MAY SPEAK (active). So the 38 active-but-silent stations above own sheds but no cut, and the
  cut set moves with the season the roster was fetched in. Should the cuts follow `may_speak`? It would add up to 38
  gauge cuts (fewer: lake stations are never cut) and needs an atlas rebuild (section ids move). **Built: unchanged**
  — cuts still read `speaks_now`, named in `cuts.py` and `liveness.py`.

## Left / follow-ups
- The app reads the feed index's `listed` (its own "speaks now"); not in `pipeline/`, not touched here.

## Adversarial review (2026-10-23)

- **Missed reader (fixed).** `pipeline/tools/check_curated.py` decided "may speak" on its own: the gauge-match artifact
  declared `roster_where="active"` and `undecided` read `r.get(a.roster_where)` — the `active` field through a variable
  key, which the grep pin cannot see. It is the CI gate deciding which unmatched stations make the gauge match STALE
  (AGENTS 62). Now `roster_where` is a predicate and the gauge artifact passes `liveness.may_speak`
  (`roster_where_word="active"` keeps the report's wording). Same verdict on the live roster: `ok gauge match 2,074 of
  2,324 promoted; 250 in the review queue`.
  Pin: `test_check_curated.py::test_the_gauge_queue_is_committed_curated_data` asserts the predicate IS `may_speak`.
  Mutation: gauge `roster_where` back to a `realtime` read fails it (and `test_nothing_else_reads_liveness`).
- **Pin widened.** It missed a subscript `r["active"]`, and any `getattr(row, "realtime")`. `_READS` now covers
  subscripts of either field (the answers' unrelated `g["vessel"]["active"]` excepted, pinned by
  `test_the_pin_leaves_the_answers_vessel_alone`) and `getattr`; `test_the_pin_would_see_a_second_reader` gained both
  spellings. Variable-key reads stay out of reach of any grep; that is why the check_curated pin is by identity.
- **No liveness change, confirmed.** Every replaced read is `bool(s.get("active"))` → `may_speak`, or
  `bool(s.get("realtime"))` → `speaks_now(s)` (no feed), so the bundle's gauge tables and the cut set are identical
  by construction. That is the code-level equivalent of the builder's bundle diff. `fetch_data`'s `listed_stations(text)
  or set()` matches the old parser except that a blank id is no longer added, which no reader matched anyway.
- Not touched, noted: `handoff/PLAN/app-data/own-gauge/scripts/{gates,rebuild}.py` still read `s.get("realtime")`;
  they are archived handoff scripts outside `pipeline/` and run nowhere. `build.py`'s comment says 27 active-but-silent
  stations; today's roster has 38.

Ships with: the next deliver (bundle code) and next feed run, as before; `check_curated` is live on the next CI run.
