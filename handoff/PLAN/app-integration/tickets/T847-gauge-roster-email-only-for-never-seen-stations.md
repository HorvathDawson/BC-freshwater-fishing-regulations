# T847 — The gauge roster email: only stations the roster has never seen, each told once

Status: DONE 2026-11-18 (6432825f5, 7371677ef, 1ff1db9f2; staging feeds worker deployed 13:2x, version 5d796dcd).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T847 | The gauge roster email names only stations the roster has never seen, each once (owner: the feed already follows ECCC's list) | DONE | 6432825f5 7371677ef 1ff1db9f2 | owner 2026-11-18 12:18 |

Source: owner 2026-11-18 ~12:05, on the "ECCC's real-time station list changed" email (10 joined, 13 left), verbatim:
"why does it have to refresh here? … shouldnt it automatically pick up the changes and handle it without needing a
refresh unless it was. anever before seen gauge?"

## What is true today (checked 2026-11-18)

- The feed already follows ECCC's list every run (`liveness.stations_to_ask`: the list wins). The 13 that left are not
  asked; their waters say "not reporting". The joined ones are asked.
- A gauge is attached to a water by ECCC's durable status (`may_speak` = `active`), never by the roster's `realtime`
  flag, and the app reads no `realtime` at all. So a known station going silent or coming back needs no rebuild.
- `realtime` (the roster snapshot) is read only as the feed's fallback when ECCC's list can't be fetched, the shed
  tie-break between two equal stations, and printed counts.
- Of the 10 "joined": 6 are in the roster, active and matched (08CG001, 08KB006, 08KE018, 08NM240, 08NM241, 10CD003),
  i.e. already working. 4 are absent from the roster (07FA008, 08DA013, 08DB015, 08FB012): the bundle can't place them.
  Those same 4 were noted 2026-10-23 (`pipeline/gauges/liveness.py` docstring).
- The notice's text is every joined + left id (`maintenance_notices`), and the event key is `roster:<that text>`, so any
  wobble in ECCC's list makes a new key and a new email.

## Fix

1. The roster notice is about stations ECCC lists that the roster **does not hold at all** (never seen), nothing else.
   Known stations joining or leaving the list raise no notice (the feed already follows them).
2. Each never-seen station is told once: one event key per station (`roster:new:<id>`), so the told-keys ledger
   (`events.ts`) never repeats it.
3. The email says what it means: "ECCC lists N stations we've never seen", the ids, and the refresh + rebuild + match
   steps (a new station needs a match before any water can show it). /status may still show the full drift counts.
4. Python and the worker stay twins (`pipeline/gauges/liveness.py` + `feed/publish.py`, `app/workers/feeds/src/gauges/
   liveness.ts` + `publish.ts`, `index.ts gaugeEvents`, `email.ts rosterSection`), parity tests updated.

## Done when

Both suites green (worker tests + the gauges pytest files), an independent review, merged, the staging feeds worker
deployed with 0.8.0, and the first run after deploy raises one notice per never-seen station (the 4 above), no others.

## Done (branch worktree-agent-a6918514b349ce963, not merged)

`roster_drift`/`rosterDrift` gain `unknown` (listed, no roster row at all); the `roster` notice's `latest` is those ids
space-separated, nothing else (a kept pre-T847 `+id -id` notice is dropped). `gaugeEvents` makes one event per id,
key `roster:new:<id>`; the email: "ECCC lists N stations we've never seen", the ids, refresh → match → rebuild → sync.
Worker 521/521 (parity fixture now carries one never-seen station), pytest feed + liveness files 126/126. Owes: an
independent review, merge, the 0.8.0 staging worker deploy, and the first-run check (the 4 stations, no others).

Review (main chat, 2026-11-18): OK after one fix, made: the per-station event text named "match" but printed the
fetch && build action; it now lists the fetch, `MATCH_ACTION([id])`, the build and the sync as separate steps
(`splitRosterAction`, shared with the email), pinned in events.test. Worker 521/521, tsc clean, pytest 52/52 on the four
roster files. Notes kept on purpose:
- "Once" means at most once a year: the told-keys ledger forgets a key after a year, so a station still unmatched a year
  later is told again (the roster was not refreshed).
- The Python `notify.py` path still dedupes on the whole id list, not per station. No workflow runs it and the webhook
  gets the rendered email; the worker's email outbox is the path that tells the owner.
- The match command is the owner's hand command (no `scripts/heavy.sh` in the email); a Claude session wraps it itself.

Deployed 2026-11-18 (deploy/staging/feeds.sh: worker tests 521/521, every job ok). The first run's `maintenance` holds one
roster notice, latest = "07FA008 08DA013 08DB015 08FB012": the four never-seen stations, no others. Production: with the
owner's prod feeds deploy.
