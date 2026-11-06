# T333 audit — `pipeline/`, `project_config.py`, `config.yaml` (finder: pipeline, 2026-11-05)

Read-only. Lenses: (1) duplicate data types · (2) duplicate pathways / path bypass · (3) silent fallbacks ·
(4) non-determinism · (5) dead code · (6) missing write-time guards · (7) contradicts AGENTS.md.
Ranked most harmful first. Line numbers are from the working tree at commit 0152c48b6.

---

1. **A missing or moved review-candidates file empties a curated file through the write-through** · lenses 3 + 6
   · `pipeline/regs/review/names.py:95-99` (`_load` → `{"by_id": {}, …}` on `OSError`),
   `pipeline/regs/review/slough.py:104-112` (`_load` → `{"by_id": {}, …}` / `fin = {}` on `OSError`),
   `pipeline/tools/mined_names.py:41-60` (`build` iterates `N.load()["by_id"]`),
   `pipeline/tools/slough_calls.py:36-58` (`prints_of → None` → every mark "candidate gone"),
   `pipeline/regs/review/apply.py:85-100` (`write_files` writes whatever the writer returns, no size check).
   · What happens: `data/generated/review/name-variants/candidates.json` (or the slough automark/finder files)
   absent, renamed, or pointed elsewhere → `N.load()` returns an empty candidate set with no error → `mined_names.build`
   returns `[]` → `apply.write_files("names")` sees the text differs from the curated file and `atomic_write`s
   `data/curated/waters/found_names.json` as an empty `waters` list. Same shape for `slough_calls.json` (every
   confirmed mark becomes "skipped: candidate gone", rows `[]`, file rewritten empty). The write-through runs on every
   saved mark in the review app, so this needs no human command.
   · Why it matters: these are PROMOTED curated files (AGENTS 35/37: a loader must raise, absent data is a bug); an
   empty `found_names.json` silently drops every accepted name from the next atlas; an empty `slough_calls.json`
   un-folds 811 owner calls. The apply prints "wrote … commit it", which looks like success.
   · Fix: `names._load` / `slough._load` raise on a missing file (as `names._owner_calls` already does); and
   `apply.write_files` refuses a writer output whose row count falls below the existing file's by more than N (or to
   zero) without `--accept-shrink`, the way `gauges.generate.promote` refuses demotions.
   · Confidence: high.

2. **Reach-run pairing keeps a by-folder-name fallback and picks by mtime** · lenses 3 + 4
   · `pipeline/deliver/bundle/build.py:124-149` (`_reach_run`).
   · What happens: `want = digest_for(build_dir)`; every `report.json` under `GENERATED.reaches` that fails to parse is
   `continue`d (L127-129); a report with NO `handles` key whose `build` equals the atlas folder name is kept in
   `by_name`; when no digest match exists, `chosen = by_name` and the bundle is built from a run matched by name
   only — the exact pairing the docstring (L115-117, 136-144) says was wrong and was replaced. Among several
   matches, `max(chosen)` picks the newest `st_mtime`, so a `cp -r`/clone/`git checkout` of the reaches tree changes
   which run the bundle takes.
   · Why it matters: the comment at L118-120 says "NO SILENT FALLBACK … It stops, naming the file", but the
   name-only branch is still live and silent; a legacy run paired by name binds rules to section ids that may not
   exist (the handle-table check in `rules.write` is the only backstop). The mtime tie-break is non-deterministic
   across copies of the same inputs.
   · Fix: drop the `by_name` branch (every live run carries `handles` since the digest landed); on >1 digest match,
   refuse and name both runs instead of taking the newest; log (not skip) an unreadable `report.json`.
   · Confidence: high (name branch), medium (mtime harm in practice).

3. **Two implementations of the release id and of `said`** · lens 2
   · `pipeline/deliver/release/build.py:609-614` (inline `said` + sha256 formula) vs
   `pipeline/deliver/release/rebase.py:29-41` (`said_of`, `release_id`).
   · What happens: `build.py` composes `said` from local variables and hashes it inline; `rebase.py` recomputes the
   same id from a manifest with its own `said_of`. The only guard is `rebase.py:48` (the rebase refuses a source
   that does not recompute to its own id) and `test_release_basemap.py:129`, which pins one fixture.
   · Why it matters: add a key to `said` in `build.py` (as `bundle` was added, optionally) and forget `said_of`, and
   every rebase of a basemap refuses or — worse — a rebase mints a different id for identical bytes. Two files saying
   one fact.
   · Fix: `build.py` imports `rebase.release_id`/`said_of` (or both import one `release_id(files, said)` helper);
   one formula, one test.
   · Confidence: high.

4. **Per-file env overrides of generated review inputs survived T422** · lenses 2 + 7
   · `pipeline/regs/review/tidal.py:28-45` (`CURATION_TIDAL_CANDIDATES`, `CURATION_TIDAL_PASS2`),
   `names.py:90-91` (`CURATION_NAME_CANDIDATES`), `q154.py:29-30` (`CURATION_Q154_SAMPLE`),
   `slough.py:42-44` (`CURATION_SLOUGH_AUTOMARK`, `CURATION_SLOUGH_FINDER`),
   `pipeline/deliver/answers/common.py:53-56` (`UI_EXPORT_BUNDLE`),
   `pipeline/regs/review/gate.py:173-178` / `parity.py:199-201` (set `CURATION_BUILD`, `CURATION_REACH_DIR`,
   `CURATION_BUNDLE`, `CURATION_RELEASE_ROOT` into `os.environ` for the review backend).
   · What happens: `config.yaml` (curated comment block) and `curated.py` docstring say per-file overrides were
   retired for `CURATED_ROOT` because "a test or a side build read a mix without noticing"; `RETIRED_ENV` refuses six
   names. These seven-plus live on for GENERATED inputs. Combined with finding 1, `CURATION_NAME_CANDIDATES=/nowhere`
   in a shell that then runs the write-through empties a curated file.
   · Why it matters: the same failure class the T422 note describes (a process reads a mix of real and copied
   generated inputs with nothing saying so); the env names look exactly like the refused ones, so a reader assumes
   they are refused too.
   · Fix: one `GENERATED_ROOT` (mirror of `CURATED_ROOT`, or fold into it as a "data root") and add these names to
   `RETIRED_ENV`; `gate._app`/`parity._load_app` pass paths as arguments to the backend's loader instead of env.
   · Confidence: high.

5. **`CURATED.waters.slough_page_calls` loader returns `{}` on a missing curated file** · lenses 3 + 7
   · `pipeline/regs/review/slough.py:154-160` (`_page_notes`: `except OSError: return {}`), read via
   `page_note()` L163-167 from `PAGE_MARKS = CURATED.waters.slough_page_calls` (L32).
   · What happens: the file is a declared curated `FilePath`, so `CURATED` itself refuses to load when it is absent
   — but `_page_notes` takes a `str` path and is called with `_mtime(p)` which returns `0.0` on `OSError` (L48-51),
   so a file that vanishes AFTER first access, or any caller passing another path, yields "no notes" silently.
   · Why it matters: AGENTS 37 verbatim ("A loader for a curated file must RAISE on a missing file, never return
   `[]`"); the owner's free-text notes exist only here (comment L31).
   · Fix: let the `OSError` propagate; drop the `_mtime` → `0.0` swallow for curated paths.
   · Confidence: medium (low blast radius today; a rule violation on a curated path).

6. **Readers bypass `CURATED`/`GENERATED` and the `CURATED_ROOT` sandbox with literal paths** · lenses 2 + 7
   · `pipeline/regs/dfo_salmon/preverify.py:51-54, 598, 611, 622` (`ROOT / "data/generated/atlas/full/registry.json"`,
   `ROOT / "data/curated/waters/cuts.json"`, `…/splits.resolved.json`);
   `pipeline/regs/review/slough.py:28-30` (`ROOT / "data/generated/sloughs/…"` — a generated folder `config.yaml`
   does not declare; T330 moved the review inputs to `GENERATED.review`);
   `pipeline/regs/review/proposals.py:1089` (`"data/generated/side/…"` literal);
   `pipeline/regs/review/dfo.py:290-291` (`parents[3] / "cache" / "dfo_salmon" / "history"` — a fourth data root,
   `cache/`, outside `source/generated/curated`);
   `pipeline/tools/audit/check_curated.py:152, 160` (`_C.stocking.matches or ROOT / "data/curated/stocking/matches.json"`
   — a literal fallback for a key `config.yaml` deliberately sets to `null`);
   `pipeline/tools/audit/tidal_candidates.py:458, 826-827, 1162`, `tidal_pass2.py:752, 765`, `tidal_words.py:290`,
   `tidal_preverify.py:48` (curated + generated literals in audit tools);
   `pipeline/regs/review/tidal.py:145` (`DEFAULT_CACHE = "data/source/dfo_tidal_areas/2026-10-28/…"`, a dated source
   path as a string constant).
   · What happens: `preverify.py` and `tidal_pass2.py` read `atlas/full` by name (not `GENERATED.require_build()`),
   and `cuts.json` by literal, so under `CURATED_ROOT=<copy>` they read the LIVE cuts while everything else reads the
   copy — precisely the mix T422 was written to prevent. `check_curated`'s stocking/bathymetry fallbacks turn a
   "declared, not built" `null` into a path that is then reported "absent" instead of "not configured".
   · Why it matters: AGENTS 36/41 verbatim (`Never Path("data/generated/...") as a literal`); CI has a "no hard-coded
   curated paths" guard (data-flow §2) that evidently excludes these files.
   · Fix: `GENERATED.require_build()` / `CURATED.waters.cuts` in `preverify`, `tidal_pass2`, `tidal_words`,
   `tidal_candidates`; declare `generated.sloughs` (or move the two files under `review/`) and `generated.side`;
   declare the DFO history cache under `generated:`; delete the `or ROOT/…` fallbacks in `check_curated`; widen the CI
   literal-path grep to `pipeline/tools` and `pipeline/regs/dfo_salmon`.
   · Confidence: high.

7. **Superseded run-timing code kept beside the live feed; its package docstring describes the dead design** · lenses 5 + 7
   · `pipeline/runtiming/generate/{index,passage,profiles,indicators,crosswalk}.py` and
   `pipeline/runtiming/consume/bundle.py` — no importer outside themselves (scan of 452 modules; only
   `generate/extract.py` is reached, from `parse.py:290`). `pipeline/runtiming/README.md` "Older code kept" says the
   frozen-index path is replaced by `feed/`; `pipeline/runtiming/__init__.py:1-30` still states "consume/ … Runs on
   every build" and "generate/ … Writes the frozen index".
   · What happens: six modules (~35 KB) with `__main__` entry points that write `run_cube.bin`/`run_profile.csv`
   (`profiles.py:136-158`) into a layout nothing reads; a reader of the package docstring is told the opposite of the
   README.
   · Why it matters: T335 is deleting `archive/` for the same reason; this is an archive inside a live package, and
   its docstring contradicts the map.
   · Fix: delete the six modules (git keeps them), rewrite `__init__.py`'s docstring to the feed design, keep
   `generate/extract.py`.
   · Confidence: high.

8. **DFO places writer skips the atomic write and the re-validation every other curated writer has** · lens 6
   · `pipeline/regs/dfo_salmon/entries.py:675-679` (`save`: `p.write_text(json.dumps(ef.to_dict()…))`), called from
   the CLI commands at L1088, 1101, 1120, 1131, 1147, 1176 and `match.py:407`.
   · What happens: a crash mid-write leaves a truncated `data/curated/dfo_salmon/places/region-*.json`; nothing
   re-validates `to_dict()` through the model before it lands. Compare `pipeline/regs/parsing/io.py:368-375`
   (`atomic_write`: temp + `os.replace`) and `write_entryfile` L443 (`CatalogueFile.model_validate` on the way out),
   `steelhead/accept.py:102`, and `apply.write_files` which routes through `atomic_write`.
   · Why it matters: these files are curated and synced to the feeds worker (`sync_inputs`); a half-written region
   file fails the next DFO job (data-flow §3.3) or, worse, parses with a region missing.
   · Fix: `save` → `atomic_write(p, …)` and `load(json.loads(text))` round-trip before writing.
   · Confidence: high.

9. **Gauge-match staleness is judged by wall-clock `build_ts` and file mtimes** · lenses 3 + 4
   · `pipeline/common/io/serialize.py:21` (`"build_ts": time.time()` written into every `*.meta.json`),
   `pipeline/gauges/matches.py:275-280, 291-300` (`roster.stat().st_mtime > built` → refuse; `stamp_atlas` likewise).
   · What happens: the only consumer of `build_ts` is a mtime comparison against the station roster / matches file.
   A `git checkout`, `cp`, or rsync without `-t` moves the roster's mtime past the atlas's `build_ts` and the record
   write is refused ("rebuild the atlas") although the bytes are the ones the atlas read; conversely a roster edited
   and then `touch -d`'d older passes. `build_ts` is also the one clock in every atlas artifact (memory note
   "Atlas build IS deterministic … only timings, build_ts").
   · Why it matters: AGENTS 19 ("no clocks in output"); T324 made every stage key content-based, this check is the
   holdout; the refusal message sends the owner to an 18-min rebuild for a timestamp.
   · Fix: `matches.py` already has `matches_digest`/`cuts_digest`; record the roster's sha256 in `RECORD` at build
   and compare content; drop `build_ts` from `serialize.write_artifact` (or keep it out of the meta the build hashes).
   · Confidence: high.

10. **Tolerant "swallow and return empty" in review-side DFO helpers** · lens 3
    · `pipeline/regs/dfo_salmon/splitwork.py:322-357` (`except Exception: return {}` around a gpkg read + registry
    lookup; the empty dict is then cached in `_COORD_CACHE` L358 for the process lifetime),
    `pipeline/regs/review/carry.py:35-44` (`except KeyError: return None`, read by `carry_marks` as "target gone"),
    `pipeline/regs/dfo_salmon/churn.py:225-229` (`include_live` → `FileNotFoundError: pass`, the live version is
    silently absent from a churn report that was asked to include it).
    · What happens: in `splitwork`, any error (a missing gpkg, a bad `where` clause, a pyogrio import failure) is
    reported as "no split coordinates", and the empty answer is cached so a retry in the same process cannot
    recover. In `carry`, a `KeyError` from inside `entry_fingerprints` (a malformed entry) reads as "the mark's target
    is not there" → the mark is carried as stale/orphaned rather than the defect surfacing.
    · Why it matters: AGENTS "no silent fallbacks"; the review app is where a human signs off (AGENTS 16).
    · Fix: catch only the expected exception and never cache a failure; in `carry`, catch `KeyError` only around the
    `entries.get` and let fingerprint errors raise.
    · Confidence: medium.

11. **`pipeline/regs/dfo_salmon/entries.py:1117` and `bundle/rules/presence.py:27`: `.get(...) or {}` on required inputs**
    · lens 3
    · `entries.py:1117`: `pre = json.loads(src.read_text()).get("preamble") or [] if src.exists() else []` — a
    scrape file missing `preamble` (or missing entirely) seeds zero page defaults and prints `+0`, indistinguishable
    from "the page has none".
    · `presence.py:27-28`: `sh_report = report.get("steelhead") or {}`; `by_entry = sh_report.get("by_entry") or {}`.
    A reach `report.json` without a `steelhead` block (an older run) yields `placed = ∅`; the gate at L37
    (`flagged - placed`) then fires with a misleading "re-run" message, but `want != ran` at L33 compares against
    `sh_report.get("list_fingerprint")` = `None`, so the first error the owner sees is the fingerprint one, not "this
    run has no steelhead report". Both paths fail eventually, but by accident of ordering.
    · Fix: `report["steelhead"]` (KeyError names the missing block) and a `preamble` key required by the scrape model.
    · Confidence: medium (presence.py fails loudly in the end; entries.py does not).

12. **Review-input `_load` caches keyed on a swallowed mtime** · lens 3
    · `pipeline/regs/review/slough.py:46-51`, `names.py:114-119`, `q154.py:33-37`: `_mtime` returns `0.0` on
    `OSError`; the `lru_cache(maxsize=2)` key is `(path, mtime)`.
    · What happens: a file that is missing (mtime 0.0) then appears is served from the empty cached entry until the
    cache evicts; two files toggling can keep the stale empty answer alive. Pairs with finding 1.
    · Fix: let `stat()` raise.
    · Confidence: medium.

13. **AGENTS 59 vs the gate: `CURATED.` under `pipeline/deliver/bundle`** · lens 7
    · AGENTS.md:446 ("it opens NO curated waters file (`CURATED.` under `pipeline/deliver/bundle` may name only the
    corpus)") vs `pipeline/deliver/bundle/continuation.py:453` (`CURATED.waters.continuations`) and
    `glossary_source.py:52` (`CURATED.regulations.reference`); `pipeline/tests/test_one_data_flow.py:44-53` allows
    exactly those three.
    · What happens: the rule text and the test disagree; the code follows the test. (Owner 2026-10-29 decided the
    continuation gate — the AGENTS line was not updated.)
    · Fix: one sentence in AGENTS 59 naming the three allowed keys (docs finder's lane; listed here because the
    contradiction is in pipeline code).
    · Confidence: high.

14. **Run-timing feed stamps `built_at` into the base that the vintage check reads** · lens 4
    · `pipeline/runtiming/feed/base.py:202-203` (`"built_at": now` in `base.json`),
    `pipeline/runtiming/feed/build.py:261-262` (`built_at` in `index.json`), read by
    `pipeline/common/vintage.py:285-299` (staged `runtiming/base.json`).
    · What happens: two builds of identical inputs produce different `base.json` / `index.json` bytes; `diff.py`
    compensates by comparing per-file sha256 and documenting "`built_at` alone is not a change" (L5-6).
    · Why it matters: AGENTS 19; the planned "slow feed, content-hashed, publish only on a real change" (data-flow
    §3.6) will need the same exemption, and `index.json` cannot be byte-compared. This is the same class as the two
    T416 findings.
    · Fix: derive the feed's identity from the sha256 of `runs`/`paint`/`waters` (already computed, `build.py:252`)
    and put `built_at` only in the published-time stamp (as the release's `STAMP_KEYS` do).
    · Confidence: medium.

15. **`CURATED_ROOT` sandbox is bypassed by `project_config.get_config().fwa_data_gpkg` and `SOURCE`** · lens 2
    · `project_config.py:63-85` (`get_path` still returns `Path()` for a missing key — the behaviour AGENTS 36 names
    as the cause of the 376-cut loss; `_RETIRED` covers only eight old names), used at
    `pipeline/regs/dfo_salmon/splitwork.py:336` and 20+ other sites for the FWA gpkg.
    · What happens: `get_config().get_path("data_accessor", "gpkg_pathx")` (typo) → `Path()` → cwd, no error. The two
    front doors (`ProjectConfig` for the gpkg, `curated.SOURCE` for everything else) are still both open, which the
    module's own comment (L92-104) calls "the condition that let it hide".
    · Fix: `fwa_data_gpkg` → `SOURCE / "bc_fisheries_data.gpkg"` via `curated.py`; `get_path` raises `KeyError` on a
    missing key; then `project_config.py` holds only the `.env` loader.
    · Confidence: high (latent; nothing wrong today, the trap is still set).

16. **`generated()` helper creates directories for any caller, including readers** · lenses 2 + 7
    · `pipeline/common/curated.py:572-577` (`generated(*parts)` mkdirs the parent), used by
    `pipeline/atlas/waters/added_streams/clean.py:28` at import time (`_CLEANED = generated("municipal",
    "cleaned").parent`).
    · What happens: importing `clean.py` creates `data/generated/municipal/` whether or not anything will be written;
    the helper has no reader/writer asymmetry (`Generated.mkdir` is documented "For WRITERS only"; this is the same
    thing without the name).
    · Fix: delete `generated()`; use `GENERATED.mkdir(...)` inside the writer function, not at import.
    · Confidence: medium (low harm, rule 41 drift).

17. **Dead or orphaned modules and config keys** · lens 5
    · `pipeline/bathymetry/generate/audit.py`, `pipeline/stocking/generate/audit.py` — CLI-only, for domains
    `config.yaml` marks "not built"; nothing references them.
    · `pipeline/tools/audit/review_comments.py`, `pipeline/tools/audit/slough_fold_preview.py` — no reference outside
    themselves (the slough fold is live in the registry; the preview was a T322 dry-run).
    · `config.yaml generated.scratch` — zero readers (`GENERATED.scratch`); `generated.regs.entries_backup` — two.
    · `pipeline/regs/review/slough.py:28` `ROOT`, `tidal.py:24`, `q154.py:24`, `names.py:30`: `ROOT =
    parents[3]` computed and unused (or used only for the literals in finding 6).
    · Fix: delete with T335's sweep; drop the unused config key or point `scratch` writers at it.
    · Confidence: high (dead), low (harm).

18. **Section-id parsing is re-implemented per module** · lenses 1 + 2
    · `pipeline/deliver/bundle/section_measure.py:22-29` (`stream_head`), `continuation.py:135-144` (`line_of`),
    `pipeline/regs/review/proposals.py:586`, `regs/steelhead/known_waters.py:587`, `tools/audit/tidal_preview.py:40,
    77`, `tidal_pass2.py:771`, `tidal_candidates.py:815` — each splits `{blk}:{measure}` / `lake:<wbk>` on its own,
    with differing `None` rules (`line_of` returns `None` for `head == "lake"`; `stream_head` for any non-int).
    · Why it matters: AGENTS 7 fixes the id form; one parser means one place to change if `lake:` ids ever carry
    measures (T281 lake splits is open).
    · Fix: one `section_id.parse()` in `pipeline/common/section_handles.py`; the two bundle helpers become aliases.
    · Confidence: medium.

---

## Checked and found sound (so the verifier need not re-walk them)

- `pipeline/common/curated.py`: lazy, pydantic-validated, `CURATED_ROOT` whole-tree only, retired env names refused,
  `require_build` refuses a missing build. `_checked_root` is cached per value.
- Catalogue `species`/`while`/`symbols`/`source_pages` are `List[...]`, not sets: the bundle's `json.dumps(list(…))`
  rows (`rules/row.py:157-165`, `rules/tables.py:193-199`) are order-stable.
- Stage keys (`pipeline/build.py:148-170, 267-300`) are by content, marks excluded by design (`NOT_BUILD_INPUTS`).
- Python→TS: `pipeline/tools/emit --check` covers gauge policy, calendar, release contract, answers model; CI runs it
  (`pipeline-ci.yml:84`). The `undrawn_part` vocabulary appears in `app/packages/core/src/regs/{caveats,inputs,
  rules}.ts` by name, but as a string the TS maps to its own `not_yet_mapped`, with a comment pointing at the pipeline
  — not a parallel enum.
- `random` is seeded where used (`names.py:105`, `card_check.py:571`); the DFO fetch jitter never reaches a file.
- Tile metadata normalises the tippecanoe temp dir and archive path before hashing (`tiles/version.py:55-75`).
- `read_entryfile` raises on a missing region file unless a writer passes `missing_ok` (`parsing/io.py:304-307`).
- `gauges.generate.promote` is the only writer of `matches.json` and refuses demotions without `--accept-demotions`.
