# Handoff — DFO salmon curation (the dossier loop)

Written 2026-09-02, mid-corpus. This says **where the work is, how a sitting is run, and what
was learned the hard way**. It does not say what to do next.

`pipeline/regs/dfo_salmon/CURATION.md` is the *design* — what is manual, what is not, and why. This is
the *operating manual*: the exact commands, the per-water loop, and the traps.

---

## 1. Where it stands

| | |
|---|--:|
| waters bound to a registry item | **160 of 161 (99%)** |
| still open | 1 — **Sheldens Creek** (user is handling it) |
| waters carrying place-naming locators | 74 |
| …still needing at least one cut | **66** |
| ACTIVE locators needing a cut | 82 coordinate · 11 confluence *(28 already satisfied)* |
| DORMANT locators needing a cut | 54 |
| curated split blocks / splits | 196 blocks · 392 splits |
| DFO-tagged overrides | 7 load-bearing *(17 more were deleted as redundant after the rebuild)* |
| DFO-sourced name variants | 17 |

**Matching is done. What remains is geometry** — 82 live coordinate anchors, plus 54 dormant ones.

Fully curated so far: **Somass** (8 locators → 4 extents, 0 unbound, 6 cut-points).
In flight: **Skeena** — 23 cuts already exist, 9 more needed, and it is blocked on user pins (§6).

---

## 2. The two tools

Both read `pipeline/regs/dfo_salmon/entries/region-*.json` (the match + locator state) and
`pipeline/atlas/splits.json`. Neither writes anything — **you** write the JSON.

### `dossier.py` — "which registry item is this water?"

```bash
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.dfo_salmon.dossier list
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.dfo_salmon.dossier "Yakoun River"
```

Prints different evidence per status, because the question is different:

* **ambiguous** — the candidates *are* the question. Each is shown with kind, MUs, section count,
  the cut-points already on it, and an **OSM pin link**. Picking between `Yakoun River gnis:3485`
  and `Yakoun Lake wbk:329163190` is a decision about which blue line DFO means.
* **unmatched** — no candidates, so nothing registry-side to show. What *is* known is what DFO
  wrote: name verbatim, region, and every locator phrase.

With matching at 99% this tool is now mostly for re-checks; `splitwork` is the daily driver.

### `splitwork.py` — "which cut-points does this water's regulation need?"

```bash
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.dfo_salmon.splitwork              # the worklist
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.dfo_salmon.splitwork "Morice"     # one water
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.dfo_salmon.splitwork --all        # every water
PYTHONPATH="$PWD" .venv/bin/python -m pipeline.regs.dfo_salmon.splitwork "Somass" --extents
```

Grouped **by water**, on purpose: a river's locators reference each other ("from the signs 200 m
above the bridge down to the cable car 200 m below it"), its existing cuts are the vocabulary the
next one should reuse, and a curator opening a map opens it once per river, not once per rule.

Each anchor is classified by **what it would take to resolve it**:

| class | meaning | who resolves it |
|---|---|---|
| `satisfied` | an existing cut's label already appears in the locator text | nobody — reuse it |
| `confluence` | the locator names a tributary whose **wsc is a child** of this water's | derivable from FWA — write it without a pin |
| `lake` | names a lake and an auto lake boundary exists | derivable |
| `coordinate` | a bridge, sign, road, dam | **a human places it on a map** |

`--extents` shows what the locators **resolve to once bound**, grouped by section rather than by
wording — this is what proves two differently-worded locators are the same reach. Needs a built
graph (`output/v2/full/graph.pkl`).

**Read the `maybe?` column before pinning anything.** It counts active locators that already have
a *plausible* existing cut — a rewording the strict label test cannot see. Somass proved why this
matters: three wordings of one reach score **0.126 / 0.485 / 0.511** on string similarity and
resolve to **identical sections**. Geometry beats text; never dedupe locators by wording.

---

## 3. The per-water loop

1. **`splitwork "<Water>"`** — read the whole card. Locators print **verbatim** from `source_text`.
   *A locator paraphrased is a locator that cannot be checked against the page.*
2. **Check the `maybe?` candidates first.** If an existing cut is the same place under another
   wording, reuse the split id — do not mint a second one.
3. **Write the derivable ones now** — `confluence` and `lake` anchors need no pin. Confirm the
   child relationship with the **watershed-code child test** (§5) before writing.
4. **Collect the `coordinate` ones into one list for the user**, each with the verbatim locator and
   an OSM link near the reach. One list per water, not one question per anchor.
5. **Bind the extents in the same pass.** Do not do the work twice: while the locators are on
   screen, record `(op, split_ids)` per locator. `--extents` verifies afterwards.
6. **Mark duplicates as duplicates** — `duplicate_of` + `duplicate_confirmed` + a note. Two
   locators that resolve to the same sections are one extent, even if the wordings differ.

### Where the artefacts go

| what | file — **after the restructure** | before |
|---|---|---|
| cut-points | `data/curated/waters/splits.json` | `pipeline/atlas/splits.json` |
| a DFO name DFO uses and the province does not | `data/curated/waters/name_variants.json` | `pipeline/name_variants.json` |
| a forced item binding | `data/curated/regulations/overrides.json`, **tagged `"source": "dfo"`** | `pipeline/regs/matching/overrides.json` |
| DFO entry files | `data/curated/regulations/entries/dfo_salmon/` | `pipeline/regs/dfo_salmon/entries/` |
| synopsis entry files | `data/curated/regulations/entries/synopsis/` | `pipeline/regs/parsing/entries/` |
| extents / bindings | ⛔ **not yet decided — see §7** | |

⚠️ **Reach every one of these through `pipeline.common.curated`, never as a literal path.**

```python
from pipeline.common.curated import CURATED
CURATED.waters.splits            # data/curated/waters/splits.json
CURATED.entries.dfo_salmon       # data/curated/regulations/entries/dfo_salmon/
```

The paths are validated by pydantic when the process starts, so a typo fails naming the key
instead of returning an empty list. `dossier.py` and `splitwork.py` both resolved these by hand
(`Path(__file__).parents[1] / "splits.json"`, and one bare `Path("pipeline/atlas/splits.json")` that
only worked from the repo root); both go through config now.

`load_overrides(path, source="dfo")` — the **provincial matcher never loads DFO overrides**. Keep
the tag on every one you add or a DFO decision leaks into the synopsis build.

`NameSource.dfo` ranks **below** `gazette`, so a DFO name can never relabel a gazetted water.

---

## 4. Split-authoring conventions

Anchor shapes: `point` · `confluence` · `lake` · `mu_boundary` · `area_boundary`, plus
`offset_m` / `offset_dir` for "200 m above the bridge".

* **Offsets, not new points.** "from the signs 200 m above the CNR bridge" is the bridge point
  with `offset_m: 200, offset_dir: up` — one coordinate serving both cuts, and it stays right if
  the coordinate is later refined.
* **`applies_to` must name a real registry item.** `test_every_waterbody_block_targets_a_real_registry_item`
  enforces this **via the id index**, not the item-id list — an earlier version false-positived on
  `gnis:29662`. Inventing an id (I once wrote `gnis:3773` for the Telkwa; the real one is
  `gnis:24733`) strands the split silently.
* **A split landing in a lake becomes an alias on the lake boundary.** Verify the cross-layer
  feature at the far end — three layers dropped it silently once.
* `sign_bounded_zone` and "within a 400 m radius" are **not supported anchor shapes**. The designed
  answer is `Binding.notes` + `spatial_caveat`, and the rule must not render as a plain fill.

---

## 5. The watershed-code child test

The reliable defence against name collisions, and the thing that turns a `confluence` anchor into
a no-pin write:

> a tributary's `wsc` starting with `mainstem_wsc + "-"` **proves** the relationship.

Name matching does not. "Cedar Creek" and "Howson Creek" both exist several times over. Run the wsc
test before writing any confluence split, and quote the code in the commit.

`kinds` filter on name-variant targets: **`blks` is stream-only by construction; `wscs` is not.**
If a variant must not hit the lake, say so — the Docee/Long Lake pair is the worked example.

---

## 6. Open items

* **Skeena** — 23 cuts exist, 9 needed. Blocked on user pins for: the **CNR Railway Bridge at
  Terrace** (also the B(i)/B(ii) scope divide — one split, two scopes), **Cedarvale**, the
  **Highway 37 Bridge**, the **Classified Waters boundary at the top of Hell's Gate**, and "a point
  above the Babine confluence". **Mill Creek** (`gnis:36435`, wsc `400-362851`) is derivable by the
  child test and can be written with no pin.
* **Sheldens Creek** — the last unmatched water. User: "i havent found the sheldens creek yet."
* **~64 waters** remain in the worklist, by water, binding extents in the same pass.
* **The 3 `E:areas-*` scopes** need `drains_to_area(N)` — outlet at the existing tidal boundary,
  point-in-polygon against the PFMA Area, watershed above it. See CURATION.md §5.
* **54 dormant locators.** They are *work*, not noise: **"we assume no pins are dormant, they could
  be reintroduced with a regulation update."** Pin them, just after the live page.

---

## 7. The one thing that blocks writing entries

Somass is curated but **has no entry file**, deliberately. The decision was:

> *"wait we shouldn't add to the current entries… DFO should have a different set of entries."*

So DFO extents currently live only as splits + the `--extents` view.

**The location is now decided**, which unblocks this: `data/curated/regulations/entries/dfo_salmon/`.
The reasoning and the full migration are in the restructure plan (which supersedes
`16-curated-data-layout.md`); the short version is that curated data is grouped by DOMAIN under
`data/curated/`, beside `source/` and `generated/`, so the three kinds are visible in one listing.

**Sequencing, and it matters for you specifically:** the entries move is the LAST phase of the
migration and lands in its own commit on a clean tree, because `git mv` conflicts with any local
edit to an entry file. So:

* **Keep curating into `splits.json` now.** It moves with everything else; `git mv` preserves
  history and does not touch contents.
* **Do not create the DFO entries tree by hand.** The migration creates it. Minting it early means
  moving it twice and resolving a conflict for nothing.
* **Do not have uncommitted entry edits when the move runs.** Commit or stash first.

---

## 8. Learned the hard way

* **Never build without `--splits`.** It once silently dropped 376 curated splits and 373
  boundaries, and a build was promoted that way. Splits are now **default-on** and a missing file
  is a hard error; `build_parity` reports boundaries on their own line with a LOST block. A missed
  path gives an *empty load*, not a crash — that is the failure mode to fear.
* **Print locators in full.** Two separate corrections were about truncation: *"you are cutting off
  text"* and *"this isn't giving the full unedited locators."*
* **Substring greps find the wrong lake.** "Treston Lake is not Redsand Lake, it is just right next
  to it." The string I matched turned out to be a *bathymetry* label, not FWA at all. Resolve
  through the id index, never through a grep of a name.
* **Don't assume the tool is wrong when it disagrees with you.** `idx['redsand lake'] -> None` was
  correct; my "fix" was the bug.
* **A confirmed grouping may cross ops.** `test_automatic_groupings_never_pair_different_ops` was
  renamed from `test_real_…` and exempts `duplicate_confirmed`, because a curator confirmed an
  island grouping that legitimately crosses. A companion test requires the note.
* **Added lakes**: negative wbk (`-1, -2, …`) + negative gnis (`-9000001, …`), documented in
  `pipeline/atlas/waters/added_lakes/README.md`. The mechanism is re-stamping `FidRow.wbk` inside a
  polygon, which **breaks the fid run in `_assign_owners`** — that is the part to watch.
* **Waters never move; rules do.** Region 6 held 77/77 waters over 2.3 years while rules turned over
  ~50%/yr and scope text drifted. **Curate geography once; re-scrape rules.** This is why a sitting
  spent on cut-points is worth more than one spent on rule text.

---

## 9. House rules that apply to this work

* ⛔ **Never run the LLM parser** — `pipeline/regs/parsing/run_parse.sh parse`,
  `python -m pipeline.regs.parsing.dispatch`, or anything spawning `claude -p`. It spends the user's
  credits and is human-only. Hand over the command, even when told "run it."
* **Never write `pipeline/regs/parsing/entries/*.json` (or DFO entries) without backing up and diffing.**
* **Prefix shell commands with `rtk`.**
* **Run `graphify query` before grepping.**
* A concurrent agent commits with `git add -A`. **Add files explicitly, never `-A`** — otherwise
  your edits land in its commits under unrelated messages.
