"""Collapse the section->CU join into profiles, and emit what each surface needs.

THE FLAT JOIN IS 3.9 MILLION ROWS, which is not a thing to put in a bundle. But reaches do
not carry arbitrary CU sets — a whole watershed shares one — so the sets dedupe to 581.
That is the same observation `ruleset` / `section_ruleset` is built on, applied to a
different join.

TWO OUTPUTS, BECAUSE THE MAP AND THE SHEET ASK DIFFERENT QUESTIONS.

    run_cube.bin      "how strong is Steelhead here in pentad 45" -> one byte, no join.
                      The MAXIMUM over that species' runs, which is right for a colour:
                      the question a colour answers is "is anything of this species running
                      here now", and on the Skeena mainstem the answer is yes in August
                      (summer run) AND in January (winter run).

    profile_runs.json "WHICH steelhead" -> the list, with labels and peaks.
                      The cube cannot answer this and must not pretend to. A reach with
                      `Skeena Coastal Summers` and `Skeena Coastal Winters` has two runs,
                      and a sheet that says "Steelhead: peak Aug 11" has silently deleted
                      the winter fishery.

BROOD LINES ARE NOT RUNS. Pink (even) and Pink (odd) never overlap in a given year, so they
are carried as separate `form`s and the cube takes the max of one line, not of both — see
`Run.form` in `runs.py`.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
from collections import Counter, defaultdict
from pathlib import Path

from pipeline.runtiming.runs import read_runs

#: THE CUBE'S SLOTS, which are not quite the species list.
#:
#: Pink splits into its two brood lines because they are not two runs — they are the same
#: run in alternate years, and only one of them happens in any year you can fish. Maxing
#: them together lit 415 profiles with a "second pink run" that never coexists with the
#: first. The map already knows the date, so it knows the parity: `slot_for_year`.
#:
#: Sockeye's river- and lake-type do NOT split: both rear in the same season and a river
#: really can carry both at once, so a maximum over them is the honest colour.
SLOTS = ["Chinook", "Chum", "Coho", "Pink (even)", "Pink (odd)", "Sockeye", "Steelhead"]


def slot_of(run) -> str:
    """Which cube slot a run belongs to."""
    if run.species == "Pink":
        return "Pink (even)" if run.form == "even" else "Pink (odd)"
    return run.species


def slot_for_year(species: str, year: int) -> str:
    """The slot to colour by for a given calendar year — pink alternates."""
    if species == "Pink":
        return "Pink (even)" if year % 2 == 0 else "Pink (odd)"
    return species


def _profiles(sets: dict[str, list[int]]) -> tuple[dict[tuple, int], dict[str, int]]:
    profiles: dict[tuple, int] = {}
    sec_prof: dict[str, int] = {}
    for sid, cus in sets.items():
        sec_prof[sid] = profiles.setdefault(tuple(sorted(set(cus))), len(profiles))
    return profiles, sec_prof


def _cube(order, runs, peak, mk_entry=False):
    """intensity[profile][slot][pentad], 0-255, plus the per-profile run lists."""
    cube = bytearray(len(order) * len(SLOTS) * 73)
    entries, multi = {}, Counter()
    for key, pid in order:
        entry = {}
        for si, sp in enumerate(SLOTS):
            mine = [c for c in key if slot_of(runs[c]) == sp]
            curved = [c for c in mine if c in peak]
            if mine and mk_entry:
                entry[sp] = [{
                    "cuid": c, "name": runs[c].name, "label": runs[c].label,
                    "form": runs[c].form, "peak": runs[c].peak_date,
                    "span": list(runs[c].span) if runs[c].span else None,
                    "quality": runs[c].quality, "quality_word": runs[c].quality_word,
                    "has_curve": c in peak,
                } for c in sorted(mine, key=lambda c: (runs[c].peak_date or "~", c))]
            if len(curved) > 1:
                multi[sp] += 1
            for w in range(73):
                if curved:
                    v = max(runs[c].pentads[w] / peak[c] for c in curved)
                    cube[(pid * len(SLOTS) + si) * 73 + w] = min(255, round(v * 255))
        if mk_entry:
            entries[pid] = entry
    return cube, entries, multi


def main(gen: Path, review: Path | None) -> None:
    runs = read_runs(gen / "runs.json")
    peak = {c: max(r.pentads) or 1.0 for c, r in runs.items() if r.has_curve}

    sets: dict[str, list[int]] = defaultdict(list)
    with open(gen / "section_cu.csv") as f:
        r = csv.reader(f)
        next(r)
        for sid, cuid, _rel in r:
            sets[sid].append(int(cuid))

    # PASSAGE IS A SECOND, INDEPENDENT PROFILE SPACE. A reach can spawn one set of runs and
    # be crossed by a completely different set — the lower Skeena spawns coastal winter
    # steelhead and is crossed by six tributary summer runs — so one profile id cannot carry
    # both, and the two must render differently anyway.
    pass_sets: dict[str, list[int]] = defaultdict(list)
    pkm: dict[str, float] = {}
    pfile = gen / "section_cu_passage.csv"
    if pfile.is_file():
        with open(pfile) as f:
            r = csv.reader(f)
            next(r)
            for sid, cuid, _rel, km in r:
                pass_sets[sid].append(int(cuid))
                pkm[sid] = float(km)

    profiles, sec_prof = _profiles(sets)
    pprofiles, sec_pprof = _profiles(pass_sets)
    order = sorted(profiles.items(), key=lambda kv: kv[1])
    porder = sorted(pprofiles.items(), key=lambda kv: kv[1])
    print(f"sections {len(sec_prof):,}  profiles {len(profiles):,}  | "
          f"passage sections {len(sec_pprof):,}  passage profiles {len(pprofiles):,}", flush=True)

    cube, prof_runs, multi = _cube(order, runs, peak, mk_entry=True)
    pcube, pprof_runs, _ = _cube(porder, runs, peak, mk_entry=True)

    (gen / "run_cube.bin").write_bytes(bytes(cube))
    (gen / "passage_cube.bin").write_bytes(bytes(pcube))
    (gen / "profile_runs.json").write_text(json.dumps(prof_runs, separators=(",", ":")))
    (gen / "passage_profile_runs.json").write_text(json.dumps(pprof_runs, separators=(",", ":")))
    with open(gen / "passage_profile.csv", "w") as f:
        f.write("profile_id,cuid\n")
        for key, pid in porder:
            for c in key:
                f.write(f"{pid},{c}\n")
    with open(gen / "section_passage.csv", "w") as f:
        f.write("section_id,passage_profile_id,km_to_sea\n")
        for sid, pid in sec_pprof.items():
            f.write(f"{sid},{pid},{pkm.get(sid,0.0)}\n")
    with open(gen / "run_profile.csv", "w") as f:
        f.write("profile_id,cuid\n")
        for key, pid in order:
            for c in key:
                f.write(f"{pid},{c}\n")
    with open(gen / "section_profile.csv", "w") as f:
        f.write("section_id,profile_id\n")
        for sid, pid in sec_prof.items():
            f.write(f"{sid},{pid}\n")
    with open(gen / "run_clim.csv", "w") as f:
        f.write("cuid,pentad,pct\n")
        for c, r in runs.items():
            for w, v in enumerate(r.pentads):
                f.write(f"{c},{w},{v}\n")

    gaps = {}
    if review and Path(review).is_file():
        gaps = json.loads(Path(review).read_text()).get("coverage_gaps", {})

    meta = {
        "profiles": len(profiles), "sections": len(sec_prof),
        "cube_bytes": len(cube), "cube_gzip": len(gzip.compress(bytes(cube))),
        "passage_profiles": len(pprofiles), "passage_sections": len(sec_pprof),
        "passage_cube_bytes": len(pcube),
        "passage_cube_gzip": len(gzip.compress(bytes(pcube))),
        "slots": SLOTS,
        "profiles_with_multiple_runs": dict(multi),
        "coverage_gaps": len(gaps),
    }
    (gen / "profiles_meta.json").write_text(json.dumps(meta, indent=1))
    print(f"cube {len(cube)/1024:.1f} KB ({meta['cube_gzip']/1024:.1f} KB gzipped) | "
          f"passage cube {len(pcube)/1024:.1f} KB ({meta['passage_cube_gzip']/1024:.1f} KB)")
    print("profiles carrying MORE THAN ONE run of a species:", dict(multi))


if __name__ == "__main__":
    from pipeline.common.curated import CURATED, GENERATED
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--gen", type=Path, default=GENERATED.runtiming)
    ap.add_argument("--review", type=Path, default=CURATED.runtiming.review)
    a = ap.parse_args()
    main(a.gen, a.review)
