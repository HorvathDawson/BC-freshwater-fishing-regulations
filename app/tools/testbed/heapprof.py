"""(T360) What a heapprofd trace says is still held: by library (the first frame outside the allocator), and the top
functions in it. Needs the `perfetto` package (it fetches its own trace_processor on first use):

    python -m venv /tmp/pf && /tmp/pf/bin/pip install perfetto
    /tmp/pf/bin/python app/tools/testbed/heapprof.py app/tools/testbed/out/runs/<run>/heapprof-conditions.pftrace
"""
import sys
from collections import Counter

from perfetto.trace_processor import TraceProcessor

ALLOC = ("libc.so", "libc++", "heapprofd", "libheapprofd", "malloc", "libdl.so", "libutils.so")


def main(path: str, top: int = 25) -> None:
    tp = TraceProcessor(trace=path)
    q = lambda sql: list(tp.query(sql))
    mapping = {r.id: (r.name or "?").rsplit("/", 1)[-1] for r in q("SELECT id, name FROM stack_profile_mapping")}
    frame = {r.id: (mapping.get(r.mapping, "?"), r.name or "?") for r in q("SELECT id, name, mapping FROM stack_profile_frame")}
    site = {r.id: (r.parent_id, r.frame_id) for r in q("SELECT id, parent_id, frame_id FROM stack_profile_callsite")}
    held = q("SELECT callsite_id AS cs, SUM(size) AS held FROM heap_profile_allocation GROUP BY callsite_id HAVING held > 0")
    by_lib, by_fn, by_path = Counter(), Counter(), Counter()
    total = 0
    for r in held:
        total += r.held
        # leaf → root: the first frame outside the allocator names it; the first two app-side frames give its path
        chain, cs = [], r.cs
        while cs is not None:
            parent, fid = site[cs]
            chain.append(frame[fid])
            cs = parent
        own = [f for f in chain if not any(a in f[0] for a in ALLOC)] or [("?", "?")]
        by_lib[own[0][0]] += r.held
        by_fn[f"{own[0][0]} :: {own[0][1]}"[:150]] += r.held
        by_path[" <- ".join(f"{l}:{n}"[:70] for l, n in own[:4])] += r.held
    mb = lambda b: f"{b / 1048576:7.1f} MB"
    print(f"held since attach (sampled): {mb(total)}\n\nby library:")
    for k, v in by_lib.most_common(top):
        print(f"  {mb(v)}  {k}")
    print("\nby function (first frame outside the allocator):")
    for k, v in by_fn.most_common(top):
        print(f"  {mb(v)}  {k}")
    print("\nby path (four frames, leaf first):")
    for k, v in by_path.most_common(15):
        print(f"  {mb(v)}  {k}")


if __name__ == "__main__":
    main(sys.argv[1])
