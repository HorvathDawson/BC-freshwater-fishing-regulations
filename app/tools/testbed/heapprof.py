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
    # every frame of every still-held sample's stack, leaf first (depth descending)
    rows = tp.query("""
      SELECT a.callsite_id AS cs, SUM(a.size) AS held
      FROM heap_profile_allocation a GROUP BY a.callsite_id HAVING held > 0
    """).as_pandas_dataframe()
    by_lib, by_fn = Counter(), Counter()
    total = 0
    for cs, held in zip(rows["cs"], rows["held"]):
        total += held
        stack = tp.query(f"""
          SELECT m.name AS lib, f.name AS fn FROM experimental_ancestor_stack_profile_callsite({cs}) c
          JOIN stack_profile_frame f ON f.id = c.frame_id JOIN stack_profile_mapping m ON m.id = f.mapping
          UNION ALL
          SELECT m.name, f.name FROM stack_profile_callsite c JOIN stack_profile_frame f ON f.id = c.frame_id
          JOIN stack_profile_mapping m ON m.id = f.mapping WHERE c.id = {cs}
        """).as_pandas_dataframe()
        # ancestors come root first; the callsite itself is the leaf (last row): walk leaf → root
        lib, fn = "?", "?"
        for l, f in zip(reversed(list(stack["lib"])), reversed(list(stack["fn"]))):
            base = (l or "?").rsplit("/", 1)[-1]
            if not any(a in base for a in ALLOC):
                lib, fn = base, f or "?"
                break
        by_lib[lib] += held
        by_fn[f"{lib} :: {fn}"[:140]] += held
    mb = lambda b: f"{b / 1048576:7.1f} MB"
    print(f"held since attach (sampled): {mb(total)}\n\nby library:")
    for k, v in by_lib.most_common(top):
        print(f"  {mb(v)}  {k}")
    print("\nby function (first frame outside the allocator):")
    for k, v in by_fn.most_common(top):
        print(f"  {mb(v)}  {k}")


if __name__ == "__main__":
    main(sys.argv[1])
