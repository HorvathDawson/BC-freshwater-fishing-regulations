"""Old path vs new at settle: every rendered section's standing after each pan and each change (`--dump` files from
conditions_measure.py, same place, same steps). `python conditions_compare.py <before.json> <after.json>`.

Counts, over the sections both runs rendered at the same step: equal values (to 1e-9), different values, coloured only
before, coloured only after — and lists the first differences by source-layer."""
import json, sys
from collections import Counter

a, b = (json.load(open(p)) for p in sys.argv[1:3])
tot = Counter()
diffs = []
for step in a:
    if step not in b:
        tot["steps missing after"] += 1
        continue
    for k, va in a[step].items():
        if k not in b[step]:
            continue
        vb = b[step][k]
        layer = k.split(":")[0]
        if va is None and vb is None:
            tot["both none"] += 1
        elif va is None:
            tot[f"only after ({layer})"] += 1
            diffs.append((step, k, va, vb))
        elif vb is None:
            tot[f"only before ({layer})"] += 1
            diffs.append((step, k, va, vb))
        elif abs(va - vb) <= 1e-9:
            tot["equal"] += 1
        else:
            tot[f"different ({layer})"] += 1
            diffs.append((step, k, va, vb))
print(dict(tot))
for d in diffs[:20]:
    print("  ", d)
