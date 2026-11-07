# Reading a phone's system trace (T420)
The owner records with Android's on-device System Tracing (Developer options → System Tracing, default categories,
Quick Settings tile; no USB). Read it with Perfetto's trace processor: `python3 -m venv /tmp/pf && /tmp/pf/bin/pip install
perfetto pandas numpy`, then `/tmp/pf/bin/python q1.py <trace>` (app threads' CPU, CPUs, frame jank), `q2.py` (one thread's
states, slices, per-second CPU — edit its `tid`, from q1's output), `q3.py` (CPU/GPU frequency, GPU counters).
Traces live in `app/tools/testbed/out/traces/` (git-ignored).
