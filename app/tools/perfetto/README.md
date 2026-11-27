# Reading a phone's system trace (T420)
The owner records with Android's on-device System Tracing (Developer options → System Tracing, default categories,
Quick Settings tile; no USB). Read it with Perfetto's trace processor: `python3 -m venv /tmp/pf && /tmp/pf/bin/pip install
perfetto pandas numpy`, then `/tmp/pf/bin/python pinch.py <run dir>` (the render thread's CPU share for the `zoom-pinch` scenario; `--perf`, `--threads`).
The older one-off queries (q1-q3, T420) are in git history.
Traces live in `app/tools/testbed/out/traces/` (git-ignored).
