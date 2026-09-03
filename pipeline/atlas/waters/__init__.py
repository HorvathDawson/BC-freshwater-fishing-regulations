"""Waters the FWA does not have, and the tools that mint them.

WHY THIS IS NOT `hack/`. It was, and the name was wrong twice over: these are not
throwaway scripts, and their output is load-bearing. `added_streams` mints 361 municipal
streams whose negative `blk` ids are a live ABI — splits and name variants are bound to
them, and reminting strands every one. `added_lakes` does the same for polygons the
gazetteer never named, with negative `wbk`/`gnis` ids.

They run rarely — a municipality publishes a new layer, or a bug is found — which is what
made them look like one-offs. What they actually are is the third kind of builder: run
seldom, output curated forever. See `data/curated/README.md`.
"""
