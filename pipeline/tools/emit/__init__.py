"""The GENERATORS (AGENTS 40): a value on both sides of the Python/TypeScript boundary is written from its one Python
source, never typed twice.

    python -m pipeline.tools.emit            # write every generated file
    python -m pipeline.tools.emit --check    # fail if any is stale or hand-edited (CI, pnpm check, test-full.sh)

    gauge_policy   app/packages/core/src/gauge-policy.generated.{ts,json}   (the gauge consume/feed policy)
    calendar       app/packages/core/src/calendar.generated.{ts,json} + the reference page's JS (`calendar_spec`)
    pack           app/packages/data/src/pack.generated.ts                  (the release contract)
    answers model  the answers schema + TypeScript (`pipeline.deliver.answers.model`, which owns its own emit)
    feed_formats   app/packages/core/src/feedFormats.generated.ts           (every live feed's format string, T556)
    dfo_vectors    app/workers/feeds/test/fixtures/dfo/python-vectors.generated.json  (what the Python reads on the saved DFO
                   pages — the feeds worker's test reproduces it, T556)

Each still runs alone: `python -m pipeline.tools.emit.<name> [--check]`.
"""
