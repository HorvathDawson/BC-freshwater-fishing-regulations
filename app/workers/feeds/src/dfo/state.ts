// (T905, owner 2026-11-24 "Move worker state out") THE KEYS OF A REGION'S OWN BOOKKEEPING, generated from
// pipeline/common/feed_formats.py DFO_STATE_KEYS, in a file that imports only that generated table: push.ts reads them
// too, and the test bed's crash stub runs push.ts under Node's type stripping, which the DFO reader's import graph does
// not pass (a parameter property in health.ts).
/**
 * What only the DFO run reads back — the page hash and when it last moved, the fetches that failed in a row, the
 * header's digest, the header names no water binds, the page's general text. In priv `state.json`, never in the
 * published index (they were ~11 kB of it); no app reads one (core salmonFeed.ts `SalmonFeedRegion`). live.py's
 * STATE_KEYS is the same table (the vectors' `live.state_keys` check it too).
 */
export { DFO_STATE_KEYS as STATE_KEYS } from "../../../../packages/core/src/feedFormats.generated";
