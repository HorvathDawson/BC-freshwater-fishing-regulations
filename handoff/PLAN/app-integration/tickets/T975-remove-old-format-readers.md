# T975 — Remove the old-format data readers once packed formats are live

Status: DONE — shipped in 0.8.5 (packed readers only) (ee1b47819, 2026-11-21).

Source: owner 2026-11-20 13:37: "Yes add the ticket to clean up dead code please". This came after the main chat
explained the order:
1. 0.8.4 reads both formats while production data stays old (this release).
2. Production data switches to packed once 0.8.4 is live in both stores (T686, `SHIP_PACKED_FORMATS`).
3. The old-format readers then become dead code.

## Work
After production data ships packed, with the contract's oldest supported app at 0.8.4 or later:
- **App:** delete the readers for the formats packed replaced: near/1, down/1, climate/1, paint/5 and any other
  format T686 bumped. Delete their fallbacks, block-cache branches, tests and fixtures. Keep only the packed readers.
- **Pipeline:**
  - Remove the `SHIP_PACKED_FORMATS` switch and the old-format writers in `pipeline/deliver/release/packed.py` and
    the release build.
  - Remove the lowered-gate pairs that only existed for the old formats.
  - Remove the old-format rows in `app_reads.json` and the contract.
- **Checks:**
  - Inventories: update app/tools/failures.test.ts counts for the readers removed.
  - A test that the release writes only packed formats.
  - A grep that no reader for a replaced format remains.
- **Ship:** in the next app version after the switch (the one that stops reading old formats). Raise the contract's
  oldest supported app in the same data release.

## Acceptance
- No code path reads or writes a replaced format.
- The app's tests and the release build pass with packed only.
- The release diff against the last packed release is byte-identical, apart from the removed switch.

Depends: T686 switch-on in production. Size: M. Owner: no.

## Owner 2026-11-21 12:44
Joins the 0.8.5 batch ("Yes, add it"): app/worker readers + the switch, one lane after T686 is flipped and staging data passes smoke; data min app 0.8.4; data ships before the apps.

## Owner 2026-11-21 (answer)
Asked: 0.8.5 without the old readers cannot open offline on a phone still holding unpacked data. Hold to 0.8.6?
Owner: "Everybody is on new versions already (at least 0.8.4) so ship in 0.8.5". Ships in 0.8.5. Strict order kept:
production data (packed) first, then the 0.8.5 apps and the production website.

## Lane (2026-11-21, branch `lane/T975-old-format-readers`)
- **App:** near/1 row reads (`NEAR_PLACE`, `RINGS_FOR_ORDS`, `TOWNS_FOR_ORDS`), down/1 row reads (`DOWN_FROM`, `LEN_OF`,
  `LAKE_OF`, the two has-column probes), the climate/1 codec choice and `*_ALSO_READS` are gone; `source.ts` reads the
  packed forms only. `PAINT_READS` = paint/6 + paint/4 (paint/4 only because the frozen fixture releases carry it; no
  live release is paint/4). The feeds worker reads no release file (its climatology is the `clim.json` feed): nothing
  to change there.
- **Fixtures:** the three frozen fixture releases' `near` (near/1 → near/2) and their conditions' `section_down`
  (down/1 rows → down/2 blocks; `lake` = 1 where the section's water is a lake in that release's core, what the reader
  inferred by kind before) and `gauge_clim` (clim/2 → clim/3) were converted losslessly (every row decoded back); the
  mini release re-cut by `python -m pipeline.tools.mini_release --write`.
- **Pipeline:** `SHIP_PACKED_FORMATS`, `contract.UNPACKED` / `written_files`, `build.UNPACKED_GATES` / `PACKED_GATES` /
  `size_gates()` / `formats()` / `_climate_f16` and paint's version choice are gone: `build.FORMATS` is the contract,
  `SIZE_GATES` holds the packed gates, `paint.encode` writes paint/6 only.
- **app_reads.json:** only the 0.8.5 row re-emitted (`--replace`: no 0.8.5 build had shipped): near/2, down/2,
  climate/2, paint/6 + paint/4.
- **Checks:** `test_t975_a_release_writes_only_the_packed_formats` (the committed mini release, cut by the real
  pipeline), `test_t975_no_reader_or_writer_for_a_replaced_format_remains` (the grep guard over pipeline/deliver,
  pipeline/tools/emit, scripts, app/packages, app/apps/mobile/src, app/workers/feeds/src; mutation-checked),
  `manifest.readable.test.ts` (an old release is refused by `readsRelease`).
- **An app holding an older release:** an app from this tree given a pre-0.8.5 release (near/1 …) refuses it at
  `readsRelease` ("release r-… is not one this app reads: near is near/1, not near/2 …"): its files are never opened. Shipping order (owner): data 0.8.5 (packed, min app 0.8.4) before the apps.
  Exactly (boot.ts T403/T845): a phone updated to this app while holding an unpacked release installs the server's set
  over it before the map opens; offline it cannot start (no map until it is online once); if the server's release is
  still unpacked it stops on `INSTALL_WORDS.unreadable` ("This version of the app can't read the data on the server
  yet…"). The web site built from this tree refuses an unpacked LATEST the same way: deploy it only after the packed
  data is live.
