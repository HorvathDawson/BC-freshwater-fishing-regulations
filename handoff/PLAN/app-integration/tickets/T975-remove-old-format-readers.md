# T975 — Remove the old-format data readers once packed formats are live

Status: OPEN — waits on T686's switch-on (packed formats in production, oldest supported app = one that reads them).

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
