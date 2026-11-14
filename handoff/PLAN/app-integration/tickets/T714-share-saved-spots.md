# T714 — Share a saved spot, and a group of spots

Status: OPEN (priority for the owner to place; not in the 0.7.8 build; the store text does not promise it)

Source: owner 2026-11-13, on the store description: "we should add a ticket to share saved fishing spots tho and maybe
every a group of spots". Today Share sends a water link only (water, view, place, day); a spot, its notes and photos
never leave the phone (`handoff/PLAN/play/listing.md`, `handoff/PLAN/play/data-safety.md`).

## Goal
1. Share one saved spot: the friend opens the link and gets the spot (place, name, parking point, the water it is on)
   and can save it to their own spots.
2. Share a group of spots in one link (pick several, or "all spots on this water").

## Decisions for the owner (ask by dialog before building)
- What travels: place and name only; plus the note; plus visit log; plus photos.
- How it travels:
  a. Everything inside the link itself (no server keeps anything; works for place, name, short note; a group makes a
     long link; photos cannot go this way).
  b. A file the phone's share sheet sends (e.g. one spots file a friend opens in the app; can carry photos; nothing on
     our server).
  c. Stored on our server behind a link (needed for photos by link; changes the privacy answers: a spot's exact place
     and photos would be kept by us; needs retention, deletion and abuse rules; self-hosted only).
- Exact place or rounded: a shared spot is useless rounded, so sharing is an explicit act with the exact place shown
  before sending.
- Receiving on the web (canifishthis.ca) as well as the phone, or phone only.

## Constraints
- No account, no silent upload: nothing leaves the phone until the reader presses Share and sees what is sent.
- Self-host everything (owner 2026-10-29); no third-party storage.
- If anything is kept on a server, `data-safety.md`, the privacy policy and the store text change with it.
- One load path: a spot opened from a link lands on the same spot page as one opened from the list.
- The store description may say "share saved spots" only once this ships.

## Acceptance
- A spot shared from phone A opens on phone B with the same place, name and (if chosen) note, and can be saved.
- A group link carries every picked spot; a too-long link is refused with words, never cut silently.
- A bad or tampered link shows an error and saves nothing (reported with reportFailure).
- Tests for encode/decode round trip, size limit and the bad-link case; an emulator flow for share and open.
