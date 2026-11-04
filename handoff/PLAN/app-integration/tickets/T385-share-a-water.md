# T385 — Share a water: a button that sends the website link

Status: DONE (owner 2026-11-04: "in the app we should have a share button on a river that sends the website link", in 0.5.0) — a4873c54e.

## Done (a4873c54e)
- Share sits in the water header's back row, beside × (every view: Regulations, Conditions, Map), Lucide `share-2`.
- `@app/ui` `useShare` / `shareLink`: `linkSearch` (the address bar's function) over the route alone + day + horizon,
  on `@app/core` `SITE_ORIGIN` (https://canifishthis.ca); made ahead so a browser's sheet opens inside the tap. Words:
  core `SHARE_WORDS` ("Link copied", failed, none).
- Ports (`apps/mobile` `share.native.ts` RN `Share`; `share.web.ts` `navigator.share`, else clipboard). Dismissed (or
  AbortError) → nothing said or reported; any other failure said in the row and reported.
- Tests: `share.test.ts` (round trip through `parseStartLink` / `resolveStartLink` incl. an unnamed reach's place; the
  press outcomes), `Faces.share.test.tsx`. Note: the link is `/?regs=…`, not the web's `/water/<slug>` path.

A Share control on a water's page (the header, beside the day): the phone's share sheet (React Native `Share.share`;
web `navigator.share`, else copy) with the water's public web link — the same address a reload opens (T369: water +
view + place, the day when not today), on the production web origin. A dismissed share sheet is not a failure (T368).
Unnamed reaches share their place link (`?blk=&m=`, T386 makes it open everywhere). Tests: the link built from the
frame is the start link the web parses back to the same frame (round trip). Size: S.

## Owner 2026-11-04: web and app
App: the phone's share sheet. Web: `navigator.share` where the browser has it (phone browsers), else the link copied
with "Link copied". The same link either way.
