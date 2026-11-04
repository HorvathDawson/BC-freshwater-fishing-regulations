# T385 — Share a water: a button that sends the website link

Status: OPEN (owner 2026-11-04: "in the app we should have a share button on a river that sends the website link", in 0.5.0).

A Share control on a water's page (the header, beside the day): the phone's share sheet (React Native `Share.share`;
web `navigator.share`, else copy) with the water's public web link — the same address a reload opens (T369: water +
view + place, the day when not today), on the production web origin. A dismissed share sheet is not a failure (T368).
Unnamed reaches share their place link (`?blk=&m=`, T386 makes it open everywhere). Tests: the link built from the
frame is the start link the web parses back to the same frame (round trip). Size: S.
