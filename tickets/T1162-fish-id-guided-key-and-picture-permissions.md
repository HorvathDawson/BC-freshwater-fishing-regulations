# T1162 — Fish ID: a guided key on the existing picture guide (no photo recognition now); each picture's source and permission recorded

Status: OPEN — a later release; the permissions record is small and first

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1162 | Fish ID: a guided key on the existing picture guide (no photo recognition now); each picture's source and permission recorded | OPEN — a later release; the permissions record is small and first | — | first: record each of the 41 pictures' source and permission in NOTICE (owner supplies what he holds); then a guided key with pictures, ending "not sure, see the rule" on look-alikes; pictures to the owner first |

Source: the photo-fish-ID research of 2026-11-25 night (notes: `tickets/archive/night-notes-2026-11-25.tar.gz` `photo-fish-id.md`), asked for
because a competitor advertises "an offline photo fish ID". Owner, 2026-11-25: "we have id with pictures and words
desacribing how to id."
- What ships: a picture-and-words guide. `app/apps/mobile/assets/fish/manifest.json` holds 41 pictures (1.9 MB)
  for about 27 fish codes (WDFW 13, GoFishBC 8, USFWS paintings and public-domain plates); salmon and steelhead
  have ocean and spawning looks; tips cover 26 codes (`FISH_TIPS`, `SALMON_TIPS` in
  `core/src/regs/sheets/salmonTips.ts`); a drawing sheet for the adipose fin (`sheets.ts:147`). The only licence
  statement is the manifest line "owner holds permission"; NOTICE lists no fish pictures.
- No ready photo model fits: Fishial's code is MIT but its weights and data have no stated licence (93% over 866
  classes); FishNet has no stated data licence; iNaturalist / Seek weights have no confirmed reuse terms. Bull
  trout and Dolly Varden are not separable by one external feature; no photo-only accuracy study was found for
  that pair or for wild against hatchery by the adipose fin. The competitor's claim is confirmed only by an
  aggregator summary. Model sizes and speeds in the notes are guesses.

## Work
1. NOTICE (and the Sources page) record each picture's source and the permission held.
2. A guided key built on the guide: a few questions with pictures (adipose fin clipped? spots on the tail? mouth
   colour?) that end on a species or, for look-alike pairs, on "not sure: see the rule for this water".
3. A photo model only later, as a hybrid (the model proposes three, the key confirms), and only if a held-out
   look-alike test justifies it; a photo never leaves the phone.

## Acceptance
Every shipped fish picture has a recorded source and permission; the key never names a species on a pair it
cannot separate (a test per pair). Severity: feature. Model: Sonnet. Size: S (1), M (2). Owner: the permissions,
the look.

## Owner

- [x] 2026-11-25 — Picks (defaults first): photo ID not now, the guided key first / key then hybrid later / a model now; record each picture's permission in NOTICE: yes / no (and tell me what permission you hold for the WDFW and GoFishBC pictures); bull trout against Dolly Varden: the key ends on "cannot tell, see the rule" / names a species; a biologist reads the key: yes / you alone; photos never leave the phone: yes / with a notice. → photo ID: "recommended but ticket for future model explorration" (2026-11-26 00:50); the pictures: "dont worry about this ... just list source location files" (2026-11-26 01:20: no permission chase; the notices list each picture's source from the manifest, T1100)
