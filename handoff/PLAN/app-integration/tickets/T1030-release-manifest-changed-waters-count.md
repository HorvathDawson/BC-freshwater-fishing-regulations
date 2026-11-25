# T1030 — The release manifest carries a changed-waters count (the inbox's "N waters changed")

Status: DONE — f75e6aac1, 35f55b74d, 7287c181a, c2c1e832c, d52a04440 (0.9.1; needs the 0.9.1 data release + app build; the first release carrying the list states no count)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1030 | The release manifest carries a changed-waters count (the inbox data row's "N waters changed") | DONE | f75e6aac1, 35f55b74d, 7287c181a, c2c1e832c, d52a04440 | (T867) per-water digest list per release; count vs LATEST, said only on a device that held it (changed_waters_from); wording not counted; owner 11-24 "Rules only, hide 0": 0 says nothing (core drops it, manifest keeps 0) |

T867 (PR feat/T867-inbox) shows a data-release row in the notifications inbox: "Regulations data updated <day>", and
"N waters changed" once the manifest says it. Nothing in the manifest says it today, so the row omits it (rule 32); the
slot is `InboxTrack.release.changed` in `@app/ui` `useInSeasonInbox` (Shell reads it from the manifest).

Wanted: `manifest.changed_waters` (integer, absent when unknown), the number of waters whose resolved rules differ from the
previous release; app reads it (pack.generated `Manifest`), Shell passes it as `release.changed`.

Why not small (stopped, 2026-11-23, lane L19): (1) the previous release's per-water resolved rules must be available at
build time, and old releases are pruned to live + previous, so the diff needs a stored per-water digest list (the bundle's
`entry_digests` are per ENTRY, not per water); (2) `release_id` is content-hashed (`rebase.id_of`), so a new manifest field
must be excluded from or deliberately part of the id; (3) proving it needs a release build (heavy, ~25 min) and the
manifest/pack.generated/format gates (`feed-formats`, `pack`). Decide the digest source first.

## Built (lane L7, 0.9.1 overnight, 2026-11-24)

**The digest source.** Every release writes its own per-water list, `<release>/water-digests.json.gz`
(`water-digests/1`, gzip mtime 0, canonical JSON), named in its manifest as `water_digests` {url, bytes, gz, sha256,
format, waters} and held to its sha256 when read. Keyed on `item_id` (rule 5), read from the SHIPPED files only
(`answers_diff.Release`, the changed-answers list's reference decoder), so it is a pure function of the release's files.
The next build counts against the list of the release it replaces: `deliver all` passes `--previous <the release LATEST
names>` (what `pipeline release`'s binding diff compares against too). Code: `pipeline/deliver/release/water_digests.py`.

**What "changed" counts** (the owner may want to say otherwise): a water the app shows counts when, on any day, what an
angler may do or must hold differs — any of its parts' regs answer (the ladder, rows, gear, licence, display frames and
the trout table, with rule and licensing refs read as ids, never indexes), its DFO salmon chains, the place a part
splits (its end tokens), closed-all-year, the rules asking for a paper licence, its unresolved licensing — or the
water is new or gone. **Wording alone does not count** (a key the changed-answers list calls "wording only",
`TEXT_KEYS`, where it holds words: a string or a list of strings — review 2026-11-24: the licence frame's
`requirements[].when` / `documents[].when` are rule data {act, species, lengths, on} and are kept), nor geometry (a part's length, which sections it holds). Measured on two real pairs, the count equals the
set of waters `answers_diff` lists outside "wording only", exactly: 0.8.5 -> 0.9.0 = 2 (Kitimat, Nakina; with wording
it would have been 8,247: the "rule book" -> "B.C. Synopsis" sweep), r-fec3fab4c5aa -> 0.8.5 = 315 (with rule indexes
read raw it was 19,765: 0.8.5 added rules, every index moved — the paper-licence rules and the trout cell's `src`).

**No count, never 0**: no previous release named, the previous folder gone, a previous release built before this
(**the first release carrying the list — 0.9.1 if 0.9.0 is live — states no count; the one after it does**), or the
two lists of another basis (the regs codec versions, the salmon format, the list's own `SCHEME`). The build log says
which. A rebuild of the release LATEST already names keeps that release's own count and base; a rebase (basemap only)
carries the list and states 0 from its source.

**The count names its base** (review 2026-11-24): `changed_waters_from` is the release the count was taken against
(the local LATEST, which is not always what a phone held: today LATEST is 0.9.0 while PUBLISHED is 0.8.5; a
staging-only release in between; a plain `pipeline build` moves LATEST; a phone that skipped a release). The app says
the count ONLY when that is the release this device held before (`inboxObserve`: `changedFrom === base.release`);
otherwise the row says nothing of it (rule 32). Additive and outside the id, like the count.

**Not in the release id.** `water_digests` and `changed_waters` are written after `ship` mints the id and `rebase.said_of`
reads neither: the count depends on the predecessor, so the same content cut after two different releases is one id;
the list is a function of the files the id already covers. Pinned by `test_neither_the_list_nor_the_count_is_part_of_the_release_id`.

**Formats**: additive. No file format moves (`app_reads` unchanged; the list is not in `files`). An installed 0.9.0 app's
`parseManifest` ignores unknown keys; a 0.9.1 app reads `changed_waters?: number` (pack.generated `Manifest`) and refuses
a value that is not a count at the boundary. Shell passes it as `release.changed`; core's `INBOX_WORDS.changed` (T867,
pinned whole, singular/plural) says it; absent says nothing.

**Proof.** Unit tests `pipeline/tests/test_water_digests.py` (18; each guard mutation-killed), the app's
`release.test.ts` (the boundary) and `InboxRelease.shell.test.tsx` (the real Shell: "12 waters changed", "1 water
changed", absent -> no text). One release step on the live 0.9.0 bundle into a scratch root (no basemap, no book, no
tiles), `--previous` a scratch copy of 0.8.5 given its list: `changed_waters: 2 of 19766 waters differ from
r-741815c6090d`; its list is byte-for-byte the one read back from 0.9.0's shipped files (14 s, ~1.2 GB, 108 kB gzip).
The committed mini release was re-cut (same id; `--check` matches a rebuild).

## Owner question (answered 2026-11-24)

With wording left out, 0.9.0's sweep ("rule book" -> "B.C. Synopsis" on 8,247 waters' salmon cards) would have read
"2 waters changed" (Kitimat, Nakina: the only waters whose rules moved); a release changing nothing but words reads
"0 waters changed" (the T867 sentence prints 0 as a number). Options:
  A. count wording too ("8,247 waters changed" for 0.9.0) — every word an angler reads, but a code wording sweep looks
     like a regulations change;
  B. keep rules-only (built now) — the number says where the rules moved; a 0 reads "0 waters changed";
  C. rules-only, and say nothing when it is 0 (the row keeps "Regulations data updated <day>").
Recommended default: B or C (C if "0 waters changed" reads odd on a data release).

**Owner's answer (2026-11-24, verbatim): "Rules only, hide 0 (Recommended)"** — option C.

## Built (lane L17, 0.9.1, 2026-11-24): d52a04440

The count stays rules-only. When it is 0 the inbox row says NO count, as when the release states none: core's
`INBOX_WORDS.changed(n)` composes absent (null) and 0 as `""`, 1 as "1 water changed", N as "N waters changed", and
`inboxVM` uses it for every data row. **Where 0 is dropped: the app's composer, not Python.** The manifest keeps stating
`changed_waters: 0` (a true answer the release computed; "we asked and there are none" is a different claim from
"we have not asked", rule 32), and the saved inbox history keeps the 0 too; only the words leave it out. Proof:
`inseasonInbox.test.ts` pins the whole `text` and `label` for absent / 0 / 1 / 37 and the four sentences;
`InboxRelease.shell.test.tsx` (the real Shell) has a release stating 0 -> history 0, text "". Both fail on main ("0
waters changed"); the mutation dropping the 0 guard is killed by both.
