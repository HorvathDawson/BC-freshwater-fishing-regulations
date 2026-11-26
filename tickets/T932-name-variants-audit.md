# T932 — Audit the name variants file (`data/curated/waters/name_variants.json`)

Status: POSTPONED (deferred with T930, owner 2026-11-20: after the bathymetry map, stocked lakes and charts; T200/T201/T634).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T932 | Audit name_variants.json: joined multi-lake lines, wrong targets, twins, book-print forms (owner) | POSTPONED (deferred with T930: after bathy map, stocked lakes, charts) | | owner 2026-11-20 "audit the name variants" |

Source: owner 2026-11-20 00:03 (main chat), verbatim: "Add another ticket alongside the indigenous name one to audit the
name variants." Context: T926 makes search find every entry as the file stands (owner: "use the name variants file as it
stands … less hacky"); this ticket checks what the file itself holds.

## What is there (2026-11-20)
3,708 targets; names by source: stocking 3,361, bathymetry 3,102, marker 749, regulation 465, others. 290 generic numbered
labels ("LAKE #1013", 288 stocking) stay as search-only names (owner, T926). 483 other names carry a digit, including
joined stocking lines that are several lakes in one string ("1. SKOOKUM L.  2. MOWICH L.  3. DEADMANS L.",
"(1) UPPER QUINSAM L.  (2) WOKAS LAKE", "JADE L. & NO.1 & 3").

## Work (read-only report first; every change is a proposal, ask-before-filtering rule)
1. Each name finds the water it names: a joined line on one target names other lakes too (split it, or point each part
   at its own water); a name pointing at the wrong water (compare against the FWA/GNIS name and the stocking coordinate).
2. Duplicates by search key on one target (case/punctuation twins such as "Lake 5565" / "LAKE 5565").
3. One name on several unrelated targets (which one is right, or both).
4. Book-print forms ("CAMPBELL L.", trailing "*") kept or tidied: owner call per class, shown with examples.
5. `display` flags: every displayed variant is a real name with a source.
Output: a report (counts + examples per class) and an owner page of proposed changes; nothing removed or renamed unasked.
Depends: T926 (search reads the file as is). Size: M. Owner: yes (the proposals).
