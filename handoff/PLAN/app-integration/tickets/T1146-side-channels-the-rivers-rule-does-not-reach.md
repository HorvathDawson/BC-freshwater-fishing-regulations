# T1146 — Side channels with no main stem that the river's own rule does not reach become nameless parts

Status: OPEN — 0.10.0 by the owner's pick; a read-only finding first (one cause or 15 cases)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1146 | Side channels with no main stem that the river's own rule does not reach become nameless parts | OPEN — 0.10.0 by the owner's pick; a read-only finding first (one cause or 15 cases) | | owner 2026-11-25: "Fix the binding in 0.10.0"; 0.10.0 chat |

Source: the 0.10.0 chat's read-only investigation for T1140, 2026-11-25, on the candidate r-71d09ad82e81. Of 1,476 parts
over the 478 multi-part waters, 26 have no name and show the bare water name; all 26 are side channels with no
main stem. 15 of them exist only because the river's own or regional rule does not reach the channel (Seymour p3,
Skeena p4, Manson and 12 more); 6 because a Strathcona Park rule does not reach the channel (Burman, Bedwell,
Thelwood and 3 more; Thelwood lies inside Strathcona - Westmin Park). If the binding is wrong, the fix makes the
part take its river's rule and merge into its neighbour, and it needs no name. Related: T1140, the memory notes
`receiving-river-not-a-tributary`, `split-fix-changes-trib-walk`, `reach-only-rerun`.

Owner, 2026-11-25 (planning chat dialog), to "name them now and fix the binding next patch (recommended) / fix the
binding in 0.10.0": **"Fix the binding in 0.10.0"**.

Work:
1. FIRST, read-only: is this one mechanical cause in the reach walk (a side channel without a main stem does not
   take its river's water-level rule), or separate curation cases? Are the 6 Strathcona parts the same fault?
2. If mechanical: fix in code with a test that fails without it (mutation), rerun the reach step, and diff every
   rule binding against the candidate. Expected: only those channels gain their river's rule and merge away.
   Anything else stops the release and comes to the planning chat.
3. If it needs the owner's marks case by case: stop; the cases go to the owner. No agent writes a mark.
4. Parts that still have no real-world name afterwards (Rees p0, Comox p0, ...) are T1140's question to the owner.

Acceptance: the count of bare-name parts before and after, by cause; the binding diff attributed line by line;
the tiles and bundle of one vintage (rule: tiles and bundle share a vintage). Severity: high (a part with the
wrong rule set, the Squamish case's family). Model: Opus. Size: S if mechanical, otherwise unknown. Owner: the
marks if step 3.
