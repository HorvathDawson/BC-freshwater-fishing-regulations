# T1143 — The repo's Contributors list: a write-up of what removing the other entries would cost

Status: OPEN — after 0.11 (owner); a write-up only, nothing is rewritten

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1143 | The repo's Contributors list: a write-up of what removing the other entries would cost | OPEN — after 0.11 (owner); a write-up only, nothing is rewritten | | owner 2026-11-25 (repo-move chat): "it is a future thing to write. yes. like afgter 0.11" |

Source: owner 2026-11-25 about 15:32, in the repo-move chat (relayed). He asked to "drop all contributors except"
his own account. Access is already only his: collaborators, org members, outside collaborators, teams and
invitations were all checked by the repo-move chat after the transfer. What remains is GitHub's Contributors list,
which is built from commit authors: it still shows one bot account (2 commits) and one other account (1 commit).
The repo-move chat offered a write-up of the cost; his answer: "it is a future thing to write. yes. like afgter
0.11". The repo-move chat's reading, and the planning chat's: he wants the write-up after 0.11, not the rewrite.

## The write-up says
1. Which commits carry the other authors (ids, dates, what they changed), and whether a co-author trailer or a
   merge commit is what lists them.
2. The only way to remove them: rewriting `main`'s history from the first such commit, which changes every later
   commit id. What that breaks: every open PR and branch, every chat's worktree, the release tags (`v*`, `data/*`)
   and what the release ledger and R2 manifests record as commit ids, commit ids quoted in tickets and docs
   (thousands), GitHub's PR and review history links, the merge queue, forks and clones.
3. Options that do not rewrite: leave it; a `.mailmap` (changes what git shows locally, not GitHub's list); ask
   GitHub support whether a contributor entry can be hidden.
4. If he still wants the rewrite: the quietest moment (no release in flight, no open PR, right after a release is
   merged back), a rehearsal on a throwaway copy of the repo, a full backup first, and that it is his command to
   run: a force-push to a protected trunk is never an agent's.

Acceptance: one page with the commit list, the cost, the options and a recommendation; the owner decides.
Severity: none. Model: Sonnet. Size: S. Owner: yes (decides; nothing happens without him).
