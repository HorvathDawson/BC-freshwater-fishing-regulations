# T1160 — Agents push with their own GitHub identity; production approval cannot be given by an agent; CI secrets and actions tightened

Status: OPEN — before the ship workflow (T1133) can make production an approval click; the security review's findings join it

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1160 | Agents push with their own GitHub identity; production approval cannot be given by an agent; CI secrets and actions tightened | OPEN — before the ship workflow (T1133) can make production an approval click; the security review's findings join it | — | owner creates the identity (steps handed over); a build lane: environments, secrets moved, actions pinned by SHA, a workflow lint; the security review of 2026-11-25 night adds its list; M |

Source: the ship-workflow design pass of 2026-11-25 night (notes: `tickets/archive/night-notes-2026-11-25.tar.gz` `ship-workflow-design.md`):
agent sessions use the owner's `gh` login, and GitHub accepts any `repo`-scoped token of a required reviewer, so
with a production Environment an agent could approve a deployment through the API: an approval click would be no
stronger than CONFIRM_PROD is today. Also found: no GitHub environments exist; the R2 keys and an unused
`CLOUDFLARE_API_TOKEN` (permissions unknown) are repo-wide secrets; actions are pinned by tag, not commit; there is
no tag ruleset. Related: T1133, T1144, T1143 (a trunk rewrite is never an agent's), the memory note
`confirm-prod-owner-only`. A read-only security review of the public repo is running the same night; its
findings are added here or get their own tickets.

## Work
1. Agents push and open PRs as their own GitHub identity (a GitHub App or a machine account that is never a
   required reviewer); the owner's token leaves agent sessions; then "Prevent self-review" on the production
   environment. Until then production stays behind the typed CONFIRM_PROD.
2. Delete the unused repo secret `CLOUDFLARE_API_TOKEN` and revoke it in Cloudflare; move `R2_ACCESS_KEY_ID` /
   `R2_SECRET_ACCESS_KEY` from the repo level into an environment; Cloudflare tokens limited to the named Workers.
3. Every action pinned by commit SHA, Dependabot for `github-actions` to bump the pins, the "require SHA pinning"
   policy on; a workflow lint (zizmor or actionlint) that refuses `pull_request_target`; a tag ruleset for `v*`.
4. Dependabot security updates and CodeQL back on (both were turned off for the repo move).

## Acceptance
A token held by an agent session cannot approve a production deployment (tried in a dry environment); no secret
is repo-wide that only one environment needs; the lint fails a workflow with an unpinned action (mutation).
Severity: high for the release path. Model: Opus. Size: M. Owner: the identity, the tokens, the switches.

## Owner

- [ ] 2026-11-25 — Create the agents' GitHub identity and the scoped tokens when the steps are handed to you; turn Dependabot security updates and CodeQL back on (repo and org switches). Default until then: nothing changes and production stays behind CONFIRM_PROD.
