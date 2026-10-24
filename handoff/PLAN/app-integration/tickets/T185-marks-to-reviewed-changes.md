# T185 — From review marks to catalogue changes: proposals the owner accepts, never a tool writing files

Status: OPEN. Owner 2026-10-24: "we want to avoid making the review curation tool update files directly anyways. must
be a better process to get the manual results in to entries etc."

## The process
1. Review (T184 artifact, anyone): writes MARKS only.
2. Tickets: `python -m pipeline.regs.review.tickets` turns every wrong / unsure mark into a ticket (exists today).
3. Proposals (Claude): per ticket, a concrete edit to the curated entry / split (data/curated/...), applied on a side
   branch; side rebuild → binding diff + card gate → a proposal record: the ticket's note, entry before/after, map
   effect, each rule's bound-length delta.
4. Change review (artifact, like the braid page): one card per proposal, accept / reject / note → read back.
5. Apply: accepted proposals land as one commit per ticket (`git commit -- <paths>`), then the normal rebuild; the
   changed target re-enters the queue as a "verify" check (its fingerprint moved, T97).
- The local curation tool's FIX MODE loses its writers (entry/split edit, rename, /api/check-driven writes): the screen
  becomes read-only everywhere; a pin test that no curation-review route writes under data/curated.

Depends: T184 (marks from the artifact). Size: M (proposal builder + change-review page + retiring the writers).
