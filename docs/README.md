# docs: what is here

A map of this folder for a person. The pages in `agents/` are written for the Claude chats that work in this repo; you can
skip them. Tickets are in `../tickets/` (`../tickets/README.md`). Rules for working here: `../AGENTS.md`.

## Run it and release it
| page | what |
|---|---|
| `runbooks/fresh-machine.md` | set up and build from a fresh clone: tests, the web app, a data build, the phone builds |
| `runbooks/release-by-hand.md` | a release yourself, start to finish, in order (the page for a person) |
| `runbooks/release.md` | the release command's flags |
| `runbooks/release-gates-explained.md` | why a release stopped: every gate, in plain words |
| `runbooks/annual-synopsis.md` | a new synopsis book: from the PDF to a staging release |
| `runbooks/secrets.md` | what the secrets are, where they live, how to replace them (names and places only) |
| `release/PRODUCTION.md` | production releases: the site, the data bucket, the feeds worker, the Play app |

## How it works
| page | what |
|---|---|
| `data-flow.md` | every data stream from the sources and the owner's decisions to the app |
| `design/trout-table-contract.md` | the trout table's contract |
| `../pipeline/docs/MAP.md`, `../app/MAP.md` | maps of the pipeline and of the app as they are now |
| `../pipeline/docs/` | the pipeline's own pages: how regulations are stored, live data flow, gotchas, the reach builder |

## Decisions and rulings
| page | what |
|---|---|
| `../pipeline/docs/RULINGS.md` | every owner ruling on how the book is read; it wins over any other note |
| `OWNER-RULINGS.md` | the owner's standing rulings, in his words |
| `app-integration/decisions.md` | the decision log, by subject |
| `app-integration/README.md` | the folder's guide; most of it is ticket pictures (`T*-shots/`) and working files |
| `COMPETITORS.md` | other apps and sites that show BC fishing waters, rules or maps |

## For agent chats: `agents/` (skip it unless you run a chat)
| page | what |
|---|---|
| `agents/START-HERE.md` | a new chat's first page: what to read, in order (`START-HERE.md` here is a one-line pointer) |
| `agents/restart/*.md`, `agents/RESTART-*.md` | one short restart page per live chat |
| `agents/ROADMAP-after-0.9.0.md` | the chats in order, and their prompts |
| `agents/STATUS.md` | where things stand: the release state, newest entry first |
| `agents/INBOX.md` | the owner's notes for Claude to turn into tickets |
| `agents/OWNER-PHONE.md` | the phone checks the owner can run |
| `agents/START-HERE-ios.md`, `agents/RESTART-ios-release-chat.md` | the iOS chat's start and hand-over pages |

## Stores and the sites
| folder | what |
|---|---|
| `play/`, `play-production-access/` | the Google Play listing and data safety; the closed-test record for production access |
| `ios/` | the App Store submission page and listing words (`ios/store/`); the iOS chat's pages are in `agents/` |
| `seo/` | what is built for search and how to measure it; outreach emails |

## Research and records
| folder | what |
|---|---|
| `research/` | feasibility notes (stocking, bathymetry), hard cases in the book, the satellite comparison |
| `repo-move/` | the record of the 2026-11 move to `canifishthis/canifishthis`, release-candidate tags, what lives outside git |
| `sloughs/`, `curation-review/` | the owner's slough marks and the name-variant and tidal working files for the review tool |

## Pictures
Screenshots sit beside the page or ticket that cites them: `app-integration/T*-shots/`, `ios/shots/`,
`../tickets/shots/` and the ticket folders. `views/` is built by `node app/tools/ledger.mjs` (git-ignored): the open
ledger and the owner's open asks.
