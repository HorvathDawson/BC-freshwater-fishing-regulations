# T837 — The sun line writes "sunset" in lower case after the dot

Status: DONE (745d59ae8; on branch `app-fix/t830-level-no-band`; lands with T830).

Source: owner 2026-11-17 ~11:45, app chat (verbatim): "sun set is not capitalised (Sunrise 08:50 PT · sunset 17:43 PT) too".

Fix: `core/src/sun.ts` SUN_WORDS.line writes "Sunrise … · Sunset …"; the web hook and the regs header read it, tests and
doc comments follow.
