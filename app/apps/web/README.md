# apps/web — two form factors, one bundle boundary

Web hosts **both** phone and desktop. They are not the same UI and are not pretending to be.

```
src/
  phone/     renders @app/ui-native through react-native-web
             -> pixel-identical to the native app, by construction
  desktop/   its own DOM components — a different layout, not a stretched phone
```

Both call the **same `@app/ui` hooks** and the same `@app/data` source, so they cannot
disagree about what the regulations say — only about how they are laid out.

## Why desktop having its own components is free

Because components hold no logic. Behaviour is in `@app/ui`. If duplicating a component
ever looks like it duplicates logic, the logic is in the wrong place — move it to a hook.

## Code splitting

`formFactorFor(width)` (in `@app/ui`, the single place the split is decided) picks the
tree, and each is lazy-loaded: a desktop visitor never downloads the react-native-web
phone tree, and a phone visitor never downloads the desktop one.

## Not scaffolded yet — and where mobile web runs meanwhile

This package has no bundler and no entry point. Until the desktop tree exists there is
nothing here a second bundler would add: mobile web is Expo's web platform, served out of
`apps/mobile` by `pnpm dev:web`, rendering `@app/ui-native` through react-native-web
exactly as this README describes. `layers.json` reflects that — `expo` is granted to
`apps/mobile` and withheld here.

What is deferred, and what unblocks it:

- **the desktop DOM tree** (`src/desktop/`) — needs a design, and needs the content schema
  (13-build-plan steps 6-7) before there is anything to lay out
- **the `formFactorFor` code split** — meaningless with one tree; it is the reason this
  package will exist
- **`data-web`** (range reads) — waits on the packaging spike (step 6)

When desktop lands, `src/phone/` re-exports `@app/ui-native` and adds nothing else.
