/**
 * REGULATIONS — the Regulations page (regs v38), ported from the prototype `app/design/regs-v11.html` @ 7fb6809f and
 * `app/design/regs-v11/reference/page_v36.js`. Data: the release files (water, regs-<region>, regs-sid) through
 * the `ReleaseLoader`, read by `@app/data` `loadRegsInputs()`; logic: `./regs` (pure); hooks: `@app/ui` `useRegulations`;
 * components: `@app/ui-native` `regs/`. Spec: handoff/PLAN/app-integration/. Rules kept from the old placeholder:
 * licensing never affects open/closed; the angler is unknown until they say (who defaults to adult resident, T11);
 * every sentence is data or a words.ts constant.
 */
export * from "./regs";
