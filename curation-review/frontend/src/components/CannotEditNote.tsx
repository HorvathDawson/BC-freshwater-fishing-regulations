import type { CannotEdit } from "../types";

/** T755 follow-up: why an extent cannot be edited here, and what the owner can do (`dfoExtent.cannotEditOf`) */
export function CannotEditNote({ why }: { why: CannotEdit }) {
  return (
    <div className="warn-text" data-testid="cannot-edit" style={{ fontSize: 12 }}>
      <b>Cannot be edited here.</b> {why.why.join(" ")}
      <div className="muted" data-testid="cannot-edit-can">{why.can}</div>
    </div>
  );
}
