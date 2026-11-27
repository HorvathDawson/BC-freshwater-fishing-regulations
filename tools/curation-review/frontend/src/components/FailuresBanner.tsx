import { useEffect, useState } from "react";
import { currentFailures, subscribeFailures, type Failure } from "../failures";
import { Notice } from "./NoticeStack";

/* T434: every failed read, in the notice stack — "could not load <what>" and the server's reason. Nothing that failed
   is shown as empty; this is where it says so. */
export function FailuresBanner() {
  const [list, setList] = useState<Failure[]>(currentFailures);
  useEffect(() => subscribeFailures(setList), []);
  if (!list.length) return null;
  return (
    <Notice id="load-failures" contentKey={list.map((f) => `${f.where}|${f.message}|${f.n}`).join(",")} tone="error"
      testid="load-failures"
      summary={<strong>{list.length === 1 ? `Could not load ${list[0]!.where}` : `${list.length} reads failed`}</strong>}>
      <ul>
        {list.map((f) => (
          <li key={f.where} data-testid="load-failure">
            could not load <strong>{f.where}</strong> — {f.message}{f.n > 1 ? ` (${f.n}×)` : ""}
          </li>
        ))}
      </ul>
    </Notice>
  );
}
