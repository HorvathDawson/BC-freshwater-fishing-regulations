/* OLDER WORDINGS OF A DFO PLACE (owner 2026-11-09: "we cant blindly assume variants … they must match and that requires
   human check too"): each listed with its own DFO words (what differs from the shown place's marked), its extent now in
   words and a "same place" tick that starts UNTICKED; the line under them names exactly what the save writes. One
   component for the checklist and the change editor (logic: `checklist.ts` wordDiff / untickable / groupLine). */
import { groupLine, untickable, wordDiff } from "../checklist";
import type { ReactNode } from "react";
import type { DfoGroupMember, DfoSeenExtent } from "../types";

export function OlderWordings({ members, shownText, same, setSame, fix, checklist, extentWords, link }: {
  members: DfoGroupMember[]; shownText: string; same: Set<string>; setSame: (s: Set<string>) => void;
  fix: boolean; checklist: boolean; extentWords: (e: DfoSeenExtent) => ReactNode; link: (id: string) => string;
}) {
  const others = members.filter((m) => !m.this);
  if (!others.length) return null;
  return (
    <div className="ext-row" data-testid="older-wordings" style={{ display: "block" }}>
      <div className="dim">Older wordings of this place — tick each one you have read and is the same place:</div>
      <ul className="named-splits">
        {others.map((m) => {
          const why = untickable(m, fix, checklist);
          return (
            <li key={m.id} data-testid="older-wording">
              <label className="check" title={why || "same place: the save writes this wording too"}>
                <input type="checkbox" checked={!why && same.has(m.id)} disabled={!!why} data-testid="older-same"
                  onChange={(e) => { const s = new Set(same); if (e.target.checked) s.add(m.id); else s.delete(m.id); setSame(s); }} />
                {" "}same place
              </label>{" "}
              <a href={link(m.id)}>{m.text ? wordDiff(shownText, m.text).map((t, i) => (
                <span key={i}>{i ? " " : ""}{t.diff ? <mark data-testid="older-diff">{t.w}</mark> : t.w}</span>))
                : "(the whole water)"}</a>
              <div className="dim" style={{ fontSize: 12 }}>
                its extent now: {m.extent.extents.length ? extentWords(m.extent) : "none"}
                {m.same_extent ? " (the same as this place's)" : <b className="warn-text"> — differs: a tick gives it this place's extent</b>}
                {checklist && m.check !== "checked" && m.check !== "wrong" ? " · older wording not confirmed" : ""}
              </div>
              {why && <div className="warn-text" style={{ fontSize: 12 }} data-testid="older-untickable">Cannot be ticked: {why}. Left for you.</div>}
            </li>);
        })}
      </ul>
      <div className="hint" data-testid="older-line">{groupLine(members, same, fix, checklist)}</div>
    </div>
  );
}
