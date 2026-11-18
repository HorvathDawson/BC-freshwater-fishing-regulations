/**
 * (T810 cutover) THE GROUP CARD A REAL PAGE STILL DRAWS. Since the cutover no Keep tab draws a group card (checked over
 * every key and segment of the cutover side release r-d274bf0a1e26: the trout and char rows are the table's, and the
 * groups left, whitefish / bass / grayling / pike, all sit in the Other fish list), so `keep.groups` is empty in every
 * frozen page. The card is still drawn in an Other fish row's sheet (`other:oN`, SheetBody's `group` block, build spec:
 * "GroupCard stays (the Other fish sheet draws it)"): the group-card tests read that VM from the frozen pages.
 */
import type { GroupVM } from "@app/core";

export function otherGroup(page: unknown, key: string): GroupVM {
  const sheet = (page as { sheets?: Record<string, { blocks: readonly { k: string; group?: GroupVM }[] } | null> }).sheets?.[key];
  const g = sheet?.blocks.find((b) => b.k === "group")?.group;
  if (!g) throw new Error(`no group card in the frozen page's ${key} sheet`);
  return g;
}
