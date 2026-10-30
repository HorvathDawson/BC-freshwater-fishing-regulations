/**
 * The open spot's edit, held by the Shell (`SpotHost` draws it): out of `SpotHost.tsx` so the spot screen's code can
 * load later on the web (T282) while the Shell keeps the draft from its first render.
 */
import { useState } from "react";
import { useOnLeave, usePhotoEdit, type SpotPhotos } from "@app/ui";
import type { TabKey } from "../TabBar";

/** The spot edit's state, held across the spot screen's life. */
export function useSpotEdit(photos: SpotPhotos | undefined, openSpot: string | null) {
  // Non-null only while editing. The draft lives here rather than in SpotScreen so that
  // leaving the screen cannot strand half an edit inside an unmounted component.
  const [edit, setEdit] = useState<{ title: string; notes: string } | null>(null);
  // S1: the photos of the spot being edited; files added in an edit that is not saved are deleted with it
  const photoEdit = usePhotoEdit(photos);
  // P01: a failed save or delete, said on the spot's screen; and a tab change asked for while an edit is unsaved
  const [spotError, setSpotError] = useState<string | null>(null);
  const [pendingTab, setPendingTab] = useState<TabKey | null>(null);
  // (T294) the visit shown (null: the latest) and whether "Add a visit" is open; held here so the Shell's strip and
  // day-flow read the same visit the page shows
  const [shownVisit, setShownVisit] = useState<string | null>(null);
  const [addingVisit, setAddingVisit] = useState(false);
  // G1: however the spot is left (‹, Android's back, the browser's back, a tab), its edit ends with it — else the next
  // spot opens in edit mode with this one's text and Save writes it there
  useOnLeave(openSpot, () => {
    if (edit) void photoEdit.discard();
    setEdit(null); setSpotError(null); setPendingTab(null); setShownVisit(null); setAddingVisit(false);
  });
  return { edit, setEdit, photoEdit, spotError, setSpotError, pendingTab, setPendingTab, shownVisit, setShownVisit,
           addingVisit, setAddingVisit };
}

