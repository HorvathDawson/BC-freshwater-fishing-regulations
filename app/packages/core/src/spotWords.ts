/**
 * (T60, T180) THE WORDS OF THE SPOT SCREENS that were typed in their components — one table per screen: the spot
 * itself (SpotScreen), adding one (SpotCapture), its confirm (spot/Confirm), what it recorded (spot/Record and the
 * weather window), and the Spots list. Started with the one FIGURE (the weather window's column heads, an hour either
 * side of the visit) so that no component types a number of its own (`tools/data-vs-ui.test.ts`).
 */
export const SPOT_WINDOW = {
  /** The column heads: the hour before, the visit, the hour after. */
  before: "−1 h", at: "visit", after: "+1 h",
  /** The rows: the four measures across the three hours. */
  cloud: "Cloud", pressure: "Pressure", humidity: "Humidity", temp: "Temp",
} as const;

/** One saved (or being-saved) spot: the header's links, the title and notes fields, the refresh row, the save button. */
export const SPOT_WORDS = {
  back: "Back",
  /** where a viewed spot's ‹ goes when the caller does not say (the Spots tab) */
  spots: "Spots",
  cancel: "Cancel", cancelEditing: "Cancel editing",
  edit: "Edit", editSpot: "Edit spot",
  delete: "Delete", deleteSpot: "Delete spot",
  name: "Name this spot",
  addTitle: "Add a title",
  refresh: "Refresh", refreshSpot: "Refresh this spot",
  showOnMap: "Show on map",
  notes: "Notes",
  notesHint: "What happened, what you used, what the water looked like",
  noNotes: "None.",
  save: "Save spot", saveChanges: "Save changes",
} as const;

/** Adding a spot: the coach's steps, the when-were-you-there page and its steppers, the way on and back. */
export const SPOT_CAPTURE_WORDS = {
  reading: "Reading the water",
  pickWater: "Pick the water",
  pickWaterDetail: "Tap the stream or lake your spot is on.",
  exactSpot: "Now the exact spot",
  /** `water`: the picked water's name, or null when it has none */
  tapPoint: (water: string | null) => `Tap the point on ${water ?? "this reach"}.`,
  when: "When were you there?",
  earlierDay: "Earlier day", laterDay: "Later day", earlierHour: "Earlier hour", laterHour: "Later hour",
  back: "Back", cancel: "Cancel", next: "Next",
} as const;

/** A destructive or lossy step on a saved spot, asked first (delete; leave an edit with changes). */
export const SPOT_CONFIRM_WORDS = {
  delete: {
    label: "Confirm delete",
    title: "Delete this spot?",
    detail: "The gauge reading, the weather and the rules that were in force are recorded nowhere else. This cannot be "
      + "undone.",
    keep: "Keep it",
    go: "Delete",
  },
  discard: {
    label: "Unsaved changes",
    title: "You have unsaved changes.",
    detail: "Keep them, or throw them away and go back to what was saved.",
    keep: "Keep editing",
    go: "Discard",
  },
  /** NOT "Save changes" (the footer's button): this one saves AND leaves, so it says so */
  saveAndClose: "Save and close",
} as const;

/** What a saved spot recorded: the water's block and the weather's, and what each says when nothing was recorded. */
export const SPOT_RECORD_WORDS = {
  water: "What the water was doing",
  noReading: "No gauge is entitled to speak for this water, so no reading was recorded. A station draining a far "
    + "larger watershed would have given a number, and the number would have been wrong.",
  weather: "Weather",
  noWeather: "No weather was recorded for this spot.",
} as const;

/** The Spots list: a row whose spot has no photo. */
export const SPOTS_LIST_WORDS = { noPhoto: "NO PHOTO" } as const;
