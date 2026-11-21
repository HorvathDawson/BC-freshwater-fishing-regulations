/**
 * (T60, T180) THE WORDS OF THE SPOT SCREENS that were typed in their components — one table per screen: the spot
 * itself (SpotScreen), adding one (SpotCapture), its confirm (spot/Confirm), what it recorded (spot/Record and the
 * weather window), and the Spots list. Started with the one FIGURE (the weather window's column heads, an hour either
 * side of the visit) so that no component types a number of its own (`tools/data-vs-ui.test.ts`).
 */
import { standingLong } from "./standing";
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
  /** (T284) the page's three headed blocks: the day as it was, what the person added, how the next days compare */
  thatDay: "That day", added: "What you added", nextDays: "The next few days",
  /** (T819 item 5) the draft's record, folded to one card: its open and close, and what the card says it holds */
  showRecord: "Show the record", hideRecord: "Hide the record",
  recordDetail: "The flow, the weather and the week before, as recorded for this moment.",
  /** where a spot has nothing added yet */
  nothingAdded: "No notes or photos yet.",
  /** (T310) where to park: a fact of the spot (every visit's), shown on the page and as a P on the map */
  parking: "Parking", noParking: "No parking added.",
  /** (T819 item 14) a spot's parking, said plainly (its coordinates small under it) */
  parkingSaved: "Parking saved",
  parkingHere: "Use my location", parkingPick: "Pick on the map", parkingRemove: "Remove parking",
  directions: "Directions", directionsLabel: "Directions to the parking, in your maps app",
  parkingNoFix: "Could not find your location.",
  pickParking: "Where do you park?", pickParkingDetail: "Tap the map where you leave the car.",
  parkingUse: "Use this point",
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
  /** (T819 item 2) under the question: what is recorded for the moment picked. A spot keeps no rules, so it says none. */
  whenDetail: (water: string | null) =>
    `The weather and the flow are recorded for this moment${water ? ` on ${water}` : ""}, so it is worth getting right.`,
  day: "Day", hour: "Hour",
  earlierDay: "Earlier day", laterDay: "Later day", earlierHour: "Earlier hour", laterHour: "Later hour",
  back: "Back", cancel: "Cancel", next: "Next",
} as const;

/** A destructive or lossy step on a saved spot, asked first (delete; leave an edit with changes). */
export const SPOT_CONFIRM_WORDS = {
  delete: {
    label: "Confirm delete",
    title: "Delete this spot?",
    /** (T819 item 2) a spot keeps no rules: only its reading and its weather are lost with it */
    detail: "The gauge reading and the weather recorded with it are kept nowhere else. This cannot be undone.",
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
  noReading: "There is no gauge on this water, so no conditions were recorded. The nearest gauges are on much bigger rivers, so their numbers would be wrong here.",
  /** the reading's standing, said plainly: "42nd percentile for the time of year" */
  standing: (percentile: number) => standingLong(percentile),
  weather: "Weather",
  noWeather: "No weather was recorded for this spot.",
} as const;

/** The Spots list: a row whose spot has no photo. */
export const SPOTS_LIST_WORDS = { noPhoto: "NO PHOTO" } as const;

/**
 * (T302, owner 2026-10-30: "spots in the webapp instead just say something about how they work locally on the phone app
 * but they don't sync to web yet. Coming soon will be accounts etc") THE WEB'S SPOTS: a short notice in the Spots tab's
 * place and the capture's, and a link to the app. Spots saved in a browser earlier are left where they are, not shown.
 */
export const WEB_SPOTS_WORDS = {
  title: "Spots",
  lead: "Save your fishing spots in the phone app",
  body: [
    "Spots are saved on your phone in the Can I Fish This? app. There you can mark a place, add notes and photos, and "
      + "see which coming days look like the day you fished.",
    "Spots don't show up on the website yet. Accounts are coming, so your spots will be on your phone and here.",
  ],
  getApp: "Get the app",
  /** (T901) one button per store; the addresses are core's storeLinks.ts */
  playButton: "Google Play",
  appStoreButton: "App Store",
  /** (T901) one plain line naming only the stores still coming (none left: no line) */
  coming: { both: "Coming soon to Google Play and the App Store.", play: "Coming soon to Google Play.",
            appStore: "Coming soon to the App Store." },
  /**
   * (T901, owner 2026-11-19: "we could add a contact to get into testing maybe?") under a store that is not public yet:
   * one line and one press per store, an email to the app's contact (More → Contact), or the in-app Report form where
   * no address is set. The body asks for what the store's tester list needs. The app speaks as "I".
   */
  join: {
    lead: "Want to try it early?",
    android: { button: "Join the Android test", subject: "Join the Android test",
               body: "Hi, I'd like to join the Android test of the Can I Fish This? app. The email on my Google account is: " },
    iphone: { button: "Join the iPhone test", subject: "Join the iPhone test",
              body: "Hi, I'd like to join the iPhone test of the Can I Fish This? app. The email on my Apple ID is: " },
    /**
     * (T911, owner 2026-11-19: "a popup with either a straight to email button or a copy email option") what a Join the
     * test press opens: one line in the owner's voice (T860: I / me) on how to join, "Email me" (the mailto above), "Copy
     * email" (the address, then "Copied"), and where the clipboard is refused, the address to select, said plainly.
     */
    popup: {
      android: "Email me the address of your Google account and I'll add you to the Android test.",
      iphone: "Email me the address of your Apple ID and I'll add you to the iPhone test.",
      email: "Email me", copy: "Copy email", copied: "Copied",
      refused: "I couldn't copy it from here. Select the address and copy it yourself:",
      close: "Close",
    },
  },
  back: "Back to the map",
} as const;

/**
 * (T723 G8) Where "Directions" to a parking pin goes. The label promises "your maps app": on an iPhone that is Apple Maps
 * (the `maps.apple.com` universal link opens it with no `LSApplicationQueriesSchemes` entry), everywhere else Google
 * Maps (Android opens its app; the web opens the page). `os` is `Platform.OS`'s value.
 */
export const directionsUrl = (os: string, lat: number, lon: number): string =>
  os === "ios" ? `http://maps.apple.com/?daddr=${lat},${lon}` : `https://www.google.com/maps/dir/?api=1&destination=${lat},${lon}`;
