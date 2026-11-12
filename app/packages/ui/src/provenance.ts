/**
 * (T585 F12, F13) WHICH DATA THIS BUILD IS ANSWERING FROM, for the Conditions gauge card's foot: the finished line
 * (`@app/core` `dataProvenanceLine`: the data release, and the data source / channel when they are not the build's own),
 * or null in a production build. Provided once by the app (it holds the booted release and the environment), read by
 * the one Conditions body — so every way into Conditions (the tab, a water's sheet, a search, a tap, a cold start) says
 * the same thing, and it follows an applied update because the app re-renders on the new release.
 */
import { createContext } from "react";

export const DataProvenanceContext = createContext<string | null>(null);
