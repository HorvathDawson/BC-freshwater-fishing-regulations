/**
 * The app's questions, as hooks. Data out, never elements — which is what lets the phone
 * and the desktop render the same answers with completely different components.
 *
 * One module per subject: `water` (a water and finding one), `gauge` (stations, readings, series) and `facts` (what
 * the data says about itself).
 */
export * from "./water";
export * from "./gauge";
export * from "./facts";
