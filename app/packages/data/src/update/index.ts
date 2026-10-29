/** (T231) THE UPDATE SERVICE: feeds and the release, checked cheaply, fetched verified, applied at a safe moment. */
export { createUpdateService, manifestDate, type UpdateService, type UpdateSnapshot } from "./service";
/** (T251) the Updates log's ring buffer, and the fetch that names the app version on every request */
export { createUpdateLog, versionedFetch, type UpdateLog } from "./log";
