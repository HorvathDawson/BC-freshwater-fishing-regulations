/** (T231) THE UPDATE SERVICE: feeds and the release, checked cheaply, fetched verified, applied at a safe moment. */
export { createUpdateService, manifestDate, type UpdateService, type UpdateSnapshot } from "./service";
/** (T251) the Updates log's ring buffer, and the fetch that names the app version on every request */
export { createUpdateLog, versionedFetch, type UpdateLog } from "./log";
/** (T237) a problem report, sent now or kept on the device and sent with the service's retry */
export { createReportOutbox, reportId, type ReportSendResult } from "../report/outbox";
