import CryptoKit
import ExpoModulesCore
import Foundation
import UIKit

/*
 * (T722, owner 2026-11-14 "we want it to progress with app closed") THE RELEASE'S BIG FILES ON A BACKGROUND URLSESSION:
 * the iOS half of modules/bg-download, the same JS surface as the Android DownloadManager half (`jobs`, `enqueue`,
 * `remove`, `claim`, `sha256`), so one JS state machine (`@app/data` release/bgDownload.ts) drives both.
 *
 * Why not expo-file-system's own background session (T313, b8e33ca80): its task lives in JS — a `DownloadTask` promise —
 * so when the app was killed the JS side of the transfer was gone, and nothing re-attached the finished file. Here the
 * state is native and on disk:
 *   - ONE session with a FIXED identifier, made at launch (the delegate subscriber): iOS re-attaches the transfers it kept
 *     going while the app was away and delivers what finished meanwhile (also when it wakes the app for it:
 *     `handleEventsForBackgroundURLSession`, whose completion handler is called once the events are delivered);
 *   - each job a record in `Application Support/bg-download/jobs.json` (id, url, file, metered, status, size); the task
 *     carries its job id (`taskDescription`), so a re-attached task finds its record;
 *   - a finished file is MOVED into `bg-download/<name>` inside the delegate callback (iOS deletes it after the return),
 *     and `claim` moves it onto the install's `.part`, which JS then checks (size, sha256) before anything uses it;
 *   - a transfer that ends in an error with resume data (a dropped line; the app swiped away in the app switcher, which
 *     iOS answers by cancelling every background transfer of the app, delivered at the next launch) is resumed from where
 *     it was, up to 3 times; otherwise the job is "failed" and JS queues it again (as on Android).
 * `jobs` merges the live tasks' bytes in, so progress shows the same whichever launch asks. A record with no task and no
 * word from iOS for 10 s is reported "failed" (reason -1): JS queues it again, never waits on it for ever.
 * Not metered: the request may not use cellular / expensive / constrained networks; a background session then waits for
 * Wi-Fi by itself (Android's `setAllowedOverMetered(false)`). The folder is kept out of the device's backup.
 */

private let SESSION_ID = "ca.canifishthis.bgdownload"
private let MAX_RESUMES = 3
private let MISSING_SECONDS = 10.0

private struct BgError: LocalizedError {
  let message: String
  var errorDescription: String? { message }
}

private struct JobRecord: Codable {
  var id: Int
  var url: String
  var name: String
  var metered: Bool
  var status: String          // pending · running · paused · done · failed
  var reason: Int
  var bytes: Int64
  var total: Int64
  var resumes: Int
}

private struct SavedState: Codable {
  var next: Int
  var jobs: [JobRecord]
}

final class BgDownloader: NSObject, URLSessionDownloadDelegate {
  static let shared = BgDownloader()

  /** every read and write of the records goes through this queue (the delegate's queue and the module's are others) */
  private let q = DispatchQueue(label: "ca.canifishthis.bgdownload.state")
  private var jobs: [Int: JobRecord] = [:]
  private var next = 1
  /** when a record was first seen with no task (this process), for the "gone" verdict */
  private var missingSince: [Int: Date] = [:]
  /** iOS's completion handler for a wake to deliver this session's events (main thread only) */
  var finishEvents: (() -> Void)?

  let dir: URL
  private let stateFile: URL

  lazy var session: URLSession = {
    let c = URLSessionConfiguration.background(withIdentifier: SESSION_ID)
    c.isDiscretionary = false
    c.sessionSendsLaunchEvents = true
    return URLSession(configuration: c, delegate: self, delegateQueue: nil)
  }()

  private override init() {
    let base = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
    var d = base.appendingPathComponent("bg-download", isDirectory: true)
    try? FileManager.default.createDirectory(at: d, withIntermediateDirectories: true)
    var keep = URLResourceValues()
    keep.isExcludedFromBackup = true
    try? d.setResourceValues(keep)          // re-downloadable: never in the backup
    dir = d
    stateFile = d.appendingPathComponent("jobs.json")
    super.init()
    if let data = try? Data(contentsOf: stateFile), let s = try? JSONDecoder().decode(SavedState.self, from: data) {
      next = s.next
      for j in s.jobs { jobs[j.id] = j }
    }
  }

  /** Make the session (re-attaching the transfers iOS kept): at launch, and on a wake for its events. */
  func start() { _ = session }

  /** on `q` only */
  private func save() {
    let s = SavedState(next: next, jobs: jobs.values.sorted { $0.id < $1.id })
    if let data = try? JSONEncoder().encode(s) { try? data.write(to: stateFile, options: .atomic) }
  }

  private func fileOf(_ name: String) -> URL { dir.appendingPathComponent(name) }

  func list(_ done: @escaping ([[String: Any]]) -> Void) {
    session.getAllTasks { tasks in
      var byId: [Int: URLSessionTask] = [:]
      for t in tasks { if let d = t.taskDescription, let id = Int(d), t.state != .completed { byId[id] = t } }
      let out: [[String: Any]] = self.q.sync {
        let now = Date()
        return self.jobs.values.sorted { $0.id < $1.id }.map { rec -> [String: Any] in
          var j = rec
          if j.status != "done" && j.status != "failed" {
            if let t = byId[j.id] {
              self.missingSince[j.id] = nil
              j.bytes = t.countOfBytesReceived
              j.total = t.countOfBytesExpectedToReceive > 0 ? t.countOfBytesExpectedToReceive : -1
              j.status = t.state == .suspended ? "paused" : (t.countOfBytesReceived > 0 ? "running" : "pending")
            } else {
              let since = self.missingSince[j.id] ?? now
              self.missingSince[j.id] = since
              if now.timeIntervalSince(since) > MISSING_SECONDS { j.status = "failed"; j.reason = -1 }
            }
          }
          let present = j.status != "done" || FileManager.default.fileExists(atPath: self.fileOf(j.name).path)
          return ["id": Double(j.id), "url": j.url, "status": j.status, "reason": j.reason, "bytes": Double(j.bytes),
                  "total": Double(j.total), "metered": j.metered, "present": present]
        }
      }
      done(out)
    }
  }

  func enqueue(url: String, name: String, metered: Bool) throws -> Int {
    guard let u = URL(string: url) else { throw BgError(message: "not a url: \(url)") }
    var req = URLRequest(url: u)
    // the bytes as stored: no transparent gzip (a compressed body has no length to show or resume by)
    req.setValue("identity", forHTTPHeaderField: "Accept-Encoding")
    req.allowsCellularAccess = metered
    req.allowsExpensiveNetworkAccess = metered
    req.allowsConstrainedNetworkAccess = metered
    let id: Int = q.sync {
      let id = next
      next += 1
      try? FileManager.default.removeItem(at: fileOf(name))
      jobs[id] = JobRecord(id: id, url: url, name: name, metered: metered, status: "pending", reason: 0, bytes: 0,
                           total: -1, resumes: 0)
      save()
      return id
    }
    let task = session.downloadTask(with: req)
    task.taskDescription = String(id)
    task.resume()
    return id
  }

  func remove(_ id: Int) {
    let name: String? = q.sync {
      let n = jobs[id]?.name
      jobs[id] = nil
      missingSince[id] = nil
      save()
      return n
    }
    if let n = name { try? FileManager.default.removeItem(at: fileOf(n)) }
    session.getAllTasks { tasks in
      for t in tasks where t.taskDescription == String(id) { t.cancel() }
    }
  }

  func claim(_ id: Int, to uri: String) throws {
    try q.sync {
      guard let j = jobs[id], j.status == "done" else { throw BgError(message: "download \(id) is not done") }
      guard let dest = URL(string: uri), dest.isFileURL else { throw BgError(message: "not a file url: \(uri)") }
      let fm = FileManager.default
      try fm.createDirectory(at: dest.deletingLastPathComponent(), withIntermediateDirectories: true)
      if fm.fileExists(atPath: dest.path) { try fm.removeItem(at: dest) }
      try fm.moveItem(at: fileOf(j.name), to: dest)
      jobs[id] = nil
      save()
    }
  }

  // MARK: the session's delegate (its own serial queue)

  func urlSession(_ session: URLSession, downloadTask: URLSessionDownloadTask, didFinishDownloadingTo location: URL) {
    guard let id = Int(downloadTask.taskDescription ?? "") else { return }
    let code = (downloadTask.response as? HTTPURLResponse)?.statusCode ?? 0
    q.sync {
      guard var j = jobs[id] else { return }          // removed meanwhile: iOS deletes the file after this returns
      if !(200...299).contains(code) {
        j.status = "failed"; j.reason = code
      } else {
        let dest = fileOf(j.name)
        do {
          try? FileManager.default.removeItem(at: dest)
          try FileManager.default.moveItem(at: location, to: dest)   // now: the temp file is gone after the return
          let attrs = try? FileManager.default.attributesOfItem(atPath: dest.path)
          let size = (attrs?[.size] as? NSNumber)?.int64Value ?? 0
          j.status = "done"; j.reason = 0; j.bytes = size; j.total = size
        } catch {
          j.status = "failed"; j.reason = (error as NSError).code
        }
      }
      jobs[id] = j
      missingSince[id] = nil
      save()
    }
  }

  func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
    guard let error = error, let id = Int(task.taskDescription ?? "") else { return }
    let ns = error as NSError
    let resumeData = ns.userInfo[NSURLSessionDownloadTaskResumeData] as? Data
    let again: Data? = q.sync {
      guard var j = jobs[id], j.status != "done" else { return nil }
      var d: Data? = nil
      if let r = resumeData, j.resumes < MAX_RESUMES {
        j.resumes += 1; j.status = "pending"; d = r
        NSLog("[bg-download] job %d: %@; resumed (%d)", id, ns.localizedDescription, j.resumes)
      } else {
        j.status = "failed"; j.reason = ns.code
        NSLog("[bg-download] job %d failed: %@ (%d)", id, ns.localizedDescription, ns.code)
      }
      jobs[id] = j
      missingSince[id] = nil
      save()
      return d
    }
    if let d = again {
      let t = session.downloadTask(withResumeData: d)
      t.taskDescription = String(id)
      t.resume()
    }
  }

  func urlSessionDidFinishEvents(forBackgroundURLSession session: URLSession) {
    DispatchQueue.main.async {
      let done = self.finishEvents
      self.finishEvents = nil
      done?()
    }
  }
}

public class BgDownloadModule: Module {
  public func definition() -> ModuleDefinition {
    Name("BgDownload")

    OnCreate { BgDownloader.shared.start() }   // already done by the delegate subscriber; a guard if it was not registered

    AsyncFunction("jobs") { (promise: Promise) in
      BgDownloader.shared.list { promise.resolve($0) }
    }

    // `title` / `text` are Android's notification lines: iOS shows no notification for a background download
    AsyncFunction("enqueue") { (url: String, name: String, title: String, text: String, metered: Bool) -> Double in
      let id = try BgDownloader.shared.enqueue(url: url, name: name, metered: metered)
      return Double(id)
    }

    AsyncFunction("remove") { (id: Double) in
      BgDownloader.shared.remove(Int(id))
    }

    AsyncFunction("claim") { (id: Double, toUri: String) in
      try BgDownloader.shared.claim(Int(id), to: toUri)
    }

    // the sha256 of a file, hex, a megabyte at a time (T722 F14: JS took ~23 s for the basemap)
    AsyncFunction("sha256") { (uri: String) -> String in
      guard let url = URL(string: uri), url.isFileURL else { throw BgError(message: "not a file url: \(uri)") }
      let h = try FileHandle(forReadingFrom: url)
      defer { try? h.close() }
      var hasher = SHA256()
      var more = true
      while more {
        autoreleasepool {
          let data = h.readData(ofLength: 1 << 20)
          if data.isEmpty { more = false } else { hasher.update(data: data) }
        }
      }
      return hasher.finalize().map { String(format: "%02x", $0) }.joined()
    }

    // (T722 F6) a file or folder out of the device's backup (the flag is per item: set again after each install)
    AsyncFunction("excludeFromBackup") { (uri: String) in
      guard var url = URL(string: uri), url.isFileURL else { throw BgError(message: "not a file url: \(uri)") }
      var values = URLResourceValues()
      values.isExcludedFromBackup = true
      try url.setResourceValues(values)
    }
  }
}

/** The session is made before the app's own launch code, and iOS's wake for its events is answered. */
public class BgDownloadAppDelegate: ExpoAppDelegateSubscriber {
  public func subscriberDidRegister() { BgDownloader.shared.start() }

  public func application(_ application: UIApplication, handleEventsForBackgroundURLSession identifier: String,
                          completionHandler: @escaping () -> Void) {
    guard identifier == SESSION_ID else { completionHandler(); return }
    BgDownloader.shared.finishEvents = completionHandler
    BgDownloader.shared.start()
  }
}
