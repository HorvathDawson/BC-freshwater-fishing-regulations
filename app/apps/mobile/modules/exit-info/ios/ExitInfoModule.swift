import Darwin
import ExpoModulesCore
import Foundation
import UIKit

/*
 * (T362) WHY THE LAST RUN OF THE APP ENDED, on iOS. Android keeps a record an app can read at its next start
 * (ApplicationExitInfo, the Kotlin half); iOS keeps none, so the app writes its own at the moment it dies:
 *   - a fatal signal (SIGSEGV, SIGBUS, SIGILL, SIGFPE, SIGABRT, SIGTRAP: a native crash, a Swift trap) → CRASH_NATIVE,
 *     status = the signal number;
 *   - an uncaught Objective-C exception (incl. React Native's RCTFatal) → CRASH, its name and reason as the description.
 * One small file per crash under Library/Caches/exit-info (`<ms> <signal> <state> <footprint kB> <S|E>\n<text>`): the
 * time, the app's state (FOREGROUND active · VISIBLE inactive · BACKGROUND), the memory footprint (what iOS's memory killer
 * reads) and, for an exception, its words. Nothing else: no stack, no paths, no memory contents. JS reads them at the next
 * start (`recent`, the same shape as Android's) and sends them under the "Send crash reports" switch, scrubbed
 * (`reportPastExits`, @app/ui crashReport.ts). The newest 8 files are kept, as Android's system keeps its own.
 *
 * Not caught (said, not faked): a kill by the system (memory, watchdog) or the user sends no signal the app can catch
 * (SIGKILL). MetricKit's diagnostics (a day late, with stacks) are the follow-up for those.
 *
 * THE SIGNAL HANDLER is async-signal-safe: no allocation, no locks, no Swift strings. Everything it touches is made at
 * install (before any handler can run): the directory path, a fixed buffer, the previous actions to chain to.
 */

private let SIGNALS: [Int32] = [SIGSEGV, SIGBUS, SIGILL, SIGFPE, SIGABRT, SIGTRAP]
private let KEEP = 8
private var crashDir: UnsafeMutablePointer<CChar>? = nil          // strdup'd at install
private var pathBuf = UnsafeMutablePointer<UInt8>.allocate(capacity: 1024)
private var lineBuf = UnsafeMutablePointer<UInt8>.allocate(capacity: 128)
private var previous = UnsafeMutablePointer<sigaction>.allocate(capacity: 32)
private var previousException: (@convention(c) (NSException) -> Void)? = nil
/** 0 FOREGROUND (active) · 1 VISIBLE (inactive: a system sheet, the app switcher) · 2 BACKGROUND; set by the delegate */
private var appState: Int32 = 0
/** an exception's record is written: the SIGABRT that follows it does not write a second one */
private var exceptionWritten: Int32 = 0
private var installed = false

@inline(__always) private func put(_ b: UnsafeMutablePointer<UInt8>, _ n: inout Int, _ c: UInt8) { if n < 1023 { b[n] = c; n += 1 } }
private func putInt(_ b: UnsafeMutablePointer<UInt8>, _ n: inout Int, _ v: Int64) {
  if v == 0 { put(b, &n, 48); return }
  var x = v < 0 ? -v : v
  if v < 0 { put(b, &n, 45) }
  var digits: (UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8, UInt8) =
    (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
  var k = 0
  withUnsafeMutableBytes(of: &digits) { d in
    while x > 0 && k < 20 { d[k] = UInt8(48 + x % 10); x /= 10; k += 1 }
    while k > 0 { k -= 1; put(b, &n, d[k]) }
  }
}

private func nowMs() -> Int64 {
  var tv = timeval()
  gettimeofday(&tv, nil)
  return Int64(tv.tv_sec) * 1000 + Int64(tv.tv_usec) / 1000
}

private func footprintKb() -> Int64 {
  var info = task_vm_info_data_t()
  var count = mach_msg_type_number_t(MemoryLayout<task_vm_info_data_t>.size / MemoryLayout<integer_t>.size)
  let kr = withUnsafeMutablePointer(to: &info) {
    $0.withMemoryRebound(to: integer_t.self, capacity: Int(count)) { task_info(mach_task_self_, task_flavor_t(TASK_VM_INFO), $0, &count) }
  }
  return kr == KERN_SUCCESS ? Int64(info.phys_footprint / 1024) : 0
}

/** Write one record: `<dir>/<ms>.crash`. Async-signal-safe (open / write / close only). */
private func writeRecord(signal sig: Int32, kind: UInt8, text: UnsafePointer<UInt8>?, textLen: Int) {
  guard let dir = crashDir else { return }
  let ms = nowMs()
  var n = 0
  var i = 0
  while dir[i] != 0 { put(pathBuf, &n, UInt8(bitPattern: dir[i])); i += 1 }
  put(pathBuf, &n, 47)                                    // "/"
  putInt(pathBuf, &n, ms)
  // ".crash", byte by byte (an array literal would allocate)
  put(pathBuf, &n, 46); put(pathBuf, &n, 99); put(pathBuf, &n, 114); put(pathBuf, &n, 97); put(pathBuf, &n, 115); put(pathBuf, &n, 104)
  pathBuf[n] = 0
  var m = 0
  putInt(lineBuf, &m, ms); put(lineBuf, &m, 32)
  putInt(lineBuf, &m, Int64(sig)); put(lineBuf, &m, 32)
  putInt(lineBuf, &m, Int64(appState)); put(lineBuf, &m, 32)
  putInt(lineBuf, &m, footprintKb()); put(lineBuf, &m, 32)
  put(lineBuf, &m, kind); put(lineBuf, &m, 10)
  let fd = pathBuf.withMemoryRebound(to: CChar.self, capacity: 1024) { open($0, O_WRONLY | O_CREAT | O_TRUNC, 0o644) }
  if fd < 0 { return }
  _ = write(fd, lineBuf, m)
  if let t = text, textLen > 0 { _ = write(fd, t, textLen) }
  close(fd)
}

private func onSignal(_ sig: Int32, _ info: UnsafeMutablePointer<__siginfo>?, _ ctx: UnsafeMutableRawPointer?) {
  if exceptionWritten == 0 { writeRecord(signal: sig, kind: 83 /* S */, text: nil, textLen: 0) }
  // the previous action (the system's default, or another handler) takes it from here: re-raised once this returns
  sigaction(sig, previous.advanced(by: Int(sig)), nil)
  raise(sig)
}

private func onException(_ e: NSException) {
  let words = String("\(e.name.rawValue): \(e.reason ?? "")".prefix(400))
  var bytes = Array(words.utf8)
  bytes.withUnsafeMutableBufferPointer { b in writeRecord(signal: 0, kind: 69 /* E */, text: UnsafePointer(b.baseAddress), textLen: b.count) }
  exceptionWritten = 1
  previousException?(e)
}

private func recordsDir() -> URL {
  let base = FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask)[0]
  return base.appendingPathComponent("exit-info", isDirectory: true)
}

/** Install the handlers once, as early as the app starts (the delegate subscriber's registration). */
func installExitRecorder() {
  if installed { return }
  installed = true
  let dir = recordsDir()
  try? FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
  crashDir = strdup(dir.path)
  _ = footprintKb(); _ = nowMs()                          // touched once here, never first in a handler
  // a stack of its own: a stack overflow's SIGSEGV has no room on the thread's stack
  let size = 64 * 1024
  var ss = stack_t(ss_sp: UnsafeMutableRawPointer.allocate(byteCount: size, alignment: 16), ss_size: size, ss_flags: 0)
  sigaltstack(&ss, nil)
  for s in SIGNALS {
    var sa = sigaction()
    sa.__sigaction_u.__sa_sigaction = onSignal
    sa.sa_flags = SA_SIGINFO | SA_ONSTACK
    sigemptyset(&sa.sa_mask)
    sigaction(s, &sa, previous.advanced(by: Int(s)))
  }
  previousException = NSGetUncaughtExceptionHandler()
  NSSetUncaughtExceptionHandler(onException)
}

private let SIGNAL_NAMES: [Int32: String] = [SIGSEGV: "SIGSEGV", SIGBUS: "SIGBUS", SIGILL: "SIGILL", SIGFPE: "SIGFPE", SIGABRT: "SIGABRT", SIGTRAP: "SIGTRAP"]
private let STATES = ["FOREGROUND", "VISIBLE", "BACKGROUND"]

/** The records newer than `since` (epoch ms), newest first, at most 8 (Android's shape); older files pruned to 8. */
private func readRecords(since: Double) -> [[String: Any]] {
  let fm = FileManager.default
  let dir = recordsDir()
  let files = ((try? fm.contentsOfDirectory(atPath: dir.path)) ?? []).filter { $0.hasSuffix(".crash") }
    .sorted { (Int64($0.dropLast(6)) ?? 0) > (Int64($1.dropLast(6)) ?? 0) }
  for f in files.dropFirst(KEEP) { try? fm.removeItem(at: dir.appendingPathComponent(f)) }
  var out: [[String: Any]] = []
  for f in files.prefix(KEEP) {
    guard let data = fm.contents(atPath: dir.appendingPathComponent(f).path), let text = String(data: data, encoding: .utf8) else { continue }
    let lines = text.split(separator: "\n", maxSplits: 1, omittingEmptySubsequences: false)
    let head = (lines.first ?? "").split(separator: " ")
    guard head.count >= 5, let ms = Double(head[0]), let sig = Int32(head[1]), let st = Int(head[2]), let kb = Double(head[3]) else { continue }
    if ms <= since { continue }
    let exception = head[4] == "E"
    let words = lines.count > 1 ? String(lines[1]) : ""
    out.append([
      "timestamp": ms,
      "reason": exception ? "CRASH" : "CRASH_NATIVE",
      "importance": STATES[min(max(st, 0), 2)],
      "status": Int(sig),
      "rssKb": kb,
      "description": exception ? words : (SIGNAL_NAMES[sig] ?? "signal \(sig)"),
    ])
  }
  return out
}

public class ExitInfoModule: Module {
  public func definition() -> ModuleDefinition {
    Name("ExitInfo")

    OnCreate { installExitRecorder() }   // already done by the delegate subscriber; a guard if it was not registered

    // exits of this app after `since` (epoch ms), newest first, at most 8
    AsyncFunction("recent") { (since: Double) -> [[String: Any]] in
      readRecords(since: since)
    }
  }
}

/** The handlers go in before the app's own launch code; the app's state is followed for the record. */
public class ExitInfoAppDelegate: ExpoAppDelegateSubscriber {
  public func subscriberDidRegister() { installExitRecorder() }
  public func applicationDidBecomeActive(_ application: UIApplication) { appState = 0 }
  public func applicationWillResignActive(_ application: UIApplication) { appState = 1 }
  public func applicationDidEnterBackground(_ application: UIApplication) { appState = 2 }
  public func applicationWillEnterForeground(_ application: UIApplication) { appState = 1 }
}
