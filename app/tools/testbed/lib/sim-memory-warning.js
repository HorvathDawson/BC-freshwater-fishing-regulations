// (T725) THE SIMULATOR'S "Simulate Memory Warning", without its window. `simctl spawn … notifyutil -p
// UISimulatedMemoryWarningNotification` never reached the app (no `[trim]` line, T725 2026-11-14), the menu item needs a
// Simulator window, and driving the menu through System Events needs an Automation grant this Mac does not give a
// script. The menu item calls CoreSimulator's own `-[SimDevice simulateMemoryWarning]`; this JXA script calls the same
// method on the device named by its UDID (Foundation's bridge, no Apple events).
//
//   osascript -l JavaScript app/tools/testbed/lib/sim-memory-warning.js <udid> [--check]
//   --check: find the device and the method only (nothing sent). Prints one line; exits non-zero when it cannot.
ObjC.import('Foundation');
function run(argv) {
  const [udid, flag] = argv;
  if (!udid) throw new Error('usage: sim-memory-warning.js <udid> [--check]');
  const fw = $.NSBundle.bundleWithPath('/Library/Developer/PrivateFrameworks/CoreSimulator.framework');
  if (!fw || !fw.load) throw new Error('CoreSimulator.framework would not load');
  const task = $.NSTask.alloc.init;
  const pipe = $.NSPipe.pipe;
  task.launchPath = '/usr/bin/xcode-select';
  task.arguments = $(['-p']);
  task.standardOutput = pipe;
  task.launch; task.waitUntilExit;
  const dev = $.NSString.alloc.initWithDataEncoding(pipe.fileHandleForReading.readDataToEndOfFile, $.NSUTF8StringEncoding).js.trim();
  const Ctx = $.NSClassFromString("SimServiceContext");
  if (!Ctx || Ctx.isNil()) throw new Error("no SimServiceContext class in CoreSimulator");
  const ctx = Ctx.sharedServiceContextForDeveloperDirError(dev, null);
  if (!ctx || ctx.isNil()) throw new Error(`no CoreSimulator service context for ${dev}`);
  const set = ctx.defaultDeviceSetWithError(null);
  const all = set.devices.js;
  const d = all.find((x) => x.UDID.UUIDString.js === udid);
  if (!d) throw new Error(`no simulator ${udid} in the default device set (${all.length} devices)`);
  if (!d.respondsToSelector('simulateMemoryWarning')) throw new Error('SimDevice has no simulateMemoryWarning on this Xcode');
  const state = d.stateString.js;
  if (flag === '--check') return `ok: ${d.name.js} ${udid} (${state}); simulateMemoryWarning available`;
  if (state !== 'Booted') throw new Error(`${udid} is ${state}, not booted`);
  d.simulateMemoryWarning;
  return `sent: simulateMemoryWarning to ${d.name.js} ${udid}`;
}
