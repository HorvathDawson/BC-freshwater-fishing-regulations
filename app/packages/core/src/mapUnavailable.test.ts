import { describe, expect, it } from "vitest";
import { isNoWebGL, noWebGLError } from "./mapUnavailable";

/** what MapLibre 5's `_setupPainter` throws: the bare words, or JSON with the context's details (crash 1bd22eea73a9dfb8) */
const bare = new Error("Failed to initialize WebGL");
const detailed = new Error(JSON.stringify({
  requestedAttributes: { alpha: false, antialias: false, depth: true, failIfMajorPerformanceCaveat: false, powerPreference: "high-performance",
                         premultipliedAlpha: true, preserveDrawingBuffer: false, stencil: true, desynchronized: true },
  statusMessage: "Could not create a WebGL2 context, VENDOR = 0xffff, DEVICE = 0xffff, Sandboxed = no", type: "webglcontextcreationerror",
  message: "Failed to initialize WebGL",
}));

describe("T1005 no WebGL", () => {
  it("is told from MapLibre's two shapes, and nothing else", () => {
    expect(isNoWebGL(bare)).toBe(true);
    expect(isNoWebGL(detailed)).toBe(true);
    expect(isNoWebGL(new Error("Wrong magic number for PMTiles archive"))).toBe(false);
    expect(isNoWebGL("Failed to initialize WebGL")).toBe(false);
  });
  it("is reported as one stable message, the browser's reason unquoted (the scrubber keeps it), the cause kept", () => {
    const e = noWebGLError(detailed);
    expect(e.name).toBe("MapUnavailable");
    expect(e.message).toBe("Failed to initialize WebGL (webglcontextcreationerror: Could not create a WebGL2 context, VENDOR = 0xffff, DEVICE = 0xffff, Sandboxed = no)");
    expect(e.cause).toBe(detailed);
    expect(noWebGLError(bare).message).toBe("Failed to initialize WebGL");
  });
});
