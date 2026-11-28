import { describe, expect, it } from "vitest";
import { isNoWebGL, mapUnavailableWords, noWebGLError } from "./mapUnavailable";

/**
 * What MapLibre 6's `_setupPainter` throws (src/util/gpu_initialization_error.ts): a `GPUInitializationError` whose
 * `statusMessage` is the browser's reason from its `webglcontextcreationerror`, or null when the browser gave none.
 */
const gpuError = (statusMessage: string | null) => Object.assign(
  new Error("WebGL2 is required to display this map. We are sorry, but it seems that your browser does not support WebGL2, "
            + "a technology for rendering 3D graphics on the web. Read more on https://wiki.openstreetmap.org/wiki/This_map_requires_WebGL"),
  { name: "GPUInitializationError", statusMessage,
    requestedAttributes: { alpha: true, antialias: false, depth: true, stencil: true, premultipliedAlpha: true } });
const bare = gpuError(null);
const detailed = gpuError("Could not create a WebGL2 context, VENDOR = 0xffff, DEVICE = 0xffff, Sandboxed = no");

describe("T1005 no WebGL", () => {
  it("is told from MapLibre's GPUInitializationError, and nothing else", () => {
    expect(isNoWebGL(bare)).toBe(true);
    expect(isNoWebGL(detailed)).toBe(true);
    expect(isNoWebGL(new Error("Wrong magic number for PMTiles archive"))).toBe(false);
    expect(isNoWebGL(new Error("WebGL2 is required to display this map."))).toBe(false);
    expect(isNoWebGL("GPUInitializationError")).toBe(false);
  });
  it("is reported as one stable message, the browser's reason unquoted (the scrubber keeps it), the cause kept", () => {
    const e = noWebGLError(detailed);
    expect(e.name).toBe("MapUnavailable");
    expect(e.message).toBe("Failed to initialize WebGL (Could not create a WebGL2 context, VENDOR = 0xffff, DEVICE = 0xffff, Sandboxed = no)");
    expect(e.cause).toBe(detailed);
    expect(noWebGLError(bare).message).toBe("Failed to initialize WebGL");
    expect(noWebGLError(gpuError(`it said "no"`)).message).toBe("Failed to initialize WebGL (it said no)");
  });
});

describe("T1252 what a map that cannot draw says, by reason (the whole words, rule 33)", () => {
  it("no WebGL: the browser's settings", () => {
    const w = mapUnavailableWords("webgl");
    expect(`${w.title}. ${w.body}`).toBe("This browser cannot draw the map. The map needs WebGL, which is turned off or not available in "
      + "this browser. Turn on hardware acceleration in the browser's settings, or open the map in another browser. Search and "
      + "the regulations pages still work.");
  });
  it("GL's worker did not load: the site's fault, never the WebGL words", () => {
    const w = mapUnavailableWords("worker");
    expect(`${w.title}. ${w.body}`).toBe("The map could not load. Reload the page; search and the regulations pages still work.");
    expect(w.body).not.toMatch(/WebGL|hardware/);
  });
});
