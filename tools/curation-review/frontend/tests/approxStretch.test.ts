// T532 round 3: the approximate stretch drawn in the app for a draft whose cut is not built yet.
import { test } from "node:test";
import assert from "node:assert/strict";
import { approxBetween } from "../src/approxStretch.ts";

const line = (coords: number[][]): GeoJSON.Feature => ({ type: "Feature", properties: {}, geometry: { type: "LineString", coordinates: coords } });
// two sections sharing an end, and a side channel that leaves and rejoins the longer way
const FC: GeoJSON.FeatureCollection = { type: "FeatureCollection", features: [
  line([[0, 0], [0.01, 0], [0.02, 0]]), line([[0.04, 0], [0.03, 0], [0.02, 0]]),
  line([[0.01, 0], [0.02, 0.02], [0.03, 0]]), line([[1, 1], [1.01, 1]])] };

test("the stretch walks the water's own lines between the vertices nearest the two points, the short way", () => {
  const got = approxBetween(FC, [0.0101, 0.0001], [0.0399, 0]);
  assert.ok("line" in got);
  assert.deepEqual(got.line, [[0.01, 0], [0.02, 0], [0.03, 0], [0.04, 0]]);
  assert.ok(got.km > 3.2 && got.km < 3.5);
});
test("it says why when it cannot: a point off the water, lines that do not join, no lines", () => {
  assert.match((approxBetween(FC, [0, 0], [0.5, 0.5]) as { why: string }).why, /m from this water's lines/);
  assert.match((approxBetween(FC, [0, 0], [1.01, 1]) as { why: string }).why, /do not join up/);
  assert.match((approxBetween(null, [0, 0], [1, 1]) as { why: string }).why, /not on the map yet/);
});
