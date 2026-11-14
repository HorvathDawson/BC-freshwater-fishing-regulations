// (T734) the one Maestro failure that is run again: its driver never started (no command of the flow ran)
import assert from 'node:assert/strict';
import test from 'node:test';
import { driverNeverStarted } from './maestro.mjs';

test('the driver that did not start up is told apart from a failed flow', () => {
  assert.equal(driverNeverStarted('Maestro Android driver did not start up in time on emulator [  ] (driver port 57965)'), true);
  assert.equal(driverNeverStarted('maestro.MaestroDriverStartupException$AndroidDriverTimeoutException: x'), true);
  assert.equal(driverNeverStarted('Element not found: Id matching regex: water-close'), false);
  assert.equal(driverNeverStarted('Assertion is false: "Open .*" is visible'), false);
});
