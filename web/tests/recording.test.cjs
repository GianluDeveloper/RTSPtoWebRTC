"use strict";

const { test } = require("node:test");
const assert = require("node:assert/strict");
const { StreamRecording } = require("../static/js/recording.js");

const flush = async () => {
  for (let index = 0; index < 12; index++) await Promise.resolve();
};
const state = (changes = {}) => ({ active: false, state: "idle", chunk_seconds: 0, files: [], ...changes });
const response = (body, status = 200) => ({ ok: status < 400, status, json: async () => body });

function makeRecording(t) {
  const elements = Object.fromEntries(["button", "status", "mode", "files", "callback"].map((name) => [name, { textContent: "" }]));
  const recording = new StreamRecording(elements, "example stream");
  t.after(() => recording.pause());
  return recording;
}

test("initial server state controls recording mode, shared state, and output filenames", async (t) => {
  t.mock.method(globalThis, "fetch", async () => response(state({
    active: true, state: "recording", chunk_seconds: 30,
    files: ["example/part-1.mp4"], current_file: "example/part-2.mp4",
  })));
  const recording = makeRecording(t);
  assert.equal(recording.elements.button.disabled, true);
  await recording.resume();
  assert.equal(recording.elements.button.disabled, false);
  assert.equal(recording.elements.button.textContent, "Stop recording");
  assert.match(recording.elements.mode.textContent, /approximately every 30 seconds/);
  assert.match(recording.elements.files.textContent, /Files completed in this session: 1/);
  assert.match(recording.elements.files.textContent, /part-2\.mp4/);
});

test("start and stop use explicit POST actions and prevent duplicate clicks", async (t) => {
  const requests = [];
  let finishStart;
  t.mock.method(globalThis, "fetch", async (path, options) => {
    requests.push({ path, options });
    if (path.endsWith("/start")) return new Promise((resolve) => { finishStart = resolve; });
    if (path.endsWith("/stop")) return response(state({ state: "stopped", files: ["example/recording.mp4"] }));
    return response(state());
  });
  const recording = makeRecording(t);
  await recording.resume();
  const starting = recording.toggle();
  recording.toggle();
  assert.equal(recording.elements.button.disabled, true);
  assert.equal(requests.length, 2);
  assert.equal(requests[1].path, "/stream/recording/example%20stream/start");
  assert.equal(requests[1].options.method, "POST");
  finishStart(response(state({ active: true, state: "starting" })));
  await starting;
  assert.equal(recording.elements.button.disabled, false, "waiting for a keyframe can be cancelled");
  await recording.toggle();
  assert.equal(requests[2].path, "/stream/recording/example%20stream/stop");
  assert.equal(requests[2].options.method, "POST");
  assert.equal(recording.elements.button.textContent, "Start recording");
  assert.match(recording.elements.files.textContent, /Last file: example\/recording.mp4/);
});

test("polls reflect another viewer's actions, allow cancelling startup, and disable stopping", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const states = [state(), state({ active: true, state: "starting" }), state({ active: true, state: "recording" }), state({ active: true, state: "stopping" })];
  const requests = t.mock.method(globalThis, "fetch", async () => response(states.shift()));
  const recording = makeRecording(t);
  await recording.resume();
  t.mock.timers.tick(2000);
  await flush();
  assert.equal(recording.elements.button.disabled, false);
  assert.equal(recording.elements.button.textContent, "Stop recording");
  assert.match(recording.elements.status.textContent, /first keyframe/);
  t.mock.timers.tick(2000);
  await flush();
  assert.equal(recording.elements.button.disabled, false);
  assert.equal(recording.elements.button.textContent, "Stop recording");
  t.mock.timers.tick(2000);
  await flush();
  assert.equal(recording.elements.button.disabled, true);
  assert.equal(requests.mock.callCount(), 4);
});

test("requests never overlap, pause aborts polling without stopping recording, and resume refreshes", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let finishOld;
  const requests = [];
  t.mock.method(globalThis, "fetch", (path, options) => {
    requests.push({ path, options });
    if (requests.length === 1) return new Promise((resolve) => { finishOld = resolve; });
    return Promise.resolve(response(state({ active: true, state: "recording" })));
  });
  const recording = makeRecording(t);
  const oldRequest = recording.resume();
  recording.resume();
  recording.update();
  t.mock.timers.tick(6000);
  await flush();
  assert.equal(requests.length, 1);
  recording.pause();
  assert.equal(requests[0].options.signal.aborted, true);
  t.mock.timers.tick(100000);
  assert.equal(requests.length, 1);
  await recording.resume();
  finishOld(response(state()));
  await oldRequest;
  assert.equal(recording.data.active, true, "an old request must not replace the new page state");
  assert.ok(requests.every(({ options }) => options.method === "GET"), "page lifecycle never sends stop");
  assert.equal(requests.length, 2);
});

test("failed requests show readable errors and retry with capped backoff before recovering", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let healthy = false;
  const requests = t.mock.method(globalThis, "fetch", async () => healthy
    ? response(state())
    : response({ error: "Not enough disk space" }, 500));
  const recording = makeRecording(t);
  await recording.resume();
  assert.equal(recording.elements.button.disabled, true);
  assert.match(recording.elements.status.textContent, /Not enough disk space/);
  for (const delay of [4000, 8000, 10000, 10000]) {
    const count = requests.mock.callCount();
    t.mock.timers.tick(delay - 1);
    await flush();
    assert.equal(requests.mock.callCount(), count);
    t.mock.timers.tick(1);
    await flush();
    assert.equal(requests.mock.callCount(), count + 1);
  }
  healthy = true;
  t.mock.timers.tick(10000);
  await flush();
  assert.equal(recording.elements.button.disabled, false);
  assert.equal(recording.elements.status.textContent, "Recording is stopped.");
});

test("request timeout cancels a hung response body and eventually retries", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let signal;
  t.mock.method(globalThis, "fetch", async (_path, options) => {
    signal = options.signal;
    return {
      ok: true,
      json: () => new Promise((_resolve, reject) => {
        signal.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")), { once: true });
      }),
    };
  });
  const recording = makeRecording(t);
  const request = recording.resume();
  await flush();
  t.mock.timers.tick(10000);
  await request;
  assert.equal(signal.aborted, true);
  assert.match(recording.elements.status.textContent, /10 seconds/);
  assert.equal(recording.pending, false);
  assert.notEqual(recording.pollTimer, null);
});

test("malformed status is rejected and backend failure states can be restarted", async (t) => {
  const replies = [response({ active: true }), response(state({ state: "error", error: "Stream unavailable" }))];
  t.mock.method(globalThis, "fetch", async () => replies.shift());
  const recording = makeRecording(t);
  await recording.resume();
  assert.equal(recording.elements.button.disabled, true);
  assert.match(recording.elements.status.textContent, /Invalid recording status/);
  await recording.update();
  assert.equal(recording.elements.button.disabled, false);
  assert.match(recording.elements.status.textContent, /Stream unavailable/);
});

test("callback errors remain visible without blocking recording controls", async (t) => {
  t.mock.method(globalThis, "fetch", async () => response(state({
    active: true, state: "recording", chunk_seconds: 30,
    callback: { enabled: true, completed: 3, pending: 1, failed: 1, last_error: "Script timeout" },
  })));
  const recording = makeRecording(t);
  await recording.resume();
  assert.equal(recording.elements.button.disabled, false);
  assert.equal(recording.elements.button.textContent, "Stop recording");
  assert.match(recording.elements.callback.textContent, /3 completed, 1 pending, 1 failed/);
  assert.match(recording.elements.callback.textContent, /Script timeout/);
});
