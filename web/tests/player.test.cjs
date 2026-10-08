"use strict";

const { test } = require("node:test");
const assert = require("node:assert/strict");
const { StreamPlayer, waitForICEGathering } = require("../static/js/app.js");

const flush = async () => {
  for (let index = 0; index < 12; index++) await Promise.resolve();
};

class FakePeer extends EventTarget {
  constructor(config) {
    super();
    this.config = config;
    this.iceGatheringState = "gathering";
    this.connectionState = "connected";
    this.iceConnectionState = "connected";
    this.transceivers = [];
    this.framesDecoded = 0;
    this.bytesReceived = 0;
  }
  addTransceiver(kind, options) {
    this.transceivers.push({ kind, ...options });
  }
  async createOffer() {
    return { type: "offer", sdp: "v=0\r\n" };
  }
  async setLocalDescription(offer) {
    this.localDescription = offer;
  }
  gather() {
    this.localDescription = { ...this.localDescription, sdp: "v=0\r\na=candidate:complete\r\n" };
    this.iceGatheringState = "complete";
    this.dispatchEvent(new Event("icegatheringstatechange"));
  }
  async setRemoteDescription(answer) {
    this.remoteDescription = answer;
  }
  async getStats() {
    return new Map([["video", {
      type: "inbound-rtp", kind: "video", framesDecoded: this.framesDecoded, bytesReceived: this.bytesReceived,
    }]]);
  }
  close() {
    this.closed = true;
    this.connectionState = "closed";
  }
}

function makePlayer(t) {
  const video = { currentTime: 0, paused: false, srcObject: null, play: async () => {} };
  const status = { textContent: "" };
  const player = new StreamPlayer(video, status, "camera165");
  t.after(() => player.stop());
  return player;
}

function fakeBrowser(t) {
  globalThis.RTCPeerConnection = FakePeer;
  globalThis.MediaStream = class {
    constructor() { this.tracks = []; }
    addTrack(track) { this.tracks.push(track); }
    getTracks() { return this.tracks; }
  };
  t.after(() => {
    delete globalThis.RTCPeerConnection;
    delete globalThis.MediaStream;
  });
}

test("non-trickle signaling waits for complete candidates and uses recvonly tracks", async (t) => {
  fakeBrowser(t);
  const requests = [];
  t.mock.method(globalThis, "fetch", async (path, options) => {
    requests.push({ path, options });
    return {
      ok: true,
      text: async () => path.includes("/codec/") ? '[{"Type":"video"}]' : btoa("v=0\r\nanswer"),
    };
  });
  const player = makePlayer(t);
  player.start();
  await flush();
  assert.equal(requests.length, 1, "SDP must not be posted until ICE completes");
  assert.deepEqual(player.pc.transceivers, [{ kind: "video", direction: "recvonly" }]);
  player.pc.gather();
  await flush();
  assert.equal(requests.length, 2);
  assert.equal(requests[1].path, "/stream/receiver/camera165");
  assert.equal(requests[1].options.body.get("suuid"), "camera165");
  assert.match(atob(requests[1].options.body.get("data")), /a=candidate:complete/);
  assert.equal(player.pc.remoteDescription.type, "answer");
  assert.equal(player.session, 1);
  assert.equal(player.reconnectCount, 0);
});

test("closing a session cancels ICE gathering and prevents a stale SDP POST", async (t) => {
  fakeBrowser(t);
  const request = t.mock.method(globalThis, "fetch", async () => ({ ok: true, text: async () => '[{"Type":"video"}]' }));
  const player = makePlayer(t);
  player.start();
  await flush();
  const oldPeer = player.pc;
  const signal = player.abortController.signal;
  player.stop();
  oldPeer.gather();
  await flush();
  assert.equal(signal.aborted, true);
  assert.equal(oldPeer.closed, true);
  assert.equal(request.mock.callCount(), 1);
  assert.equal(player.pc, null);
  assert.equal(player.reconnectCount, 0);
});

test("ICE gathering fails explicitly on timeout and respects abort", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const controller = new AbortController();
  const timeoutResult = assert.rejects(waitForICEGathering(new FakePeer(), controller.signal, 500), /timed out/);
  t.mock.timers.tick(500);
  await timeoutResult;
  const abortResult = assert.rejects(waitForICEGathering(new FakePeer(), controller.signal), { name: "AbortError" });
  controller.abort();
  await abortResult;
});

test("reconnect cleans up resources, coalesces failures, and caps backoff at 30 seconds", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
  const player = makePlayer(t);
  player.stopped = false;
  const calls = t.mock.method(player, "connect", () => {});
  const oldPeer = new FakePeer();
  player.pc = oldPeer;
  let trackStopped = false;
  player.video.srcObject = { getTracks: () => [{ stop: () => { trackStopped = true; } }] };
  player.abortController = new AbortController();
  const oldSignal = player.abortController.signal;
  player.reconnect("Failure");
  player.reconnect("Duplicate failure");
  assert.equal(player.reconnectCount, 1);
  assert.equal(oldPeer.closed, true);
  assert.equal(trackStopped, true);
  assert.equal(oldSignal.aborted, true);
  assert.equal(player.video.srcObject, null);
  t.mock.timers.tick(999);
  assert.equal(calls.mock.callCount(), 0);
  t.mock.timers.tick(1);
  assert.equal(calls.mock.callCount(), 1);
  player.retryAttempt = 100;
  player.reconnect("Still unavailable");
  t.mock.timers.tick(29999);
  assert.equal(calls.mock.callCount(), 1);
  t.mock.timers.tick(1);
  assert.equal(calls.mock.callCount(), 2);
});

test("long first keyframe wait has a full minute of grace, then retries if no media arrives", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "setInterval", "Date"], now: 0 });
  const player = makePlayer(t);
  player.stopped = false;
  const peer = new FakePeer();
  player.pc = peer;
  player.monitor(peer, true, () => !player.stopped && player.pc === peer);
  t.mock.timers.tick(59000);
  await flush();
  assert.equal(player.reconnectCount, 0);
  t.mock.timers.tick(3000);
  await flush();
  assert.equal(player.reconnectCount, 1);
  assert.match(player.status.textContent, /No media received/);
});

test("healthy decoded frames remain live beyond timeout; bytes alone cannot hide a frozen picture", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "setInterval", "Date"], now: 0 });
  const player = makePlayer(t);
  player.stopped = false;
  const peer = new FakePeer();
  player.pc = peer;
  player.monitor(peer, true, () => !player.stopped && player.pc === peer);
  for (let step = 0; step < 40; step++) {
    peer.framesDecoded += 50;
    peer.bytesReceived += 10000;
    t.mock.timers.tick(2000);
    await flush();
  }
  assert.equal(player.reconnectCount, 0);
  assert.equal(player.status.textContent, "Live");
  peer.bytesReceived += 1000000;
  t.mock.timers.tick(22000);
  await flush();
  assert.equal(player.reconnectCount, 1);
  assert.match(player.status.textContent, /Playback stalled/);
});

test("deliberately paused video does not trigger stall recovery", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "setInterval", "Date"], now: 0 });
  const player = makePlayer(t);
  player.stopped = false;
  const peer = new FakePeer();
  peer.framesDecoded = 50;
  player.pc = peer;
  player.monitor(peer, true, () => !player.stopped && player.pc === peer);
  t.mock.timers.tick(2000);
  await flush();
  player.video.paused = true;
  t.mock.timers.tick(90000);
  await flush();
  assert.equal(player.reconnectCount, 0);
});
