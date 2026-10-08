"use strict";

// The signaling API exchanges a complete SDP once, without trickle ICE.
function waitForICEGathering(pc, signal, timeoutMs = 15000) {
  return new Promise((resolve, reject) => {
    let timer;
    const finish = (error) => {
      clearTimeout(timer);
      pc.removeEventListener("icegatheringstatechange", onStateChange);
      signal.removeEventListener("abort", onAbort);
      error ? reject(error) : resolve();
    };
    const onStateChange = () => {
      if (pc.iceGatheringState === "complete") finish();
    };
    const onAbort = () => finish(new DOMException("Connection cancelled", "AbortError"));
    pc.addEventListener("icegatheringstatechange", onStateChange);
    signal.addEventListener("abort", onAbort, { once: true });
    timer = setTimeout(() => finish(new Error("ICE gathering timed out")), timeoutMs);
    if (signal.aborted) onAbort();
    else onStateChange();
  });
}

class StreamPlayer {
  constructor(video, status, streamID, config = {}) {
    this.video = video;
    this.status = status;
    this.streamID = streamID;
    this.config = { iceServers: Array.isArray(config.iceServers) ? config.iceServers : [] };
    this.pc = null;
    this.session = 0;
    this.reconnectCount = 0;
    this.retryAttempt = 0;
    this.stopped = true;
    this.retryTimer = null;
    this.healthTimer = null;
    this.abortController = null;
    this.stats = {};
  }

  get currentTime() {
    return this.video.currentTime;
  }

  setStatus(message) {
    this.status.textContent = message;
  }

  start() {
    if (!this.stopped) return;
    this.stopped = false;
    this.connect();
  }

  stop() {
    this.stopped = true;
    clearTimeout(this.retryTimer);
    this.retryTimer = null;
    this.closeSession();
  }

  closeSession() {
    clearInterval(this.healthTimer);
    this.healthTimer = null;
    this.abortController?.abort();
    this.abortController = null;
    const pc = this.pc;
    this.pc = null;
    if (pc) {
      pc.ontrack = null;
      pc.onconnectionstatechange = null;
      pc.oniceconnectionstatechange = null;
      pc.close();
    }
    if (this.video.srcObject) {
      for (const track of this.video.srcObject.getTracks()) track.stop();
      this.video.srcObject = null;
    }
  }

  reconnect(reason) {
    if (this.stopped || this.retryTimer !== null) return;
    this.closeSession();
    this.reconnectCount++;
    const delay = Math.min(1000 * 2 ** Math.min(this.retryAttempt++, 5), 30000);
    this.setStatus(`${reason}. Reconnecting in ${delay / 1000}s…`);
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this.connect();
    }, delay);
  }

  async request(path, options, signal) {
    // Abort both reads and pending response bodies on timeout or session cleanup.
    const requestSignal = AbortSignal.any([signal, AbortSignal.timeout(20000)]);
    const response = await fetch(path, { ...options, signal: requestSignal, cache: "no-store" });
    if (!response.ok) throw new Error(`Stream request failed (${response.status})`);
    return response.text();
  }

  async connect() {
    if (this.stopped) return;
    this.closeSession();
    const session = ++this.session;
    const controller = new AbortController();
    this.abortController = controller;
    const { signal } = controller;
    const isCurrent = () => !this.stopped && !signal.aborted && session === this.session;
    this.setStatus("Connecting…");
    this.stats = {};

    try {
      const pc = new RTCPeerConnection(this.config);
      this.pc = pc;
      const media = new MediaStream();
      this.video.srcObject = media;
      pc.ontrack = ({ track }) => {
        if (!isCurrent()) return;
        media.addTrack(track);
        track.addEventListener("ended", () => {
          if (isCurrent()) this.reconnect("Media track ended");
        }, { once: true });
        this.video.play().catch(() => {
          if (isCurrent()) this.setStatus("Press play to start playback");
        });
      };
      const onConnectionChange = () => {
        if (!isCurrent()) return;
        if (pc.connectionState === "failed" || pc.iceConnectionState === "failed") {
          this.reconnect("Connection lost");
        }
      };
      pc.onconnectionstatechange = onConnectionChange;
      pc.oniceconnectionstatechange = onConnectionChange;

      const codecs = JSON.parse(await this.request(`/stream/codec/${encodeURIComponent(this.streamID)}`, {}, signal));
      if (!isCurrent()) return;
      if (!Array.isArray(codecs) || codecs.length === 0) throw new Error("No supported media tracks");
      for (const codec of codecs) {
        if (codec.Type !== "audio" && codec.Type !== "video") throw new Error("Unsupported media track");
        pc.addTransceiver(codec.Type, { direction: "recvonly" });
      }
      const hasVideo = codecs.some((codec) => codec.Type === "video");
      await pc.setLocalDescription(await pc.createOffer());
      await waitForICEGathering(pc, signal);
      if (!isCurrent()) return;
      const answer = await this.request(`/stream/receiver/${encodeURIComponent(this.streamID)}`, {
        method: "POST",
        body: new URLSearchParams({ suuid: this.streamID, data: btoa(pc.localDescription.sdp) }),
      }, signal);
      if (!isCurrent()) return;
      await pc.setRemoteDescription({ type: "answer", sdp: atob(answer.trim()) });
      if (!isCurrent()) return;
      this.setStatus("Connected. Waiting for media…");
      this.monitor(pc, hasVideo, isCurrent);
    } catch (error) {
      if (isCurrent()) this.reconnect(error.message || "Unable to connect");
    }
  }

  monitor(pc, hasVideo, isCurrent) {
    const started = Date.now();
    let lastActivity = started;
    let lastProgress = 0;
    let receivedMedia = false;
    let checking = false;
    let disconnectedAt = null;
    this.healthTimer = setInterval(async () => {
      if (!isCurrent() || checking) return;
      checking = true;
      try {
        const reports = await pc.getStats();
        if (!isCurrent()) return;
        const now = Date.now();
        let framesDecoded = 0;
        let hasFrameStats = false;
        let bytesReceived = 0;
        for (const report of reports.values()) {
          if (report.type !== "inbound-rtp") continue;
          bytesReceived += report.bytesReceived || 0;
          if ((report.kind || report.mediaType) === "video" && typeof report.framesDecoded === "number") {
            hasFrameStats = true;
            framesDecoded += report.framesDecoded;
          }
        }
        this.stats = { framesDecoded, bytesReceived, currentTime: this.video.currentTime };
        const progress = hasVideo ? (hasFrameStats ? framesDecoded : this.video.currentTime) : bytesReceived;
        if (progress > lastProgress) {
          receivedMedia = true;
          lastActivity = now;
          this.setStatus("Live");
          // Keep backoff across short-lived failures, reset after a stable minute.
          if (now - started >= 60000) this.retryAttempt = 0;
        }
        lastProgress = progress;
        // Pausing the video is a user action, not a transport failure.
        if (this.video.paused && receivedMedia) lastActivity = now;
        const disconnected = pc.connectionState === "disconnected" || pc.iceConnectionState === "disconnected";
        disconnectedAt = disconnected ? (disconnectedAt ?? now) : null;
        if (disconnectedAt !== null && now - disconnectedAt >= 10000) {
          this.reconnect("Connection interrupted");
        } else if (now - lastActivity > (receivedMedia ? 20000 : 60000)) {
          this.reconnect(receivedMedia ? "Playback stalled" : "No media received");
        }
      } catch (error) {
        if (isCurrent()) this.reconnect("Unable to monitor playback");
      } finally {
        checking = false;
      }
    }, 2000);
  }
}

if (typeof module !== "undefined" && module.exports) {
  module.exports = { StreamPlayer, waitForICEGathering };
}

if (typeof window !== "undefined") {
  const video = document.getElementById("videoElem");
  if (video) {
    const streamID = document.getElementById("suuid").value;
    for (const link of document.querySelectorAll("[data-stream-id]")) {
      if (link.dataset.streamId === streamID) {
        link.classList.add("active");
        link.setAttribute("aria-current", "page");
      }
    }
    window.player = new StreamPlayer(video, document.getElementById("playerStatus"), streamID, window.streamPlayerConfig);
    if (typeof RTCPeerConnection === "undefined") {
      window.player.setStatus("This browser does not support WebRTC");
    } else {
      window.addEventListener("pagehide", () => window.player.stop());
      window.addEventListener("pageshow", () => window.player.start());
      window.player.start();
    }
  }
}
