"use strict";

class StreamRecording {
  constructor(elements, streamID) {
    this.elements = elements;
    this.path = `/stream/recording/${encodeURIComponent(streamID)}`;
    this.running = false;
    this.generation = 0;
    this.pending = false;
    this.known = false;
    this.data = null;
    this.message = "";
    this.pollTimer = null;
    this.requestTimer = null;
    this.controller = null;
    this.failures = 0;
    this.render();
  }

  resume() {
    if (this.running) return;
    this.running = true;
    return this.update();
  }

  // Leaving the page cancels status requests only. Recording belongs to the server.
  pause() {
    this.running = false;
    this.generation++;
    clearTimeout(this.pollTimer);
    clearTimeout(this.requestTimer);
    this.pollTimer = null;
    this.requestTimer = null;
    this.controller?.abort();
    this.controller = null;
    this.pending = false;
    this.known = false;
    this.render();
  }

  toggle() {
    if (!this.running || !this.known || this.pending || this.transitioning()) return;
    return this.update(this.data.active ? "stop" : "start");
  }

  transitioning() {
    return this.data?.state === "stopping";
  }

  async update(action) {
    if (!this.running || this.pending) return;
    clearTimeout(this.pollTimer);
    this.pollTimer = null;
    this.pending = true;
    this.message = "";
    const generation = this.generation;
    const controller = new AbortController();
    this.controller = controller;
    let timedOut = false;
    this.requestTimer = setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, 10000);
    this.render(action);

    try {
      const response = await fetch(action ? `${this.path}/${action}` : this.path, {
        method: action ? "POST" : "GET",
        cache: "no-store",
        signal: controller.signal,
        headers: { Accept: "application/json" },
      });
      let data;
      try {
        data = await response.json();
      } catch (error) {
        if (controller.signal.aborted) throw error;
        throw new Error(response.ok ? "Invalid server response" : `HTTP error ${response.status}`);
      }
      if (!response.ok) throw new Error(typeof data?.error === "string" ? data.error : `HTTP error ${response.status}`);
      if (typeof data?.active !== "boolean" || !["idle", "starting", "recording", "reconnecting", "stopping", "stopped", "error"].includes(data.state)) {
        throw new Error("Invalid recording status");
      }
      if (!this.running || generation !== this.generation) return;
      this.data = data;
      this.known = true;
      this.failures = 0;
    } catch (error) {
      if (!this.running || generation !== this.generation) return;
      this.known = false;
      this.failures++;
      const detail = timedOut ? "The server did not respond within 10 seconds" : error.message || "Server connection unavailable";
      this.message = `Unable to check recording status: ${detail}. Retrying automatically.`;
    } finally {
      if (generation === this.generation) {
        clearTimeout(this.requestTimer);
        this.requestTimer = null;
        this.controller = null;
        this.pending = false;
        this.render();
        if (this.running) {
          const delay = Math.min(2000 * 2 ** Math.min(this.failures, 3), 10000);
          this.pollTimer = setTimeout(() => this.update(), delay);
        }
      }
    }
  }

  render(action) {
    const { button, status, mode, files, callback } = this.elements;
    const data = this.data;
    button.disabled = !this.running || !this.known || this.pending || this.transitioning();
    button.textContent = data?.active ? "Stop recording" : "Start recording";
    button.className = data?.active ? "btn btn-danger" : "btn btn-primary";
    if (action) {
      status.textContent = action === "start" ? "Starting recording…" : "Finalizing MP4 files…";
    } else if (this.message) {
      status.textContent = this.message;
    } else if (!this.known) {
      status.textContent = "Checking recording status…";
    } else {
      const labels = {
        idle: "Recording is stopped.",
        stopped: "Recording finished. Files saved to the shared host folder.",
        starting: "Waiting for the first keyframe to start recording…",
        recording: "Recording in progress.",
        reconnecting: "Video stream interrupted. Recording remains active and will resume automatically.",
        stopping: "Finalizing MP4 files…",
        error: "Recording interrupted.",
      };
      status.textContent = labels[data.state];
      if (data.state === "reconnecting") {
        if (data.retry_count) status.textContent += ` Recovery attempt: ${data.retry_count}.`;
        if (data.last_retry_error) status.textContent += ` ${data.last_retry_error}`;
      }
      if (data.error) status.textContent += ` ${data.error}`;
      if (data.note) status.textContent += ` ${data.note}`;
    }
    if (!data) return;
    mode.textContent = data.chunk_seconds > 0
      ? `Mode: files split approximately every ${data.chunk_seconds} seconds, at the next keyframe.`
      : "Mode: one MP4 file until recording stops.";
    const completed = Array.isArray(data.files) ? data.files : [];
    files.textContent = `Files completed in this session: ${completed.length}.`;
    if (data.current_file) files.textContent += ` Current file: ${data.current_file}`;
    else if (completed.length) files.textContent += ` Last file: ${completed[completed.length - 1]}`;
    if (callback) {
      const job = data.callback;
      callback.textContent = job?.enabled
        ? `Chunk processing enabled: ${job.completed || 0} completed, ${job.pending || 0} pending, ${job.failed || 0} failed.`
        : "Automatic chunk processing is disabled.";
      if (job?.last_error) callback.textContent += ` Last error: ${job.last_error}`;
      callback.className = job?.failed ? "small mb-2 text-danger" : "small mb-2 text-secondary";
    }
  }
}

if (typeof module !== "undefined" && module.exports) {
  module.exports = { StreamRecording };
}

if (typeof window !== "undefined") {
  const button = document.getElementById("recordingButton");
  if (button) {
    const recording = new StreamRecording({
      button,
      status: document.getElementById("recordingStatus"),
      mode: document.getElementById("recordingMode"),
      files: document.getElementById("recordingFiles"),
      callback: document.getElementById("recordingCallback"),
    }, document.getElementById("suuid").value);
    window.recording = recording;
    button.addEventListener("click", () => recording.toggle());
    window.addEventListener("pagehide", () => recording.pause());
    window.addEventListener("pageshow", () => recording.resume());
    recording.resume();
  }
}
