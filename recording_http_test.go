package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/deepch/vdk/av"
)

func recordingTestRouter(t *testing.T, options RecordingOptions) http.Handler {
	t.Helper()
	previousConfig, previousManager, previousCallbacks := Config, Recordings, RecordingCallbacks
	Config = &ConfigST{Streams: map[string]StreamST{"example": {URL: "rtsp://camera.example/live", Recording: options, RunLock: true, Cl: make(map[string]viewer)}}}
	Recordings = NewRecordingManager(Config, t.TempDir())
	RecordingCallbacks = NewRecordingCallbackRunner(Config)
	Recordings.OnChunk = RecordingCallbacks.Submit
	t.Cleanup(func() {
		Recordings.StopAll()
		RecordingCallbacks.Close()
		Config, Recordings, RecordingCallbacks = previousConfig, previousManager, previousCallbacks
	})
	return newRouter()
}

func TestRecordingHTTPStatusAndUnknownStream(t *testing.T) {
	router := recordingTestRouter(t, RecordingOptions{ChunkSeconds: 30})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest("GET", "/stream/recording/example", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status %d", response.Code)
	}
	var status recordingAPIStatus
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.Active || status.State != "idle" || status.ChunkSeconds != 30 || status.Callback.Enabled {
		t.Fatalf("unexpected status: %+v", status)
	}
	for _, action := range []string{"", "/start", "/stop"} {
		method := "POST"
		if action == "" {
			method = "GET"
		}
		response = httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(method, "/stream/recording/unknown"+action, nil))
		if response.Code != http.StatusNotFound {
			t.Fatalf("unknown %s status %d", action, response.Code)
		}
	}
}

func TestRecordingHTTPRejectsBadDurationAndReturnsError(t *testing.T) {
	router := recordingTestRouter(t, RecordingOptions{ChunkSeconds: -1})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest("POST", "/stream/recording/example/start", nil))
	if response.Code != http.StatusConflict {
		t.Fatalf("got %d", response.Code)
	}
	var status recordingAPIStatus
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.Error == "" || status.Active || status.State != "error" {
		t.Fatalf("missing error: %+v", status)
	}
}

func TestRecordingHTTPCancelsPendingSourceAndStopsIdempotently(t *testing.T) {
	router := recordingTestRouter(t, RecordingOptions{})
	for _, action := range []string{"start", "stop", "stop"} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest("POST", "/stream/recording/example/"+action, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("%s: status %d, %s", action, response.Code, response.Body.String())
		}
	}
	if Recordings.Status("example").Active {
		t.Fatal("recorder did not stop while waiting for source")
	}
}

func TestRecordingHTTPKeepsRecoveringSessionActiveAndCancellable(t *testing.T) {
	router := recordingTestRouter(t, RecordingOptions{})
	codec, packets := recordingFixture(t)
	Config.coAd("example", []av.CodecData{codec})
	request := func(method, action string) recordingAPIStatus {
		t.Helper()
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(method, "/stream/recording/example"+action, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("%s %s: status %d, %s", method, action, response.Code, response.Body.String())
		}
		var status recordingAPIStatus
		if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
			t.Fatal(err)
		}
		return status
	}
	started := request("POST", "/start")
	awaitRecording(t, func() bool { return Config.HasViewer("example") })
	for _, packet := range packets {
		Config.cast("example", packet)
	}
	awaitRecording(t, func() bool { return Recordings.Status("example").State == "recording" })
	Config.sourceDisconnected("example", ErrSourceDisconnected)
	awaitRecording(t, func() bool { return Recordings.Status("example").State == "reconnecting" })
	status := request("GET", "")
	if !status.Active || status.State != "reconnecting" || status.Error != "" {
		t.Fatalf("transient outage ended the recording session: %+v", status)
	}
	duplicate := request("POST", "/start")
	if duplicate.StartedAt == nil || started.StartedAt == nil || !duplicate.StartedAt.Equal(*started.StartedAt) {
		t.Fatal("start created another session during recovery")
	}
	now := time.Now()
	stopped := request("POST", "/stop")
	if time.Since(now) > 2*time.Second || stopped.Active || stopped.State != "stopped" || stopped.Error != "" {
		t.Fatalf("stop did not cancel camera recovery promptly: %+v", stopped)
	}
	if len(stopped.Files) != 1 || stopped.Callback.Completed != 0 {
		t.Fatalf("single-file recording was not finalized correctly after cancellation: %+v", stopped)
	}
}
