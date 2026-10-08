package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
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
