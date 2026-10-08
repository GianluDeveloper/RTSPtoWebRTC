package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func callbackTestRunner(t *testing.T, body string) (*RecordingCallbackRunner, RecordingChunk) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("callback integration test requires a POSIX shell")
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "on-chunk.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nset -eu\n"+body+"\n"), 0755); err != nil {
		t.Fatal(err)
	}
	chunk := RecordingChunk{
		StreamID: "test-camera", Path: filepath.Join(dir, "chunk.mp4"), RelativePath: "test-camera/chunk.mp4",
		StartedAt: time.Now().UTC(), Duration: 10250 * time.Millisecond, Sequence: 7, ChunkSeconds: 10, Final: true,
	}
	if err := os.WriteFile(chunk.Path, []byte("completed test file"), 0640); err != nil {
		t.Fatal(err)
	}
	config := &ConfigST{
		Server: ServerST{RecordingCallbackScript: script, RecordingCallbackTimeoutSeconds: 60},
		Streams: map[string]StreamST{
			chunk.StreamID: {URL: "rtsp://viewer:USERINFO_SECRET@camera.example:8554/password=PATH_SECRET/channel=0?token=QUERY_SECRET", Recording: RecordingOptions{ChunkSeconds: 10, CallbackEnabled: true}},
		},
	}
	runner := NewRecordingCallbackRunner(config)
	t.Cleanup(runner.Close)
	return runner, chunk
}

func waitCallbackStatus(t *testing.T, runner *RecordingCallbackRunner, streamID string, done func(RecordingCallbackStatus) bool) RecordingCallbackStatus {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		status := runner.Status(streamID)
		if done(status) {
			return status
		}
		if time.Now().After(deadline) {
			t.Fatalf("callback did not finish: %+v", status)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestRecordingCallbackReceivesFinalizedChunkAndSafeMetadata(t *testing.T) {
	runner, chunk := callbackTestRunner(t, "test -f \"$1\"\nprintf '%s' \"$1\" > \"$1.argument\"\ncat > \"$1.metadata\"")
	runner.Submit(chunk)
	status := waitCallbackStatus(t, runner, chunk.StreamID, func(s RecordingCallbackStatus) bool { return s.Completed == 1 })
	if !status.Enabled || status.Pending != 0 || status.Failed != 0 || status.LastCompletedAt == nil {
		t.Fatalf("unexpected callback status: %+v", status)
	}
	argument, err := os.ReadFile(chunk.Path + ".argument")
	if err != nil || string(argument) != chunk.Path {
		t.Fatalf("callback argument = %q, error %v", argument, err)
	}
	data, err := os.ReadFile(chunk.Path + ".metadata")
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"USERINFO_SECRET", "PATH_SECRET", "QUERY_SECRET", "viewer"} {
		if strings.Contains(string(data), secret) {
			t.Fatalf("callback metadata leaked URL credentials: %s", data)
		}
	}
	var metadata recordingChunkMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata.Source.Host != "camera.example" || metadata.Source.Port != "8554" || metadata.Source.Scheme != "rtsp" {
		t.Fatalf("unexpected camera identity: %+v", metadata.Source)
	}
	if metadata.Path != chunk.Path || metadata.RelativePath != chunk.RelativePath || metadata.StreamID != chunk.StreamID || metadata.Sequence != 7 || metadata.DurationSeconds != 10.25 || !metadata.Final || metadata.Codec != "h264" || metadata.ChunkSeconds != 10 {
		t.Fatalf("incorrect chunk metadata: %+v", metadata)
	}
}

func TestRecordingCallbackOnlyRunsForEnabledChunks(t *testing.T) {
	for _, variant := range []string{"disabled", "single-file-config", "single-file-event", "unfinished", "missing"} {
		t.Run(variant, func(t *testing.T) {
			runner, chunk := callbackTestRunner(t, "touch \"$1.called\"")
			stream := runner.config.Streams[chunk.StreamID]
			switch variant {
			case "disabled":
				stream.Recording.CallbackEnabled = false
			case "single-file-config":
				stream.Recording.ChunkSeconds = 0
			case "single-file-event":
				chunk.ChunkSeconds = 0
			case "unfinished":
				chunk.Path += ".part"
			case "missing":
				if err := os.Remove(chunk.Path); err != nil {
					t.Fatal(err)
				}
			}
			runner.config.Streams[chunk.StreamID] = stream
			runner.Submit(chunk)
			runner.Close()
			if _, err := os.Stat(chunk.Path + ".called"); !os.IsNotExist(err) {
				t.Fatalf("callback ran for %s: %v", variant, err)
			}
			status := runner.Status(chunk.StreamID)
			if status.Completed != 0 || status.Pending != 0 {
				t.Fatalf("unexpected callback status: %+v", status)
			}
		})
	}
}

func TestRecordingCallbackFailureIsVisibleWithoutScriptOutput(t *testing.T) {
	runner, chunk := callbackTestRunner(t, "printf 'SECRET_OUTPUT' >&2\nexit 23")
	runner.Submit(chunk)
	status := waitCallbackStatus(t, runner, chunk.StreamID, func(s RecordingCallbackStatus) bool { return s.Failed == 1 })
	if status.Completed != 0 || status.Pending != 0 || status.LastError != "callback script exited with status 23" || status.LastFile != chunk.RelativePath {
		t.Fatalf("unexpected failure status: %+v", status)
	}
}

func TestRecordingCallbackTimeoutKillsScriptChildren(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("process group lifetime check is verified on the production Linux platform")
	}
	runner, chunk := callbackTestRunner(t, "(sleep 0.3; touch \"$1.survived\") &\nwait")
	runner.timeout = 40 * time.Millisecond
	runner.Submit(chunk)
	status := waitCallbackStatus(t, runner, chunk.StreamID, func(s RecordingCallbackStatus) bool { return s.Failed == 1 })
	if !strings.Contains(status.LastError, "timed out") {
		t.Fatalf("expected timeout, got %+v", status)
	}
	// A descendant that escaped cancellation would create this marker.
	time.Sleep(350 * time.Millisecond)
	if _, err := os.Stat(chunk.Path + ".survived"); !os.IsNotExist(err) {
		t.Fatalf("callback child survived timeout: %v", err)
	}
}

func TestRecordingCallbackQueueDoesNotBlockRecorder(t *testing.T) {
	runner, chunk := callbackTestRunner(t, "while [ ! -f \"$1.release\" ]; do sleep 0.01; done")
	count := recordingCallbackQueueSize + recordingCallbackWorkers + 8
	start := time.Now()
	for i := 0; i < count; i++ {
		runner.Submit(chunk)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("submitting queued callbacks blocked for %s", elapsed)
	}
	status := runner.Status(chunk.StreamID)
	if status.Failed == 0 || status.Pending > recordingCallbackQueueSize+recordingCallbackWorkers || !strings.Contains(status.LastError, "queue is full") {
		t.Fatalf("expected bounded queue overflow: %+v", status)
	}
	if err := os.WriteFile(chunk.Path+".release", nil, 0640); err != nil {
		t.Fatal(err)
	}
	runner.Close()
	status = runner.Status(chunk.StreamID)
	if status.Pending != 0 || status.Completed+status.Failed != uint64(count) {
		t.Fatalf("queued callbacks were not fully accounted for: %+v", status)
	}
}

func TestRecordingCallbackShutdownCancelsQueuedAndRunningJobs(t *testing.T) {
	runner, chunk := callbackTestRunner(t, "sleep 60")
	for i := 0; i < 5; i++ {
		runner.Submit(chunk)
	}
	// Cancellation follows Close's drain deadline in production. Cancel directly
	// here to verify accounting and process cleanup without a five-second test.
	runner.cancel()
	start := time.Now()
	runner.Close()
	if time.Since(start) > 2*time.Second {
		t.Fatal("callback shutdown did not respect cancellation")
	}
	status := runner.Status(chunk.StreamID)
	if status.Pending != 0 || status.Failed != 5 || status.Completed != 0 {
		t.Fatalf("shutdown lost callback outcomes: %+v", status)
	}
	runner.Submit(chunk)
	if status = runner.Status(chunk.StreamID); status.Failed != 6 || !strings.Contains(status.LastError, "shutting down") {
		t.Fatalf("submission after shutdown was not rejected: %+v", status)
	}
}

func TestRecordingCallbackSourceStripsSensitiveURLComponents(t *testing.T) {
	for _, tc := range []struct {
		url, host, port string
	}{
		{"rtsp://camera.example/path", "camera.example", "554"},
		{"rtsp://admin:secret@[2001:db8::1]:8554/path?password=secret", "2001:db8::1", "8554"},
		{"not a URL", "", ""},
		{"rtsp://user:secret@camera.example:bad/path", "", ""},
	} {
		got := callbackSource(tc.url)
		if got.Host != tc.host || got.Port != tc.port {
			t.Errorf("source host/port = %+v, expected %s:%s", got, tc.host, tc.port)
		}
	}
}
