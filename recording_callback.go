package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

const (
	recordingCallbackQueueSize = 64
	recordingCallbackWorkers   = 2
	recordingCallbackDrain     = 5 * time.Second
)

// RecordingCallbackStatus is cumulative for this process, across recording
// sessions. Callback failures never change the state of the recorder itself.
type RecordingCallbackStatus struct {
	Enabled         bool       `json:"enabled"`
	Pending         int        `json:"pending"`
	Completed       uint64     `json:"completed"`
	Failed          uint64     `json:"failed"`
	LastError       string     `json:"last_error,omitempty"`
	LastFile        string     `json:"last_file,omitempty"`
	LastCompletedAt *time.Time `json:"last_completed_at,omitempty"`
}

// Do not add URL paths, query strings, or userinfo here: cameras can encode
// their credentials in any of those URL components.
type recordingCallbackSource struct {
	Scheme string `json:"scheme"`
	Host   string `json:"host"`
	Port   string `json:"port"`
}

type recordingChunkMetadata struct {
	Event           string                  `json:"event"`
	StreamID        string                  `json:"stream_id"`
	Source          recordingCallbackSource `json:"source"`
	Path            string                  `json:"path"`
	RelativePath    string                  `json:"relative_path"`
	StartedAt       time.Time               `json:"started_at"`
	FinalizedAt     time.Time               `json:"finalized_at"`
	DurationSeconds float64                 `json:"duration_seconds"`
	Sequence        int                     `json:"sequence"`
	ChunkSeconds    int                     `json:"chunk_seconds"`
	Final           bool                    `json:"final"`
	Codec           string                  `json:"codec"`
}

type recordingCallbackJob struct {
	metadata recordingChunkMetadata
}

// RecordingCallbackRunner executes a fixed, operator-configured script. The
// recording goroutine only submits to a bounded in-memory queue. Jobs are not
// retried or persisted; skipped jobs and execution failures are exposed in
// Status and logs so a slow callback cannot interrupt recording or playback.
type RecordingCallbackRunner struct {
	config  *ConfigST
	script  string
	timeout time.Duration
	ctx     context.Context
	cancel  context.CancelFunc
	queue   chan recordingCallbackJob
	done    chan struct{}
	mu      sync.Mutex
	started bool
	closed  bool
	status  map[string]RecordingCallbackStatus
}

func NewRecordingCallbackRunner(config *ConfigST) *RecordingCallbackRunner {
	config.mutex.RLock()
	script, timeoutSeconds := config.Server.RecordingCallbackScript, config.Server.RecordingCallbackTimeoutSeconds
	config.mutex.RUnlock()
	if timeoutSeconds <= 0 {
		timeoutSeconds = 60
	}
	// An accidental enormous timeout must not overflow time.Duration.
	if timeoutSeconds > 3600 {
		timeoutSeconds = 3600
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &RecordingCallbackRunner{
		config: config, script: script, timeout: time.Duration(timeoutSeconds) * time.Second,
		ctx: ctx, cancel: cancel, queue: make(chan recordingCallbackJob, recordingCallbackQueueSize),
		done: make(chan struct{}), status: make(map[string]RecordingCallbackStatus),
	}
}

func (r *RecordingCallbackRunner) enabled(streamID string) (bool, string) {
	r.config.mutex.RLock()
	defer r.config.mutex.RUnlock()
	stream, exists := r.config.Streams[streamID]
	return exists && stream.Recording.CallbackEnabled && stream.Recording.ChunkSeconds > 0, stream.URL
}

func callbackSource(rawURL string) recordingCallbackSource {
	u, err := url.Parse(rawURL)
	if err != nil || u.Hostname() == "" {
		return recordingCallbackSource{}
	}
	port := u.Port()
	if port == "" && u.Scheme == "rtsp" {
		port = "554"
	}
	return recordingCallbackSource{Scheme: u.Scheme, Host: u.Hostname(), Port: port}
}

// Submit must be called only after atomic publication of the MP4. It checks
// chunking again so even an incorrectly wired single-file event cannot run a
// callback. Scripts and arguments can never be supplied by an HTTP request.
func (r *RecordingCallbackRunner) Submit(chunk RecordingChunk) {
	enabled, sourceURL := r.enabled(chunk.StreamID)
	if !enabled || chunk.ChunkSeconds <= 0 {
		return
	}
	metadata := recordingChunkMetadata{
		Event: "recording.chunk.completed", StreamID: chunk.StreamID, Source: callbackSource(sourceURL),
		Path: chunk.Path, RelativePath: chunk.RelativePath, StartedAt: chunk.StartedAt.UTC(),
		FinalizedAt: time.Now().UTC(), DurationSeconds: chunk.Duration.Seconds(),
		Sequence: chunk.Sequence, ChunkSeconds: chunk.ChunkSeconds, Final: chunk.Final, Codec: "h264",
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var failure string
	switch {
	case r.closed:
		failure = "callback skipped because the server is shutting down"
	case !filepath.IsAbs(r.script):
		failure = "callback script must be configured as an absolute path"
	case !filepath.IsAbs(chunk.Path) || filepath.Ext(chunk.Path) != ".mp4":
		failure = "callback requires a finalized MP4 with an absolute path"
	default:
		if !r.started {
			r.startLocked()
		}
		select {
		case r.queue <- recordingCallbackJob{metadata: metadata}:
			status := r.status[chunk.StreamID]
			status.Pending++
			r.status[chunk.StreamID] = status
			return
		default:
			failure = "callback queue is full; chunk callback skipped"
		}
	}
	r.failedLocked(chunk.StreamID, chunk.RelativePath, failure)
}

func (r *RecordingCallbackRunner) startLocked() {
	r.started = true
	var workers sync.WaitGroup
	workers.Add(recordingCallbackWorkers)
	for range recordingCallbackWorkers {
		go func() {
			defer workers.Done()
			for job := range r.queue {
				err := r.execute(job)
				r.mu.Lock()
				status := r.status[job.metadata.StreamID]
				status.Pending--
				r.status[job.metadata.StreamID] = status
				if err != nil {
					r.failedLocked(job.metadata.StreamID, job.metadata.RelativePath, err.Error())
				} else {
					status.Completed++
					status.LastError = ""
					status.LastFile = job.metadata.RelativePath
					now := time.Now().UTC()
					status.LastCompletedAt = &now
					r.status[job.metadata.StreamID] = status
					log.Printf("recording callback stream=%q file=%q completed", job.metadata.StreamID, job.metadata.RelativePath)
				}
				r.mu.Unlock()
			}
		}()
	}
	go func() {
		workers.Wait()
		close(r.done)
	}()
}

func (r *RecordingCallbackRunner) failedLocked(streamID, file, message string) {
	status := r.status[streamID]
	status.Failed++
	status.LastFile, status.LastError = file, message
	r.status[streamID] = status
	log.Printf("recording callback stream=%q file=%q failed: %s", streamID, file, message)
}

func (r *RecordingCallbackRunner) execute(job recordingCallbackJob) error {
	if r.ctx.Err() != nil {
		return errors.New("callback cancelled during shutdown")
	}
	// A queued file might have been removed by the host before this worker ran.
	info, err := os.Lstat(job.metadata.Path)
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("callback MP4 is missing or is not a regular file")
	}
	metadata, err := json.Marshal(job.metadata)
	if err != nil {
		return errors.New("callback metadata could not be encoded")
	}
	ctx, cancel := context.WithTimeout(r.ctx, r.timeout)
	defer cancel()
	command := exec.CommandContext(ctx, r.script, job.metadata.Path)
	command.Stdin = bytes.NewReader(append(metadata, '\n'))
	// Script output is intentionally not retained or logged: unbounded output
	// cannot exhaust memory, and a custom script cannot leak credentials to the
	// recording status API via its stderr.
	command.Stdout, command.Stderr = io.Discard, io.Discard
	command.WaitDelay = time.Second
	configureRecordingCallbackProcess(command)
	err = command.Run()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("callback timed out after %s", r.timeout)
	}
	if ctx.Err() != nil {
		return errors.New("callback cancelled during shutdown")
	}
	if err == nil {
		return nil
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		return fmt.Errorf("callback script exited with status %d", exitError.ExitCode())
	}
	if errors.Is(err, os.ErrNotExist) {
		return errors.New("callback script not found")
	}
	if errors.Is(err, os.ErrPermission) {
		return errors.New("callback script is not executable")
	}
	return errors.New("callback script could not be executed")
}

func (r *RecordingCallbackRunner) Status(streamID string) RecordingCallbackStatus {
	enabled, _ := r.enabled(streamID)
	r.mu.Lock()
	defer r.mu.Unlock()
	status := r.status[streamID]
	status.Enabled = enabled
	if status.LastCompletedAt != nil {
		completed := *status.LastCompletedAt
		status.LastCompletedAt = &completed
	}
	return status
}

// Close should follow RecordingManager.StopAll so the final short chunks are
// submitted first. It drains briefly, then cancels active scripts and marks
// queued jobs as failed, staying within Docker's shutdown grace period.
func (r *RecordingCallbackRunner) Close() {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
		close(r.queue)
		if !r.started {
			close(r.done)
		}
	}
	r.mu.Unlock()
	defer r.cancel()
	timer := time.NewTimer(recordingCallbackDrain)
	defer timer.Stop()
	select {
	case <-r.done:
		return
	case <-timer.C:
		r.cancel()
	}
	timer.Reset(2 * time.Second)
	select {
	case <-r.done:
	case <-timer.C:
		log.Print("recording callback shutdown timed out")
	}
}
