package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/deepch/vdk/av"
	"github.com/deepch/vdk/codec/h264parser"
	"github.com/deepch/vdk/format/fmp4"
)

const recordingStopTimeout = 10 * time.Second

// RecordingStatus describes one recording session. Paths are relative to the
// configured recording directory; only completed files appear in Files.
type RecordingStatus struct {
	Active         bool       `json:"active"`
	State          string     `json:"state"`
	Error          string     `json:"error,omitempty"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	ChunkSeconds   int        `json:"chunk_seconds"`
	FrameRate      float64    `json:"frame_rate"`
	Files          []string   `json:"files"`
	CurrentFile    string     `json:"current_file,omitempty"`
	Note           string     `json:"note,omitempty"`
	RetryCount     int        `json:"retry_count"`
	LastRetryError string     `json:"last_retry_error,omitempty"`
	NextRetryAt    *time.Time `json:"next_retry_at,omitempty"`
}

type recordingSession struct {
	status RecordingStatus // protected by the manager mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// RecordingChunk is delivered after a completed chunk is finalized and published.
// Duration follows the source clock; StartedAt is the wall time recording began.
type RecordingChunk struct {
	StreamID     string
	Path         string
	RelativePath string
	StartedAt    time.Time
	Duration     time.Duration
	Sequence     int
	ChunkSeconds int
	Final        bool
	Codec        string
	Width        int
	Height       int
}

// RecordingManager adds a subscriber to the existing RTSP fanout. Disk I/O only
// happens on the recorder goroutine, never while holding the source's mutex.
type RecordingManager struct {
	// OnChunk must be configured before Start and enqueue work without blocking.
	// It is called only for chunked recordings, never single-file recordings.
	OnChunk  func(RecordingChunk)
	mu       sync.Mutex
	config   *ConfigST
	dir      string
	sessions map[string]*recordingSession
	closing  bool
}

func NewRecordingManager(config *ConfigST, dir string) *RecordingManager {
	if dir == "" {
		dir = "recordings"
	}
	return &RecordingManager{config: config, dir: dir, sessions: make(map[string]*recordingSession)}
}

func (m *RecordingManager) Start(id string) (RecordingStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing {
		return RecordingStatus{}, errors.New("server is shutting down")
	}
	if session := m.sessions[id]; session != nil && session.status.Active {
		return copyRecordingStatus(session.status), nil
	}
	m.config.mutex.RLock()
	stream, exists := m.config.Streams[id]
	m.config.mutex.RUnlock()
	if !exists {
		return RecordingStatus{}, errors.New("stream not found")
	}
	if stream.Recording.ChunkSeconds < 0 || uint64(stream.Recording.ChunkSeconds) > uint64((1<<63-1)/time.Second) {
		return RecordingStatus{}, errors.New("recording chunk_seconds must be nonnegative and fit a time duration")
	}
	rate := stream.Recording.FrameRate
	if math.IsNaN(rate) || math.IsInf(rate, 0) || rate != 0 && (rate < 1 || rate > 240) {
		return RecordingStatus{}, errors.New("recording frame_rate must be 0 or a finite value between 1 and 240 fps")
	}
	now := time.Now().UTC()
	ctx, cancel := context.WithCancel(context.Background())
	session := &recordingSession{
		status: RecordingStatus{Active: true, State: "starting", StartedAt: &now, ChunkSeconds: stream.Recording.ChunkSeconds, FrameRate: rate, Files: []string{}},
		cancel: cancel, done: make(chan struct{}),
	}
	if rate > 0 {
		session.status.Note = fmt.Sprintf("MP4 timing uses the configured frame rate: %g fps.", rate)
	}
	m.sessions[id] = session
	go m.run(ctx, id, session)
	return copyRecordingStatus(session.status), nil
}

func (m *RecordingManager) Status(id string) RecordingStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	if session := m.sessions[id]; session != nil {
		return copyRecordingStatus(session.status)
	}
	m.config.mutex.RLock()
	options := m.config.Streams[id].Recording
	m.config.mutex.RUnlock()
	return RecordingStatus{State: "idle", ChunkSeconds: options.ChunkSeconds, FrameRate: options.FrameRate, Files: []string{}}
}

func copyRecordingStatus(status RecordingStatus) RecordingStatus {
	status.Files = append([]string{}, status.Files...)
	if status.StartedAt != nil {
		started := *status.StartedAt
		status.StartedAt = &started
	}
	if status.NextRetryAt != nil {
		next := *status.NextRetryAt
		status.NextRetryAt = &next
	}
	return status
}

func (m *RecordingManager) Stop(id string) (RecordingStatus, error) {
	m.mu.Lock()
	session := m.sessions[id]
	if session == nil || !session.status.Active {
		m.mu.Unlock()
		return m.Status(id), nil
	}
	session.status.State = "stopping"
	session.cancel()
	m.mu.Unlock()
	// A stuck filesystem must not keep an HTTP request open forever. The worker
	// remains in stopping state and continues finalizing if this deadline fires.
	timer := time.NewTimer(recordingStopTimeout)
	defer timer.Stop()
	select {
	case <-session.done:
		m.mu.Lock()
		status := copyRecordingStatus(session.status)
		m.mu.Unlock()
		if status.Error != "" {
			return status, errors.New(status.Error)
		}
		return status, nil
	case <-timer.C:
		return m.Status(id), errors.New("recording is still finalizing; check its status")
	}
}

func (m *RecordingManager) StopAll() {
	m.mu.Lock()
	m.closing = true
	var pending []*recordingSession
	for _, session := range m.sessions {
		if session.status.Active {
			session.status.State = "stopping"
			session.cancel()
			pending = append(pending, session)
		}
	}
	m.mu.Unlock()
	deadline := time.NewTimer(2 * recordingStopTimeout)
	defer deadline.Stop()
	for _, session := range pending {
		select {
		case <-session.done:
		case <-deadline.C:
			log.Print("recording shutdown timed out; unfinished files retain .part suffix")
			return
		}
	}
}

func (m *RecordingManager) update(session *recordingSession, change func(*RecordingStatus)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	change(&session.status)
}

func (m *RecordingManager) run(ctx context.Context, id string, session *recordingSession) {
	err := m.record(ctx, id, session)
	m.mu.Lock()
	defer m.mu.Unlock()
	session.status.Active = false
	session.status.State = "stopped"
	session.status.LastRetryError = ""
	session.status.NextRetryAt = nil
	if err != nil {
		session.status.State = "error"
		session.status.Error = err.Error()
		log.Printf("recording stream=%q ended: %v", id, err)
	}
	close(session.done)
}

// Only this exact error type requests another source subscription. Disk and
// muxing errors, including errors joined during finalization, are terminal.
type recordingSourceFailure struct{ reason string }

func (e *recordingSourceFailure) Error() string { return e.reason }

func recordingRetryDelay(failures int) time.Duration {
	if failures >= 4 {
		return 15 * time.Second
	}
	return time.Second << max(failures, 0)
}

func (m *RecordingManager) source(ctx context.Context, id string) (*streamSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.config.RunIFNotRun(id)
	m.config.mutex.RLock()
	defer m.config.mutex.RUnlock()
	stream, exists := m.config.Streams[id]
	if !exists {
		return nil, errors.New("stream not found")
	}
	if !stream.Status || !codecsReady(stream.Codecs) {
		return nil, &recordingSourceFailure{"source is offline or its video configuration is unavailable"}
	}
	return &streamSnapshot{codecs: append([]av.CodecData(nil), stream.Codecs...), generation: stream.generation}, nil
}

func (m *RecordingManager) waitForRecordingRetry(ctx context.Context, id string, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	demand := time.NewTicker(5 * time.Second)
	defer demand.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return ctx.Err() == nil
		case <-demand.C:
			// Keep on-demand source ownership while there is no packet subscriber.
			if ctx.Err() == nil {
				m.config.RunIFNotRun(id)
			}
		}
	}
}

func (m *RecordingManager) record(ctx context.Context, id string, session *recordingSession) (result error) {
	if ctx.Err() != nil {
		return nil
	}
	root, folder, err := openRecordingDirectory(m.dir, id)
	if err != nil {
		return fmt.Errorf("open recording directory: %w", err)
	}
	defer root.Close()

	var file *recordingFile
	var codec h264parser.CodecData // codec of the currently open file
	var sequence int
	var nextTime time.Duration                  // End of the last accepted, complete video frame.
	chunkSeconds := session.status.ChunkSeconds // immutable for this session
	frameRate := session.status.FrameRate
	var frameIndex uint64
	finish := func(nextTime time.Duration, final bool) error {
		if file == nil {
			return nil
		}
		event := RecordingChunk{
			StreamID: id, RelativePath: filepath.Join(folder, file.name),
			StartedAt: file.startedAt, Duration: nextTime - file.origin,
			Sequence: sequence, ChunkSeconds: chunkSeconds, Final: final,
			Codec: "h264", Width: codec.Width(), Height: codec.Height(),
		}
		event.Path, _ = filepath.Abs(filepath.Join(m.dir, event.RelativePath))
		name, finishErr := file.finish(nextTime)
		file = nil
		if finishErr != nil {
			return fmt.Errorf("finalize MP4: %w", finishErr)
		}
		m.update(session, func(status *RecordingStatus) {
			status.Files = append(status.Files, filepath.Join(folder, name))
			status.CurrentFile = ""
		})
		if chunkSeconds > 0 && m.OnChunk != nil {
			m.OnChunk(event)
		}
		return nil
	}
	write := func(packet av.Packet, sourceCodec h264parser.CodecData) error {
		if file != nil && !bytes.Equal(codec.AVCDecoderConfRecordBytes(), sourceCodec.AVCDecoderConfRecordBytes()) {
			// Parameter-set changes require a new standalone MP4, even when the
			// resolution is unchanged. Publish using the old file's codec metadata.
			if err := finish(nextTime, false); err != nil {
				return err
			}
		}
		if file != nil && packet.IsKeyFrame && chunkSeconds > 0 && packet.Time-file.origin >= time.Duration(chunkSeconds)*time.Second {
			if err := finish(packet.Time, false); err != nil {
				return err
			}
		}
		if file == nil {
			if !packet.IsKeyFrame {
				return nil
			}
			sequence++
			codec = sourceCodec
			file, err = newRecordingFile(root, codec, sequence, packet.Time)
			if err != nil {
				return fmt.Errorf("create MP4: %w", err)
			}
			m.update(session, func(status *RecordingStatus) {
				status.CurrentFile = filepath.Join(folder, file.name+".part")
			})
		}
		return file.write(packet)
	}
	defer func() {
		if file != nil {
			result = errors.Join(result, finish(nextTime, true))
		}
	}()

	consecutiveFailures := 0
	for ctx.Err() == nil {
		snapshot, sourceErr := m.source(ctx, id)
		if sourceErr == context.Canceled {
			return nil
		}
		if sourceErr == nil {
			videoIndex := -1
			var sourceCodec h264parser.CodecData
			var audio bool
			for i, cd := range snapshot.codecs {
				if video, ok := cd.(h264parser.CodecData); ok && videoIndex == -1 {
					videoIndex, sourceCodec = i, video
				}
				audio = audio || cd.Type().IsAudio()
			}
			if videoIndex == -1 {
				return errors.New("MP4 recording requires an H264 video track")
			}
			m.update(session, func(status *RecordingStatus) {
				status.NextRetryAt = nil
				status.Note = ""
				if frameRate > 0 {
					status.Note = fmt.Sprintf("MP4 timing uses the configured frame rate: %g fps.", frameRate)
				}
				if audio {
					status.Note = strings.TrimSpace(status.Note + " Video only: audio is omitted because the source provides independent audio/video clocks.")
				}
			})
			var sourceBase, outputBase time.Duration
			recovered := false
			sourceErr = m.recordSource(ctx, id, snapshot, videoIndex, func(packet av.Packet, duration time.Duration, first bool) error {
				if first {
					sourceBase, outputBase = packet.Time, nextTime
				}
				// Every new source generation starts exactly where the previous
				// complete frame ended. Network outages add no artificial gap.
				packet.Time = outputBase + packet.Time - sourceBase
				if frameRate > 0 {
					packet.Time = recordingFrameTime(frameIndex, frameRate)
					duration = recordingFrameTime(frameIndex+1, frameRate) - packet.Time
				}
				if err := write(packet, sourceCodec); err != nil {
					return fmt.Errorf("write MP4: %w", err)
				}
				nextTime = packet.Time + duration
				frameIndex++
				if first {
					recovered = true
					m.update(session, func(status *RecordingStatus) {
						if status.State != "stopping" {
							status.State = "recording"
						}
						status.LastRetryError = ""
						status.NextRetryAt = nil
					})
				}
				return nil
			})
			if recovered {
				consecutiveFailures = 0
			}
		}
		if sourceErr == nil {
			return nil // Stop interrupted an active source subscription.
		}
		if _, retryable := sourceErr.(*recordingSourceFailure); !retryable {
			return sourceErr
		}
		if ctx.Err() != nil {
			return nil
		}
		if chunkSeconds > 0 {
			if err := finish(nextTime, false); err != nil {
				return err // Storage failures must never become reconnect attempts.
			}
		}
		delay := recordingRetryDelay(consecutiveFailures)
		consecutiveFailures++
		nextRetry := time.Now().Add(delay).UTC()
		m.update(session, func(status *RecordingStatus) {
			if status.State != "stopping" {
				status.State = "reconnecting"
			}
			status.RetryCount++
			status.LastRetryError = sourceErr.Error()
			status.NextRetryAt = &nextRetry
		})
		if !m.waitForRecordingRetry(ctx, id, delay) {
			return nil
		}
	}
	return nil
}

// A subscription owns its assembler and RTP clock. Both reset on reconnect;
// only complete frames from the next IDR can enter the persistent output file.
func (m *RecordingManager) recordSource(ctx context.Context, id string, snapshot *streamSnapshot, videoIndex int, consume func(av.Packet, time.Duration, bool) error) error {
	cid, packets := m.config.clAd(id, snapshot.generation)
	if cid == "" {
		return &recordingSourceFailure{"source changed while subscribing"}
	}
	defer m.config.clDe(id, cid)
	var assembler recordingAccessUnit
	var clock recordingClock
	started := false
	idle := time.NewTimer(mediaIdleTimeout)
	defer idle.Stop()
	initial := time.NewTimer(firstKeyframeTimeout)
	defer initial.Stop()
	initialDeadline := initial.C
	for {
		if ctx.Err() != nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-idle.C:
			return &recordingSourceFailure{"no video packets received"}
		case <-initialDeadline:
			return &recordingSourceFailure{"no H264 keyframe received"}
		case packet, ok := <-packets:
			if !ok {
				return &recordingSourceFailure{"source disconnected, changed format, or recorder could not keep up"}
			}
			if int(packet.Idx) != videoIndex || len(packet.Data) == 0 {
				continue
			}
			idle.Reset(mediaIdleTimeout)
			stamp, err := clock.normalize(packet.Time)
			if err != nil {
				return &recordingSourceFailure{err.Error()}
			}
			packet.Time = stamp
			complete, assembleErr := assembler.push(packet)
			if assembleErr != nil {
				return &recordingSourceFailure{assembleErr.Error()}
			}
			if complete == nil {
				continue
			}
			if !started && !recordingHasIDRStart(*complete) {
				continue
			}
			if err := consume(*complete, packet.Time-complete.Time, !started); err != nil {
				return err
			}
			if !started {
				started = true
				initial.Stop()
				initialDeadline = nil
			}
		}
	}
}

// A subscription can begin halfway through a multi-slice IDR. Require its first
// slice (first_mb_in_slice == 0, encoded as the Exp-Golomb bit 1) before resuming.
func recordingHasIDRStart(packet av.Packet) bool {
	nalus, _ := h264parser.SplitNALUs(packet.Data)
	for _, nalu := range nalus {
		if len(nalu) > 1 && nalu[0]&0x1f == 5 && nalu[1]&0x80 != 0 {
			return true
		}
	}
	return false
}

// Compute each timestamp from its frame index, avoiding accumulated rounding
// error for rates such as 15 or 29.97 fps and keeping chunk callbacks coherent.
func recordingFrameTime(index uint64, rate float64) time.Duration {
	return time.Duration(math.Round(float64(index) * float64(time.Second) / rate))
}

// RTSP H264 packets can contain individual slices. One MP4 sample must contain
// all NALs with the same DTS, with AVCC length prefixes and independent storage.
type recordingAccessUnit struct{ pending *av.Packet }

func (a *recordingAccessUnit) push(packet av.Packet) (*av.Packet, error) {
	var complete *av.Packet
	if a.pending != nil && packet.Time != a.pending.Time {
		complete, a.pending = a.pending, nil
	}
	if a.pending == nil {
		copy := packet
		copy.Idx, copy.Data = 0, nil
		copy.IsKeyFrame = h264Keyframe(packet)
		a.pending = &copy
	}
	a.pending.IsKeyFrame = a.pending.IsKeyFrame || h264Keyframe(packet)
	nalus, _ := h264parser.SplitNALUs(packet.Data)
	for _, nalu := range nalus {
		if len(nalu) == 0 {
			continue
		}
		if len(a.pending.Data)+len(nalu)+4 > 32<<20 {
			return nil, errors.New("H264 access unit exceeds 32 MiB")
		}
		size := len(nalu)
		a.pending.Data = append(a.pending.Data, byte(size>>24), byte(size>>16), byte(size>>8), byte(size))
		a.pending.Data = append(a.pending.Data, nalu...)
	}
	return complete, nil
}

func (a *recordingAccessUnit) take() *av.Packet {
	packet := a.pending
	a.pending = nil
	return packet
}

type recordingClock struct {
	last, epoch time.Duration
	seen        bool
}

func (c *recordingClock) normalize(value time.Duration) (time.Duration, error) {
	const period = time.Duration(uint64(1)<<32) * time.Second / 90000
	if value < 0 {
		return 0, errors.New("negative video timestamp")
	}
	if c.seen && value < c.last {
		if c.last > period-time.Minute && value < time.Minute {
			c.epoch += period
		} else {
			return 0, errors.New("video timestamps moved backwards")
		}
	}
	c.last, c.seen = value, true
	return value + c.epoch, nil
}

func recordingFolder(id string) string {
	var builder strings.Builder
	valid := id != "" && len(id) <= 64
	for _, char := range id {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '-' || char == '_' {
			if builder.Len() < 48 {
				builder.WriteRune(char)
			}
		} else {
			valid = false
		}
	}
	if valid {
		return id
	}
	sum := sha256.Sum256([]byte(id))
	return fmt.Sprintf("%s-%x", builder.String(), sum[:8])
}

func openRecordingDirectory(path, id string) (*os.Root, string, error) {
	if err := os.MkdirAll(path, 0750); err != nil {
		return nil, "", err
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, "", err
	}
	defer root.Close()
	folder := recordingFolder(id)
	if err := root.Mkdir(folder, 0750); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, "", err
	}
	info, err := root.Lstat(folder)
	if err != nil {
		return nil, "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, "", errors.New("stream recording directory must be a real directory")
	}
	streamRoot, err := root.OpenRoot(folder)
	return streamRoot, folder, err
}

type recordingFile struct {
	root             *os.Root
	file             *os.File
	name             string
	mux              *fmp4.MovieFragmenter
	origin, lastTime time.Duration
	bytes, frames    int
	failed           error
	startedAt        time.Time
}

func newRecordingFile(root *os.Root, codec h264parser.CodecData, sequence int, origin time.Duration) (*recordingFile, error) {
	mux, err := fmp4.NewMovie([]av.CodecData{codec})
	if err != nil {
		return nil, err
	}
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, err
	}
	name := fmt.Sprintf("%s_%x_%06d.mp4", time.Now().UTC().Format("20060102T150405.000000000Z"), random, sequence)
	file, err := root.OpenFile(name+".part", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0640)
	if err != nil {
		return nil, err
	}
	_, _, header := mux.MovieHeader()
	if err := writeRecordingBytes(file, header); err != nil {
		file.Close()
		return nil, err
	}
	return &recordingFile{root: root, file: file, name: name, mux: mux, origin: origin, lastTime: origin, startedAt: time.Now().UTC()}, nil
}

func writeRecordingBytes(file *os.File, data []byte) error {
	n, err := file.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	return err
}

func (f *recordingFile) write(packet av.Packet) error {
	f.lastTime = packet.Time
	packet.Time -= f.origin
	packet.Idx = 0
	if err := f.mux.WritePacket(packet); err != nil {
		f.failed = err
		return err
	}
	f.bytes += len(packet.Data)
	f.frames++
	// Fragment independently of the user-visible chunk duration, bounding memory
	// even for long GOPs, hours-long single files and a stalled source clock.
	if f.mux.Duration() >= time.Second || f.bytes >= 4<<20 || f.frames >= 256 {
		return f.flush()
	}
	return nil
}

func (f *recordingFile) flush() error {
	fragment, err := f.mux.Fragment()
	if err == nil && len(fragment.Bytes) > 0 {
		err = writeRecordingBytes(f.file, fragment.Bytes)
	}
	f.bytes, f.frames = 0, 0
	if err != nil {
		f.failed = err
	}
	return err
}

func (f *recordingFile) finish(nextTime time.Duration) (string, error) {
	defer f.file.Close()
	if f.failed != nil {
		return "", f.failed // Leave .part for recovery; never publish a failed write.
	}
	if nextTime <= f.lastTime {
		return "", errors.New("invalid final video sample duration")
	}
	// Fragmenter retains its last packet until the next DTS. This empty sentinel
	// supplies the final duration; the sentinel itself is never written to disk.
	if err := f.mux.WritePacket(av.Packet{Time: nextTime - f.origin}); err != nil {
		return "", err
	}
	if err := f.flush(); err != nil {
		return "", err
	}
	if err := f.file.Sync(); err != nil {
		return "", err
	}
	if err := f.file.Close(); err != nil {
		return "", err
	}
	// Link publishes atomically and refuses existing targets (Rename overwrites).
	// Both names reside on the same host-mounted filesystem.
	if err := f.root.Link(f.name+".part", f.name); err != nil {
		return "", err
	}
	if err := f.root.Remove(f.name + ".part"); err != nil {
		return "", err
	}
	return f.name, nil
}
