package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/deepch/vdk/av"
	"github.com/deepch/vdk/codec/h264parser"
	"github.com/deepch/vdk/format/fmp4/fmp4io"
)

// Eight real 32x32 baseline H264 frames generated from a blue color source at
// 5 fps, two slices per frame, GOP=5, no B frames. No camera data is embedded.
func recordingFixture(t *testing.T) (h264parser.CodecData, []av.Packet) {
	t.Helper()
	decode := func(value string) []byte {
		data, err := hex.DecodeString(value)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	codec, err := h264parser.NewCodecDataFromSPSAndPPS(
		decode("6742c00ad9096c044000000300400000030283c48992"), decode("68cb83cb20"))
	if err != nil {
		t.Fatal(err)
	}
	frames := []string{
		"00000015658884047c4628000c48c700016d68e00021f3278000000015656221011f118a00031231c0005b5a3800087cc9e0",
		"00000006419a3808fb800000000641668e023ee0",
		"00000006419a54023ee000000006416695008fb8",
		"00000005419a6011f700000006416698047dc0",
		"00000005419a8011f7000000064166a0047dc0",
		"00000015658882013f118a000357f1c00061ca3800089cc9e000000015656220804fc4628000d5fc700018728e0002273278",
		"00000006419a38087b800000000641668e021ee0",
		"00000006419a5407fb800000000641669501fee0",
	}
	packets := make([]av.Packet, len(frames))
	for i, data := range frames {
		packets[i] = av.Packet{Data: decode(data), Time: 10*time.Second + time.Duration(i)*200*time.Millisecond, IsKeyFrame: i%5 == 0, CompositionTime: time.Millisecond}
	}
	return codec, packets
}

type recordedSample struct {
	dts, duration uint64
	flags         fmp4io.SampleFlags
	cts           int32
}

func recordingSamples(t *testing.T, path string) []recordedSample {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	atoms, err := fmp4io.ReadFileAtoms(file)
	if err != nil {
		t.Fatal(err)
	}
	var samples []recordedSample
	var header bool
	for _, atom := range atoms {
		header = header || atom.Tag() == fmp4io.MOOV
		movie, ok := atom.(*fmp4io.MovieFrag)
		if !ok {
			continue
		}
		for _, track := range movie.Tracks {
			dts := track.DecodeTime.Time
			for i, entry := range track.Run.Entries {
				duration := entry.Duration
				if track.Run.Flags&fmp4io.TrackRunSampleDuration == 0 {
					duration = track.Header.DefaultDuration
				}
				flags := entry.Flags
				if track.Run.Flags&fmp4io.TrackRunSampleFlags == 0 {
					flags = track.Header.DefaultFlags
				}
				if i == 0 && track.Run.Flags&fmp4io.TrackRunFirstSampleFlags != 0 {
					flags = track.Run.FirstSampleFlags
				}
				samples = append(samples, recordedSample{dts: dts, duration: uint64(duration), flags: flags, cts: entry.CTS})
				dts += uint64(duration)
			}
		}
	}
	if !header || len(samples) == 0 {
		t.Fatal("file lacks a standalone movie header or media samples")
	}
	return samples
}

func TestRecordingFilePreservesSamplesAndFinalDuration(t *testing.T) {
	codec, packets := recordingFixture(t)
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	file, err := newRecordingFile(root, codec, 1, packets[0].Time)
	if err != nil {
		t.Fatal(err)
	}
	for _, packet := range packets {
		if err := file.write(packet); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := root.Stat(file.name); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unfinished file was published")
	}
	name, err := file.finish(packets[len(packets)-1].Time + 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	if _, err := root.Stat(name + ".part"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("temporary file remains after finalization")
	}
	samples := recordingSamples(t, path)
	if len(samples) != len(packets) {
		t.Fatalf("wrote %d samples, want %d (sentinel must not become a sample)", len(samples), len(packets))
	}
	for i, sample := range samples {
		if sample.dts != uint64(i*18000) || sample.duration != 18000 || sample.cts != 90 {
			t.Fatalf("sample %d lost timing: %+v", i, sample)
		}
		if (sample.flags&fmp4io.SampleNoDependencies != 0) != packets[i].IsKeyFrame {
			t.Fatalf("sample %d keyframe flag incorrect", i)
		}
	}
	// This independent decoder check runs when ffprobe/ffmpeg are installed.
	if probe, err := exec.LookPath("ffprobe"); err == nil {
		data, err := exec.Command(probe, "-v", "error", "-count_frames", "-select_streams", "v:0", "-show_entries", "stream=codec_name,nb_read_frames,duration", "-of", "json", path).CombinedOutput()
		if err != nil {
			t.Fatalf("ffprobe: %v: %s", err, data)
		}
		var info struct {
			Streams []struct {
				CodecName string `json:"codec_name"`
				Frames    string `json:"nb_read_frames"`
			} `json:"streams"`
		}
		if json.Unmarshal(data, &info) != nil || len(info.Streams) != 1 || info.Streams[0].CodecName != "h264" || info.Streams[0].Frames != strconv.Itoa(len(packets)) {
			t.Fatalf("independent decode failed: %s", data)
		}
	}
	if ffmpeg, err := exec.LookPath("ffmpeg"); err == nil {
		if data, err := exec.Command(ffmpeg, "-v", "error", "-xerror", "-i", path, "-f", "null", "-").CombinedOutput(); err != nil {
			t.Fatalf("decode recorded MP4: %v: %s", err, data)
		}
	}
}

func TestRecordingAggregatesSlicesAndRepairsIDRFlag(t *testing.T) {
	_, packets := recordingFixture(t)
	var assembler recordingAccessUnit
	for frame, packet := range packets[:2] {
		nalus, _ := h264parser.SplitNALUs(packet.Data)
		for slice, nalu := range nalus {
			part := packet
			part.IsKeyFrame = false
			part.Data = append([]byte{0, 0, 0, 1}, nalu...)
			complete, err := assembler.push(part)
			if err != nil {
				t.Fatal(err)
			}
			if frame == 1 && slice == 0 {
				if complete == nil || !complete.IsKeyFrame || !bytes.Equal(complete.Data, packets[0].Data) {
					t.Fatal("IDR slices were not assembled into one independent AVCC sample")
				}
			} else if complete != nil {
				t.Fatal("emitted incomplete multi-slice frame")
			}
		}
	}
	if got := assembler.take(); got == nil || got.IsKeyFrame || !bytes.Equal(got.Data, packets[1].Data) {
		t.Fatal("pending inter-frame corrupted")
	}
}

func recordingTestConfig(t *testing.T, chunkSeconds int) *ConfigST {
	codec, _ := recordingFixture(t)
	return &ConfigST{Streams: map[string]StreamST{"camera": {
		Status: true, RunLock: true, OnDemand: true, Codecs: []av.CodecData{codec},
		Cl: make(map[string]viewer), Recording: RecordingOptions{ChunkSeconds: chunkSeconds},
	}}}
}

func awaitRecording(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("recording condition timed out")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRecordingChunksShareFanoutAndFinalizeOnDisconnect(t *testing.T) {
	c := recordingTestConfig(t, 1)
	dir := t.TempDir()
	m := NewRecordingManager(c, dir)
	defer m.StopAll()
	var chunks []RecordingChunk
	m.OnChunk = func(chunk RecordingChunk) {
		if _, err := os.Stat(chunk.Path); err != nil {
			t.Errorf("callback ran before completed file was visible: %v", err)
		}
		chunks = append(chunks, chunk)
	}
	viewerID, viewer := c.clAd("camera")
	defer c.clDe("camera", viewerID)
	var group sync.WaitGroup
	for range 12 {
		group.Go(func() {
			if status, err := m.Start("camera"); err != nil || !status.Active {
				t.Errorf("start: %+v %v", status, err)
			}
		})
	}
	group.Wait()
	awaitRecording(t, func() bool {
		c.mutex.RLock()
		defer c.mutex.RUnlock()
		return len(c.Streams["camera"].Cl) == 2
	})
	if !c.HasDemand("camera") {
		t.Fatal("recording did not retain on-demand source")
	}
	_, packets := recordingFixture(t)
	// The leading non-IDR belongs to the prior GOP and must not enter the file.
	leading := packets[1]
	leading.Time = packets[0].Time - time.Second
	c.cast("camera", leading)
	<-viewer
	for _, packet := range packets {
		c.cast("camera", packet)
		if got := <-viewer; !bytes.Equal(got.Data, packet.Data) {
			t.Fatal("recorder affected the other viewer")
		}
	}
	c.sourceDisconnected("camera", errors.New("test disconnection"))
	awaitRecording(t, func() bool { return m.Status("camera").State == "reconnecting" })
	status := m.Status("camera")
	if !status.Active || status.Error != "" || !strings.Contains(status.LastRetryError, "source disconnected") || len(status.Files) != 2 || status.CurrentFile != "" {
		t.Fatalf("disconnect failed to finalize chunks: %+v", status)
	}
	if _, err := m.Stop("camera"); err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 2 || chunks[0].Final || chunks[1].Final || chunks[0].Duration != time.Second || chunks[1].Duration != 400*time.Millisecond || chunks[0].Codec != "h264" {
		t.Fatalf("incorrect finalized chunk notifications: %+v", chunks)
	}
	for i, name := range status.Files {
		samples := recordingSamples(t, filepath.Join(dir, name))
		want := 5
		if i == 1 {
			want = 2 // Final unconfirmed access unit is discarded after source failure.
		}
		if len(samples) != want || samples[0].dts != 0 || samples[0].flags&fmp4io.SampleNoDependencies == 0 {
			t.Fatalf("chunk %d isn't independent or has wrong length: %+v", i, samples)
		}
	}
	status.Files[0] = "mutated"
	if m.Status("camera").Files[0] == "mutated" {
		t.Fatal("status exposed internal mutable memory")
	}
}

func TestRecordingStopAllAndRestart(t *testing.T) {
	c := recordingTestConfig(t, 0)
	m := NewRecordingManager(c, t.TempDir())
	m.OnChunk = func(RecordingChunk) { t.Error("single-file recording invoked chunk hook") }
	for range 2 {
		if _, err := m.Start("camera"); err != nil {
			t.Fatal(err)
		}
		awaitRecording(t, func() bool { return c.HasViewer("camera") })
		_, packets := recordingFixture(t)
		for _, packet := range packets {
			c.cast("camera", packet)
		}
		awaitRecording(t, func() bool { return m.Status("camera").State == "recording" })
		status, err := m.Stop("camera")
		if err != nil || status.Active || status.State != "stopped" || len(status.Files) != 1 || status.CurrentFile != "" {
			t.Fatalf("stop: %+v %v", status, err)
		}
		if c.HasViewer("camera") {
			t.Fatal("stopped recorder retained fanout subscription")
		}
		if _, err := m.Stop("camera"); err != nil {
			t.Fatal("repeated stop is not idempotent")
		}
	}
	m.StopAll()
	if _, err := m.Start("camera"); err == nil {
		t.Fatal("recording started while server is shutting down")
	}
}

func TestRecordingClockWrapAndDiscontinuity(t *testing.T) {
	var clock recordingClock
	const period = time.Duration(uint64(1)<<32) * time.Second / 90000
	before, err := clock.normalize(period - 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	after, err := clock.normalize(20 * time.Millisecond)
	if err != nil || after-before != 40*time.Millisecond {
		t.Fatalf("RTP wrap lost timing: %s %v", after-before, err)
	}
	if _, err := clock.normalize(10 * time.Millisecond); err == nil {
		t.Fatal("backwards clock accepted")
	}
}

func TestRecordingPathConfinementAndNoOverwrite(t *testing.T) {
	dir := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(dir, "camera")); err != nil {
		t.Fatal(err)
	}
	if root, _, err := openRecordingDirectory(dir, "camera"); err == nil {
		root.Close()
		t.Fatal("symlink recording directory accepted")
	}
	root, folder, err := openRecordingDirectory(dir, "../../outside")
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if strings.Contains(folder, "/") || strings.Contains(folder, "..") || folder == recordingFolder("outside") {
		t.Fatal("unsafe or colliding stream folder")
	}
	codec, packets := recordingFixture(t)
	file, err := newRecordingFile(root, codec, 1, packets[0].Time)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.write(packets[0]); err != nil {
		t.Fatal(err)
	}
	if err := root.WriteFile(file.name, []byte("existing recording"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := file.finish(packets[1].Time); !errors.Is(err, os.ErrExist) {
		t.Fatalf("publication overwrote destination: %v", err)
	}
	data, err := root.ReadFile(file.name)
	if err != nil || string(data) != "existing recording" {
		t.Fatal("pre-existing file was changed")
	}
}

func TestRecordingWriteFailureNeverPublishesMP4(t *testing.T) {
	full, err := os.OpenFile("/dev/full", os.O_WRONLY, 0)
	if err != nil {
		t.Skip("/dev/full unavailable")
	}
	defer full.Close()
	root, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	codec, packets := recordingFixture(t)
	file, err := newRecordingFile(root, codec, 1, packets[0].Time)
	if err != nil {
		t.Fatal(err)
	}
	file.file.Close()
	file.file = full
	for _, packet := range packets {
		if err = file.write(packet); err != nil {
			break
		}
	}
	if err == nil {
		t.Fatal("disk full did not surface")
	}
	if _, err := file.finish(packets[len(packets)-1].Time + time.Second); err == nil {
		t.Fatal("failed media write reported successful finalization")
	}
	if _, err := root.Stat(file.name); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("corrupt file was published with .mp4 suffix")
	}
}

func TestRecordingRejectsInvalidConfigAndCodec(t *testing.T) {
	c := recordingTestConfig(t, -1)
	m := NewRecordingManager(c, t.TempDir())
	defer m.StopAll()
	if _, err := m.Start("missing"); err == nil {
		t.Fatal("unknown stream accepted")
	}
	if _, err := m.Start("camera"); err == nil {
		t.Fatal("negative chunk duration accepted")
	}
	c.mutex.Lock()
	stream := c.Streams["camera"]
	stream.Recording.ChunkSeconds = 0
	stream.Codecs = []av.CodecData{flowTestCodec(av.PCM_ALAW)}
	c.Streams["camera"] = stream
	c.mutex.Unlock()
	if _, err := m.Start("camera"); err != nil {
		t.Fatal(err)
	}
	awaitRecording(t, func() bool { return !m.Status("camera").Active })
	if status := m.Status("camera"); status.State != "error" || !strings.Contains(status.Error, "H264") {
		t.Fatalf("unsupported codec not reported: %+v", status)
	}
}

func TestRecordingAudioOmissionAndCancelableStartup(t *testing.T) {
	c := recordingTestConfig(t, 0)
	stream := c.Streams["camera"]
	stream.Codecs = append(stream.Codecs, flowTestCodec(av.PCM_ALAW))
	c.Streams["camera"] = stream
	m := NewRecordingManager(c, t.TempDir())
	if _, err := m.Start("camera"); err != nil {
		t.Fatal(err)
	}
	awaitRecording(t, func() bool { return c.HasViewer("camera") })
	if !strings.Contains(m.Status("camera").Note, "audio is omitted") {
		t.Fatal("silently omitted audio")
	}
	m.StopAll()
	if status := m.Status("camera"); status.Active || status.State != "stopped" || len(status.Files) != 0 {
		t.Fatalf("stopping before initial keyframe produced a file: %+v", status)
	}
	c = recordingTestConfig(t, 0)
	stream = c.Streams["camera"]
	stream.Codecs, stream.Status = nil, false
	c.Streams["camera"] = stream
	m = NewRecordingManager(c, t.TempDir())
	if _, err := m.Start("camera"); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	m.StopAll()
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("shutdown waited for codec discovery timeout: %s", elapsed)
	}
	if status := m.Status("camera"); status.Active || status.State != "stopped" {
		t.Fatalf("codec discovery survived cancellation: %+v", status)
	}
}

func TestRecordingOverflowStopsWithoutBlockingViewer(t *testing.T) {
	c := recordingTestConfig(t, 1)
	m := NewRecordingManager(c, t.TempDir())
	defer m.StopAll()
	blocked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	// Inject a stalled recorder at the publication boundary to exercise its
	// bounded source queue. Production callbacks enqueue work without blocking.
	m.OnChunk = func(RecordingChunk) {
		once.Do(func() { close(blocked); <-release })
	}
	viewerID, viewer := c.clAd("camera")
	defer c.clDe("camera", viewerID)
	if _, err := m.Start("camera"); err != nil {
		t.Fatal(err)
	}
	awaitRecording(t, func() bool {
		c.mutex.RLock()
		defer c.mutex.RUnlock()
		return len(c.Streams["camera"].Cl) == 2
	})
	_, packets := recordingFixture(t)
	for _, packet := range packets {
		c.cast("camera", packet)
		<-viewer
	}
	select {
	case <-blocked:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("recorder never reached injected stall")
	}
	for i := range 300 {
		packet := packets[1]
		packet.Time = packets[len(packets)-1].Time + time.Duration(i+1)*200*time.Millisecond
		c.cast("camera", packet)
		select {
		case <-viewer:
		default:
			t.Error("stalled recorder blocked the independent viewer")
		}
	}
	close(release)
	awaitRecording(t, func() bool { return m.Status("camera").State == "reconnecting" })
	if status := m.Status("camera"); !status.Active || status.Error != "" || !strings.Contains(status.LastRetryError, "could not keep up") || len(status.Files) < 1 {
		t.Fatalf("overflow was silent or lost completed files: %+v", status)
	}
	if _, err := m.Stop("camera"); err != nil {
		t.Fatal(err)
	}
	c.mutex.RLock()
	_, survives := c.Streams["camera"].Cl[viewerID]
	c.mutex.RUnlock()
	if !survives {
		t.Fatal("recorder overflow disconnected the healthy viewer")
	}
}

func TestRecordingFrameRateOverrideCorrectsSourceClockAfterAssembly(t *testing.T) {
	for _, rate := range []float64{0, 15} {
		t.Run(strconv.FormatFloat(rate, 'g', -1, 64), func(t *testing.T) {
			c := recordingTestConfig(t, 1)
			stream := c.Streams["camera"]
			stream.Recording.FrameRate = rate
			c.Streams["camera"] = stream
			dir := t.TempDir()
			m := NewRecordingManager(c, dir)
			defer m.StopAll()
			var chunks []RecordingChunk
			m.OnChunk = func(chunk RecordingChunk) { chunks = append(chunks, chunk) }
			if status, err := m.Start("camera"); err != nil || status.FrameRate != rate {
				t.Fatalf("start with frame rate override: %+v %v", status, err)
			}
			awaitRecording(t, func() bool { return c.HasViewer("camera") })
			_, fixture := recordingFixture(t)
			// Reproduce a camera delivering 15 fps while its RTP timestamps claim
			// 50 fps. Each access unit has two separately delivered NAL slices.
			for i := range 32 {
				packet := fixture[i%len(fixture)]
				packet.Time = time.Hour + time.Duration(i)*20*time.Millisecond
				nalus, _ := h264parser.SplitNALUs(packet.Data)
				for _, nalu := range nalus {
					part := packet
					part.Data = append([]byte{0, 0, 0, 1}, nalu...)
					c.cast("camera", part)
				}
			}
			c.sourceDisconnected("camera", errors.New("end of timing fixture"))
			awaitRecording(t, func() bool { return m.Status("camera").State == "reconnecting" })
			if _, err := m.Stop("camera"); err != nil {
				t.Fatal(err)
			}
			status := m.Status("camera")
			wantCounts, ticksPerFrame := []int{31}, uint64(1800)
			if rate > 0 {
				// First chunk rotates at the first IDR after 1s: frame16. The
				// final pending frame31 is deliberately discarded on disconnect.
				wantCounts, ticksPerFrame = []int{16, 15}, 6000
				if !strings.Contains(status.Note, "15 fps") {
					t.Fatal("configured clock override was not visible")
				}
			}
			if len(status.Files) != len(wantCounts) || len(chunks) != len(wantCounts) {
				t.Fatalf("chunk scheduling used wrong clock: %+v", status)
			}
			var totalTicks uint64
			for i, name := range status.Files {
				samples := recordingSamples(t, filepath.Join(dir, name))
				if len(samples) != wantCounts[i] || samples[0].flags&fmp4io.SampleNoDependencies == 0 {
					t.Fatalf("chunk %d split an access unit or lost independent start", i)
				}
				for j, sample := range samples {
					if sample.dts != uint64(j)*ticksPerFrame || sample.duration != ticksPerFrame {
						t.Fatalf("chunk %d frame %d has wrong timing: %+v", i, j, sample)
					}
					totalTicks += sample.duration
				}
				wantDuration := time.Duration(math.Round(float64(wantCounts[i]) * float64(ticksPerFrame) * float64(time.Second) / 90000))
				if difference := chunks[i].Duration - wantDuration; difference < -time.Nanosecond || difference > time.Nanosecond {
					t.Fatalf("callback duration %s disagrees with media duration %s", chunks[i].Duration, wantDuration)
				}
			}
			if totalTicks != uint64(31)*ticksPerFrame {
				t.Fatal("recording duration lost frames")
			}
		})
	}
}

func TestRecordingFrameRateValidationAndFractionalTiming(t *testing.T) {
	for _, rate := range []float64{-1, math.SmallestNonzeroFloat64, 0.5, 240.1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		c := recordingTestConfig(t, 0)
		stream := c.Streams["camera"]
		stream.Recording.FrameRate = rate
		c.Streams["camera"] = stream
		m := NewRecordingManager(c, t.TempDir())
		if _, err := m.Start("camera"); err == nil {
			m.StopAll()
			t.Fatalf("invalid frame rate accepted: %g", rate)
		}
	}
	for _, rate := range []float64{0, 1, 15, 29.97, 240} {
		c := recordingTestConfig(t, 0)
		stream := c.Streams["camera"]
		stream.Recording.FrameRate = rate
		stream.Codecs = append(stream.Codecs, flowTestCodec(av.PCM_ALAW))
		c.Streams["camera"] = stream
		m := NewRecordingManager(c, t.TempDir())
		if _, err := m.Start("camera"); err != nil {
			t.Fatalf("valid frame rate rejected: %g: %v", rate, err)
		}
		awaitRecording(t, func() bool { return c.HasViewer("camera") })
		if status := m.Status("camera"); !strings.Contains(status.Note, "audio is omitted") || rate > 0 && !strings.Contains(status.Note, "configured frame rate") {
			t.Fatalf("audio and clock notes were not both preserved: %+v", status)
		}
		m.StopAll()
	}
	const count = 1_000_000
	const rate = 29.97
	var accumulated time.Duration
	for i := uint64(0); i < count; i++ {
		accumulated += recordingFrameTime(i+1, rate) - recordingFrameTime(i, rate)
	}
	want := time.Duration(math.Round(float64(count) * float64(time.Second) / rate))
	if accumulated != want {
		t.Fatalf("fractional frame rate accumulated clock drift: %s vs %s", accumulated, want)
	}
}

func TestRecordingReconnectsKeepOneFileAndContinuousTimestamps(t *testing.T) {
	for _, rate := range []float64{0, 15} {
		t.Run(strconv.FormatFloat(rate, 'g', -1, 64), func(t *testing.T) {
			c := recordingTestConfig(t, 0)
			stream := c.Streams["camera"]
			stream.Recording.FrameRate = rate
			c.Streams["camera"] = stream
			dir := t.TempDir()
			m := NewRecordingManager(c, dir)
			defer m.StopAll()
			started, err := m.Start("camera")
			if err != nil {
				t.Fatal(err)
			}
			codec, packets := recordingFixture(t)
			var current string
			for cycle, origin := range []time.Duration{time.Hour, time.Second, 30 * time.Second} {
				if cycle > 0 {
					c.coAd("camera", []av.CodecData{codec})
				}
				awaitRecording(t, func() bool { return c.HasViewer("camera") })
				leading := packets[1]
				leading.Time = origin - time.Second
				c.cast("camera", leading) // Discarded pre-IDR packets must not advance the output clock.
				for i, packet := range packets {
					packet.Time = origin + time.Duration(i)*200*time.Millisecond
					c.cast("camera", packet)
				}
				c.sourceDisconnected("camera", errors.New("synthetic camera outage"))
				awaitRecording(t, func() bool {
					status := m.Status("camera")
					return status.State == "reconnecting" && status.RetryCount == cycle+1
				})
				status := m.Status("camera")
				if !status.Active || status.Error != "" || status.LastRetryError == "" || status.CurrentFile == "" || len(status.Files) != 0 {
					t.Fatalf("single-file recording intent was lost: %+v", status)
				}
				if cycle == 0 {
					current = status.CurrentFile
				} else if status.CurrentFile != current {
					t.Fatal("unchanged H264 source unexpectedly rotated the single MP4")
				}
				if !status.StartedAt.Equal(*started.StartedAt) {
					t.Fatal("reconnection replaced the recording session")
				}
				if status.NextRetryAt == nil || time.Until(*status.NextRetryAt) > time.Second || time.Until(*status.NextRetryAt) < 500*time.Millisecond {
					t.Fatalf("backoff did not reset after accepted media: %+v", status.NextRetryAt)
				}
				duplicate, err := m.Start("camera")
				if err != nil || !duplicate.StartedAt.Equal(*started.StartedAt) || duplicate.RetryCount != status.RetryCount {
					t.Fatal("duplicate start replaced reconnecting recording")
				}
			}
			stopStarted := time.Now()
			status, err := m.Stop("camera")
			if err != nil || time.Since(stopStarted) > time.Second || status.Active || status.State != "stopped" || status.LastRetryError != "" || status.NextRetryAt != nil || len(status.Files) != 1 {
				t.Fatalf("stop during retry failed: %+v %v", status, err)
			}
			samples := recordingSamples(t, filepath.Join(dir, status.Files[0]))
			if len(samples) != 21 {
				t.Fatalf("got %d frames, want 7 complete frames from each generation", len(samples))
			}
			step := uint64(18000)
			if rate > 0 {
				step = 6000
			}
			for i, sample := range samples {
				if sample.dts != uint64(i)*step || sample.duration != step {
					t.Fatalf("source reset leaked into MP4 timing at frame %d: %+v", i, sample)
				}
				if i%7 == 0 && sample.flags&fmp4io.SampleNoDependencies == 0 {
					t.Fatal("reconnection resumed without an IDR")
				}
			}
		})
	}
}

func TestRecordingInitialOfflineRecoveryAndCancelableBackoff(t *testing.T) {
	c := recordingTestConfig(t, 0)
	codec, packets := recordingFixture(t)
	c.sourceDisconnected("camera", errors.New("initially offline"))
	m := NewRecordingManager(c, t.TempDir())
	defer m.StopAll()
	if _, err := m.Start("camera"); err != nil {
		t.Fatal(err)
	}
	awaitRecording(t, func() bool { return m.Status("camera").State == "reconnecting" })
	status := m.Status("camera")
	if !status.Active || status.RetryCount != 1 || status.Error != "" || status.NextRetryAt == nil || !c.HasDemand("camera") {
		t.Fatalf("initial outage lost recording demand: %+v", status)
	}
	*status.NextRetryAt = time.Time{}
	if m.Status("camera").NextRetryAt.IsZero() {
		t.Fatal("retry timestamp exposed internal mutable memory")
	}
	c.coAd("camera", []av.CodecData{codec})
	awaitRecording(t, func() bool { return c.HasViewer("camera") })
	for _, packet := range packets {
		c.cast("camera", packet)
	}
	awaitRecording(t, func() bool { return m.Status("camera").State == "recording" })
	status = m.Status("camera")
	if status.LastRetryError != "" || status.NextRetryAt != nil || status.RetryCount != 1 {
		t.Fatalf("successful media did not clear transient status: %+v", status)
	}
	c.sourceDisconnected("camera", errors.New("offline again"))
	awaitRecording(t, func() bool { return m.Status("camera").State == "reconnecting" })
	stopStarted := time.Now()
	m.StopAll()
	if time.Since(stopStarted) > time.Second {
		t.Fatal("shutdown waited for reconnect backoff")
	}
	if status = m.Status("camera"); status.Active || status.State != "stopped" || status.LastRetryError != "" {
		t.Fatalf("shutdown while offline failed: %+v", status)
	}
	for failures, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 15 * time.Second, 15 * time.Second, 15 * time.Second} {
		if got := recordingRetryDelay(failures); got != want {
			t.Fatalf("retry %d delay = %s, want %s", failures, got, want)
		}
	}
}

func TestRecordingReconnectChunkCallbacksKeepSequenceAndFinalFlags(t *testing.T) {
	c := recordingTestConfig(t, 30)
	m := NewRecordingManager(c, t.TempDir())
	defer m.StopAll()
	events := make(chan RecordingChunk, 8)
	m.OnChunk = func(chunk RecordingChunk) { events <- chunk }
	if _, err := m.Start("camera"); err != nil {
		t.Fatal(err)
	}
	codec, packets := recordingFixture(t)
	for cycle := range 3 {
		if cycle > 0 {
			c.coAd("camera", []av.CodecData{codec})
		}
		awaitRecording(t, func() bool { return c.HasViewer("camera") })
		for _, packet := range packets {
			c.cast("camera", packet)
		}
		if cycle < 2 {
			c.sourceDisconnected("camera", errors.New("chunk boundary outage"))
			awaitRecording(t, func() bool {
				status := m.Status("camera")
				return status.State == "reconnecting" && status.RetryCount == cycle+1
			})
			if status := m.Status("camera"); len(status.Files) != cycle+1 || status.CurrentFile != "" {
				t.Fatalf("outage chunk was not finalized exactly once: %+v", status)
			}
		} else {
			awaitRecording(t, func() bool { return m.Status("camera").State == "recording" })
		}
	}
	status, err := m.Stop("camera")
	if err != nil || len(status.Files) != 3 || len(events) != 3 {
		t.Fatalf("wrong completed chunks: %+v %v", status, err)
	}
	for i := range 3 {
		event := <-events
		if event.Sequence != i+1 || event.Final != (i == 2) || event.Duration <= 0 || event.RelativePath != status.Files[i] {
			t.Fatalf("callback sequence/final flag incorrect: %+v", event)
		}
	}
}

func TestRecordingCodecChangeRotatesSingleFile(t *testing.T) {
	c := recordingTestConfig(t, 0)
	dir := t.TempDir()
	m := NewRecordingManager(c, dir)
	defer m.StopAll()
	codec, packets := recordingFixture(t)
	pps := append([]byte{}, codec.PPS()...)
	pps[len(pps)-1] ^= 1
	changedCodec, err := h264parser.NewCodecDataFromSPSAndPPS(codec.SPS(), pps)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Start("camera"); err != nil {
		t.Fatal(err)
	}
	awaitRecording(t, func() bool { return c.HasViewer("camera") })
	for _, packet := range packets {
		c.cast("camera", packet)
	}
	c.coAd("camera", []av.CodecData{changedCodec}) // Same resolution, different PPS.
	awaitRecording(t, func() bool { return m.Status("camera").State == "reconnecting" })
	awaitRecording(t, func() bool { return c.HasViewer("camera") })
	for _, packet := range packets {
		packet.Time += time.Hour
		c.cast("camera", packet)
	}
	c.sourceDisconnected("camera", errors.New("end of changed source"))
	awaitRecording(t, func() bool {
		status := m.Status("camera")
		return status.State == "reconnecting" && status.RetryCount == 2
	})
	if status := m.Status("camera"); len(status.Files) != 1 || status.CurrentFile == "" {
		t.Fatalf("changed parameter sets did not rotate the old file: %+v", status)
	}
	status, err := m.Stop("camera")
	if err != nil || len(status.Files) != 2 {
		t.Fatalf("codec change lost output: %+v %v", status, err)
	}
	for _, name := range status.Files {
		samples := recordingSamples(t, filepath.Join(dir, name))
		if len(samples) != 7 || samples[0].dts != 0 || samples[0].flags&fmp4io.SampleNoDependencies == 0 {
			t.Fatal("codec rollover file is not independently decodable")
		}
	}
}

func TestRecordingStorageFailureStaysTerminalOnOutageAndStop(t *testing.T) {
	for _, chunkSeconds := range []int{0, 30} {
		t.Run(strconv.Itoa(chunkSeconds), func(t *testing.T) {
			c := recordingTestConfig(t, chunkSeconds)
			dir := t.TempDir()
			m := NewRecordingManager(c, dir)
			defer m.StopAll()
			if _, err := m.Start("camera"); err != nil {
				t.Fatal(err)
			}
			awaitRecording(t, func() bool { return c.HasViewer("camera") })
			_, packets := recordingFixture(t)
			for _, packet := range packets {
				c.cast("camera", packet)
			}
			awaitRecording(t, func() bool { return m.Status("camera").State == "recording" })
			target := filepath.Join(dir, strings.TrimSuffix(m.Status("camera").CurrentFile, ".part"))
			if err := os.WriteFile(target, []byte("existing file"), 0600); err != nil {
				t.Fatal(err)
			}
			if chunkSeconds > 0 {
				c.sourceDisconnected("camera", errors.New("outage while publishing fails"))
			} else if _, err := m.Stop("camera"); err == nil {
				t.Fatal("cancellation masked the finalization failure")
			}
			awaitRecording(t, func() bool { return !m.Status("camera").Active })
			if status := m.Status("camera"); status.State != "error" || status.Error == "" || status.RetryCount != 0 || status.NextRetryAt != nil {
				t.Fatalf("disk failure was incorrectly retried: %+v", status)
			}
			if data, err := os.ReadFile(target); err != nil || string(data) != "existing file" {
				t.Fatal("failed publication overwrote another file")
			}
		})
	}
}

func TestRecordingReconnectWaitsForFirstSliceOfIDR(t *testing.T) {
	c := recordingTestConfig(t, 30)
	stream := c.Streams["camera"]
	stream.Recording.FrameRate = 15
	c.Streams["camera"] = stream
	dir := t.TempDir()
	m := NewRecordingManager(c, dir)
	defer m.StopAll()
	if _, err := m.Start("camera"); err != nil {
		t.Fatal(err)
	}
	awaitRecording(t, func() bool { return c.HasViewer("camera") })
	_, packets := recordingFixture(t)
	nalus, _ := h264parser.SplitNALUs(packets[0].Data)
	partial := packets[0]
	partial.Data = append([]byte{0, 0, 0, 1}, nalus[1]...)
	c.cast("camera", partial)
	for _, packet := range packets[1:] {
		c.cast("camera", packet)
	}
	c.sourceDisconnected("camera", errors.New("end of partial-IDR fixture"))
	awaitRecording(t, func() bool { return m.Status("camera").State == "reconnecting" })
	status, err := m.Stop("camera")
	if err != nil || len(status.Files) != 1 {
		t.Fatalf("failed to resume after complete IDR: %+v %v", status, err)
	}
	samples := recordingSamples(t, filepath.Join(dir, status.Files[0]))
	if len(samples) != 2 || samples[0].dts != 0 || samples[1].dts != 6000 {
		t.Fatalf("partial IDR was accepted or skipped frames advanced the override clock: %+v", samples)
	}
}
