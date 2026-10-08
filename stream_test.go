package main

import (
	"errors"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"github.com/deepch/vdk/av"
	"github.com/deepch/vdk/codec/h264parser"
)

type flowTestCodec av.CodecType

func (c flowTestCodec) Type() av.CodecType { return av.CodecType(c) }

type recordingWriter struct{ packets []av.Packet }

func (w *recordingWriter) WritePacket(packet av.Packet) error {
	w.packets = append(w.packets, packet)
	return nil
}

// A camera with a long GOP must stay connected while inter-frames arrive.
// Fake time exercises minutes of media without sleeps or flaky timing margins.
func TestForwardLongGOPStaysAlive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		packets := make(chan av.Packet)
		done := make(chan struct{})
		result := make(chan error, 1)
		writer := &recordingWriter{}
		go func() {
			result <- forwardPackets(packets, done, writer, []av.CodecData{flowTestCodec(av.H264)}, 10*time.Second)
		}()
		for second := range 90 {
			packets <- av.Packet{Idx: 0, IsKeyFrame: second%30 == 0}
			time.Sleep(time.Second)
			synctest.Wait()
			select {
			case err := <-result:
				t.Fatalf("healthy stream ended at %ds: %v", second, err)
			default:
			}
		}
		close(done)
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		if len(writer.packets) != 90 {
			t.Fatalf("forwarded %d frames, want 90", len(writer.packets))
		}
	})
}

func TestForwardWaitsForInitialKeyframe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		packets := make(chan av.Packet)
		done := make(chan struct{})
		result := make(chan error, 1)
		writer := &recordingWriter{}
		go func() {
			result <- forwardPackets(packets, done, writer, []av.CodecData{flowTestCodec(av.H264)}, 5*time.Second)
		}()
		for second := range 15 {
			packets <- av.Packet{IsKeyFrame: second == 12}
			time.Sleep(time.Second)
		}
		close(done)
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		if len(writer.packets) != 3 || !writer.packets[0].IsKeyFrame {
			t.Fatal("forwarded undecodable frames before first IDR")
		}
	})
}

func TestForwardStopsOnTrueInactivity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		packets := make(chan av.Packet)
		result := make(chan error, 1)
		go func() {
			result <- forwardPackets(packets, nil, &recordingWriter{}, []av.CodecData{flowTestCodec(av.H264)}, 5*time.Second)
		}()
		packets <- av.Packet{IsKeyFrame: true}
		time.Sleep(6 * time.Second)
		if err := <-result; !errors.Is(err, ErrMediaTimeout) {
			t.Fatalf("got %v", err)
		}
	})
}

func TestAudioDoesNotMaskFrozenVideo(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		packets := make(chan av.Packet, 16)
		result := make(chan error, 1)
		go func() {
			result <- forwardPackets(packets, nil, &recordingWriter{}, []av.CodecData{flowTestCodec(av.H264), flowTestCodec(av.PCM_ALAW)}, 5*time.Second)
		}()
		packets <- av.Packet{IsKeyFrame: true}
		for range 6 {
			packets <- av.Packet{Idx: 1}
			time.Sleep(time.Second)
		}
		if err := <-result; !errors.Is(err, ErrMediaTimeout) {
			t.Fatalf("got %v", err)
		}
	})
}

func TestAudioOnlyAndSourceClosure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		packets := make(chan av.Packet)
		result := make(chan error, 1)
		writer := &recordingWriter{}
		go func() {
			result <- forwardPackets(packets, nil, writer, []av.CodecData{flowTestCodec(av.PCM_ALAW)}, 5*time.Second)
		}()
		for range 20 {
			packets <- av.Packet{}
			time.Sleep(time.Second)
		}
		close(packets)
		if err := <-result; !errors.Is(err, io.EOF) {
			t.Fatalf("got %v", err)
		}
		if len(writer.packets) != 20 {
			t.Fatal("audio dropped")
		}
	})
}

func TestSourceResetAndSlowViewerIsolation(t *testing.T) {
	c := &ConfigST{Streams: map[string]StreamST{"camera": {Status: true, Cl: make(map[string]viewer)}}}
	slowID, slow := c.clAd("camera")
	fastID, fast := c.clAd("camera")
	for i := 0; i <= cap(slow); i++ {
		c.cast("camera", av.Packet{})
		<-fast
	}
	c.mutex.RLock()
	_, slowPresent := c.Streams["camera"].Cl[slowID]
	_, fastPresent := c.Streams["camera"].Cl[fastID]
	c.mutex.RUnlock()
	if slowPresent || !fastPresent {
		t.Fatal("slow viewer did not isolate backpressure")
	}
	c.coAd("camera", []av.CodecData{flowTestCodec(av.PCM_ALAW)})
	c.sourceDisconnected("camera", io.EOF)
	if _, ok := <-fast; ok {
		t.Fatal("old viewer survived source reset")
	}
	if c.HasViewer("camera") || c.Streams["camera"].Codecs != nil || c.Streams["camera"].Status {
		t.Fatal("stale source state retained")
	}
	c.clDe("camera", slowID)
	c.clDe("camera", fastID) // Cleanup remains safe after upstream disconnect.
}

func TestInvalidH264ParametersAreNotReady(t *testing.T) {
	if codecsReady([]av.CodecData{h264parser.CodecData{}}) {
		t.Fatal("incomplete SPS/PPS accepted")
	}
	if codecsReady(nil) {
		t.Fatal("empty codecs accepted")
	}
}

func TestNegotiationCannotSubscribeAcrossSourceRestart(t *testing.T) {
	c := &ConfigST{Streams: map[string]StreamST{"camera": {Status: true, Cl: make(map[string]viewer), Codecs: []av.CodecData{flowTestCodec(av.PCM_ALAW)}}}}
	snapshot := c.coGe("camera")
	c.sourceDisconnected("camera", io.EOF)
	c.coAd("camera", []av.CodecData{flowTestCodec(av.PCM_ALAW)})
	_, stale := c.clAd("camera", snapshot.generation)
	if _, ok := <-stale; ok {
		t.Fatal("stale negotiation subscribed to replacement source")
	}
	fresh := c.coGe("camera")
	id, packets := c.clAd("camera", fresh.generation)
	if id == "" {
		t.Fatal("fresh negotiation rejected")
	}
	c.cast("camera", av.Packet{})
	if _, ok := <-packets; !ok {
		t.Fatal("fresh source prematurely closed")
	}
	c.clDe("camera", id)
}

func TestOnDemandGraceCoversNegotiationAndSourceRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := &ConfigST{Streams: map[string]StreamST{"camera": {Status: true, lastDemand: time.Now(), Cl: make(map[string]viewer)}}}
		time.Sleep(60 * time.Second)
		if !c.HasDemand("camera") {
			t.Fatal("source expired during ICE negotiation")
		}
		id, _ := c.clAd("camera")
		time.Sleep(60 * time.Second)
		if !c.HasDemand("camera") {
			t.Fatal("active viewer not counted")
		}
		c.sourceDisconnected("camera", io.EOF)
		c.clDe("camera", id)
		if !c.HasDemand("camera") {
			t.Fatal("demand lost immediately after source failure")
		}
		time.Sleep(91 * time.Second)
		if c.HasDemand("camera") {
			t.Fatal("idle source never expires")
		}
	})
}

func TestAggregatedIDRWithoutKeyframeFlag(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		packets := make(chan av.Packet)
		result := make(chan error, 1)
		writer := &recordingWriter{}
		go func() {
			result <- forwardPackets(packets, nil, writer, []av.CodecData{flowTestCodec(av.H264)}, 5*time.Second)
		}()
		packets <- av.Packet{Data: []byte{0, 0, 0, 2, 0x65, 0x80}}
		close(packets)
		if err := <-result; !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
		if len(writer.packets) != 1 {
			t.Fatal("IDR missing upstream keyframe flag was dropped")
		}
	})
}

func TestCodecChangeInvalidatesActiveSession(t *testing.T) {
	c := &ConfigST{Streams: map[string]StreamST{"camera": {Status: true, Cl: make(map[string]viewer), Codecs: []av.CodecData{flowTestCodec(av.PCM_ALAW)}}}}
	id, packets := c.clAd("camera")
	generation := c.Streams["camera"].generation
	c.coAd("camera", []av.CodecData{flowTestCodec(av.PCM_MULAW)})
	if _, ok := <-packets; ok {
		t.Fatal("old codec session remained active")
	}
	if c.Streams["camera"].generation == generation {
		t.Fatal("codec change did not advance generation")
	}
	c.clDe("camera", id)
}
