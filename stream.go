package main

import (
	"errors"
	"io"
	"log"
	"time"

	"github.com/deepch/vdk/av"
	"github.com/deepch/vdk/codec/h264parser"
	"github.com/deepch/vdk/format/rtspv2"
)

var (
	ErrMediaTimeout       = errors.New("no media packets received")
	ErrKeyframeTimeout    = errors.New("no initial video keyframe received")
	ErrSourceDisconnected = errors.New("RTSP source disconnected")
	ErrNoViewers          = errors.New("on-demand stream has no viewers")
)

const mediaIdleTimeout = 20 * time.Second
const firstKeyframeTimeout = 60 * time.Second

func serveStreams() {
	_, ids := Config.list()
	for _, id := range ids {
		Config.mutex.RLock()
		onDemand := Config.Streams[id].OnDemand
		Config.mutex.RUnlock()
		if !onDemand {
			Config.RunIFNotRun(id)
		}
	}
}

func RTSPWorkerLoop(id, url string, onDemand, disableAudio, debug bool) {
	defer Config.RunUnlock(id)
	for {
		log.Printf("stream=%s RTSP connecting", id)
		err := RTSPWorker(id, url, onDemand, disableAudio, debug)
		Config.sourceDisconnected(id, err)
		// Library errors may embed camera credentials in the source URL.
		log.Printf("stream=%s RTSP session ended (%T)", id, err)
		if onDemand && !Config.HasDemand(id) {
			return
		}
		time.Sleep(time.Second)
	}
}

func RTSPWorker(id, url string, onDemand, disableAudio, debug bool) error {
	client, err := rtspv2.Dial(rtspv2.RTSPClientOptions{
		URL: url, DisableAudio: disableAudio, DialTimeout: 10 * time.Second,
		ReadWriteTimeout: 15 * time.Second, Debug: debug,
	})
	if err != nil {
		return err
	}
	defer client.Close()
	Config.coAd(id, client.CodecData)
	log.Printf("stream=%s RTSP connected", id)
	idle := time.NewTimer(mediaIdleTimeout)
	defer idle.Stop()
	viewers := time.NewTicker(20 * time.Second)
	defer viewers.Stop()
	for {
		select {
		case <-viewers.C:
			if onDemand && !Config.HasDemand(id) {
				return ErrNoViewers
			}
		case <-idle.C:
			return ErrMediaTimeout
		case signal, ok := <-client.Signals:
			if !ok {
				return ErrSourceDisconnected
			}
			switch signal {
			case rtspv2.SignalCodecUpdate:
				Config.coAd(id, client.CodecData)
			case rtspv2.SignalStreamRTPStop:
				return ErrSourceDisconnected
			}
		case packet, ok := <-client.OutgoingPacketQueue:
			if !ok {
				return ErrSourceDisconnected
			}
			if packet == nil {
				continue
			}
			// Packet arrival measures activity independently of the keyframe interval.
			idle.Reset(mediaIdleTimeout)
			Config.cast(id, *packet)
		}
	}
}

type packetWriter interface{ WritePacket(av.Packet) error }

// Both signaling endpoints share this loop. Only initial decoding needs an
// IDR; ordinary video frames keep a healthy viewer session alive afterward.
func forwardPackets(packets <-chan av.Packet, done <-chan struct{}, writer packetWriter, codecs []av.CodecData, timeout time.Duration) error {
	hasVideo := false
	for _, codec := range codecs {
		if codec.Type() == av.H264 {
			hasVideo = true
		}
	}
	started := !hasVideo
	idle := time.NewTimer(timeout)
	defer idle.Stop()
	initial := time.NewTimer(firstKeyframeTimeout)
	defer initial.Stop()
	var initialDeadline <-chan time.Time
	if hasVideo {
		initialDeadline = initial.C
	}
	for {
		select {
		case <-done:
			return nil
		case <-idle.C:
			return ErrMediaTimeout
		case <-initialDeadline:
			return ErrKeyframeTimeout
		case packet, ok := <-packets:
			if !ok {
				return io.EOF
			}
			index := int(packet.Idx)
			if index < 0 || index >= len(codecs) {
				continue
			}
			kind := codecs[index].Type()
			if !supportedCodec(kind) {
				continue
			}
			isVideo := kind == av.H264
			if isVideo || !hasVideo {
				idle.Reset(timeout)
			}
			if isVideo && h264Keyframe(packet) && !started {
				started = true
				initial.Stop()
				initialDeadline = nil
			}
			if !started {
				continue
			}
			if err := writer.WritePacket(packet); err != nil {
				return err
			}
		}
	}
}

func supportedCodec(kind av.CodecType) bool {
	return kind == av.H264 || kind == av.PCM_ALAW || kind == av.PCM_MULAW || kind == av.OPUS
}

// Some RTSP packetizers omit IsKeyFrame on IDRs extracted from STAP-A.
func h264Keyframe(packet av.Packet) bool {
	if packet.IsKeyFrame {
		return true
	}
	nalus, _ := h264parser.SplitNALUs(packet.Data)
	for _, nalu := range nalus {
		if len(nalu) > 0 && nalu[0]&0x1f == 5 {
			return true
		}
	}
	return false
}
