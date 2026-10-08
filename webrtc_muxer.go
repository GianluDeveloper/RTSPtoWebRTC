package main

import (
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/deepch/vdk/av"
	"github.com/deepch/vdk/codec/h264parser"
	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
)

var errWebRTCClosed = errors.New("WebRTC connection closed")

type WebRTCOptions struct {
	ICEServers    []string
	ICEUsername   string
	ICECredential string
	PortMin       uint16
	PortMax       uint16
}

// WebRTCMuxer forwards the camera's media clock directly to a WebRTC peer.
// Its lifetime depends on peer/media activity, never the keyframe interval.
type WebRTCMuxer struct {
	options WebRTCOptions
	// settings is configured before WriteHeader; the zero value is Pion's production default.
	settings  webrtc.SettingEngine
	mu        sync.Mutex
	pc        *webrtc.PeerConnection
	tracks    map[int8]*webRTCTrack
	started   bool
	done      chan struct{}
	ready     chan struct{}
	connected sync.Once
	closed    sync.Once
}

type webRTCTrack struct {
	codec      av.CodecData
	clockRate  uint32
	packetizer rtp.Packetizer
	writeRTP   func(*rtp.Packet) error
}

func NewWebRTCMuxer(options WebRTCOptions) *WebRTCMuxer {
	return &WebRTCMuxer{options: options, tracks: make(map[int8]*webRTCTrack), done: make(chan struct{}), ready: make(chan struct{})}
}

func (m *WebRTCMuxer) Done() <-chan struct{} { return m.done }

// Ready closes after ICE and DTLS connect, so callers can subscribe without losing the first IDR.
func (m *WebRTCMuxer) Ready() <-chan struct{} { return m.ready }

func (m *WebRTCMuxer) WriteHeader(streams []av.CodecData, encodedOffer string) (answer string, err error) {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return "", errors.New("WebRTC header already written")
	}
	m.started = true
	m.mu.Unlock()
	defer func() {
		if err != nil {
			_ = m.Close()
		}
	}()
	offer, err := base64.StdEncoding.DecodeString(encodedOffer)
	if err != nil {
		return "", fmt.Errorf("decode SDP offer: %w", err)
	}
	mediaEngine := &webrtc.MediaEngine{}
	if err = mediaEngine.RegisterDefaultCodecs(); err != nil {
		return "", err
	}
	registry := &interceptor.Registry{}
	if err = webrtc.RegisterDefaultInterceptors(mediaEngine, registry); err != nil {
		return "", err
	}
	settings := m.settings
	if m.options.PortMin != 0 || m.options.PortMax != 0 {
		if m.options.PortMin == 0 || m.options.PortMax == 0 {
			return "", errors.New("both WebRTC UDP port bounds must be configured")
		}
		if err = settings.SetEphemeralUDPPortRange(m.options.PortMin, m.options.PortMax); err != nil {
			return "", err
		}
	}
	configuration := webrtc.Configuration{}
	if len(m.options.ICEServers) != 0 {
		configuration.ICEServers = []webrtc.ICEServer{{
			URLs: m.options.ICEServers, Username: m.options.ICEUsername,
			Credential: m.options.ICECredential, CredentialType: webrtc.ICECredentialTypePassword,
		}}
	}
	api := webrtc.NewAPI(webrtc.WithMediaEngine(mediaEngine), webrtc.WithInterceptorRegistry(registry), webrtc.WithSettingEngine(settings))
	pc, err := api.NewPeerConnection(configuration)
	if err != nil {
		return "", err
	}
	m.mu.Lock()
	select {
	case <-m.done:
		m.mu.Unlock()
		_ = pc.Close()
		return "", errWebRTCClosed
	default:
		m.pc = pc
	}
	m.mu.Unlock()
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateConnected {
			m.connected.Do(func() { close(m.ready) })
		}
		// Disconnected is recoverable; ICE decides when connectivity has failed.
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
			go m.Close()
		}
	})
	for index, codec := range streams {
		capability, payloader, supported := webRTCCodec(codec)
		if !supported {
			continue
		}
		track, trackErr := webrtc.NewTrackLocalStaticRTP(capability, fmt.Sprintf("camera-%d", index), "camera")
		if trackErr != nil {
			return "", trackErr
		}
		sender, trackErr := pc.AddTrack(track)
		if trackErr != nil {
			return "", trackErr
		}
		// Reading RTCP drives NACK processing in the default interceptors.
		go func() {
			buffer := make([]byte, 1500)
			for {
				if _, _, readErr := sender.Read(buffer); readErr != nil {
					return
				}
			}
		}()
		m.mu.Lock()
		m.tracks[int8(index)] = &webRTCTrack{
			codec: codec, clockRate: capability.ClockRate, writeRTP: track.WriteRTP,
			packetizer: rtp.NewPacketizer(1200, 0, 0, payloader, rtp.NewRandomSequencer(), capability.ClockRate),
		}
		m.mu.Unlock()
	}
	if len(m.tracks) == 0 {
		return "", errors.New("no supported WebRTC tracks")
	}
	if err = pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: string(offer)}); err != nil {
		return "", err
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	localAnswer, err := pc.CreateAnswer(nil)
	if err != nil {
		return "", err
	}
	if err = pc.SetLocalDescription(localAnswer); err != nil {
		return "", err
	}
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	select {
	case <-gathered:
	case <-m.done:
		return "", errWebRTCClosed
	case <-timer.C:
		return "", errors.New("WebRTC ICE gathering timed out")
	}
	return base64.StdEncoding.EncodeToString([]byte(pc.LocalDescription().SDP)), nil
}

func webRTCCodec(codec av.CodecData) (webrtc.RTPCodecCapability, rtp.Payloader, bool) {
	if codec == nil {
		return webrtc.RTPCodecCapability{}, nil, false
	}
	switch codec.Type() {
	case av.H264:
		profile := "42e01f"
		if parameters, ok := codec.(interface{ SPS() []byte }); ok && len(parameters.SPS()) >= 4 {
			profile = fmt.Sprintf("%x", parameters.SPS()[1:4])
		}
		return webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000, SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=" + profile}, &codecs.H264Payloader{}, true
	case av.PCM_ALAW:
		return webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMA, ClockRate: 8000, Channels: 1}, &codecs.G711Payloader{}, true
	case av.PCM_MULAW:
		return webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMU, ClockRate: 8000, Channels: 1}, &codecs.G711Payloader{}, true
	case av.OPUS:
		// Opus always uses the 48 kHz RTP clock and two SDP channels, including mono encoders.
		return webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}, &codecs.OpusPayloader{}, true
	default:
		return webrtc.RTPCodecCapability{}, nil, false
	}
}

func (m *WebRTCMuxer) WritePacket(packet av.Packet) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	select {
	case <-m.done:
		return errWebRTCClosed
	default:
	}
	track, exists := m.tracks[packet.Idx]
	if !exists || len(packet.Data) == 0 {
		return nil
	}
	data := packet.Data
	if track.codec.Type() == av.H264 {
		data = h264AccessUnit(packet.Data, track.codec)
	}
	packets := track.packetizer.Packetize(data, 0)
	// One RTP timestamp per camera access unit, including all of its NALs.
	// This also preserves B-frame presentation order and zero-duration slices.
	pts := packet.Time + packet.CompositionTime
	timestamp := uint32(pts/time.Second)*track.clockRate + uint32((pts%time.Second)*time.Duration(track.clockRate)/time.Second)
	for _, encoded := range packets {
		encoded.Timestamp = timestamp
		if err := track.writeRTP(encoded); err != nil {
			return err
		}
		if track.codec.Type() == av.PCM_ALAW || track.codec.Type() == av.PCM_MULAW {
			timestamp += uint32(len(encoded.Payload))
		}
	}
	return nil
}

func h264AccessUnit(data []byte, codec av.CodecData) []byte {
	nalus, _ := h264parser.SplitNALUs(data)
	var keyframe, hasSPS, hasPPS bool
	for _, nalu := range nalus {
		if len(nalu) == 0 {
			continue
		}
		switch nalu[0] & 0x1f {
		case 5:
			keyframe = true
		case 7:
			hasSPS = true
		case 8:
			hasPPS = true
		}
	}
	result := make([]byte, 0, len(data)+128)
	appendNAL := func(nalu []byte) {
		if len(nalu) > 0 {
			result = append(result, 0, 0, 0, 1)
			result = append(result, nalu...)
		}
	}
	if parameters, ok := codec.(interface {
		SPS() []byte
		PPS() []byte
	}); ok && keyframe {
		if !hasSPS {
			appendNAL(parameters.SPS())
		}
		if !hasPPS {
			appendNAL(parameters.PPS())
		}
	}
	for _, nalu := range nalus {
		appendNAL(nalu)
	}
	return result
}

func (m *WebRTCMuxer) Close() error {
	m.closed.Do(func() { close(m.done) })
	m.mu.Lock()
	pc := m.pc
	m.pc = nil
	m.mu.Unlock()
	if pc != nil {
		return pc.Close()
	}
	return nil
}
