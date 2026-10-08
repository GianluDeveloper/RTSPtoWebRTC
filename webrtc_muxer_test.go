package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/deepch/vdk/av"
	"github.com/deepch/vdk/codec/h264parser"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

type muxerTestCodec struct{ kind av.CodecType }

func (c muxerTestCodec) Type() av.CodecType { return c.kind }
func (c muxerTestCodec) SPS() []byte        { return []byte{0x67, 0x42, 0xe0, 0x1f, 0x80} }
func (c muxerTestCodec) PPS() []byte        { return []byte{0x68, 0xce, 0x38, 0x80} }

func TestH264AccessUnit(t *testing.T) {
	codec := muxerTestCodec{av.H264}
	nalus := [][]byte{{0x65, 1, 2, 3}, {0x65, 4, 5, 6}}
	var avcc, annexB []byte
	for _, nalu := range nalus {
		avcc = binary.BigEndian.AppendUint32(avcc, uint32(len(nalu)))
		avcc = append(avcc, nalu...)
		annexB = append(annexB, 0, 0, 0, 1)
		annexB = append(annexB, nalu...)
	}
	for name, data := range map[string][]byte{"AVCC": avcc, "AnnexB": annexB} {
		t.Run(name, func(t *testing.T) {
			original := bytes.Clone(data)
			actual, _ := h264parser.SplitNALUs(h264AccessUnit(data, codec))
			expected := append([][]byte{codec.SPS(), codec.PPS()}, nalus...)
			if len(actual) != len(expected) {
				t.Fatalf("got %d NALs, want %d", len(actual), len(expected))
			}
			for i := range expected {
				if !bytes.Equal(actual[i], expected[i]) {
					t.Fatalf("NAL %d: %x != %x", i, actual[i], expected[i])
				}
			}
			if !bytes.Equal(data, original) {
				t.Fatal("mutated shared camera packet")
			}
			withHeaders := h264AccessUnit(h264AccessUnit(data, codec), codec)
			if !bytes.Equal(withHeaders, h264AccessUnit(data, codec)) {
				t.Fatal("duplicated SPS/PPS")
			}
		})
	}
	for _, data := range [][]byte{nil, {}, {0, 0, 0, 0}, {0, 0, 0, 1}} {
		_ = h264AccessUnit(data, codec) // Empty NALs must not panic.
	}
}

func TestWebRTCPacketTimestampsAndFragments(t *testing.T) {
	m := NewWebRTCMuxer(WebRTCOptions{})
	defer m.Close()
	codec := muxerTestCodec{av.H264}
	capability, payloader, _ := webRTCCodec(codec)
	var received []*rtp.Packet
	m.tracks[0] = &webRTCTrack{
		codec: codec, clockRate: capability.ClockRate,
		packetizer: rtp.NewPacketizer(1200, 0, 0, payloader, rtp.NewRandomSequencer(), capability.ClockRate),
		writeRTP:   func(packet *rtp.Packet) error { received = append(received, packet.Clone()); return nil },
	}
	data := append([]byte{0, 0, 0, 1, 0x65}, bytes.Repeat([]byte{1}, 3000)...)
	data = append(data, 0, 0, 0, 1, 0x65, 2, 3, 4)
	for _, pts := range []time.Duration{time.Second, 1040 * time.Millisecond} {
		start := len(received)
		if err := m.WritePacket(av.Packet{Data: data, Time: pts, Duration: 40 * time.Millisecond}); err != nil {
			t.Fatal(err)
		}
		if len(received)-start < 3 {
			t.Fatal("expected fragmented access unit")
		}
		for i, packet := range received[start:] {
			if packet.Timestamp != uint32(pts/time.Millisecond)*90 {
				t.Fatalf("NAL/fragment %d timestamp = %d", i, packet.Timestamp)
			}
			if packet.Marker != (i == len(received)-start-1) {
				t.Fatal("marker must identify access-unit end")
			}
		}
	}
	for i := 1; i < len(received); i++ {
		if received[i].SequenceNumber != received[i-1].SequenceNumber+1 {
			t.Fatal("RTP sequence gap")
		}
	}
}

func TestWebRTCCodecClockRates(t *testing.T) {
	for _, test := range []struct {
		kind     av.CodecType
		rate     uint32
		channels uint16
	}{
		{av.H264, 90000, 0}, {av.PCM_ALAW, 8000, 1}, {av.PCM_MULAW, 8000, 1}, {av.OPUS, 48000, 2},
	} {
		capability, _, ok := webRTCCodec(muxerTestCodec{test.kind})
		if !ok || capability.ClockRate != test.rate || capability.Channels != test.channels {
			t.Fatalf("incorrect RTP capability for %v: %+v", test.kind, capability)
		}
	}
	if _, _, ok := webRTCCodec(muxerTestCodec{av.AAC}); ok {
		t.Fatal("unsupported AAC accepted")
	}
}

func TestWebRTCCloseAndInvalidOffer(t *testing.T) {
	m := NewWebRTCMuxer(WebRTCOptions{})
	if _, err := m.WriteHeader([]av.CodecData{muxerTestCodec{av.H264}}, "not base64"); err == nil {
		t.Fatal("invalid offer accepted")
	}
	select {
	case <-m.Done():
	default:
		t.Fatal("failed setup leaked connection")
	}
	var group sync.WaitGroup
	for range 10 {
		group.Go(func() { _ = m.Close() })
	}
	group.Wait()
	if err := m.WritePacket(av.Packet{}); !errors.Is(err, errWebRTCClosed) {
		t.Fatalf("closed muxer returned %v", err)
	}
}

// Exercise real SDP negotiation, ICE, DTLS, SRTP, and the receive path locally.
func TestWebRTCLoopback(t *testing.T) {
	// Keep this test independent of host VPNs, Docker bridges, firewalls and
	// candidate-pair ordering: both peers communicate only on the loopback socket.
	settings := webrtc.SettingEngine{}
	settings.SetIncludeLoopbackCandidate(true)
	settings.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	settings.SetIPFilter(func(ip net.IP) bool { return ip.IsLoopback() })
	client, err := webrtc.NewAPI(webrtc.WithSettingEngine(settings)).NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err = client.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
		t.Fatal(err)
	}
	received := make(chan *rtp.Packet, 16)
	client.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		for {
			packet, _, readErr := track.ReadRTP()
			if readErr != nil {
				return
			}
			select {
			case received <- packet:
			default:
			}
		}
	})
	offer, err := client.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	gathered := webrtc.GatheringCompletePromise(client)
	if err = client.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gathered:
	case <-time.After(10 * time.Second):
		t.Fatal("client ICE timeout")
	}
	muxer := NewWebRTCMuxer(WebRTCOptions{})
	muxer.settings = settings
	defer muxer.Close()
	answer, err := muxer.WriteHeader([]av.CodecData{muxerTestCodec{av.H264}}, base64.StdEncoding.EncodeToString([]byte(client.LocalDescription().SDP)))
	if err != nil {
		t.Fatal(err)
	}
	sdp, err := base64.StdEncoding.DecodeString(answer)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: string(sdp)}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-muxer.Ready():
	case <-time.After(10 * time.Second):
		t.Fatal("peer connection timeout")
	}
	for _, pts := range []time.Duration{time.Second, 1040 * time.Millisecond} {
		if err = muxer.WritePacket(av.Packet{Data: []byte{0, 0, 0, 1, 0x61, 0x80}, Time: pts, Duration: 40 * time.Millisecond}); err != nil {
			t.Fatal(err)
		}
		select {
		case packet := <-received:
			if packet.Timestamp != uint32(pts/time.Millisecond)*90 {
				t.Fatalf("received RTP timestamp %d", packet.Timestamp)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no media received")
		}
	}
}
