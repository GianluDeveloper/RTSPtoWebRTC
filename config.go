package main

import (
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/deepch/vdk/av"
	"github.com/deepch/vdk/codec/h264parser"
)

var Config *ConfigST

type ConfigST struct {
	mutex      sync.RWMutex
	Server     ServerST            `json:"server"`
	Streams    map[string]StreamST `json:"streams"`
	nextViewer uint64
}

type ServerST struct {
	HTTPPort                        string   `json:"http_port"`
	ICEServers                      []string `json:"ice_servers"`
	ICEUsername                     string   `json:"ice_username"`
	ICECredential                   string   `json:"ice_credential"`
	WebRTCPortMin                   uint16   `json:"webrtc_port_min"`
	WebRTCPortMax                   uint16   `json:"webrtc_port_max"`
	RecordingDir                    string   `json:"recording_dir"`
	RecordingCallbackScript         string   `json:"recording_callback_script"`
	RecordingCallbackTimeoutSeconds int      `json:"recording_callback_timeout_seconds"`
}

// Zero saves one MP4 per start/stop session; positive values rotate at an IDR.
type RecordingOptions struct {
	ChunkSeconds    int     `json:"chunk_seconds"`
	CallbackEnabled bool    `json:"callback_enabled"`
	FrameRate       float64 `json:"frame_rate"`
}

type StreamST struct {
	URL          string            `json:"url"`
	Status       bool              `json:"status"`
	OnDemand     bool              `json:"on_demand"`
	DisableAudio bool              `json:"disable_audio"`
	Debug        bool              `json:"debug"`
	Recording    RecordingOptions  `json:"recording"`
	RunLock      bool              `json:"-"`
	Codecs       []av.CodecData    `json:"-"`
	Cl           map[string]viewer `json:"-"`
	generation   uint64
	lastDemand   time.Time
	lastError    error
}

type streamSnapshot struct {
	codecs     []av.CodecData
	generation uint64
}

type viewer struct{ c chan av.Packet }

func loadConfig() *ConfigST {
	c := &ConfigST{Server: ServerST{HTTPPort: ":8083", RecordingDir: "./recordings"}, Streams: make(map[string]StreamST)}
	data, err := os.ReadFile("config.json")
	if err == nil {
		if err := json.Unmarshal(data, c); err != nil {
			log.Fatal(err)
		}
	} else if os.IsNotExist(err) {
		addr := flag.String("listen", ":8083", "HTTP host:port")
		udpMin := flag.Int("udp_min", 0, "WebRTC UDP port min")
		udpMax := flag.Int("udp_max", 0, "WebRTC UDP port max")
		iceServer := flag.String("ice_server", "", "ICE server")
		flag.Parse()
		c.Server.HTTPPort = *addr
		c.Server.WebRTCPortMin, c.Server.WebRTCPortMax = uint16(*udpMin), uint16(*udpMax)
		if *iceServer != "" {
			c.Server.ICEServers = []string{*iceServer}
		}
	} else {
		log.Fatal(err)
	}
	if c.Streams == nil {
		c.Streams = make(map[string]StreamST)
	}
	if c.Server.RecordingDir == "" {
		c.Server.RecordingDir = "./recordings"
	}
	for id, stream := range c.Streams {
		stream.Cl = make(map[string]viewer)
		c.Streams[id] = stream
	}
	return c
}

func (c *ConfigST) RunIFNotRun(id string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	stream, ok := c.Streams[id]
	if !ok {
		return
	}
	stream.lastDemand = time.Now()
	c.Streams[id] = stream
	if stream.RunLock {
		return
	}
	stream.RunLock = true
	c.Streams[id] = stream
	go RTSPWorkerLoop(id, stream.URL, stream.OnDemand, stream.DisableAudio, stream.Debug)
}

func (c *ConfigST) RunUnlock(id string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	stream, ok := c.Streams[id]
	if ok {
		stream.RunLock = false
		c.Streams[id] = stream
	}
}

func (c *ConfigST) HasViewer(id string) bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return len(c.Streams[id].Cl) > 0
}

// Demand covers codec discovery, ICE gathering and DTLS before registration.
func (c *ConfigST) HasDemand(id string) bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	stream := c.Streams[id]
	return len(stream.Cl) > 0 || time.Since(stream.lastDemand) < 90*time.Second
}

func (c *ConfigST) ext(id string) bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	_, ok := c.Streams[id]
	return ok
}

func (c *ConfigST) addURL(url string) string {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for id, stream := range c.Streams {
		if stream.URL == url {
			return id
		}
	}
	id := fmt.Sprintf("dynamic-%x", sha256.Sum256([]byte(url)))
	c.Streams[id] = StreamST{URL: url, OnDemand: true, Cl: make(map[string]viewer)}
	return id
}

// Viewers must renegotiate after a source restart: RTP timestamps and decoder
// references from the previous session cannot safely be reused.
func (c *ConfigST) sourceDisconnected(id string, err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	stream, ok := c.Streams[id]
	if !ok {
		return
	}
	if len(stream.Cl) > 0 {
		stream.lastDemand = time.Now()
	}
	stream.generation++
	for cid, v := range stream.Cl {
		close(v.c)
		delete(stream.Cl, cid)
	}
	stream.Codecs, stream.Status, stream.lastError = nil, false, err
	c.Streams[id] = stream
}

func (c *ConfigST) cast(id string, packet av.Packet) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for cid, v := range c.Streams[id].Cl {
		select {
		case v.c <- packet:
		default:
			// Arbitrary lost H264 frames corrupt decoding until the next IDR.
			// Release a slow viewer instead of silently delivering a broken GOP.
			close(v.c)
			delete(c.Streams[id].Cl, cid)
		}
	}
}

func (c *ConfigST) coAd(id string, codecs []av.CodecData) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	stream := c.Streams[id]
	if stream.Status && len(stream.Codecs) > 0 && !reflect.DeepEqual(stream.Codecs, codecs) {
		if len(stream.Cl) > 0 {
			stream.lastDemand = time.Now()
		}
		for cid, v := range stream.Cl {
			close(v.c)
			delete(stream.Cl, cid)
		}
		stream.generation++
	}
	stream.Codecs = append([]av.CodecData(nil), codecs...)
	stream.Status, stream.lastError = true, nil
	c.Streams[id] = stream
}

func codecsReady(codecs []av.CodecData) bool {
	if len(codecs) == 0 {
		return false
	}
	for _, codec := range codecs {
		if codec.Type() == av.H264 {
			video, ok := codec.(h264parser.CodecData)
			if !ok || len(video.SPS()) < 4 || len(video.PPS()) < 2 || video.SPS()[0]&0x1f != 7 || video.PPS()[0]&0x1f != 8 {
				return false
			}
		}
	}
	return true
}

func (c *ConfigST) coGe(id string) *streamSnapshot {
	for i := 0; i < 100; i++ {
		c.mutex.RLock()
		stream, ok := c.Streams[id]
		codecs := append([]av.CodecData(nil), stream.Codecs...)
		c.mutex.RUnlock()
		if !ok {
			return nil
		}
		if codecsReady(codecs) {
			return &streamSnapshot{codecs: codecs, generation: stream.generation}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

func (c *ConfigST) clAd(id string, generation ...uint64) (string, chan av.Packet) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	ch := make(chan av.Packet, 256)
	stream, ok := c.Streams[id]
	if !ok || !stream.Status || (len(generation) > 0 && generation[0] != stream.generation) {
		close(ch)
		return "", ch
	}
	c.nextViewer++
	cid := fmt.Sprint(c.nextViewer)
	stream.Cl[cid] = viewer{c: ch}
	return cid, ch
}

func (c *ConfigST) clDe(id, cid string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if v, ok := c.Streams[id].Cl[cid]; ok {
		close(v.c)
		delete(c.Streams[id].Cl, cid)
	}
}

func (c *ConfigST) list() (string, []string) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	ids := make([]string, 0, len(c.Streams))
	for id := range c.Streams {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if len(ids) == 0 {
		return "", ids
	}
	return ids[0], ids
}
