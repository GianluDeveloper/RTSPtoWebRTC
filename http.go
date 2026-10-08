package main

import (
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/deepch/vdk/av"
	"github.com/gin-gonic/gin"
)

type JCodec struct{ Type string }
type Response struct {
	Tracks []string `json:"tracks"`
	Sdp64  string   `json:"sdp64"`
}
type ResponseError struct {
	Error string `json:"error"`
}

func newRouter() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Logger(), gin.Recovery(), CORSMiddleware())
	if _, err := os.Stat("./web"); err == nil {
		router.LoadHTMLGlob("web/templates/*")
		router.GET("/", HTTPAPIServerIndex)
		router.GET("/stream/player/:uuid", HTTPAPIServerStreamPlayer)
	}
	router.GET("/healthz", func(c *gin.Context) { c.Status(http.StatusOK) })
	router.POST("/stream/receiver/:uuid", HTTPAPIServerStreamWebRTC)
	router.GET("/stream/codec/:uuid", HTTPAPIServerStreamCodec)
	router.POST("/stream", HTTPAPIServerStreamWebRTC2)
	router.StaticFS("/static", http.Dir("web/static"))
	return router
}

func serveHTTP() {
	if err := newRouter().Run(Config.Server.HTTPPort); err != nil {
		log.Fatal("HTTP server: ", err)
	}
}

func HTTPAPIServerIndex(c *gin.Context) {
	first, all := Config.list()
	c.Header("Cache-Control", "no-store")
	if len(all) > 0 {
		c.Redirect(http.StatusTemporaryRedirect, "/stream/player/"+first)
		return
	}
	c.HTML(http.StatusOK, "index.tmpl", gin.H{})
}

func HTTPAPIServerStreamPlayer(c *gin.Context) {
	id := c.Param("uuid")
	if !Config.ext(id) {
		c.JSON(http.StatusNotFound, ResponseError{"stream not found"})
		return
	}
	_, all := Config.list()
	iceServers := make([]map[string]any, 0, 1)
	if len(Config.Server.ICEServers) > 0 {
		iceServers = append(iceServers, map[string]any{
			"urls": Config.Server.ICEServers, "username": Config.Server.ICEUsername,
			"credential": Config.Server.ICECredential,
		})
	}
	c.Header("Cache-Control", "no-store")
	c.HTML(http.StatusOK, "player.tmpl", gin.H{
		"port": Config.Server.HTTPPort, "suuid": id, "suuidMap": all,
		"version": time.Now().UnixNano(), "iceServers": iceServers,
	})
}

func codecTracks(codecs []av.CodecData) []JCodec {
	tracks := make([]JCodec, 0, len(codecs))
	for _, codec := range codecs {
		if !supportedCodec(codec.Type()) {
			continue
		}
		kind := "audio"
		if codec.Type().IsVideo() {
			kind = "video"
		}
		tracks = append(tracks, JCodec{Type: kind})
	}
	return tracks
}

func streamCodecs(c *gin.Context, id string) *streamSnapshot {
	if !Config.ext(id) {
		c.JSON(http.StatusNotFound, ResponseError{"stream not found"})
		return nil
	}
	Config.RunIFNotRun(id)
	codecs := Config.coGe(id)
	if codecs == nil {
		c.JSON(http.StatusServiceUnavailable, ResponseError{"stream is reconnecting or unavailable"})
		return nil
	}
	if len(codecTracks(codecs.codecs)) == 0 {
		c.JSON(http.StatusUnprocessableEntity, ResponseError{"no supported media tracks"})
		return nil
	}
	return codecs
}

func HTTPAPIServerStreamCodec(c *gin.Context) {
	if codecs := streamCodecs(c, c.Param("uuid")); codecs != nil {
		c.Header("Cache-Control", "no-store")
		c.JSON(http.StatusOK, codecTracks(codecs.codecs))
	}
}

func negotiate(c *gin.Context, id, offer string) (*WebRTCMuxer, *streamSnapshot, string) {
	if offer == "" {
		c.JSON(http.StatusBadRequest, ResponseError{"SDP offer required"})
		return nil, nil, ""
	}
	codecs := streamCodecs(c, id)
	if codecs == nil {
		return nil, nil, ""
	}
	server := Config.Server
	muxer := NewWebRTCMuxer(WebRTCOptions{
		ICEServers: server.ICEServers, ICEUsername: server.ICEUsername,
		ICECredential: server.ICECredential, PortMin: server.WebRTCPortMin, PortMax: server.WebRTCPortMax,
	})
	answer, err := muxer.WriteHeader(codecs.codecs, offer)
	if err != nil {
		muxer.Close()
		log.Printf("stream=%s negotiation failed: %v", id, err)
		c.JSON(http.StatusBadRequest, ResponseError{"WebRTC negotiation failed"})
		return nil, nil, ""
	}
	return muxer, codecs, answer
}

func HTTPAPIServerStreamWebRTC(c *gin.Context) {
	id := c.Param("uuid")
	if formID := c.PostForm("suuid"); formID != "" && formID != id {
		c.JSON(http.StatusBadRequest, ResponseError{"stream identifier mismatch"})
		return
	}
	muxer, codecs, answer := negotiate(c, id, c.PostForm("data"))
	if muxer == nil {
		return
	}
	c.Header("Content-Type", "text/plain; charset=utf-8")
	if _, err := c.Writer.WriteString(answer); err != nil {
		muxer.Close()
		return
	}
	// Copy all request values before starting a goroutine; Gin reuses contexts.
	go serveViewer(id, codecs, muxer)
}

func HTTPAPIServerStreamWebRTC2(c *gin.Context) {
	rawURL := c.PostForm("url")
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "rtsp" && parsed.Scheme != "rtsps") {
		c.JSON(http.StatusBadRequest, ResponseError{"valid RTSP URL required"})
		return
	}
	id := Config.addURL(rawURL)
	muxer, codecs, answer := negotiate(c, id, c.PostForm("sdp64"))
	if muxer == nil {
		return
	}
	response := Response{Sdp64: answer, Tracks: []string{}}
	for _, track := range codecTracks(codecs.codecs) {
		response.Tracks = append(response.Tracks, track.Type)
	}
	c.JSON(http.StatusOK, response)
	go serveViewer(id, codecs, muxer)
}

func serveViewer(id string, codecs *streamSnapshot, muxer *WebRTCMuxer) {
	defer muxer.Close()
	// Subscribe only when ICE and DTLS are ready, so the first IDR is delivered.
	connectionTimeout := time.NewTimer(30 * time.Second)
	defer connectionTimeout.Stop()
	select {
	case <-muxer.Done():
		return
	case <-connectionTimeout.C:
		log.Printf("stream=%s WebRTC connection timeout", id)
		return
	case <-muxer.Ready():
	}
	cid, packets := Config.clAd(id, codecs.generation)
	defer Config.clDe(id, cid)
	log.Printf("stream=%s viewer=%s connected", id, cid)
	err := forwardPackets(packets, muxer.Done(), muxer, codecs.codecs, mediaIdleTimeout)
	if err != nil && !errors.Is(err, io.EOF) {
		log.Printf("stream=%s viewer=%s ended: %v", id, cid, err)
	}
}

func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Headers", "Origin, Content-Type, Accept, Authorization")
		c.Header("Access-Control-Allow-Methods", "POST, OPTIONS, GET")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}
