package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
)

func main() {
	Config = loadConfig()
	Recordings = NewRecordingManager(Config, Config.Server.RecordingDir)
	RecordingCallbacks = NewRecordingCallbackRunner(Config)
	Recordings.OnChunk = RecordingCallbacks.Submit
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go serveHTTP()
	go serveStreams()
	log.Println("Server Start Awaiting Signal")
	<-ctx.Done()
	log.Println("Finalizing active recordings")
	Recordings.StopAll()
	RecordingCallbacks.Close()
	log.Println("Exiting")
}
