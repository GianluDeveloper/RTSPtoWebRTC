# RTSPtoWebRTC

RTSP Stream to WebBrowser over WebRTC based on Pion (full native! not using ffmpeg or gstreamer).

**Note:** [RTSPtoWeb](https://github.com/deepch/RTSPtoWeb) is an improved service that provides the same functionality, an improved API, and supports even more protocols. *RTSPtoWeb is recommended over using this service.*


if you need RTSPtoWSMP4f use https://github.com/deepch/RTSPtoWSMP4f


![RTSPtoWebRTC image](doc/demo4.png)

### Download Source

1. Download source
   ```bash 
   $ git clone https://github.com/deepch/RTSPtoWebRTC  
   ```
3. CD to Directory
   ```bash
    $ cd RTSPtoWebRTC/
   ```
4. Test Run
   ```bash
    $ GO111MODULE=on go run *.go
   ```
5. Open Browser
    ```bash
    open web browser http://127.0.0.1:8083 work chrome, safari, firefox
    ```

## Docker Compose (Linux)

Create `config.local.json` using the configuration format below and set the HTTP
port to `:8083`. Add the camera URLs under `streams`, with `on_demand: false`.
This local file is ignored by Git and mounted read-only into the container.

```bash
mkdir -p recordings recording-hooks
docker compose up -d --build
docker compose ps
docker compose logs --tail=50
```

Open `http://localhost:8083/` or
`http://<server-lan-ip>:8083/stream/player/<stream-id>` from the same LAN.
Compose uses host networking so WebRTC can advertise the server's LAN address.
The container restarts automatically unless explicitly stopped with
`docker compose down`.

### Streaming recovery

RTSP ingestion and WebRTC forwarding use packet inactivity to detect a stalled
source. The camera's keyframe interval does not terminate an active stream.
Viewers wait for an initial H264 keyframe, and reconnect when the source session
or codec changes. Slow viewers are released independently instead of silently
dropping H264 frames into an otherwise connected session.

The browser completes ICE gathering before signaling, monitors decoded frames,
and reconnects automatically after a transport failure or frozen picture. The
player receives its ICE server configuration from `server.ice_servers`.

The current build uses Go 1.27.1, Gin 1.12.0, VDK 0.0.27, Pion WebRTC 4.2.23,
and Bootstrap 5.3.8. Runtime versions are pinned in `Dockerfile` and `go.mod`.
The frontend uses native browser APIs without jQuery or adapter.js.

### MP4 recording

Each player has **Start recording** and **Stop recording** controls.
Recording runs on the server and continues after closing the browser. All viewers
share the same recording for a stream, and recording reuses its existing RTSP
connection.

Configure the shared output directory and each stream's chunk duration in
`config.local.json`:

```json
{
  "server": {
    "http_port": ":8083",
    "recording_dir": "/recordings",
    "recording_callback_script": "/hooks/on-chunk.sh",
    "recording_callback_timeout_seconds": 60
  },
  "streams": {
    "camera": {
      "url": "rtsp://camera.example/stream",
      "on_demand": false,
      "recording": {
        "chunk_seconds": 60,
        "callback_enabled": false
      }
    }
  }
}
```

- `chunk_seconds: 0` saves one MP4 until you stop recording (default).
- A positive value creates successive MP4 segments. Each cut waits for the next
  H264 keyframe, so durations can exceed the configured value by a keyframe interval.
- Apply configuration changes with `docker compose restart`. Active recordings
  are finalized during graceful shutdown and are not restarted automatically.

The recorder normally preserves source timestamps. If a camera sends incorrect
RTP timestamps, an optional `recording.frame_rate` override can set the file's
timing to its known, constant capture rate (for example, `15`). The default `0`
keeps source timing; overrides must be between 1 and 240 fps. Use this only after
checking the actual frame rate: an incorrect override changes playback speed.
The player reports when the override is active. Live WebRTC timing is unchanged.

Compose mounts `./recordings` on the host at `/recordings` inside the container.
Files are stored in a separate subdirectory for each stream. Completed files have
an `.mp4` extension; the active file has a temporary `.part` extension. The folder
is excluded from Git and Docker build contexts. Compose runs as UID/GID 1000 by
default; set `RTSP_UID` and `RTSP_GID` if your host user has different IDs.

Recordings contain the original H264 video without re-encoding. Other tracks are
omitted, with an explicit note in the player if the source also has audio. If a
source disconnects, changes codecs, or storage fails, recording stops and reports
an error rather than silently producing a file with gaps. Playback remains
independent. Files are retained until you remove them from the host folder.

Recording API (all responses are JSON):

```text
GET  /stream/recording/<stream-id>
POST /stream/recording/<stream-id>/start
POST /stream/recording/<stream-id>/stop
```

### Callback after each completed chunk

The callback is optional and disabled by default. Enable it for a stream with
`recording.callback_enabled: true` and `recording.chunk_seconds` greater than
zero. It never runs for single-file recordings, even if the callback flag is true.
It runs for each finalized chunk, including the shorter last chunk when recording
stops, and never receives a partially written file.

The fixed script path is `server.recording_callback_script`; Compose mounts the
host directory `./recording-hooks` read-only at `/hooks`. To install the supplied
example:

```bash
mkdir -p recording-hooks
cp examples/on-chunk.sh recording-hooks/on-chunk.sh
chmod +x recording-hooks/on-chunk.sh
```

The host script runs **inside the container**, as the service user. Its first
argument is the absolute MP4 path inside the container. Standard input contains
one JSON object with these fields:

```text
event, stream_id, source.{scheme,host,port}, path, relative_path,
started_at, finalized_at, duration_seconds, sequence, chunk_seconds,
final, codec
```

`final` identifies the last chunk of a recording session. Source credentials,
URL paths and query strings are deliberately excluded from this payload. The
example writes the JSON to `<chunk>.metadata.json` next to the MP4 on the host;
replace its operation with your own processing. Scripts can use tools installed
in the container image. Changes to the host script take effect on the next run
without rebuilding the image.

Callbacks run asynchronously with two workers and a bounded queue of 64 jobs.
The timeout is `server.recording_callback_timeout_seconds` (default 60 seconds,
maximum 3600). Exit failures, timeouts and a full queue are reported in the player
and recording status API and do not interrupt playback or recording. Jobs can
finish out of order; make processing independent for each file. The queue is in
memory and jobs are not retried automatically. Shutdown briefly drains the queue,
then cancels remaining scripts to respect the container's stop deadline.

### Development checks

With Go 1.27.1 and Node installed:

```bash
go test -race ./...
go vet ./...
go mod verify
node --test web/tests/*.test.cjs
```

Regression tests cover long keyframe intervals, genuine inactivity, source
restarts during negotiation, slow viewers, H264 packetization, a real local
WebRTC connection, and browser retry/cleanup behavior.

## Configuration

### Edit file config.json

format:

```bash
{
  "server": {
    "http_port": ":8083"
  },
  "streams": {
    "demo1": {
      "on_demand" : false,
      "url": "rtsp://camera-one.example/main"
    },
    "demo2": {
      "on_demand" : true,
      "url": "rtsp://camera-two.example/main"
    },
    "demo3": {
      "on_demand" : false,
      "url": "rtsp://camera-three.example/main"
    }
  }
}
```

## Livestreams

Use option ``` "on_demand": false ``` otherwise you will get choppy jerky streams and performance issues when multiple clients connect. 

## Limitations

Video Codecs Supported: H264

Audio Codecs Supported: pcm alaw and pcm mulaw 

## Team

Deepch - https://github.com/deepch streaming developer

Dmitry - https://github.com/vdalex25 web developer

Now test work on (chrome, safari, firefox) no MAC OS

## Other Example

Examples of working with video on golang

- [RTSPtoWeb](https://github.com/deepch/RTSPtoWeb)
- [RTSPtoWebRTC](https://github.com/deepch/RTSPtoWebRTC)
- [RTSPtoWSMP4f](https://github.com/deepch/RTSPtoWSMP4f)
- [RTSPtoImage](https://github.com/deepch/RTSPtoImage)
- [RTSPtoHLS](https://github.com/deepch/RTSPtoHLS)
- [RTSPtoHLSLL](https://github.com/deepch/RTSPtoHLSLL)

[![paypal.me/AndreySemochkin](https://ionicabizau.github.io/badges/paypal.svg)](https://www.paypal.me/AndreySemochkin) - You can make one-time donations via PayPal. I'll probably buy a ~~coffee~~ tea. :tea:
