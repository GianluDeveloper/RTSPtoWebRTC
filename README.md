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

### Development checks

With Go 1.27.1 and Node installed:

```bash
go test -race ./...
go vet ./...
go mod verify
node --test web/tests/player.test.cjs
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
      "url": "rtsp://170.93.143.139/rtplive/470011e600ef003a004ee33696235daa"
    },
    "demo2": {
      "on_demand" : true,
      "url": "rtsp://admin:admin123@10.128.18.224/mpeg4"
    },
    "demo3": {
      "on_demand" : false,
      "url": "rtsp://170.93.143.139/rtplive/470011e600ef003a004ee33696235daa"
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
