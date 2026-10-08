FROM golang:1.27.1 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /rtsp-to-webrtc .

FROM alpine:3.24.2
WORKDIR /app
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /rtsp-to-webrtc /usr/local/bin/rtsp-to-webrtc
COPY web ./web

EXPOSE 8083
ENV GIN_MODE=release

CMD ["rtsp-to-webrtc"]
