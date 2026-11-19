# 多架构构建：docker buildx build --platform linux/amd64,linux/arm64 ...
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder

ARG TARGETOS=linux
ARG TARGETARCH=amd64
WORKDIR /src
COPY daemon/ .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/bili-dl-daemon .

FROM alpine:3.20
RUN apk add --no-cache ffmpeg ca-certificates
COPY --from=builder /out/bili-dl-daemon /usr/local/bin/bili-dl-daemon
ENV BILIDL_ADDR=:18080 \
    BILIDL_OUTPUT_ROOT=/data \
    BILIDL_STATE_FILE=/data/tasks.json \
    BILIDL_FFMPEG=/usr/bin/ffmpeg \
    BILIDL_MERGE_WORKERS=2 \
    BILIDL_DOWNLOAD_TRIES=3
VOLUME ["/data"]
EXPOSE 18080
ENTRYPOINT ["bili-dl-daemon"]
