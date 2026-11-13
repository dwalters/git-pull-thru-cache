FROM --platform=$BUILDPLATFORM golang:1.27 AS build
ARG TARGETOS TARGETARCH
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/git-pull-thru-cache ./cmd/git-pull-thru-cache

FROM alpine:3.20
RUN apk add --no-cache git ca-certificates
COPY --from=build /out/git-pull-thru-cache /usr/local/bin/
RUN adduser -D -u 10001 gitcache && mkdir /data && chown gitcache /data
ENV HOME=/tmp
USER 10001
VOLUME /data
ENTRYPOINT ["git-pull-thru-cache", "-data-dir=/data"]
