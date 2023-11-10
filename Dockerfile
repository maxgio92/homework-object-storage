FROM golang:1.21 as builder

ARG TARGETARCH
ARG GIT_HEAD_COMMIT
ARG GIT_TAG_COMMIT
ARG GIT_LAST_TAG
ARG GIT_MODIFIED
ARG GIT_REPO
ARG BUILD_DATE

WORKDIR /workspace

COPY go.sum go.sum
COPY go.mod go.mod

RUN go mod download

COPY main.go main.go
COPY internal/ internal/
COPY pkg/ pkg/
COPY cmd/ cmd/

RUN CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH GO111MODULE=on GOEXPERIMENT=loopvar go build \
        -gcflags "-N -l" \
        -ldflags "-X main.GitRepo=$GIT_REPO -X main.GitTag=$GIT_LAST_TAG -X main.GitCommit=$GIT_HEAD_COMMIT -X main.GitDirty=$GIT_MODIFIED -X main.BuildTime=$BUILD_DATE" \
        -o object-storage-gateway

# Docker is used as a base image so you can easily start playing around in the container using the Docker command line client.
FROM alpine
COPY --from=builder /workspace/object-storage-gateway /usr/local/bin/object-storage-gateway
RUN ls -l /usr/local/bin
RUN apk add bash curl
CMD object-storage-gateway