ARG APP_IMAGE=ubuntu:latest

# Build
FROM golang:1.25 AS build

WORKDIR /seq-db

COPY go.mod go.sum ./

RUN go mod download

COPY . .

ARG VERSION
ARG BUILD_TIME
ARG TARGETARCH

ENV CGO_ENABLED=1
ENV GOOS=linux
ENV GOARCH=${TARGETARCH:-amd64}

RUN go build -trimpath \
    -tags 'netgo osusergo' \
    -ldflags "-w -s  \
    -X github.com/ozontech/seq-db/buildinfo.Version=${VERSION}  \
    -X github.com/ozontech/seq-db/buildinfo.BuildTime=${BUILD_TIME}" \
    -o seq-db ./cmd/seq-db

# Deploy
FROM $APP_IMAGE

WORKDIR /seq-db

COPY --from=build /seq-db/seq-db /seq-db/seq-db

ENTRYPOINT [ "./seq-db" ]
