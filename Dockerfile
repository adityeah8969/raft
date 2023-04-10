# Build the application from source
FROM golang:1.19 AS build-stage

WORKDIR /app

COPY . ./

RUN go mod download

RUN go build -ldflags "-linkmode 'external' -extldflags '-static'" -o build_artifacts/raft-bin cmd/main.go

# Run the lean build
FROM gcr.io/distroless/base-debian11 AS build-run

WORKDIR /

COPY --from=build-stage /build_artifacts/raft-bin /raft-bin

ENTRYPOINT ["./raft-bin"]