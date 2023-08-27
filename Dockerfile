# Build the application from source
FROM golang:latest AS build-stage
WORKDIR /app
COPY . ./
RUN go mod download
# RUN go build -ldflags "-linkmode 'external' -extldflags '-static'" -o build_artifacts/raft-bin cmd/main.go

# Use the following, reason being rpc.Dial is not able to lookup docker services with CGO enabled. 
RUN go build -tags netgo,osusergo -o build_artifacts/raft-bin cmd/main.go

FROM ubuntu AS build-run
WORKDIR /
COPY --from=build-stage /app/build_artifacts/raft-bin /raft-bin
CMD ["./raft-bin"]