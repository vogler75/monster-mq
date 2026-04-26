# syntax=docker/dockerfile:1.7
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
ARG TARGETOS
ARG TARGETARCH

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

ENV CGO_ENABLED=0
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/monstermq-edge ./cmd/monstermq-edge

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/monstermq-edge /monstermq-edge
COPY --from=build /src/config.yaml.example /etc/monstermq/config.yaml
EXPOSE 1883 1884 8080 8883 8884
USER nonroot:nonroot
ENTRYPOINT ["/monstermq-edge"]
CMD ["-config", "/etc/monstermq/config.yaml"]
