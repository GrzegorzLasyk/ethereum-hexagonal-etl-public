FROM golang:1.27.3-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY gen ./gen
COPY internal ./internal
COPY services ./services
COPY test/fixture ./test/fixture
COPY test/cmd/ethereum-fixture ./test/cmd/ethereum-fixture
COPY tools/probe ./tools/probe
RUN CGO_ENABLED=0 go build -trimpath -o /out/extractor ./services/extractor-service/cmd/extractor && \
    CGO_ENABLED=0 go build -trimpath -o /out/transformer ./services/transformer-service/cmd/transformer && \
    CGO_ENABLED=0 go build -trimpath -o /out/loader ./services/loader-service/cmd/loader && \
    CGO_ENABLED=0 go build -trimpath -o /out/ethereum-fixture ./test/cmd/ethereum-fixture && \
    CGO_ENABLED=0 go build -trimpath -o /out/probe ./tools/probe

FROM debian:bookworm-slim
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/ /app/
USER 65532:65532
ENTRYPOINT ["/app/extractor"]
