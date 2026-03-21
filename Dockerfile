FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o rpc-aggregator ./cmd/aggregator

FROM alpine:3.21
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/rpc-aggregator .
COPY config.example.yaml ./config.yaml
EXPOSE 8080 8081
ENTRYPOINT ["./rpc-aggregator"]
CMD ["config.yaml"]
