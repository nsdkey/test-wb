FROM golang:1.22-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /trending ./cmd/trending

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /trending /usr/local/bin/trending
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/trending"]
