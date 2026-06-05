FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /raft-node ./cmd/node

FROM alpine:3.19

RUN apk add --no-cache ca-certificates

COPY --from=builder /raft-node /usr/local/bin/raft-node

ENTRYPOINT ["raft-node"]
