FROM golang:1.22 AS builder

WORKDIR /app

# Download dependencies first so this layer is cached; go.mod/go.sum are the
# source of truth (CI checks they are tidy), so the build never rewrites them.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /controller main.go

FROM alpine:3.20

# Run as an unprivileged user.
RUN adduser -D -u 10001 controller
USER 10001

COPY --from=builder /controller /controller

ENTRYPOINT ["/controller"]
