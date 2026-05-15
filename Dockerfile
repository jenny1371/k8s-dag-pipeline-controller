FROM golang:1.22 AS builder

WORKDIR /app
COPY . .

RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o controller main.go

FROM alpine:latest

WORKDIR /root/
COPY --from=builder /app/controller .

CMD ["./controller"]
