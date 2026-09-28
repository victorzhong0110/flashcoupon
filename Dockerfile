FROM golang:1.22-alpine AS build
WORKDIR /src
RUN apk add --no-cache ca-certificates git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/api ./cmd/api \
 && CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/consumer ./cmd/consumer \
 && CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/reconcile ./cmd/reconcile

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata wget
COPY --from=build /out/api /out/consumer /out/reconcile /usr/local/bin/
EXPOSE 8080 8081
USER nobody
ENTRYPOINT ["/usr/local/bin/api"]
