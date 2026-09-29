FROM golang:1.25-alpine AS build
WORKDIR /src
COPY . .
ARG APP=consumer
RUN go mod tidy && CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /app ./cmd/${APP}

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /app /app
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/app"]
