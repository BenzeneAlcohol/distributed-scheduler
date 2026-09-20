FROM golang:1.27.1-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/scheduler ./cmd/scheduler \
    && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

FROM scratch AS worker

COPY --from=build /out/worker /worker

USER 65532:65532

ENTRYPOINT ["/worker"]

FROM scratch AS scheduler

COPY --from=build /out/scheduler /scheduler

USER 65532:65532
EXPOSE 8080 9090

ENTRYPOINT ["/scheduler"]
