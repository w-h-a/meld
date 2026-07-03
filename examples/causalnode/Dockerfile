FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/causalnode ./examples/causalnode
RUN mkdir /data

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/causalnode /causalnode
COPY --from=build --chown=65532:65532 /data /data
ENTRYPOINT ["/causalnode"]
