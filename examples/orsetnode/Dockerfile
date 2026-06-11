FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/orsetnode ./examples/orsetnode

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/orsetnode /orsetnode
ENTRYPOINT ["/orsetnode"]
