FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/lwwnode ./examples/lwwnode

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/lwwnode /lwwnode
ENTRYPOINT ["/lwwnode"]
