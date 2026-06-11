FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/pncounternode ./examples/pncounternode

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/pncounternode /pncounternode
ENTRYPOINT ["/pncounternode"]