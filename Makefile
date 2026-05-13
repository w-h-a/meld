PHONY: tidy style test

tidy:
	go mod tidy

style:
	goimports -l -w $(shell find . -name '*.go' -not -path './proto/*')

test:
	go test ./...
