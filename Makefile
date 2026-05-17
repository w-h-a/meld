.PHONY: tidy
tidy:
	go mod tidy

.PHONY: style
style:
	goimports -l -w .

.PHONY: style-check
style-check:
	@output=$$(goimports -l .); \
	if [ -n "$$output" ]; then \
		echo "Files need formatting:"; \
		echo "$$output"; \
		exit 1; \
	fi

.PHONY: unit-test
unit-test:
	go clean -testcache && go test -v ./...

.PHONY: test
test:
	go clean -testcache && INTEGRATION=1 go test -v ./...
