help: ## Show help messages.
	@grep -E '^[0-9a-zA-Z_-]+:(.*?## .*)?$$' $(MAKEFILE_LIST) | sed 's/^Makefile://' | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

run="."
dir="./..."
flags=""
timeout=5m

.PHONY: unit_test
unit_test: ## Run unit tests. You can set: [run, timeout, dir, flags]. Example: make unit_test flags="-race".
	@go mod tidy; go test -trimpath -failfast --timeout=$(timeout) -short $(dir) -run $(run) $(flags)

.PHONY: unit_test_watch
unit_test_watch: ## Run unit tests in watch mode. You can set: [run, timeout, dir, flags]. Example: make unit_test flags="-race".
	@echo "running tests on $(run). waiting for changes..."
	@-zsh -c "go mod tidy; go test -trimpath -failfast --timeout=$(timeout) -short $(dir) -run $(run) $(flags); repeat 100 printf '#'; echo"
	@reflex -d none -r "(\.go$$)|(go.mod)|(\.sql$$)" -- zsh -c "go mod tidy; go test -trimpath -failfast --timeout=$(timeout) short $(dir) -run $(run) $(flags); repeat 100 printf '#'"

.PHONY: integration_test
integration_test: ## Run integration. You can set: [run, timeout, dir, flags]. Example: make integration_test flags="-race".
	@go mod tidy; go test -trimpath -failfast --timeout=$(timeout)  $(dir) -run $(run) $(flags)

.PHONY: integration_test_watch
integration_test_watch: ## Run integration in watch mode. You can set: [run, timeout, short, dir, flags]. Example: make integration_test flags="-race".
	@echo "running tests on $(run). waiting for changes..."
	@-zsh -c "go mod tidy; go test -trimpath -failfast --timeout=$(timeout) $(dir) -run $(run) $(flags); repeat 100 printf '#'; echo"
	@reflex -d none -r "(\.go$$)|(go.mod)|(\.sql$$)" -- zsh -c "go mod tidy; go test -trimpath -failfast --timeout=$(timeout) $(dir) -run $(run) $(flags); repeat 100 printf '#'"

.PHONY: lint
lint: ## Lint the code
	gofumpt -w .
	go vet ./...
	golangci-lint run --concurrency=4 ./...
	govulncheck -show traces,color,verbose ./...

.PHONY: ci_tests
ci_tests: ## Run tests for CI.
	go test -trimpath --timeout=10m -failfast -v -race -covermode=atomic -coverprofile=coverage.out ./...

.PHONY: dependencies
dependencies: ## Install dependencies requried for development operations.
	@go install github.com/vektra/mockery/v3@latest
	@go install github.com/cespare/reflex@latest
	@go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
	@go install github.com/psampaz/go-mod-outdated@latest
	@go install github.com/jondot/goweight@latest
	@go install golang.org/x/vuln/cmd/govulncheck@latest
	@go install golang.org/x/tools/cmd/stringer@latest
	@go install mvdan.cc/gofumpt@latest
	@go get -u -t ./...
	@go mod tidy

.PHONY: mocks
mocks: ## Generate mocks in all packages.
	@mockery
	@go generate ./...

.PHONY: clean
clean: ## Clean test caches and tidy up modules.
	@go clean -testcache
	@go mod tidy

.PHONY: coverage
coverage: ## Show the test coverage on browser.
	go test -covermode=count -coverprofile=coverage.out -tags=integration ./...
	go tool cover -func=coverage.out | tail -n 1
	go tool cover -html=coverage.out

.PHONY: audit
audit: ## Audit the code for updates, vulnerabilities and binary weight.
	govulncheck ./...
	go list -u -m -json all | go-mod-outdated -update -direct
	goweight | head -n 20
