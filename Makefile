help: ## Show help messages.
	@grep -E '^[0-9a-zA-Z_-]+:(.*?## .*)?$$' $(MAKEFILE_LIST) | sed 's/^Makefile://' | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

rabbitmq_image=rabbitmq:3.8-management-alpine
rabbitmq_container=harego_rabbit_1

run="."
dir="./..."
short="-short"
flags=""
timeout=5m

include ./config/dev.env
export $(shell sed 's/=.*//' ./config/dev.env)

.PHONY: unit_test
unit_test: ## Run unit tests. You can set: [run, timeout, short, dir, flags]. Example: make unit_test flags="-race".
	@go mod tidy; go test -trimpath -failfast --timeout=$(timeout) $(short) $(dir) -run $(run) $(flags)

.PHONY: unit_test_watch
unit_test_watch: ## Run unit tests in watch mode. You can set: [run, timeout, short, dir, flags]. Example: make unit_test flags="-race".
	@echo "running tests on $(run). waiting for changes..."
	@-zsh -c "go mod tidy; go test -trimpath -failfast --timeout=$(timeout) $(short) $(dir) -run $(run) $(flags); repeat 100 printf '#'; echo"
	@reflex -d none -r "(\.go$$)|(go.mod)|(\.sql$$)" -- zsh -c "go mod tidy; go test -trimpath -failfast --timeout=$(timeout) $(short) $(dir) -run $(run) $(flags); repeat 100 printf '#'"

.PHONY: integration_test
integration_test: start_test_container
integration_test: ## Run integration. You can set: [run, timeout, short, dir, flags]. Example: make integration_test flags="-race".
	@go mod tidy; go test -trimpath -failfast --timeout=$(timeout) -tags=integration $(short) $(dir) -run $(run) $(flags)

.PHONY: integration_test_watch
integration_test_watch: start_test_container
integration_test_watch: ## Run integration in watch mode. You can set: [run, timeout, short, dir, flags]. Example: make integration_test flags="-race".
	@echo "running tests on $(run). waiting for changes..."
	@-zsh -c "go mod tidy; go test -trimpath -failfast --timeout=$(timeout) -tags=integration $(short) $(dir) -run $(run) $(flags); repeat 100 printf '#'; echo"
	@reflex -d none -r "(\.go$$)|(go.mod)|(\.sql$$)" -- zsh -c "go mod tidy; go test -trimpath -failfast --timeout=$(timeout) -tags=integration $(short) $(dir) -run $(run) $(flags); repeat 100 printf '#'"

.PHONY: lint
lint: ## Lint the code
	go fmt ./...
	go vet ./...
	golangci-lint run ./...

.PHONY: ci_tests
ci_tests: start_test_container
ci_tests: ## Run tests for CI.
	go test -trimpath --timeout=10m -failfast -v -tags=integration -race -covermode=atomic -coverprofile=coverage.out ./...

.PHONY: integration_deps
integration_deps: ## Install integration test databases. It removes every existing setup.
	@-docker pull $(rabbitmq_image)
	@-docker network create harego
	@docker run -d --net harego -p $(RABBITMQ_PORT):5672 -p $(RABBITMQ_ADMIN_PORT):15672 --user rabbitmq --name $(rabbitmq_container) --hostname $(rabbitmq_container) -e RABBITMQ_DEFAULT_USER=$(RABBITMQ_USER) -e RABBITMQ_DEFAULT_PASS=$(RABBITMQ_PASSWORD) $(rabbitmq_image) rabbitmq-server --erlang-cookie=harego
	@sleep 2
	@docker exec $(rabbitmq_container) rabbitmqctl wait /var/lib/rabbitmq/mnesia/rabbit@$(rabbitmq_container).pid
	@docker exec $(rabbitmq_container) rabbitmqctl set_permissions "$(RABBITMQ_USER)" ".*" ".*" ".*"
	@echo "RabbitMQ has been setup"


.PHONY: start_test_container
start_test_container: ## Start test containers.
	@echo "Starting test containers"
	@-docker start $(rabbitmq_container)
	@docker exec $(rabbitmq_container) rabbitmqctl wait /var/lib/rabbitmq/mnesia/rabbit@$(rabbitmq_container).pid > /dev/null
	@echo "All test containers are ready"

.PHONY: stop_test_container
stop_test_container: ## Stop test containers.
	@-docker stop $(rabbitmq_container)

.PHONY: reset_docker
reset_docker: ## Reset containers and delete their data.
	@-docker rm -f $(rabbitmq_container)
	@-docker network rm harego

.PHONY: dependencies
dependencies: ## Install dependencies requried for development operations.
	@go get -u -d github.com/rubenv/sql-migrate/...
	@go get -u -d github.com/stretchr/testify/mock
	@go install github.com/vektra/mockery/v2@latest
	@go install github.com/cespare/reflex@latest
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@go install github.com/psampaz/go-mod-outdated@latest
	@go install github.com/jondot/goweight@latest
	@go install golang.org/x/vuln/cmd/govulncheck@latest
	@go get -t -u golang.org/x/tools/cmd/cover
	@go get -t -u github.com/sonatype-nexus-community/nancy@latest
	@go get -u golang.org/x/tools/cmd/stringer
	@go get -u -d ./...
	@go mod tidy

.PHONY: mocks
mocks: ## Generate mocks in all packages.
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
	go list -json -deps | nancy sleuth
	goweight | head -n 20
