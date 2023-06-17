.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: test-kafka
test-kafka:
	docker-compose -f ./kafka/docker-compose.yaml up --remove-orphans -d
	go test -v -tags=integration -count=1 ./kafka/...
	docker-compose -f ./kafka/docker-compose.yaml down
	
.PHONY: mod
mod:
	go mod tidy

.PHONY: linter-install
linter-install:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

.PHONY: linter
linter: 
	golangci-lint run ./...